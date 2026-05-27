"""Base publisher helpers and lifecycle primitives."""
from __future__ import annotations

import abc
import functools
import json
import logging
import threading
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from queue import Empty, Full, Queue
from typing import TYPE_CHECKING, ParamSpec, TypeVar, cast

if TYPE_CHECKING:
    from psycopg2.extras import ReplicationMessage

logger = logging.getLogger(__name__)

PublisherMessage = str | bytes | bytearray | memoryview | object
QueueMessage = str | bytes
P = ParamSpec('P')
R = TypeVar('R')


@dataclass
class PublishResult:
    """Result of handing a message to a publisher."""

    accepted: bool
    dropped: bool = False
    reason: str | None = None


@dataclass
class PublisherMetrics:
    """Publisher metrics exposed to callers."""

    queue_capacity: int = 0
    dropped_count: int = 0
    publish_error_count: int = 0
    reconnect_count: int = 0
    last_error: str | None = None
    last_success_at: datetime | None = None


class PublisherState(str, Enum):
    """Publisher lifecycle state."""

    IDLE = 'idle'
    STARTING = 'starting'
    RUNNING = 'running'
    DEGRADED = 'degraded'
    STOPPING = 'stopping'
    STOPPED = 'stopped'
    FAILED = 'failed'


def ensure_running(
    func: Callable[P, R],
) -> Callable[P, R]:
    """Ensure a publisher worker is running before publish is attempted."""

    @functools.wraps(func)
    def inner(*args: P.args, **kwargs: P.kwargs) -> R:
        publisher = cast(BasePublisher, args[0])
        publisher.start()
        return func(*args, **kwargs)

    return inner


class MsgQueueMixin(metaclass=abc.ABCMeta):
    """Interface for publishers backed by an internal queue."""

    _msg_queue: Queue[PublisherMessage]

    @property
    @abc.abstractmethod
    def msg_queue(self) -> Queue[PublisherMessage]:
        """Return the internal queue."""
        raise NotImplementedError

    def _get_message(self, timeout: float | None = None) -> QueueMessage | None:
        """Get one queued message, serializing structured payloads to JSON."""
        try:
            message = self.msg_queue.get(timeout=timeout)
        except Empty:
            return None
        if not isinstance(message, (bytes, str)):
            message = json.dumps(message, ensure_ascii=False)
        return message


class BasePublisher(metaclass=abc.ABCMeta):
    """Base publisher lifecycle."""

    def __init__(self) -> None:
        self._state = PublisherState.IDLE
        self._state_lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._thread_lock = threading.Lock()
        self._stop_event = threading.Event()
        self._stopped = threading.Event()
        self._stopped.set()
        self._metrics = PublisherMetrics()

    @property
    def state(self) -> PublisherState:
        """Publisher lifecycle state."""
        return self._state

    def set_state(self, value: PublisherState) -> None:
        """Set the publisher lifecycle state."""
        with self._state_lock:
            self._state = value

    @property
    def metrics(self) -> PublisherMetrics:
        """Publisher metrics."""
        if isinstance(self, MsgQueueMixin):
            self._metrics.queue_capacity = self.msg_queue.maxsize
        return self._metrics

    def mark_success(self) -> None:
        """Update success metrics."""
        self._metrics.last_success_at = datetime.now()
        if self.state is PublisherState.DEGRADED:
            self.set_state(PublisherState.RUNNING)

    def mark_error(
        self,
        exc: Exception | str,
        state: PublisherState = PublisherState.DEGRADED,
    ) -> None:
        """Update error metrics and publisher state."""
        self._metrics.publish_error_count += 1
        self._metrics.last_error = str(exc)
        self.set_state(state)

    def is_running(self) -> bool:
        """Whether this publisher is actively running."""
        return self.state in {
            PublisherState.STARTING,
            PublisherState.RUNNING,
            PublisherState.DEGRADED,
        }

    def start(self) -> None:
        """Start the publisher if it is worker-backed."""
        if self.requires_worker:
            self.start_worker()

    @property
    def requires_worker(self) -> bool:
        """Whether the publisher needs a worker thread."""
        return True

    @abc.abstractmethod
    def publish(self, msg: 'ReplicationMessage') -> PublishResult:
        """Publish a replication message."""
        raise NotImplementedError

    @abc.abstractmethod
    def run(self) -> None:
        """Run the publisher worker if present."""

    def stop(self, drain: bool = False) -> None:
        """Stop this publisher."""
        self.set_state(PublisherState.STOPPING)
        self._stop_event.set()
        if not drain and isinstance(self, MsgQueueMixin):
            self._drain_queue()

    def _drain_queue(self) -> None:
        """Drop any buffered queue items."""
        if not isinstance(self, MsgQueueMixin):
            return
        while True:
            try:
                self.msg_queue.get_nowait()
            except Empty:
                return

    def start_worker(self) -> threading.Thread:
        """Start the worker if it is not already active."""
        with self._thread_lock:
            task = self._thread
            if task is not None and task.is_alive():
                return task
            self._stop_event.clear()
            self._stopped.clear()
            self.set_state(PublisherState.STARTING)
            task = threading.Thread(
                target=self.run,
                name=f'{self.__class__.__name__}-worker',
                daemon=True,
            )
            self._thread = task
            task.start()
            return task

    def wait_stopped(self, timeout: float | None = None) -> bool:
        """Wait for the worker to stop."""
        task = self._thread
        if task is None:
            return True
        if task is threading.current_thread():
            return False
        task.join(timeout)
        if task.is_alive():
            return False
        self._stopped.wait(timeout)
        with self._thread_lock:
            if self._thread is task:
                self._thread = None
        return True

    def _queue_publish(self, payload: PublisherMessage) -> PublishResult:
        """Queue one message using the default drop-oldest policy."""
        if not isinstance(self, MsgQueueMixin):
            raise TypeError('queue-backed publish requires MsgQueueMixin')
        if self.state in {
            PublisherState.STOPPING,
            PublisherState.STOPPED,
            PublisherState.FAILED,
        }:
            return PublishResult(accepted=False, reason='publisher_unavailable')
        try:
            self.msg_queue.put_nowait(payload)
            return PublishResult(accepted=True)
        except Full:
            try:
                self.msg_queue.get_nowait()
            except Empty:
                return PublishResult(accepted=False, reason='queue_unavailable')
            self._metrics.dropped_count += 1
            self.set_state(PublisherState.DEGRADED)
            self.msg_queue.put_nowait(payload)
            return PublishResult(
                accepted=True,
                dropped=True,
                reason='queue_full_drop_oldest',
            )
