"""Base Publisher"""
from __future__ import annotations
import abc
import functools
import json
import threading
from collections.abc import Callable
from queue import (
    SimpleQueue,
    Empty,
)
from typing import TYPE_CHECKING, ParamSpec, TypeVar, cast

if TYPE_CHECKING:
    from psycopg2.extras import ReplicationMessage

PublisherMessage = str | bytes | bytearray | memoryview | object
QueueMessage = str | bytes
P = ParamSpec('P')
R = TypeVar('R')


def run_publisher_daemon(publisher: 'BasePublisher') -> None:
    """Run publisher in a tracked daemon thread if one is not active."""
    publisher.start_worker()


def ensure_running(
    func: Callable[P, R],
) -> Callable[P, R]:
    """Ensure that a publisher is running"""

    @functools.wraps(func)
    def inner(*args: P.args, **kwargs: P.kwargs) -> R:
        publisher = cast(BasePublisher, args[0])
        if publisher.is_running():
            return func(*args, **kwargs)
        run_publisher_daemon(publisher)
        return func(*args, **kwargs)

    return inner


class MsgQueueMixin(metaclass=abc.ABCMeta):
    """Abstract interface providing msg_queue property"""

    _MSG_QUEUE: SimpleQueue[PublisherMessage]

    @property
    @abc.abstractmethod
    def msg_queue(self) -> SimpleQueue[PublisherMessage]:
        """return internal msg queue to use"""
        raise NotImplementedError

    def _get_message(self) -> QueueMessage | None:
        """Get message if any from internal msg queue"""
        try:
            message = self.msg_queue.get_nowait()
        except Empty:
            return None
        if not isinstance(message, (bytes, str)):
            message = json.dumps(message, ensure_ascii=False)
        return message


class BasePublisher(metaclass=abc.ABCMeta):
    """Base Publisher"""

    _lock: threading.Lock | None = None
    _running = False
    _thread: threading.Thread | None = None
    _thread_lock: threading.Lock | None = None

    @abc.abstractmethod
    def publish(self, msg: 'ReplicationMessage') -> None:
        """publish replication message to required destination"""
        raise NotImplementedError

    @abc.abstractmethod
    def run(self) -> None:
        """run the publisher"""

    @abc.abstractmethod
    def stop(self) -> None:
        """stop the publisher"""

    def is_running(self) -> bool:
        """check if the publisher is running"""
        return self._running

    def set_running(self, value: bool) -> None:
        """Set the publisher to running status"""
        if self._lock is not None:
            with self._lock:
                self._running = value
        else:
            self._running = value

    def _get_thread_lock(self) -> threading.Lock:
        """Get or create the per-instance worker-thread lock."""
        thread_lock = self._thread_lock
        if thread_lock is None:
            thread_lock = threading.Lock()
            self._thread_lock = thread_lock
        return thread_lock

    def start_worker(self) -> threading.Thread:
        """Start the publisher worker thread if it is not already running."""
        with self._get_thread_lock():
            task = self._thread
            if task is not None and task.is_alive():
                return task
            task = threading.Thread(
                target=self.run,
                name=f'{self.__class__.__name__}-worker',
                daemon=True,
            )
            self._thread = task
            task.start()
            return task

    def wait_stopped(self, timeout: float | None = None) -> bool:
        """Wait for the tracked worker thread to stop."""
        task = self._thread
        if task is None:
            return True
        if task is threading.current_thread():
            return False
        task.join(timeout)
        stopped = not task.is_alive()
        if stopped:
            with self._get_thread_lock():
                if self._thread is task:
                    self._thread = None
        return stopped
