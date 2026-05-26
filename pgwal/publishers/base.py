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
    """Run publisher in a demon thread."""
    task = threading.Thread(target=publisher.run)
    task.daemon = True
    task.start()


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
