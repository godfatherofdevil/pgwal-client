"""Base Publisher"""
from __future__ import annotations
import abc
import functools
import json
import threading
from queue import (
    SimpleQueue,
    Empty,
)
from typing import Optional

from psycopg2._psycopg import ReplicationMessage


def run_publisher_daemon(publisher: 'BasePublisher'):
    """Run publisher in a demon thread."""
    task = threading.Thread(target=publisher.run)
    task.daemon = True
    task.start()


def ensure_running(func):
    """Ensure that a publisher is running"""

    @functools.wraps(func)
    def inner(publisher: 'BasePublisher', *args, **kwargs):
        if publisher.is_running():
            return func(publisher, *args, **kwargs)
        run_publisher_daemon(publisher)
        return func(publisher, *args, **kwargs)

    return inner


class MsgQueueMixin(metaclass=abc.ABCMeta):
    """Abstract interface providing msg_queue property"""

    _MSG_QUEUE = None

    @property
    @abc.abstractmethod
    def msg_queue(self) -> SimpleQueue:
        """return internal msg queue to use"""
        raise NotImplementedError

    def _get_message(self) -> Optional[str | bytes]:
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

    _lock = None
    _running = False

    @abc.abstractmethod
    def publish(self, msg: 'ReplicationMessage'):
        """publish replication message to required destination"""
        raise NotImplementedError

    @abc.abstractmethod
    def run(self):
        """run the publisher"""

    @abc.abstractmethod
    def stop(self):
        """stop the publisher"""

    def is_running(self) -> bool:
        """check if the publisher is running"""
        return self._running

    def set_running(self, value: bool):
        """Set the publisher to running status"""
        if self._lock is not None:
            with self._lock:
                self._running = value
