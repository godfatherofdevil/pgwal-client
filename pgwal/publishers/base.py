"""Base Publisher"""
import abc
import functools
import threading

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
