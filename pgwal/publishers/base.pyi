import abc
from collections.abc import Callable as Callable
from psycopg2.extras import ReplicationMessage
from queue import SimpleQueue
from typing import ParamSpec, TypeVar

PublisherMessage = str | bytes | bytearray | memoryview | object
QueueMessage = str | bytes
P = ParamSpec('P')
R = TypeVar('R')

def run_publisher_daemon(publisher: BasePublisher) -> None: ...
def ensure_running(func: Callable[P, R]) -> Callable[P, R]: ...

class MsgQueueMixin(metaclass=abc.ABCMeta):
    @property
    @abc.abstractmethod
    def msg_queue(self) -> SimpleQueue[PublisherMessage]: ...

class BasePublisher(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def publish(self, msg: ReplicationMessage) -> None: ...
    @abc.abstractmethod
    def run(self) -> None: ...
    @abc.abstractmethod
    def stop(self) -> None: ...
    def is_running(self) -> bool: ...
    def set_running(self, value: bool) -> None: ...
