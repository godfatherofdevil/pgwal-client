import threading
from .consumers import WALConsumer as WALConsumer
from .publishers.base import BasePublisher as BasePublisher
from _typeshed import Incomplete as Incomplete
from functools import cached_property as cached_property
from psycopg2.extras import LogicalReplicationConnection
from psycopg2.pool import ThreadedConnectionPool

POOL_MIN: int
POOL_MAX: int
logger: Incomplete

class PGWAL:
    dsn: Incomplete
    tasks: list[threading.Thread]
    publishers: list['BasePublisher']
    def __init__(self, dsn: dict[str, object]) -> None: ...
    @cached_property
    def pool(self) -> ThreadedConnectionPool: ...
    def close_pool(self) -> None: ...
    def stop_publishers(self) -> None: ...
    def get_conn(self) -> LogicalReplicationConnection: ...
    def consume(self, consumer: WALConsumer) -> threading.Thread: ...
    def run(self) -> None: ...
