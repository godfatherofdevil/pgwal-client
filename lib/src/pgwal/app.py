"""PGWAL Client"""
from functools import cached_property
import logging
import threading
from typing import TYPE_CHECKING, List

from psycopg2.pool import ThreadedConnectionPool
from psycopg2.extras import LogicalReplicationConnection

if TYPE_CHECKING:
    from .consumers import WALConsumer


POOL_MIN = 1
POOL_MAX = 5
EXIT = threading.Event()
logger = logging.getLogger(__name__)


class PGWAL:
    """A Postgres WAL stream consumer that supports routing them further to multiple destinations"""

    def __init__(self, dsn: dict):
        """
        :param dsn: connection parameters in dict format
        """
        self.dsn = dsn
        self._pool = None
        self.tasks: List[threading.Thread] = []

    @cached_property
    def pool(self) -> ThreadedConnectionPool:
        """DB connection pool to use"""
        if self._pool is None:
            self._pool = ThreadedConnectionPool(
                POOL_MIN,
                POOL_MAX,
                connection_factory=LogicalReplicationConnection,
                **self.dsn,
            )
        return self._pool

    def close_pool(self):
        """Close all connections from the pool"""
        if self._pool is not None and not self._pool.closed:
            self._pool.closeall()

    def get_conn(self) -> 'LogicalReplicationConnection':
        """Get a connection from pool"""
        return self.pool.getconn()

    def _consume(self, consumer: 'WALConsumer'):
        """consume a WAL stream"""
        conn = self.get_conn()
        cursor = conn.cursor()
        while EXIT.is_set():
            consumer.consume_async(cursor)

    def consume(self, consumer: 'WALConsumer') -> threading.Thread:
        """Consume a WAL stream in a background thread"""
        task = threading.Thread(target=self._consume, args=(consumer,))
        self.tasks.append(task)

        return task

    def run(self):
        """Run and wait for all the tasks. This is a blocking call."""
        EXIT.set()

        def _wait():
            """wait for all tasks"""
            for task in self.tasks:
                task.start()
            for task in self.tasks:
                task.join()

        try:
            _wait()
        finally:
            logger.info('Signalling all threads to close before shutting down...BYE')
            EXIT.clear()
            self.close_pool()
