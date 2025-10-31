"""PGWAL Client"""
import time
from functools import cached_property
import logging
import threading
from typing import TYPE_CHECKING, List

from psycopg2.pool import ThreadedConnectionPool
from psycopg2.extras import LogicalReplicationConnection
from .events import EXIT

if TYPE_CHECKING:
    from .consumers import WALConsumer
    from .publishers import BasePublisher


POOL_MIN = 1
POOL_MAX = 5
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
        # We maintain a list of publishers so that we can gracefully exit
        self.publishers: List['BasePublisher'] = []

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

    def stop_publishers(self):
        """Stop all publishers before exiting"""
        for publisher in self.publishers:
            publisher.stop()

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
        """
        Consume a WAL stream in a background thread.
        While This can support multiple consumer instances,
        it is not recommended to run more than one consumer instance in production b/c of following limitations
        1. WAL replication needs a dedicated db connection and that connection
        can't be shared with multiple consumer instance
        2. PGWAL supports a max of 5 database connection pool and using more than 5 consumers will lead
        to some connections being shared, this will not lead to an error but postgres will silently drop new
        start_replication commands over a connection that has already issued a start_replication.
        """
        task = threading.Thread(target=self._consume, args=(consumer,))
        self.tasks.append(task)
        self.publishers.extend(consumer.publishers)

        return task

    def run(self):
        """Run and wait for all the tasks. This is a blocking call."""
        EXIT.set()

        def _start():
            """Start all the tasks"""
            for task in self.tasks:
                task.start()

        def _wait():
            """Wait for all tasks"""
            for task in self.tasks:
                task.join()

        try:
            _start()
            _wait()
        except (KeyboardInterrupt, Exception):
            logger.info('Signalling all threads to close before shutting down...BYE')
            EXIT.clear()
            self.close_pool()
            self.stop_publishers()
        finally:
            time.sleep(1.0)
