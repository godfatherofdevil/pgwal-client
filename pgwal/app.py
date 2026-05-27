"""PGWAL application lifecycle."""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import logging
import threading
from typing import TYPE_CHECKING

import psycopg2
from psycopg2.extras import LogicalReplicationConnection

if TYPE_CHECKING:
    from .consumers import ConsumerState, WALConsumer
    from .publishers.base import BasePublisher
else:
    from .consumers import ConsumerState

logger = logging.getLogger(__name__)


class AppState(str, Enum):
    """PGWAL application lifecycle state."""

    IDLE = 'idle'
    STARTING = 'starting'
    RUNNING = 'running'
    STOPPING = 'stopping'
    STOPPED = 'stopped'
    FAILED = 'failed'


@dataclass
class ConsumerHandle:
    """Tracked consumer worker."""

    consumer: 'WALConsumer'
    thread: threading.Thread
    stop_event: threading.Event
    state: 'ConsumerState' = ConsumerState.IDLE
    last_error: str | None = None

    def stop(self) -> None:
        """Stop the consumer."""
        self.consumer.stop()
        self.state = self.consumer.state

    def join(self, timeout: float | None = None) -> bool:
        """Join the consumer worker."""
        self.thread.join(timeout)
        return not self.thread.is_alive()


class PGWAL:
    """A Postgres WAL stream consumer app."""

    def __init__(self, dsn: dict[str, object]) -> None:
        self.dsn = dsn
        self.stop_event = threading.Event()
        self.state = AppState.IDLE
        self.consumer_handles: list[ConsumerHandle] = []
        self.publishers: list['BasePublisher'] = []

    def get_conn(self) -> 'LogicalReplicationConnection':
        """Create one dedicated logical replication connection."""
        return psycopg2.connect(
            connection_factory=LogicalReplicationConnection,
            **self.dsn,
        )

    def _run_consumer(self, handle: ConsumerHandle) -> None:
        """Run one consumer on its own dedicated connection."""
        conn = None
        cursor = None
        handle.state = handle.consumer.state
        try:
            conn = self.get_conn()
            cursor = conn.cursor()
            handle.consumer.consume_async(cursor)
            handle.state = handle.consumer.state
            handle.last_error = handle.consumer.last_error
            if handle.state is ConsumerState.FAILED:
                self.state = AppState.FAILED
                self.stop()
        except Exception as exc:  # pragma: no cover - fatal path
            handle.last_error = str(exc)
            handle.state = ConsumerState.FAILED
            handle.consumer.set_state(ConsumerState.FAILED)
            self.state = AppState.FAILED
            self.stop()
            logger.exception('Consumer thread failed')
        finally:
            if cursor is not None and not cursor.closed:
                cursor.close()
            if conn is not None and not conn.closed:
                conn.close()
            if handle.state not in {ConsumerState.FAILED, ConsumerState.STOPPED}:
                handle.state = ConsumerState.STOPPED
            handle.consumer.set_state(handle.state)

    def consume(self, consumer: 'WALConsumer') -> ConsumerHandle:
        """Register a consumer and return its handle."""
        handle = ConsumerHandle(
            consumer=consumer,
            thread=threading.current_thread(),
            stop_event=consumer.stop_event,
            state=consumer.state,
        )
        task = threading.Thread(
            target=self._run_consumer,
            args=(handle,),
            name=f'{consumer.__class__.__name__}-consumer',
        )
        handle.thread = task
        self.consumer_handles.append(handle)
        self.publishers.extend(consumer.publishers)
        return handle

    def stop_publishers(self) -> None:
        """Stop all publishers and wait for them to exit."""
        for publisher in self.publishers:
            publisher.stop()
        for publisher in self.publishers:
            if not publisher.wait_stopped(timeout=10.0):
                logger.warning(
                    'Publisher %s did not stop within shutdown timeout',
                    publisher.__class__.__name__,
                )

    def stop(self) -> None:
        """Request app shutdown."""
        if self.state in {AppState.STOPPING, AppState.STOPPED}:
            return
        self.state = AppState.STOPPING
        self.stop_event.set()
        for handle in self.consumer_handles:
            handle.stop()

    def close(self) -> None:
        """Fully stop the app."""
        self.stop()
        self.stop_publishers()
        for handle in self.consumer_handles:
            handle.join(timeout=10.0)
        if self.state is not AppState.FAILED:
            self.state = AppState.STOPPED

    def status(self) -> dict[str, object]:
        """Return diagnostic runtime status."""
        return {
            'state': self.state.value,
            'consumers': [
                {
                    'slot': handle.consumer.replication_slot,
                    'state': handle.consumer.state.value,
                    'last_error': handle.last_error,
                }
                for handle in self.consumer_handles
            ],
            'publishers': [
                {
                    'name': publisher.__class__.__name__,
                    'state': publisher.state,
                    'metrics': publisher.metrics,
                }
                for publisher in self.publishers
            ],
        }

    def run(self) -> None:
        """Run the registered consumers and wait until they stop."""
        self.state = AppState.STARTING
        try:
            for handle in self.consumer_handles:
                handle.thread.start()
            self.state = AppState.RUNNING
            for handle in self.consumer_handles:
                handle.thread.join()
            if self.state not in {AppState.FAILED, AppState.STOPPED}:
                self.state = AppState.STOPPED
        except (KeyboardInterrupt, Exception):
            logger.info('Stopping PGWAL and all background workers')
            self.state = AppState.FAILED
            self.stop()
            raise
        finally:
            self.close()
