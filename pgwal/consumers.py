"""Postgres WAL consumers module."""
from __future__ import annotations

from datetime import datetime
from enum import Enum
import logging
from select import select
import threading
from typing import TYPE_CHECKING, cast

import psycopg2

if TYPE_CHECKING:
    from psycopg2.extras import ReplicationCursor, ReplicationMessage

    from .publishers.base import BasePublisher

logger = logging.getLogger(__name__)


class ConsumerState(str, Enum):
    """Consumer lifecycle state."""

    IDLE = 'idle'
    STARTING = 'starting'
    RUNNING = 'running'
    STOPPING = 'stopping'
    STOPPED = 'stopped'
    FAILED = 'failed'


class WALConsumer:
    """Base WAL stream consumer."""

    _STATUS_INTERVAL = 10.0

    def __init__(
        self,
        replication_slot: str,
        replication_opts: object,
        publishers: list['BasePublisher'] | None = None,
    ) -> None:
        self.replication_slot = replication_slot
        self.replication_opts = replication_opts
        self.publishers = publishers or []
        self._stop_event = threading.Event()
        self._state = ConsumerState.IDLE
        self._last_error: str | None = None

    @property
    def state(self) -> ConsumerState:
        """Consumer lifecycle state."""
        return self._state

    def set_state(self, value: ConsumerState) -> None:
        """Set consumer state."""
        self._state = value

    @property
    def last_error(self) -> str | None:
        """Last consumer error."""
        return self._last_error

    @property
    def stop_event(self) -> threading.Event:
        """Per-consumer stop event."""
        return self._stop_event

    def stop(self) -> None:
        """Stop this consumer."""
        self.set_state(ConsumerState.STOPPING)
        self._stop_event.set()

    @property
    def output_plugin(self) -> str:
        """Output plugin used to decode the WAL stream."""
        return 'wal2json'

    def start_replication(self, cursor: 'ReplicationCursor') -> None:
        """Start the replication stream."""
        logger.debug(
            'Consumer %s starting replication slot %s',
            id(self),
            self.replication_slot,
        )
        try:
            cursor.start_replication(
                slot_name=self.replication_slot,
                decode=True,
                options=self.replication_opts.model_dump(
                    by_alias=True,
                    exclude_unset=True,
                    exclude_defaults=True,
                ),
            )
        except psycopg2.OperationalError:
            logger.warning(
                'Replication slot %s has already started on this cursor',
                self.replication_slot,
            )

    def _publish_to_all(self, msg: 'ReplicationMessage') -> bool:
        """Publish one message to all configured publishers."""
        for publisher in self.publishers:
            result = publisher.publish(msg)
            if not result.accepted:
                self._last_error = result.reason
                self.set_state(ConsumerState.FAILED)
                return False
        return True

    def _consume(self, msg: 'ReplicationMessage') -> None:
        """Consume one message and advance feedback on successful handoff."""
        if not self._publish_to_all(msg):
            return
        msg.cursor.send_feedback(flush_lsn=msg.data_start)

    def _msg_n_consumed(self, cursor: 'ReplicationCursor') -> bool:
        """Consume a message if available."""
        msg = cursor.read_message()
        if not msg:
            return False
        self._consume(msg)
        return True

    def _get_cur_timeout(self, cursor: 'ReplicationCursor') -> float:
        """Calculate cursor timeout."""
        feedback_timestamp = cast(datetime | None, cursor.feedback_timestamp)
        if feedback_timestamp is None:
            return -1.0
        return (
            self._STATUS_INTERVAL
            - (datetime.now() - feedback_timestamp).total_seconds()
        )

    def _wait_on_repl_cursor(self, cursor: 'ReplicationCursor') -> None:
        """Wait on cursor activity or timeout."""
        timeout = self._get_cur_timeout(cursor)
        try:
            select([cursor], [], [], max(0, int(timeout)))
        except InterruptedError:
            pass

    def consume_async(self, cursor: 'ReplicationCursor') -> None:
        """Consume the WAL stream until the consumer is stopped."""
        self.set_state(ConsumerState.STARTING)
        self.start_replication(cursor)
        self.set_state(ConsumerState.RUNNING)
        while not self.stop_event.is_set():
            if cursor.closed:
                logger.warning('Cursor is already closed, returning')
                self.set_state(ConsumerState.STOPPED)
                return
            if self._msg_n_consumed(cursor):
                if self.state is ConsumerState.FAILED:
                    return
                continue
            self._wait_on_repl_cursor(cursor)
        self.set_state(ConsumerState.STOPPED)

    def consume_sync(self, cursor: 'ReplicationCursor') -> None:
        """Consume WAL stream synchronously."""
        cursor.consume_stream(self)

    def __call__(self, msg: 'ReplicationMessage') -> None:
        """Callback for `ReplicationCursor.consume_stream`."""
        self._consume(msg)
