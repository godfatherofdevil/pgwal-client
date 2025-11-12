"""Postgres WAL consumers module"""
import threading
from collections.abc import Callable
from datetime import datetime
import logging
from select import select
from typing import (
    List,
    TYPE_CHECKING,
)
import psycopg2

from .events import EXIT
from .interface import WALReplicationOpts

if TYPE_CHECKING:
    from psycopg2.extras import (
        ReplicationCursor,
        ReplicationMessage,
    )
    from .publishers import BasePublisher


logger = logging.getLogger(__name__)


class WALConsumer(Callable):
    """Base WAL Stream consumer or subscriber."""

    _lock = threading.Lock()
    _STATUS_INTERVAL = 10.0

    def __init__(
        self,
        replication_slot: str,
        replication_opts: WALReplicationOpts,
        publishers: List['BasePublisher'] = None,
    ):
        self.replication_slot = replication_slot
        self.replication_opts = replication_opts
        self.publishers = publishers or []
        # Flag to indicate if consuming from server or not
        self._consuming = False

    def set_consuming(self, value: bool):
        """Set _consuming flag"""
        with self._lock:
            self._consuming = value

    @property
    def consuming(self) -> bool:
        """Whether consuming or not"""
        return self._consuming

    def stop(self):
        """Stop this consumer"""
        self.set_consuming(False)

    @property
    def output_plugin(self):
        """Output plugin to decode WAL stream."""
        return 'wal2json'

    def start_replication(self, cursor: 'ReplicationCursor'):
        """Start replication stream"""
        logger.debug(
            'Consumer %s, Starting the replication slot %s',
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
                'Replication Slot %s has already started on this cursor',
                self.replication_slot,
            )

    def _consume(self, msg: 'ReplicationMessage'):
        """Consume one message and publish to all configured publishers"""
        for publisher in self.publishers:
            publisher.publish(msg)
        msg.cursor.send_feedback(flush_lsn=msg.data_start)

    def _msg_n_consumed(self, cursor: 'ReplicationCursor') -> bool:
        """
        Consume if message otherwise return
        :param cursor: replication cursor
        :return bool: True if there is a message to consume otherwise False
        """
        msg = cursor.read_message()
        if not msg:
            return False
        self._consume(msg)
        return True

    def _get_cur_timeout(self, cursor: 'ReplicationCursor') -> float:
        """Calculate and return the cursor timeout"""
        return (
            self._STATUS_INTERVAL
            - (datetime.now() - cursor.feedback_timestamp).total_seconds()
        )

    def _wait_on_repl_cursor(self, cursor: 'ReplicationCursor'):
        """Wait on cursor for a message or timeout and recalculate timeout and continue"""
        timeout = self._get_cur_timeout(cursor)
        try:
            # pylint: disable=W0612
            # flake8: noqa
            sel = select([cursor], [], [], max(0, int(timeout)))
        except InterruptedError:
            pass  # recalculate timeout and continue

    def consume_async(self, cursor: 'ReplicationCursor'):
        """Consume WAL stream without blocking"""
        self.start_replication(cursor)
        while True:
            if not EXIT.is_set():
                logger.warning('Received EXIT event. breaking from the consuming loop')
                self.stop()
                break
            if cursor.closed:
                logger.warning('Cursor is already closed. returning!!!')
                return
            self.set_consuming(True)
            if self._msg_n_consumed(cursor):
                continue
            self._wait_on_repl_cursor(cursor)

    def consume_sync(self, cursor: 'ReplicationCursor'):
        """Consume WAL stream and block till new messages arrive"""
        cursor.consume_stream(self)

    def __call__(self, msg: 'ReplicationMessage'):
        """
        callback to ReplicationCursor.consume_stream,
        for more details https://www.psycopg.org/docs/extras.html#psycopg2.extras.ReplicationCursor.consume_stream
        """
        self._consume(msg)
