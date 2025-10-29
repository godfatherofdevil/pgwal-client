"""Postgres WAL consumers module"""
from collections.abc import Callable
from datetime import datetime
import logging
from select import select
from typing import List, TYPE_CHECKING

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
        cursor.start_replication(
            slot_name=self.replication_slot,
            decode=True,
            options=self.replication_opts.model_dump(
                by_alias=True,
                exclude_unset=True,
                exclude_defaults=True,
            ),
        )

    def _consume(self, msg: 'ReplicationMessage'):
        """Consume one message and publish to all configured publishers"""
        for publisher in self.publishers:
            publisher.publish(msg)
        msg.cursor.send_feedback(flush_lsn=msg.data_start)

    def consume_async(self, cursor: 'ReplicationCursor'):
        """Consume WAL stream without blocking"""
        self.start_replication(cursor)
        while True:
            if not EXIT.is_set():
                logger.warning('Received EXIT event. breaking from the consuming loop')
                break
            if cursor.closed:
                logger.warning('Cursor is already closed. returning!!!')
                return
            msg = cursor.read_message()
            if msg:
                self._consume(msg)
            else:
                timeout = (
                    self._STATUS_INTERVAL
                    - (datetime.now() - cursor.feedback_timestamp).total_seconds()
                )
                try:
                    # pylint: disable=W0612
                    # flake8: noqa
                    sel = select([cursor], [], [], max(0, int(timeout)))
                except InterruptedError:
                    pass  # recalculate timeout and continue

    def consume_sync(self, cursor: 'ReplicationCursor'):
        """Consume WAL stream and block till new messages arrive"""
        cursor.consume_stream(self)

    def __call__(self, msg: 'ReplicationMessage'):
        """
        callback to ReplicationCursor.consume_stream,
        for more details https://www.psycopg.org/docs/extras.html#psycopg2.extras.ReplicationCursor.consume_stream
        """
        self._consume(msg)
