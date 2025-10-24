"""Postgres WAL consumers module"""
from collections.abc import Callable
import logging
from typing import List, TYPE_CHECKING
from .interface import Wal2JsonDecodingParams

if TYPE_CHECKING:
    from psycopg2.extras import (
        ReplicationCursor,
        ReplicationMessage,
    )
    from .publishers import BasePublisher


logger = logging.getLogger(__name__)


class WALConsumer(Callable):
    """Base WAL Stream consumer or subscriber."""

    def __init__(
        self,
        cursor: 'ReplicationCursor',
        replication_slot: str,
        replication_opts: Wal2JsonDecodingParams,
        publishers: List['BasePublisher'] = None,
    ):
        self.cursor = cursor
        self.replication_slot = replication_slot
        self.replication_opts = replication_opts
        self.publishers = publishers or []

    @property
    def output_plugin(self):
        """Output plugin to decode WAL stream."""
        return 'wal2json'

    def start_replication(self):
        """Start replication stream"""
        logger.debug('Starting the replication slot %s', self.replication_slot)
        self.cursor.start_replication(
            slot_name=self.replication_slot,
            decode=True,
            options=self.replication_opts.model_dump(
                by_alias=True,
                exclude_unset=True,
                exclude_defaults=True,
            ),
        )

    def __call__(self, msg: 'ReplicationMessage'):
        """consume WAL stream and publish using provided publishers"""
        for publisher in self.publishers:
            publisher.publish(msg)
        msg.cursor.send_feedback(flush_lsn=msg.data_start)
