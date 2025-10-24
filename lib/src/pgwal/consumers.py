"""Postgres WAL consumers module"""
from collections.abc import Callable
import logging
from typing import List, TYPE_CHECKING
import psycopg2
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
        create_repl_slot: bool = False,
        del_repl_slot: bool = False,
    ):
        self.cursor = cursor
        self.replication_slot = replication_slot
        self.replication_opts = replication_opts
        self.create_repl_slot = create_repl_slot
        self.del_repl_slot = del_repl_slot
        self.publishers = publishers or []

    @property
    def output_plugin(self):
        """Output plugin to decode WAL stream."""
        return 'wal2json'

    def start_replication(self):
        """Start replication stream"""
        logger.debug('Starting the replication slot %s', self.replication_slot)
        try:
            self.cursor.start_replication(
                slot_name=self.replication_slot,
                decode=True,
                options=self.replication_opts.model_dump(
                    by_alias=True,
                    exclude_unset=True,
                    exclude_defaults=True,
                ),
            )
        except psycopg2.ProgrammingError:
            if not self.create_repl_slot:
                logger.warning(
                    'replication slot %s is missing, '
                    'create and try again or set `create_repl_slot` to True '
                    'to force creation of slot.',
                    self.replication_slot,
                )
                raise
            self.cursor.create_replication_slot(
                self.replication_slot,
                output_plugin=self.output_plugin,
            )
            self.cursor.start_replication(
                slot_name=self.replication_slot,
                decode=True,
                options=self.replication_opts.model_dump(
                    by_alias=True,
                    exclude_unset=True,
                    exclude_defaults=True,
                ),
            )

    def stop_replication(self, cursor: 'ReplicationCursor'):
        """Stop replication stream"""
        if self.del_repl_slot:
            cursor.drop_replication_slot(self.replication_slot)

    def __call__(self, msg: 'ReplicationMessage'):
        """consume WAL stream and publish using provided publishers"""
        for publisher in self.publishers:
            publisher.publish(msg)
        msg.cursor.send_feedback(flush_lsn=msg.data_start)
