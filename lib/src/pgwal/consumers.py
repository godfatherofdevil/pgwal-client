"""Postgres WAL consumers module"""
import abc
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


logger = logging.getLogger(__name__)


class WALConsumer(Callable, metaclass=abc.ABCMeta):
    """Base WAL Stream consumer or subscriber."""

    def __init__(
        self,
        cursor: 'ReplicationCursor',
        replication_slot: str,
        replication_opts: Wal2JsonDecodingParams,
        publishers: List = None,
        create_repl_slot: bool = False,
        del_repl_slot: bool = False,
    ):
        self.cursor = cursor
        self.replication_slot = replication_slot
        self.replication_opts = replication_opts
        self.create_repl_slot = create_repl_slot
        self.del_repl_slot = del_repl_slot
        self.publishers = publishers or []

    def start_replication(self):
        """Start replication stream"""
        try:
            self.cursor.start_replication(
                slot_name=self.replication_slot,
                decode=True,
                options=self.replication_opts,
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
                self.replication_slot, output_plugin="wal2json"
            )
            self.cursor.start_replication(
                slot_name=self.replication_slot,
                decode=True,
                options=self.replication_opts,
            )

    def __call__(self, msg: 'ReplicationMessage'):
        """consume WAL stream and publish using provided publishers"""
        for publisher in self.publishers:
            publisher.publish(msg)
        msg.cursor.send_feedback(flush_lsn=msg.data_start)
