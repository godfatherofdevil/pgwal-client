"""Postgres WAL consumers module"""
import abc
from collections.abc import Callable
import logging
from typing import List, TYPE_CHECKING
import psycopg2
from .interface import Wal2JsonDecodingParams

if TYPE_CHECKING:
    from psycopg2.extras import (
        LogicalReplicationConnection,
        ReplicationMessage,
    )


logger = logging.getLogger(__name__)


class WALConsumer(Callable, metaclass=abc.ABCMeta):
    """Base WAL Stream consumer or subscriber."""

    def __init__(
        self,
        conn: 'LogicalReplicationConnection',
        replication_slot: str,
        replication_opts: Wal2JsonDecodingParams,
        publishers: List = None,
        create_repl_slot: bool = False,
        del_repl_slot: bool = False,
    ):
        self.conn = conn
        self.cursor = conn.cursor()
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

    def stop_replication(self):
        """Stop replication stream"""
        if self.del_repl_slot:
            self.cursor.drop_replication_slot(self.replication_slot)
        self.cursor.close()
        self.conn.close()

    def __call__(self, msg: 'ReplicationMessage'):
        """consume WAL stream and publish using provided publishers"""
        for publisher in self.publishers:
            publisher.publish(msg)
        msg.cursor.send_feedback(flush_lsn=msg.data_start)

    def __enter__(self):
        self.stop_replication()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        if exc_type is not None:
            logger.error(
                'Unexpected Error while consuming from WAL stream. replication_slot %s',
                self.replication_slot,
                exc_info=(
                    exc_type,
                    exc_val,
                    exc_tb,
                ),
            )
            return False
        self.stop_replication()
        return True
