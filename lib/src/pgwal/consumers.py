"""Postgres WAL consumers module"""
from collections.abc import Callable
from typing import List, TYPE_CHECKING

if TYPE_CHECKING:
    from psycopg2.extras import ReplicationMessage


class WALConsumer(Callable):
    """Base WAL Stream consumer or subscriber."""

    def __init__(self, publishers: List = None):
        self.publishers = publishers or []

    def __call__(self, msg: 'ReplicationMessage'):
        """consume WAL stream and publish using provided publishers"""
        for publisher in self.publishers:
            publisher.publish(msg)
        msg.cursor.send_feedback(flush_lsn=msg.data_start)
