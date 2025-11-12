# pylint:disable=C0103,C0114,C0115,C0116,W0621,W0402
from typing import Generator, TYPE_CHECKING

import psycopg2
from psycopg2.extras import LogicalReplicationConnection
import pytest
from pgwal.consumers import WALConsumer
from pgwal.publishers import ShellPublisher
from pgwal.interface import (
    WALReplicationOpts,
    WALReplicationValues,
)


if TYPE_CHECKING:
    from psycopg2.extras import (
        ReplicationCursor,
        ReplicationMessage,
    )


class TestWALConsumer(WALConsumer):
    """WALConsumer for tests"""

    _STATUS_INTERVAL = 10.0

    def _consume(self, msg: 'ReplicationMessage'):
        for publisher in self.publishers:
            publisher.publish(msg)
        msg.cursor.send_feedback(flush_lsn=msg.data_start, force=True)


@pytest.fixture(scope='session', autouse=True)
def db_conn() -> Generator['psycopg2.extensions.connection', None, None]:
    """Make sure we have database connection and a test schema to start with"""
    conn = psycopg2.connect(
        'host=localhost user=tests password=secret port=5432 dbname=tests'
    )
    cursor = conn.cursor()
    sql_create_test = (
        'CREATE TABLE IF NOT EXISTS demo ('
        'id bigserial not null primary key, col1 varchar(100) not null, col2 bigint'
        ')'
    )
    cursor.execute(sql_create_test)
    conn.commit()
    yield conn
    sql_drop_test = 'DROP TABLE IF EXISTS demo'
    cursor.execute(sql_drop_test)
    conn.commit()
    cursor.close()
    conn.close()


@pytest.fixture
def db_replication_cursor() -> Generator['ReplicationCursor', None, None]:
    conn = psycopg2.connect(
        'host=localhost user=tests password=secret port=5432 dbname=tests',
        connection_factory=LogicalReplicationConnection,
    )
    cursor = conn.cursor()
    yield cursor
    conn.close()
    cursor.close()


@pytest.fixture
def shell_publisher():
    yield ShellPublisher()


@pytest.fixture
def wal_consumer(db_replication_cursor, shell_publisher):
    consumer = TestWALConsumer(
        'repl_test',
        WALReplicationOpts(
            include_xids=WALReplicationValues.one,
            include_timestamp=WALReplicationValues.one,
        ),
        [shell_publisher],
    )
    try:
        db_replication_cursor.create_replication_slot(
            'repl_test', output_plugin='wal2json'
        )
    except psycopg2.errors.DuplicateObject:
        pass
    consumer.start_replication(db_replication_cursor)
    yield consumer
