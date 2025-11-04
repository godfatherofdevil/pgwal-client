# pylint:disable=C0103,C0114,C0115,C0116,W0621,W0402
from typing import Generator, TYPE_CHECKING

import psycopg2
from psycopg2.extras import LogicalReplicationConnection
import pytest
from pgwal.consumers import WALConsumer
from pgwal.publishers import ShellPublisher
from pgwal.interface import WALReplicationOpts, WALReplicationValues


if TYPE_CHECKING:
    from psycopg2.extras import ReplicationCursor


class TestWALConsumer(WALConsumer):
    """WALConsumer for tests"""

    _STATUS_INTERVAL = 10.0


@pytest.fixture(scope='session', autouse=True)
def db_conn() -> Generator['psycopg2.extensions.connection', None, None]:
    """Make sure we have database connection and a test schema to start with"""
    conn = psycopg2.connect(
        'host=localhost user=human password=secret port=5432 dbname=replication_demo'
    )
    cursor = conn.cursor()
    sql1 = (
        'CREATE TABLE IF NOT EXISTS demo ('
        'id bigserial not null primary key, col1 varchar(100) not null, col2 bigint'
        ')'
    )
    cursor.execute(sql1)
    conn.commit()
    yield conn
    sql2 = 'DROP TABLE IF EXISTS demo'
    cursor.execute(sql2)
    conn.commit()
    cursor.close()
    conn.close()


@pytest.fixture
def db_replication_cursor() -> Generator['ReplicationCursor', None, None]:
    conn = psycopg2.connect(
        'host=localhost user=human password=secret port=5432 dbname=replication_demo',
        connection_factory=LogicalReplicationConnection,
    )
    cursor = conn.cursor()
    yield cursor
    conn.close()
    cursor.close()


@pytest.fixture
def db_repl_admin_cur() -> Generator['ReplicationCursor', None, None]:
    conn = psycopg2.connect(
        'host=localhost user=human password=secret port=5432 dbname=replication_demo',
        connection_factory=LogicalReplicationConnection,
    )
    cursor = conn.cursor()
    yield cursor
    cursor.close()
    conn.close()


@pytest.fixture
def wal_consumer(db_replication_cursor):
    consumer = TestWALConsumer(
        'repl_demo',
        WALReplicationOpts(
            include_xids=WALReplicationValues.one,
            include_timestamp=WALReplicationValues.one,
        ),
        [ShellPublisher()],
    )
    try:
        db_replication_cursor.create_replication_slot(
            'repl_demo', output_plugin='wal2json'
        )
    except psycopg2.errors.DuplicateObject:
        pass
    consumer.start_replication(db_replication_cursor)
    yield consumer
