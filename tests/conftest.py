# pylint:disable=C0103,C0114,C0115,C0116,W0621,W0402
from typing import Generator, TYPE_CHECKING

import psycopg2
from psycopg2.extras import LogicalReplicationConnection
import pytest
from lib.src.pgwal.consumers import WALConsumer
from lib.src.pgwal.publishers import ShellPublisher
from lib.src.pgwal.interface import WALReplicationOpts, WALReplicationValues


if TYPE_CHECKING:
    from psycopg2.extras import ReplicationCursor


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
def wal_consumer(db_replication_cursor):
    consumer = WALConsumer(
        'repl_demo',
        WALReplicationOpts(
            include_xids=WALReplicationValues.one,
            include_timestamp=WALReplicationValues.one,
        ),
        [ShellPublisher()],
    )
    consumer.start_replication(db_replication_cursor)
    yield consumer
