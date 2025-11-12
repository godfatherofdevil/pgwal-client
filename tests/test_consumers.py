"""Test consumers"""
# pylint:disable=C0103,C0114,C0115,C0116,W0621,W0402
import datetime
import threading
import time
from unittest.mock import patch
from psycopg2.extras import ReplicationCursor

from pgwal import WALConsumer, ShellPublisher
from pgwal.events import EXIT
from tests import ReplicationMessageMock


def create_test_data(conn):
    cursor = conn.cursor()
    try:
        cursor.execute(
            'INSERT INTO demo (col1, col2) VALUES (%s, %s)', ('col1_test', 1234)
        )
        conn.commit()
    finally:
        cursor.close()


def _wait_while_msg(consumer: 'WALConsumer', cursor: 'ReplicationCursor'):
    """Simulating replication."""
    while 1:
        if consumer._msg_n_consumed(cursor):
            break
    return 1


def test__msg_n_consume_returns_if_no_msg(wal_consumer, db_replication_cursor):
    assert not wal_consumer._msg_n_consumed(db_replication_cursor)


def test__msg_n_consume_consumes_if_msg(wal_consumer, db_replication_cursor, db_conn):
    create_test_data(db_conn)
    assert _wait_while_msg(wal_consumer, db_replication_cursor)


def test_consumer_returns_when_exit_signal(
    wal_consumer, db_replication_cursor, db_conn
):
    def _consume_async():
        while EXIT.is_set():
            wal_consumer.consume_async(db_replication_cursor)

    task = threading.Thread(target=_consume_async)
    EXIT.set()
    task.start()
    assert task.is_alive()

    EXIT.clear()
    # This will make the cursor wakeup so that
    # task thread will exit
    create_test_data(db_conn)
    time.sleep(0.1)
    assert not task.is_alive()


def test__get_cur_timeout_default(wal_consumer, db_replication_cursor):
    assert wal_consumer._get_cur_timeout(db_replication_cursor) < 0


def test__get_cur_timeout_returns_timeout(wal_consumer, db_replication_cursor, db_conn):
    create_test_data(db_conn)
    assert _wait_while_msg(wal_consumer, db_replication_cursor)
    timeout = wal_consumer._get_cur_timeout(db_replication_cursor)
    assert 0 < timeout <= wal_consumer._STATUS_INTERVAL


@patch.object(ShellPublisher, 'publish')
def test__consume(publish_mock, wal_consumer, db_replication_cursor):
    msg = ReplicationMessageMock(
        {
            'data_start': 0,
            'payload': '{"key": "value"}',
            'cursor': db_replication_cursor,
        }
    )
    wal_consumer._consume(msg)
    publish_mock.assert_called_once()

    assert (
        db_replication_cursor.feedback_timestamp.replace(
            microsecond=0, tzinfo=None
        ).isoformat()
        == datetime.datetime.now().replace(microsecond=0, tzinfo=None).isoformat()
    )
