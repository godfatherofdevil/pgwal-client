"""Test consumers"""
# pylint:disable=C0103,C0114,C0115,C0116,W0621,W0402


def create_test_data(conn):
    cursor = conn.cursor()
    try:
        cursor.execute(
            'INSERT INTO demo (col1, col2) VALUES (%s, %s)', ('col1_test', 1234)
        )
        conn.commit()
    finally:
        cursor.close()


def test_msg_n_consume_returns_if_no_msg(wal_consumer, db_replication_cursor):
    assert not wal_consumer._msg_n_consumed(db_replication_cursor)


def test_msg_n_consume_consumes_if_msg(wal_consumer, db_replication_cursor, db_conn):
    def _wait_while_msg():
        """Simulating replication."""
        while 1:
            if wal_consumer._msg_n_consumed(db_replication_cursor):
                break
        return 1

    create_test_data(db_conn)
    assert _wait_while_msg()
