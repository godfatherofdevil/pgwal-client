# pylint:disable=C0103,C0114,C0115,C0116,W0621,W0402
import os
import time
import uuid
from dataclasses import dataclass
from queue import Empty
from typing import Generator, TYPE_CHECKING

import pika
import psycopg2
from psycopg2.extras import LogicalReplicationConnection
import pytest
from pgwal.consumers import WALConsumer
from pgwal.publishers import ShellPublisher
from pgwal.publishers.rabbitmq import RabbitPublisher
from pgwal.events import EXIT
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


@dataclass
class RabbitMQSettings:
    """RabbitMQ test broker settings."""

    host: str
    port: int
    user: str
    password: str
    vhost: str

    @property
    def amqp_url(self) -> str:
        """AMQP URL for the test broker."""
        vhost = self.vhost
        if vhost == '/':
            vhost = '%2F'
        return f'amqp://{self.user}:{self.password}@{self.host}:{self.port}/{vhost}'


class RabbitPayloadMessage:
    """Minimal test message for RabbitPublisher."""

    def __init__(self, payload):
        self.payload = payload


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


@pytest.fixture(scope='session')
def rabbitmq_settings() -> RabbitMQSettings:
    return RabbitMQSettings(
        host=os.getenv('TEST_RABBITMQ_HOST', 'localhost'),
        port=int(os.getenv('TEST_RABBITMQ_PORT', '5672')),
        user=os.getenv('TEST_RABBITMQ_USER', 'tests'),
        password=os.getenv('TEST_RABBITMQ_PASSWORD', 'secret'),
        vhost=os.getenv('TEST_RABBITMQ_VHOST', '/'),
    )


@pytest.fixture
def rabbitmq_connection(rabbitmq_settings):
    deadline = time.time() + 15.0
    connection = None
    while time.time() < deadline:
        try:
            connection = pika.BlockingConnection(
                pika.URLParameters(rabbitmq_settings.amqp_url)
            )
            break
        except pika.exceptions.AMQPError:
            time.sleep(0.5)
    if connection is None:
        connection = pika.BlockingConnection(
            pika.URLParameters(rabbitmq_settings.amqp_url)
        )
    yield connection
    connection.close()


@pytest.fixture
def rabbitmq_channel(rabbitmq_connection):
    channel = rabbitmq_connection.channel()
    yield channel
    if channel.is_open:
        channel.close()


@pytest.fixture
def rabbit_publisher(rabbitmq_settings):
    resource_id = uuid.uuid4().hex
    exchange_name = f'pgwal.exchange.{resource_id}'
    queue_name = f'pgwal.queue.{resource_id}'
    routing_key = f'pgwal.route.{resource_id}'
    publisher = RabbitPublisher(
        rabbitmq_settings.amqp_url,
        exchange=exchange_name,
        queue=queue_name,
        routing_key=routing_key,
    )
    publisher.exchange_name = exchange_name
    publisher.queue_name = queue_name
    publisher.routing_key = routing_key

    previous_exit_state = EXIT.is_set()
    EXIT.set()
    while True:
        try:
            publisher.msg_queue.get_nowait()
        except Empty:
            break

    def _publish_payload(payload):
        publisher.publish(RabbitPayloadMessage(payload))

    publisher.publish_payload = _publish_payload

    def _wait_until_ready(timeout: float = 10.0):
        if not publisher.ready.wait(timeout):
            raise AssertionError('timed out waiting for RabbitPublisher readiness')

    publisher.wait_until_ready = _wait_until_ready

    yield publisher

    publisher.stop()
    deadline = time.time() + 10.0
    while (
        publisher._connection is not None
        and publisher._connection.is_open
        and time.time() < deadline
    ):
        time.sleep(0.1)
    while True:
        try:
            publisher.msg_queue.get_nowait()
        except Empty:
            break
    if previous_exit_state:
        EXIT.set()
    else:
        EXIT.clear()
