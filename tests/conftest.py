# pylint:disable=C0103,C0114,C0115,C0116,W0621,W0402
import os
import time
import uuid
from dataclasses import dataclass
from typing import Generator, TYPE_CHECKING

from kafka import KafkaAdminClient, KafkaConsumer
from kafka.admin import NewTopic
from kafka.errors import NoBrokersAvailable, TopicAlreadyExistsError
import pika
import psycopg2
from psycopg2.extras import LogicalReplicationConnection
import pytest
from pgwal.consumers import WALConsumer
from pgwal.publishers import ShellPublisher
from pgwal.publishers.kafka import KafkaPublisher
from pgwal.publishers.rabbitmq import RabbitPublisher
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


@dataclass
class KafkaSettings:
    """Kafka test broker settings."""

    host: str
    port: int

    @property
    def bootstrap_servers(self) -> str:
        """Bootstrap server string for kafka-python."""
        return f'{self.host}:{self.port}'


class KafkaPayloadMessage:
    """Minimal test message for KafkaPublisher."""

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


@pytest.fixture(scope='session')
def kafka_settings() -> KafkaSettings:
    return KafkaSettings(
        host=os.getenv('TEST_KAFKA_HOST', 'localhost'),
        port=int(os.getenv('TEST_KAFKA_PORT', '9092')),
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

    def _publish_payload(payload):
        publisher.publish(RabbitPayloadMessage(payload))

    publisher.publish_payload = _publish_payload

    def _wait_until_ready(timeout: float = 10.0):
        if not publisher.ready.wait(timeout):
            raise AssertionError('timed out waiting for RabbitPublisher readiness')

    publisher.wait_until_ready = _wait_until_ready

    yield publisher

    publisher.stop()
    if not publisher.wait_stopped(timeout=10.0):
        raise AssertionError('timed out waiting for RabbitPublisher shutdown')


@pytest.fixture
def kafka_admin_client(kafka_settings):
    deadline = time.time() + 15.0
    client = None
    while time.time() < deadline:
        try:
            client = KafkaAdminClient(
                bootstrap_servers=kafka_settings.bootstrap_servers,
            )
            client.list_topics()
            break
        except NoBrokersAvailable:
            time.sleep(0.5)
    if client is None:
        client = KafkaAdminClient(
            bootstrap_servers=kafka_settings.bootstrap_servers,
        )
        client.list_topics()
    yield client
    client.close()


@pytest.fixture
def kafka_topic_name():
    return f'pgwal.topic.{uuid.uuid4().hex}'


@pytest.fixture
def kafka_topic(kafka_admin_client, kafka_topic_name):
    try:
        kafka_admin_client.create_topics(
            [NewTopic(name=kafka_topic_name, num_partitions=1, replication_factor=1)]
        )
    except TopicAlreadyExistsError:
        pass
    return kafka_topic_name


@pytest.fixture
def kafka_consumer(kafka_settings, kafka_topic):
    consumer = KafkaConsumer(
        kafka_topic,
        bootstrap_servers=kafka_settings.bootstrap_servers,
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        group_id=f'pgwal-tests-{uuid.uuid4().hex}',
        consumer_timeout_ms=1000,
        value_deserializer=lambda value: value.decode('utf8'),
    )
    yield consumer
    consumer.close()


@pytest.fixture
def kafka_publisher(kafka_settings, kafka_topic):
    publisher = KafkaPublisher(
        kafka_topic,
        bootstrap_servers=kafka_settings.bootstrap_servers,
    )
    publisher.topic_name = kafka_topic

    def _publish_payload(payload, timeout: float = 10.0):
        previous_sent = publisher._sent
        publisher.publish(KafkaPayloadMessage(payload))
        deadline = time.time() + timeout
        while publisher._sent == previous_sent and time.time() < deadline:
            time.sleep(0.1)
        if publisher._sent == previous_sent:
            raise AssertionError('timed out waiting for KafkaPublisher to send message')
        publisher.flush(timeout)

    publisher.publish_payload = _publish_payload

    yield publisher

    publisher.stop()
    publisher.wait_stopped(timeout=10.0)
    time.sleep(publisher._PUBLISH_INTERVAL + 0.2)
