"""Run an on-demand end-to-end PGWAL flow against real destinations."""
from __future__ import annotations

import argparse
import os
import sys
import threading
import time
import uuid
from dataclasses import dataclass

from kafka import KafkaAdminClient, KafkaConsumer
from kafka.admin import NewTopic
from kafka.errors import NoBrokersAvailable, TopicAlreadyExistsError
import pika
import psycopg2
from psycopg2.extras import LogicalReplicationConnection
from dotenv import load_dotenv

from pgwal.app import PGWAL
from pgwal.consumers import ConsumerState, WALConsumer
from pgwal.interface import WALReplicationOpts, WALReplicationValues
from pgwal.publishers import KafkaPublisher, RabbitPublisher


class E2EError(RuntimeError):
    """Raised when the e2e run cannot complete successfully."""


@dataclass(frozen=True)
class PostgresConfig:
    """Postgres connection settings."""

    dbname: str
    user: str
    password: str
    port: int
    startup_timeout: float

    @property
    def dsn(self) -> dict[str, object]:
        """Return PGWAL connection kwargs."""
        return {
            'host': 'localhost',
            'port': self.port,
            'dbname': self.dbname,
            'user': self.user,
            'password': self.password,
        }

    @property
    def conninfo(self) -> str:
        """Return psycopg2 connection info."""
        return (
            f'host=localhost port={self.port} dbname={self.dbname} '
            f'user={self.user} password={self.password}'
        )


@dataclass(frozen=True)
class RabbitConfig:
    """RabbitMQ destination settings."""

    host: str
    port: int
    user: str
    password: str
    vhost: str
    delivery_timeout: float

    @property
    def amqp_url(self) -> str:
        """Build the AMQP URL."""
        vhost = self.vhost
        if vhost == '/':
            vhost = '%2F'
        return f'amqp://{self.user}:{self.password}@{self.host}:{self.port}/{vhost}'


@dataclass(frozen=True)
class KafkaConfig:
    """Kafka destination settings."""

    host: str
    port: int
    delivery_timeout: float

    @property
    def bootstrap_servers(self) -> str:
        """Build the bootstrap server string."""
        return f'{self.host}:{self.port}'


def parse_args() -> argparse.Namespace:
    """Parse CLI args."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--publishers',
        required=True,
        help='Comma-separated publishers to verify: rabbitmq, kafka, or both',
    )
    parser.add_argument(
        '--consumer-workers',
        type=int,
        default=1,
        help='Number of WAL consumer workers to start',
    )
    parser.add_argument(
        '--env-file',
        default='.local/.env',
        help='Optional env file to load before reading process environment',
    )
    return parser.parse_args()


def load_env_file(path: str) -> None:
    """Load environment values from a dotenv file when it exists."""
    load_dotenv(path, override=False)


def get_env(name: str, default: str) -> str:
    """Read a string env var."""
    return os.getenv(name, default)


def get_env_int(name: str, default: int) -> int:
    """Read an integer env var."""
    value = os.getenv(name)
    if value is None:
        return default
    return int(value)


def get_env_float(name: str, default: float) -> float:
    """Read a float env var."""
    value = os.getenv(name)
    if value is None:
        return default
    return float(value)


def parse_publishers(value: str) -> list[str]:
    """Validate and normalize selected publishers."""
    publishers = []
    for item in value.split(','):
        publisher = item.strip().lower()
        if not publisher:
            continue
        if publisher not in {'rabbitmq', 'kafka'}:
            raise E2EError(
                f'Unsupported publisher {publisher!r}. Expected rabbitmq and/or kafka.'
            )
        if publisher not in publishers:
            publishers.append(publisher)
    if not publishers:
        raise E2EError('At least one publisher must be selected with --publishers.')
    return publishers


class E2ERunner:  # pylint: disable=too-many-instance-attributes
    """Standalone orchestration for one end-to-end validation run."""

    def __init__(self, args: argparse.Namespace) -> None:
        self.publishers = parse_publishers(args.publishers)
        if args.consumer_workers < 1:
            raise E2EError('--consumer-workers must be at least 1')
        self.consumer_workers = args.consumer_workers
        self.run_id = uuid.uuid4().hex[:8]
        self.table_name = f'pgwal_e2e_{self.run_id}'
        self.marker = f'pgwal-e2e-{self.run_id}'
        self.postgres = PostgresConfig(
            dbname=get_env('TEST_DB_NAME', 'tests'),
            user=get_env('TEST_DB_USER', 'tests'),
            password=get_env('TEST_DB_PASSWORD', 'secret'),
            port=get_env_int('TEST_DB_PORT', 5432),
            startup_timeout=get_env_float('E2E_APP_STARTUP_TIMEOUT', 20.0),
        )
        self.rabbit = RabbitConfig(
            host=get_env('TEST_RABBITMQ_HOST', 'localhost'),
            port=get_env_int('TEST_RABBITMQ_PORT', 5672),
            user=get_env('TEST_RABBITMQ_USER', 'tests'),
            password=get_env('TEST_RABBITMQ_PASSWORD', 'secret'),
            vhost=get_env('TEST_RABBITMQ_VHOST', '/'),
            delivery_timeout=get_env_float('E2E_DELIVERY_TIMEOUT', 30.0),
        )
        self.kafka = KafkaConfig(
            host=get_env('TEST_KAFKA_HOST', 'localhost'),
            port=get_env_int('TEST_KAFKA_PORT', 9092),
            delivery_timeout=get_env_float('E2E_DELIVERY_TIMEOUT', 30.0),
        )
        self.app = PGWAL(self.postgres.dsn)
        self.app_thread: threading.Thread | None = None
        self.rabbit_connection: pika.BlockingConnection | None = None
        self.kafka_admin: KafkaAdminClient | None = None
        self.kafka_consumer: KafkaConsumer | None = None
        self.runtime_publishers: list[object] = []
        self.slot_names = [
            f'pgwal_e2e_{self.run_id}_{index}' for index in range(self.consumer_workers)
        ]
        self.rabbit_queue = f'pgwal.e2e.queue.{self.run_id}'
        self.rabbit_exchange = f'pgwal.e2e.exchange.{self.run_id}'
        self.rabbit_routing_key = f'pgwal.e2e.route.{self.run_id}'
        self.kafka_topic = f'pgwal.e2e.topic.{self.run_id}'

    def log(self, message: str) -> None:
        """Emit one user-facing log line."""
        print(message, flush=True)

    def run(self) -> None:
        """Execute the full e2e flow."""
        try:
            self.prepare_db()
            self.prepare_destinations()
            self.prepare_slots()
            self.prepare_app()
            self.start_app()
            self.insert_row()
            self.verify_destinations()
        finally:
            self.cleanup()

    def prepare_db(self) -> None:
        """Create the run-specific test table."""
        self.log(f'Creating table {self.table_name}')
        conn = psycopg2.connect(self.postgres.conninfo)
        try:
            conn.autocommit = True
            with conn.cursor() as cursor:
                cursor.execute(
                    (
                        f'CREATE TABLE {self.table_name} ('
                        'id bigserial not null primary key, '
                        'col1 varchar(100) not null, '
                        'col2 bigint not null)'
                    )
                )
        finally:
            conn.close()

    def prepare_destinations(self) -> None:
        """Prepare selected external destinations."""
        if 'rabbitmq' in self.publishers:
            self.log('Preparing RabbitMQ destination')
            self.rabbit_connection = self.wait_for_rabbit_connection()
        if 'kafka' in self.publishers:
            self.log('Preparing Kafka destination')
            self.kafka_admin = self.wait_for_kafka_admin()
            try:
                self.kafka_admin.create_topics(
                    [
                        NewTopic(
                            name=self.kafka_topic,
                            num_partitions=1,
                            replication_factor=1,
                        )
                    ]
                )
            except TopicAlreadyExistsError:
                pass
            self.kafka_consumer = KafkaConsumer(
                self.kafka_topic,
                bootstrap_servers=self.kafka.bootstrap_servers,
                auto_offset_reset='earliest',
                enable_auto_commit=False,
                group_id=f'pgwal-e2e-{self.run_id}',
                consumer_timeout_ms=1000,
                value_deserializer=lambda value: value.decode('utf8'),
            )

    def wait_for_rabbit_connection(self) -> pika.BlockingConnection:
        """Connect to RabbitMQ within the delivery timeout."""
        deadline = time.time() + self.rabbit.delivery_timeout
        last_error: Exception | None = None
        while time.time() < deadline:
            try:
                return pika.BlockingConnection(pika.URLParameters(self.rabbit.amqp_url))
            except pika.exceptions.AMQPError as exc:
                last_error = exc
                time.sleep(0.5)
        raise E2EError(f'Could not connect to RabbitMQ: {last_error}')

    def wait_for_kafka_admin(self) -> KafkaAdminClient:
        """Connect to Kafka within the delivery timeout."""
        deadline = time.time() + self.kafka.delivery_timeout
        last_error: Exception | None = None
        while time.time() < deadline:
            try:
                client = KafkaAdminClient(
                    bootstrap_servers=self.kafka.bootstrap_servers
                )
                client.list_topics()
                return client
            except NoBrokersAvailable as exc:
                last_error = exc
                time.sleep(0.5)
        raise E2EError(f'Could not connect to Kafka: {last_error}')

    def prepare_slots(self) -> None:
        """Create one logical replication slot per worker."""
        self.log('Creating logical replication slots')
        conn = psycopg2.connect(
            self.postgres.conninfo,
            connection_factory=LogicalReplicationConnection,
        )
        try:
            with conn.cursor() as cursor:
                for slot_name in self.slot_names:
                    cursor.create_replication_slot(
                        slot_name,
                        output_plugin='wal2json',
                    )
        finally:
            conn.close()

    def prepare_app(self) -> None:
        """Register consumers and publishers on the PGWAL app."""
        self.log('Preparing PGWAL consumers and publishers')
        publisher_instances = []
        if 'rabbitmq' in self.publishers:
            publisher_instances.append(
                RabbitPublisher(
                    self.rabbit.amqp_url,
                    exchange=self.rabbit_exchange,
                    queue=self.rabbit_queue,
                    routing_key=self.rabbit_routing_key,
                )
            )
        if 'kafka' in self.publishers:
            publisher_instances.append(
                KafkaPublisher(
                    self.kafka_topic,
                    bootstrap_servers=self.kafka.bootstrap_servers,
                )
            )
        self.runtime_publishers = publisher_instances
        for slot_name in self.slot_names:
            consumer = WALConsumer(
                slot_name,
                WALReplicationOpts(
                    include_xids=WALReplicationValues.one,
                    include_timestamp=WALReplicationValues.one,
                    add_tables=[f'public.{self.table_name}'],
                ),
                publisher_instances,
            )
            self.app.consume(consumer)

    def start_app(self) -> None:
        """Start PGWAL in a background thread and wait for running state."""
        self.log('Starting PGWAL')
        self.app_thread = threading.Thread(target=self.app.run, name='pgwal-e2e-app')
        self.app_thread.start()
        deadline = time.time() + self.postgres.startup_timeout
        while time.time() < deadline:
            if self.app.state.value == 'failed':
                raise E2EError(f'PGWAL failed to start: {self.app.status()}')
            handle_states = {
                handle.consumer.state for handle in self.app.consumer_handles
            }
            if handle_states and handle_states == {ConsumerState.RUNNING}:
                return
            time.sleep(0.2)
        raise E2EError(f'PGWAL did not reach running state: {self.app.status()}')

    def insert_row(self) -> None:
        """Insert one row that should produce one WAL event per slot."""
        self.log('Writing a row to Postgres')
        conn = psycopg2.connect(self.postgres.conninfo)
        try:
            conn.autocommit = True
            with conn.cursor() as cursor:
                cursor.execute(
                    f'INSERT INTO {self.table_name} (col1, col2) VALUES (%s, %s)',
                    (self.marker, self.consumer_workers),
                )
        finally:
            conn.close()

    def verify_destinations(self) -> None:
        """Read the emitted message back from every selected destination."""
        if 'rabbitmq' in self.publishers:
            self.verify_rabbitmq_delivery()
        if 'kafka' in self.publishers:
            self.verify_kafka_delivery()

    def verify_rabbitmq_delivery(self) -> None:
        """Verify end-to-end delivery through RabbitMQ."""
        assert self.rabbit_connection is not None
        deadline = time.time() + self.rabbit.delivery_timeout
        while time.time() < deadline:
            channel = self.rabbit_connection.channel()
            try:
                method_frame, _properties, body = channel.basic_get(
                    queue=self.rabbit_queue,
                    auto_ack=True,
                )
                if method_frame is None:
                    time.sleep(0.2)
                    continue
                payload = body.decode('utf8')
                if self.marker not in payload or self.table_name not in payload:
                    raise E2EError(
                        f'RabbitMQ payload did not contain the run marker: {payload}'
                    )
                self.log('Verified RabbitMQ delivery')
                return
            except pika.exceptions.ChannelClosedByBroker:
                time.sleep(0.2)
            finally:
                if channel.is_open:
                    channel.close()
        raise E2EError('Timed out waiting for RabbitMQ delivery')

    def verify_kafka_delivery(self) -> None:
        """Verify end-to-end delivery through Kafka."""
        assert self.kafka_consumer is not None
        deadline = time.time() + self.kafka.delivery_timeout
        while time.time() < deadline:
            batches = self.kafka_consumer.poll(timeout_ms=500, max_records=10)
            for records in batches.values():
                for record in records:
                    payload = record.value
                    if self.marker not in payload or self.table_name not in payload:
                        raise E2EError(
                            f'Kafka payload did not contain the run marker: {payload}'
                        )
                    self.log('Verified Kafka delivery')
                    return
        raise E2EError('Timed out waiting for Kafka delivery')

    def cleanup(self) -> None:
        """Best-effort cleanup for the full run."""
        self.log('Cleaning up e2e resources')
        if self.app_thread is not None:
            self.app.close()
            self.app_thread.join(timeout=10.0)
        self.drop_slots()
        self.drop_table()
        if self.rabbit_connection is not None and self.rabbit_connection.is_open:
            self.rabbit_connection.close()
        if self.kafka_consumer is not None:
            self.kafka_consumer.close()
        if self.kafka_admin is not None:
            self.kafka_admin.close()

    def drop_slots(self) -> None:
        """Drop logical replication slots after the app has stopped."""
        try:
            conn = psycopg2.connect(
                self.postgres.conninfo,
                connection_factory=LogicalReplicationConnection,
            )
        except psycopg2.Error:
            return
        try:
            with conn.cursor() as cursor:
                for slot_name in self.slot_names:
                    try:
                        cursor.drop_replication_slot(slot_name)
                    except psycopg2.Error:
                        continue
        finally:
            conn.close()

    def drop_table(self) -> None:
        """Drop the run-specific table if Postgres is still reachable."""
        try:
            conn = psycopg2.connect(self.postgres.conninfo)
        except psycopg2.Error:
            return
        try:
            conn.autocommit = True
            with conn.cursor() as cursor:
                cursor.execute(f'DROP TABLE IF EXISTS {self.table_name}')
        finally:
            conn.close()


def main() -> int:
    """Run the CLI."""
    args = parse_args()
    load_env_file(args.env_file)
    try:
        runner = E2ERunner(args)
        runner.run()
    except (E2EError, ValueError, psycopg2.Error) as exc:
        print(f'error: {exc}', file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
