"""Unit tests for the redesigned thread model."""
# pylint:disable=C0103,C0115,C0116
from __future__ import annotations

from queue import Queue
import threading

import pytest

from pgwal.app import AppState, PGWAL
from pgwal.consumers import ConsumerState, WALConsumer
from pgwal.interface import WALReplicationOpts
from pgwal.publishers.base import (
    BasePublisher,
    MsgQueueMixin,
    PublishResult,
    PublisherMessage,
    PublisherState,
)
from pgwal.publishers.kafka import KafkaPublisher
from pgwal.publishers.rabbitmq import RabbitPublisher


@pytest.fixture(scope='session', autouse=True)
def db_conn():
    """Override the integration database fixture for unit tests."""
    yield None


class DummyPublisher(BasePublisher, MsgQueueMixin):
    def __init__(self, queue_size: int = 1):
        super().__init__()
        self._msg_queue: Queue[PublisherMessage] = Queue(maxsize=queue_size)

    @property
    def msg_queue(self) -> Queue[PublisherMessage]:
        return self._msg_queue

    def publish(self, msg) -> PublishResult:
        return self._queue_publish(msg.payload)

    def run(self) -> None:
        self.set_state(PublisherState.RUNNING)
        self._stopped.set()


def test_threaded_publishers_use_instance_queues():
    kafka_a = KafkaPublisher('topic-a')
    kafka_b = KafkaPublisher('topic-b')
    rabbit_a = RabbitPublisher(
        'amqp://tests:secret@localhost:5672/%2F',
        exchange='a',
        queue='a',
        routing_key='a',
    )
    rabbit_b = RabbitPublisher(
        'amqp://tests:secret@localhost:5672/%2F',
        exchange='b',
        queue='b',
        routing_key='b',
    )

    assert kafka_a.msg_queue is not kafka_b.msg_queue
    assert rabbit_a.msg_queue is not rabbit_b.msg_queue
    assert kafka_a.state is PublisherState.IDLE
    assert rabbit_a.state is PublisherState.IDLE


def test_queue_saturation_drops_oldest():
    publisher = DummyPublisher(queue_size=1)

    first = publisher.publish(type('Msg', (), {'payload': 'first'})())
    second = publisher.publish(type('Msg', (), {'payload': 'second'})())

    assert first.accepted is True
    assert second.accepted is True
    assert second.dropped is True
    assert publisher.metrics.dropped_count == 1
    assert publisher.state is PublisherState.DEGRADED
    assert publisher.msg_queue.get_nowait() == 'second'


def test_pgwal_instances_have_independent_stop_events():
    app_a = PGWAL({'dsn': 'a'})
    app_b = PGWAL({'dsn': 'b'})

    app_a.stop()

    assert app_a.stop_event.is_set() is True
    assert app_b.stop_event.is_set() is False
    assert app_a.state is AppState.STOPPING
    assert app_b.state is AppState.IDLE


def test_consumer_stop_only_affects_consumer_instance():
    consumer_a = WALConsumer('slot-a', WALReplicationOpts())
    consumer_b = WALConsumer('slot-b', WALReplicationOpts())

    consumer_a.stop()

    assert consumer_a.stop_event.is_set() is True
    assert consumer_b.stop_event.is_set() is False
    assert consumer_a.state is ConsumerState.STOPPING
    assert consumer_b.state is ConsumerState.IDLE


def test_consumer_handle_join_reports_thread_completion():
    app = PGWAL({'host': 'localhost'})
    consumer = WALConsumer('slot-a', WALReplicationOpts())
    handle = app.consume(consumer)

    handle.thread = threading.Thread(target=lambda: None)
    handle.thread.start()

    assert handle.join(timeout=1.0) is True
