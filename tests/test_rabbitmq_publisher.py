# pylint:disable=C0103,C0116
"""RabbitPublisher integration tests."""
import json
import time

import pika
import pytest
from pgwal.events import EXIT
from pgwal.publishers.rabbitmq import RabbitPublisher


@pytest.fixture(scope='session', autouse=True)
def db_conn():
    """Override the global Postgres fixture for broker-only Rabbit tests."""
    yield None


def _read_one(connection, queue_name: str, timeout: float = 10.0) -> str:
    deadline = time.time() + timeout
    while time.time() < deadline:
        channel = connection.channel()
        try:
            method_frame, _properties, body = channel.basic_get(
                queue=queue_name, auto_ack=True
            )
            if method_frame is not None:
                return body.decode('utf8')
        except pika.exceptions.ChannelClosedByBroker:
            pass
        finally:
            if channel.is_open:
                channel.close()
        time.sleep(0.1)
    raise AssertionError(
        f'timed out waiting for RabbitMQ message in queue {queue_name}'
    )


def _read_many(
    connection, queue_name: str, expected: int, timeout: float = 10.0
) -> list[str]:
    messages = []
    deadline = time.time() + timeout
    while len(messages) < expected and time.time() < deadline:
        channel = connection.channel()
        try:
            method_frame, _properties, body = channel.basic_get(
                queue=queue_name, auto_ack=True
            )
            if method_frame is not None:
                messages.append(body.decode('utf8'))
                continue
        except pika.exceptions.ChannelClosedByBroker:
            pass
        finally:
            if channel.is_open:
                channel.close()
        time.sleep(0.1)
    if len(messages) != expected:
        raise AssertionError(
            f'expected {expected} RabbitMQ messages in queue {queue_name}, got {len(messages)}'
        )
    return messages


def test_rabbit_publisher_delivers_string_payload(
    rabbit_publisher, rabbitmq_connection
):
    message = 'hello rabbitmq'

    rabbit_publisher.publish_payload(message)
    rabbit_publisher.wait_until_ready()

    assert _read_one(rabbitmq_connection, rabbit_publisher.queue_name) == message


def test_rabbit_publisher_serializes_dict_payload(
    rabbit_publisher, rabbitmq_connection
):
    message = {'kind': 'insert', 'payload': {'id': 1, 'name': 'demo'}}

    rabbit_publisher.publish_payload(message)
    rabbit_publisher.wait_until_ready()

    assert _read_one(rabbitmq_connection, rabbit_publisher.queue_name) == json.dumps(
        message, ensure_ascii=False
    )


def test_rabbit_publisher_delivers_multiple_messages_in_order(
    rabbit_publisher, rabbitmq_connection
):
    messages = ['first', 'second', 'third']

    for message in messages:
        rabbit_publisher.publish_payload(message)
    rabbit_publisher.wait_until_ready()

    assert (
        _read_many(rabbitmq_connection, rabbit_publisher.queue_name, len(messages))
        == messages
    )


def test_rabbit_publisher_stop_closes_connection(rabbit_publisher):
    rabbit_publisher.publish_payload('shutdown-check')
    rabbit_publisher.wait_until_ready()

    rabbit_publisher.stop()
    assert rabbit_publisher.wait_stopped(timeout=10.0)

    assert rabbit_publisher._stopping is True
    assert not rabbit_publisher.is_running()
    assert rabbit_publisher._thread is None
    if rabbit_publisher._connection is not None:
        assert not rabbit_publisher._connection.is_open


def test_rabbit_publisher_run_closes_ioloop(monkeypatch):
    class FakeIOLoop:
        def __init__(self):
            self.started = False
            self.closed = False

        def start(self):
            self.started = True
            publisher._stopping = True

        def close(self):
            self.closed = True

        def add_callback_threadsafe(self, callback):
            callback()

    class FakeConnection:
        def __init__(self, ioloop):
            self.ioloop = ioloop
            self.is_open = False
            self.is_closing = False

        def close(self):
            self.is_open = False

    loops = []

    def _fake_ioloop():
        loop = FakeIOLoop()
        loops.append(loop)
        return loop

    publisher = RabbitPublisher(
        'amqp://tests:secret@localhost:5672/%2F',
        exchange='pgwal.exchange.test',
        queue='pgwal.queue.test',
        routing_key='pgwal.route.test',
    )

    monkeypatch.setattr('pgwal.publishers.rabbitmq.IOLoop', _fake_ioloop)

    def _fake_connect():
        return FakeConnection(publisher._ioloop)

    monkeypatch.setattr(publisher, 'connect', _fake_connect)

    previous_exit_state = EXIT.is_set()
    EXIT.set()
    try:
        publisher.run()
    finally:
        if previous_exit_state:
            EXIT.set()
        else:
            EXIT.clear()

    assert loops
    assert loops[0].started is True
    assert loops[0].closed is True
