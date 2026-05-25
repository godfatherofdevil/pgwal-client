# pylint:disable=C0103,C0116
"""RabbitPublisher integration tests."""
import json
import time

import pika


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

    assert _read_one(rabbitmq_connection, rabbit_publisher.queue_name) == message


def test_rabbit_publisher_serializes_dict_payload(
    rabbit_publisher, rabbitmq_connection
):
    message = {'kind': 'insert', 'payload': {'id': 1, 'name': 'demo'}}

    rabbit_publisher.publish_payload(message)

    assert _read_one(rabbitmq_connection, rabbit_publisher.queue_name) == json.dumps(
        message, ensure_ascii=False
    )


def test_rabbit_publisher_delivers_multiple_messages_in_order(
    rabbit_publisher, rabbitmq_connection
):
    messages = ['first', 'second', 'third']

    for message in messages:
        rabbit_publisher.publish_payload(message)

    assert (
        _read_many(rabbitmq_connection, rabbit_publisher.queue_name, len(messages))
        == messages
    )


def test_rabbit_publisher_stop_closes_connection(rabbit_publisher):
    rabbit_publisher.publish_payload('shutdown-check')

    rabbit_publisher.stop()

    assert rabbit_publisher._stopping is True
    assert not rabbit_publisher.is_running()
    if rabbit_publisher._connection is not None:
        deadline = time.time() + 10.0
        while time.time() < deadline and rabbit_publisher._connection.is_open:
            time.sleep(0.1)
        assert not rabbit_publisher._connection.is_open
