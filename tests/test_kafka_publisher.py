# pylint:disable=C0103,C0116
"""KafkaPublisher integration tests."""
import json
import time

import pytest


@pytest.fixture(scope='session', autouse=True)
def db_conn():
    """Override the global Postgres fixture for broker-only Kafka tests."""
    yield None


def _read_many(consumer, expected: int, timeout: float = 10.0) -> list[str]:
    messages = []
    deadline = time.time() + timeout
    while len(messages) < expected and time.time() < deadline:
        batches = consumer.poll(timeout_ms=500, max_records=expected - len(messages))
        for records in batches.values():
            for record in records:
                messages.append(record.value)
                if len(messages) == expected:
                    return messages
    if len(messages) != expected:
        raise AssertionError(f'expected {expected} Kafka messages, got {len(messages)}')
    return messages


def test_kafka_publisher_delivers_string_payload(kafka_publisher, kafka_consumer):
    message = 'hello kafka'

    kafka_publisher.publish_payload(message)

    assert _read_many(kafka_consumer, 1) == [message]


def test_kafka_publisher_serializes_dict_payload(kafka_publisher, kafka_consumer):
    message = {'kind': 'insert', 'payload': {'id': 1, 'name': 'demo'}}

    kafka_publisher.publish_payload(message)

    assert _read_many(kafka_consumer, 1) == [json.dumps(message, ensure_ascii=False)]


def test_kafka_publisher_delivers_multiple_messages_in_order(
    kafka_publisher, kafka_consumer
):
    messages = ['first', 'second', 'third']

    for message in messages:
        kafka_publisher.publish_payload(message)

    assert _read_many(kafka_consumer, len(messages)) == messages


def test_kafka_publisher_stop_closes_producer(kafka_publisher):
    kafka_publisher.publish_payload('shutdown-check')

    kafka_publisher.stop()

    assert not kafka_publisher.is_running()
    assert kafka_publisher._producer is not None
    assert kafka_publisher._producer._closed is True
