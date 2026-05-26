"""Kafka Publisher"""
from __future__ import annotations

import json
import logging
import threading
import time
from functools import cached_property
from queue import SimpleQueue
from typing import TYPE_CHECKING

from kafka import KafkaProducer
from .base import (
    BasePublisher,
    PublisherMessage,
    MsgQueueMixin,
    QueueMessage,
    ensure_running,
)
from ..events import EXIT

if TYPE_CHECKING:
    from psycopg2.extras import ReplicationMessage

logger = logging.getLogger(__name__)


class KafkaPublisher(BasePublisher, MsgQueueMixin):
    """A publisher that sends the replication message to a Kafka topic"""

    _lock = threading.Lock()
    _MSG_QUEUE = SimpleQueue()
    _PUBLISH_INTERVAL = 1
    _NAME = 'publisher:KafkaPublisher'

    def __init__(self, destination: str, **config: object) -> None:
        """

        :param destination: destination topic name
        :param config: Kafka broker configuration dict
        """
        self.destination = destination
        self._kafka_config: dict[str, object] = config
        self._producer: KafkaProducer | None = None
        # TODO: keep separate counters for successful deliveries and future deliveries
        self._sent = 0

    @cached_property
    def producer(self) -> KafkaProducer:
        """Initialize an instance of kafka producer and return it"""
        self._producer = self._producer or KafkaProducer(**self._kafka_config)

        return self._producer

    @property
    def msg_queue(self) -> SimpleQueue[PublisherMessage]:
        """return internal message queue to use"""
        return self._MSG_QUEUE

    def publish_message(self, message: QueueMessage) -> None:
        """Publish a message to Kafka broker"""
        if isinstance(message, str):
            message = message.encode('utf8')
        self.producer.send(self.destination, message)
        self._sent += 1
        logger.info('TOTAL Published: %i', self._sent)

    def run(self) -> None:
        """Run this publisher"""
        self.set_running(True)
        while True:
            if not EXIT.is_set():
                logger.warning(
                    'Received EXIT event, breaking from Kafka publisher loop.'
                )
                break
            message = self._get_message()
            if message is None:
                logger.debug(
                    'Producer metric %s',
                    json.dumps(
                        self.producer.metrics(),
                        indent=4,
                    ),
                )
                logger.info(
                    'Nothing to publish!!! '
                    'Waiting for % seconds before reading next message',
                    self._PUBLISH_INTERVAL,
                )
                time.sleep(self._PUBLISH_INTERVAL)
                continue
            self.publish_message(message)

    def stop(self) -> None:
        """Stop this publisher"""
        self.set_running(False)
        self.flush()
        self.producer.close()

    def flush(self, timeout: float | None = None) -> None:
        """makes all buffered records immediately available to send"""
        self.producer.flush(timeout)

    @ensure_running
    def publish(self, msg: 'ReplicationMessage') -> None:
        """Publish a replication message"""
        self.msg_queue.put_nowait(msg.payload)
