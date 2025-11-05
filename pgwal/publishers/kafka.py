"""Kafka Publisher"""
import logging
import threading
import time
from queue import SimpleQueue
from typing import (
    TYPE_CHECKING,
    Optional,
)

from kafka import KafkaProducer
from .base import (
    BasePublisher,
    MsgQueueMixin,
    ensure_running,
)
from ..events import EXIT

if TYPE_CHECKING:
    from psycopg2._psycopg import ReplicationMessage

logger = logging.getLogger(__name__)


class KafkaPublisher(BasePublisher, MsgQueueMixin):
    """A publisher that sends the replication message to a Kafka topic"""

    _lock = threading.Lock()
    _MSG_QUEUE = SimpleQueue()
    _PUBLISH_INTERVAL = 0.5
    _NAME = 'publisher:KafkaPublisher'

    def __init__(self, destination: str, **config):
        """

        :param destination: destination topic name
        :param config: Kafka broker configuration dict
        """
        self.destination = destination
        self._producer = KafkaProducer(**config)

    @property
    def msg_queue(self) -> SimpleQueue:
        """return internal message queue to use"""
        return self._MSG_QUEUE

    def run(self):
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
                logger.info(
                    'Nothing to publish!!! '
                    'Waiting for % seconds before reading next message',
                    self._PUBLISH_INTERVAL,
                )
                time.sleep(self._PUBLISH_INTERVAL)
                continue
            self._producer.send(self.destination, message)

    def stop(self):
        """Stop this publisher"""
        self.set_running(False)
        self.flush()
        self._producer.close()

    def flush(self, timeout: Optional[float] = None):
        """makes all buffered records immediately available to send"""
        self._producer.flush(timeout)

    @ensure_running
    def publish(self, msg: 'ReplicationMessage'):
        """Publish a replication message"""
        self.msg_queue.put_nowait(msg.payload)
