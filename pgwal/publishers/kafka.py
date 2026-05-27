"""Kafka publisher."""
from __future__ import annotations

import logging
from queue import Queue
from typing import TYPE_CHECKING

from kafka import KafkaProducer

from .base import (
    BasePublisher,
    MsgQueueMixin,
    PublishResult,
    PublisherMessage,
    PublisherState,
    QueueMessage,
    ensure_running,
)

if TYPE_CHECKING:
    from psycopg2.extras import ReplicationMessage

logger = logging.getLogger(__name__)


class KafkaPublisher(BasePublisher, MsgQueueMixin):
    """A publisher that sends replication messages to Kafka."""

    _PUBLISH_INTERVAL = 1.0

    def __init__(
        self,
        destination: str,
        queue_size: int = 1000,
        **config: object,
    ) -> None:
        super().__init__()
        self.destination = destination
        self._kafka_config: dict[str, object] = config
        self._producer: KafkaProducer | None = None
        self._msg_queue: Queue[PublisherMessage] = Queue(maxsize=queue_size)
        self._sent = 0

    @property
    def producer(self) -> KafkaProducer:
        """Initialize Kafka producer lazily."""
        if self._producer is None:
            self._producer = KafkaProducer(**self._kafka_config)
        return self._producer

    @property
    def msg_queue(self) -> Queue[PublisherMessage]:
        """Return internal queue."""
        return self._msg_queue

    def publish_message(self, message: QueueMessage) -> None:
        """Publish one message to Kafka."""
        if isinstance(message, str):
            message = message.encode('utf8')
        self.producer.send(self.destination, message)
        self._sent += 1
        self.mark_success()

    def run(self) -> None:
        """Drain the instance queue until stopped."""
        self.set_state(PublisherState.RUNNING)
        try:
            while not self._stop_event.is_set():
                message = self._get_message(timeout=self._PUBLISH_INTERVAL)
                if message is None:
                    continue
                try:
                    self.publish_message(message)
                except Exception as exc:  # pragma: no cover - broker failure path
                    self.mark_error(exc)
                    logger.exception('Kafka publish failed')
        finally:
            try:
                if self._producer is not None:
                    self._producer.flush()
                    self._producer.close()
            except Exception as exc:  # pragma: no cover - close failure path
                self.mark_error(exc, state=PublisherState.FAILED)
            if self.state is not PublisherState.FAILED:
                self.set_state(PublisherState.STOPPED)
            self._stopped.set()

    def stop(self, drain: bool = False) -> None:
        """Stop this publisher."""
        super().stop(drain=drain)
        self.wait_stopped(timeout=10.0)
        if self._producer is not None and not self._producer._closed:
            self._producer.close()
        self.set_state(PublisherState.STOPPED)

    def flush(self, timeout: float | None = None) -> None:
        """Flush Kafka producer buffers."""
        self.producer.flush(timeout)

    @ensure_running
    def publish(self, msg: 'ReplicationMessage') -> PublishResult:
        """Queue a replication message for Kafka delivery."""
        return self._queue_publish(msg.payload)
