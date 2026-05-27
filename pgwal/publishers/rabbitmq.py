"""RabbitMQ publisher."""
# pylint: disable=R0902,R0904
from __future__ import annotations

import functools
import logging
from queue import Queue
import threading
from typing import TYPE_CHECKING, Any

import pika
from pika.adapters.select_connection import IOLoop
from pika.exchange_type import ExchangeType

from .base import (
    BasePublisher,
    MsgQueueMixin,
    PublishResult,
    PublisherMessage,
    PublisherState,
    ensure_running,
)

if TYPE_CHECKING:
    from psycopg2.extras import ReplicationMessage

logger = logging.getLogger(__name__)


class RabbitPublisher(BasePublisher, MsgQueueMixin):
    """A publisher that sends replication messages to RabbitMQ."""

    _PUBLISH_INTERVAL = 0.5
    _NAME = 'publisher:RabbitPublisher'

    def __init__(
        self,
        amqp_url: str,
        exchange: str,
        queue: str,
        routing_key: str,
        exchange_type: ExchangeType = ExchangeType.topic,
        queue_size: int = 1000,
    ) -> None:
        super().__init__()
        self._connection: pika.SelectConnection | None = None
        self._channel: Any = None
        self._ioloop: IOLoop | None = None
        self._deliveries: dict[int, bool] = {}
        self._acked = 0
        self._nacked = 0
        self._message_number = 0
        self._stopping = False
        self._url = amqp_url
        self._exchange = exchange
        self._queue = queue
        self._routing_key = routing_key
        self._exchange_type = exchange_type
        self._ready = threading.Event()
        self.msg_headers: dict[str, object] = {}
        self._msg_queue: Queue[PublisherMessage] = Queue(maxsize=queue_size)

    @property
    def ready(self) -> threading.Event:
        """Event set after exchange, queue, and binding are ready."""
        return self._ready

    @property
    def msg_queue(self) -> Queue[PublisherMessage]:
        """Return internal queue."""
        return self._msg_queue

    def connect(self) -> pika.SelectConnection:
        """Connect to RabbitMQ."""
        logger.info('Connecting to %s', self._url)
        if self._ioloop is None:
            self._ioloop = IOLoop()
        return pika.SelectConnection(
            pika.URLParameters(self._url),
            on_open_callback=self.on_connection_open,
            on_open_error_callback=self.on_connection_open_error,
            on_close_callback=self.on_connection_closed,
            custom_ioloop=self._ioloop,
        )

    def on_connection_open(self, _unused_connection: object) -> None:
        """Handle open connection."""
        self.open_channel()

    def on_connection_open_error(
        self,
        _unused_connection: object,
        err: Exception,
    ) -> None:
        """Handle connection open failure."""
        self._metrics.reconnect_count += 1
        self.mark_error(err)
        if self._connection is not None:
            self._connection.ioloop.call_later(5, self._connection.ioloop.stop)

    def on_connection_closed(
        self,
        _unused_connection: object,
        reason: Exception,
    ) -> None:
        """Handle connection closed."""
        self._channel = None
        self._ready.clear()
        if self._stopping and self._connection is not None:
            self._connection.ioloop.stop()
            return
        self._metrics.reconnect_count += 1
        self.mark_error(reason)
        if self._connection is not None:
            self._connection.ioloop.call_later(5, self._connection.ioloop.stop)

    def open_channel(self) -> None:
        """Open RabbitMQ channel."""
        if self._connection is not None:
            self._connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel: Any) -> None:
        """Handle opened channel."""
        self._channel = channel
        self.add_on_channel_close_callback()
        self.setup_exchange(self._exchange)

    def add_on_channel_close_callback(self) -> None:
        """Watch for unexpected channel close."""
        self._channel.add_on_close_callback(self.on_channel_closed)

    def on_channel_closed(self, channel: Any, reason: Exception) -> None:
        """Handle closed channel."""
        logger.warning('Channel %s was closed: %s', channel, reason)
        self._channel = None
        self._ready.clear()
        if self._stopping:
            if self._connection is not None and self._connection.is_open:
                self.close_connection()
            elif self._connection is not None:
                self._connection.ioloop.stop()
            return
        self.mark_error(reason)
        if self._connection is not None and self._connection.is_open:
            self.close_connection()

    def setup_exchange(self, exchange_name: str) -> None:
        """Declare exchange."""
        callback = functools.partial(
            self.on_exchange_declareok,
            userdata=exchange_name,
        )
        self._channel.exchange_declare(
            exchange=exchange_name,
            exchange_type=self._exchange_type,
            callback=callback,
        )

    def on_exchange_declareok(self, _unused_frame: object, userdata: str) -> None:
        """Handle declared exchange."""
        logger.info('Exchange declared: %s', userdata)
        self.setup_queue(self._queue)

    def setup_queue(self, queue_name: str) -> None:
        """Declare queue."""
        self._channel.queue_declare(
            queue=queue_name,
            durable=True,
            callback=self.on_queue_declareok,
        )

    def on_queue_declareok(self, _unused_frame: object) -> None:
        """Handle declared queue."""
        self._channel.queue_bind(
            self._queue,
            self._exchange,
            routing_key=self._routing_key,
            callback=self.on_bindok,
        )

    def on_bindok(self, _unused_frame: object) -> None:
        """Handle completed queue bind."""
        self._ready.set()
        self.set_state(PublisherState.RUNNING)
        self.start_publishing()

    def start_publishing(self) -> None:
        """Enable confirms and schedule the publish loop."""
        self.enable_delivery_confirmations()
        self.schedule_next_message()

    def enable_delivery_confirmations(self) -> None:
        """Enable RabbitMQ confirms."""
        self._channel.confirm_delivery(self.on_delivery_confirmation)

    def on_delivery_confirmation(self, method_frame: Any) -> None:
        """Track confirms for health metrics."""
        confirmation_type = method_frame.method.NAME.split('.')[1].lower()
        ack_multiple = method_frame.method.multiple
        delivery_tag = method_frame.method.delivery_tag
        if confirmation_type == 'ack':
            self._acked += 1
            self.mark_success()
        else:
            self._nacked += 1
            self.mark_error('rabbit_delivery_nack')
        self._deliveries.pop(delivery_tag, None)
        if ack_multiple:
            for tmp_tag in list(self._deliveries.keys()):
                if tmp_tag <= delivery_tag:
                    self._deliveries.pop(tmp_tag, None)

    def schedule_next_message(self) -> None:
        """Schedule the next publish attempt."""
        if self._connection is not None and not self._stopping:
            self._connection.ioloop.call_later(
                self._PUBLISH_INTERVAL,
                self.publish_message,
            )

    def publish_message(self) -> None:
        """Drain one message from the queue and publish it."""
        if self._channel is None or not self._channel.is_open:
            self.schedule_next_message()
            return
        properties = pika.BasicProperties(
            app_id=self._NAME,
            content_type='application/json',
            headers=self.msg_headers,
        )
        message = self._get_message(timeout=0.0)
        if message:
            self._channel.basic_publish(
                self._exchange,
                self._routing_key,
                message,
                properties,
            )
            self._message_number += 1
            self._deliveries[self._message_number] = True
        self.schedule_next_message()

    def run(self) -> None:
        """Run the RabbitMQ worker."""
        self._ready.clear()
        self._stopping = False
        try:
            while not self._stop_event.is_set() and not self._stopping:
                self._connection = None
                self._deliveries = {}
                self._acked = 0
                self._nacked = 0
                self._message_number = 0
                self._ioloop = IOLoop()
                self._connection = self.connect()
                ioloop = self._ioloop
                try:
                    if ioloop is not None:
                        ioloop.start()
                finally:
                    if ioloop is not None:
                        ioloop.close()
                if self._stop_event.is_set() or self._stopping:
                    break
        finally:
            self._channel = None
            self._connection = None
            self._ioloop = None
            self._ready.clear()
            if self.state is not PublisherState.FAILED:
                self.set_state(PublisherState.STOPPED)
            self._stopped.set()

    def stop(self, drain: bool = False) -> None:
        """Stop the RabbitMQ worker."""
        self._stopping = True
        self._ready.clear()
        super().stop(drain=drain)
        self._request_shutdown()

    def close_channel(self) -> None:
        """Close channel if open."""
        if self._channel is not None and self._channel.is_open:
            self._channel.close()

    def close_connection(self) -> None:
        """Close connection if open."""
        if self._connection is not None and self._connection.is_open:
            self._connection.close()

    def _close_from_ioloop(self) -> None:
        """Close broker resources from the pika loop thread."""
        self._ready.clear()
        if self._channel is not None and self._channel.is_open:
            self.close_channel()
            return
        if self._connection is not None and self._connection.is_open:
            self.close_connection()
            return
        if self._connection is not None:
            self._connection.ioloop.stop()

    def _request_shutdown(self) -> None:
        """Request shutdown using the broker loop when available."""
        connection = self._connection
        if connection is None:
            return
        try:
            connection.ioloop.add_callback_threadsafe(self._close_from_ioloop)
        except Exception:  # pragma: no cover - defensive fallback
            self._close_from_ioloop()

    @ensure_running
    def publish(self, msg: 'ReplicationMessage') -> PublishResult:
        """Queue replication message for RabbitMQ delivery."""
        return self._queue_publish(msg.payload)
