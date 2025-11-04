"""Replication Message Publishers module"""
# pylint: disable=W0107,R0902,C0103,W0105,R0904
import abc
import functools
import json
import logging
from queue import SimpleQueue, Empty
import threading
from typing import TYPE_CHECKING, Optional

import pika
from pika.exchange_type import ExchangeType

from pgwal.events import EXIT

if TYPE_CHECKING:
    from psycopg2.extras import ReplicationMessage
logger = logging.getLogger(__name__)


def run_publisher_daemon(publisher: 'BasePublisher'):
    """Run publisher in a demon thread."""
    task = threading.Thread(target=publisher.run)
    task.daemon = True
    task.start()


def ensure_running(func):
    """Ensure that a publisher is running"""

    @functools.wraps(func)
    def inner(publisher: 'BasePublisher', *args, **kwargs):
        if publisher.is_running():
            return func(publisher, *args, **kwargs)
        run_publisher_daemon(publisher)
        return func(publisher, *args, **kwargs)

    return inner


class BasePublisher(metaclass=abc.ABCMeta):
    """Base Publisher"""

    _lock = None
    _running = False

    @abc.abstractmethod
    def publish(self, msg: 'ReplicationMessage'):
        """publish replication message to required destination"""
        raise NotImplementedError

    @abc.abstractmethod
    def run(self):
        """run the publisher"""

    @abc.abstractmethod
    def stop(self):
        """stop the publisher"""

    def is_running(self) -> bool:
        """check if the publisher is running"""
        return self._running

    def set_running(self, value: bool):
        """Set the publisher to running status"""
        if self._lock is not None:
            with self._lock:
                self._running = value


class ShellPublisher(BasePublisher):
    """A Publisher that logs the replication message to shell"""

    # this is always running
    _running = True

    def publish(self, msg: 'ReplicationMessage'):
        logger.info(
            'payload %s, send_time %s',
            msg.payload,
            msg.send_time,
        )

    def run(self):
        """Run ShellPublisher"""
        pass

    def stop(self):
        """Stop ShellPublisher"""
        pass


class RabbitPublisher(BasePublisher):
    """A Publisher that sends the replication message to RabbitMQ"""

    _lock = threading.Lock()
    _MSG_QUEUE = SimpleQueue()
    _PUBLISH_INTERVAL = 0.5
    _NAME = 'publisher:RabbitPublisher'

    def __init__(
        self,
        amqp_url,
        exchange: str,
        queue: str,
        routing_key: str,
        exchange_type: ExchangeType = ExchangeType.topic,
    ):
        """
        Set up the RabbitPublisher object with required connection url, exchange, queue and routing_key
        :param str amqp_url: The URL for connecting to RabbitMQ
        :param exchange: exchange to which we should bind
        :param queue: queue to which we should bind
        :param routing_key: routing key for the message
        :param exchange_type
        """
        self._connection = None
        self._channel = None

        self._deliveries = None
        self._acked = None
        self._nacked = None
        self._message_number = None

        self._stopping = False
        self._url = amqp_url
        self._exchange = exchange
        self._queue = queue
        self._routing_key = routing_key
        self._exchange_type = exchange_type
        self.msg_headers = {}

    def connect(self):
        """This method connects to RabbitMQ, returning the connection handle.
        When the connection is established, the on_connection_open method
        will be invoked by pika.

        :rtype: pika.SelectConnection

        """
        logger.info('Connecting to %s', self._url)
        return pika.SelectConnection(
            pika.URLParameters(self._url),
            on_open_callback=self.on_connection_open,
            on_open_error_callback=self.on_connection_open_error,
            on_close_callback=self.on_connection_closed,
        )

    def on_connection_open(self, _unused_connection):
        """This method is called by pika once the connection to RabbitMQ has
        been established. It passes the handle to the connection object in
        case we need it, but in this case, we'll just mark it unused.

        :param pika.SelectConnection _unused_connection: The connection

        """
        logger.info('Connection opened')
        self.open_channel()

    def on_connection_open_error(self, _unused_connection, err):
        """This method is called by pika if the connection to RabbitMQ
        can't be established.

        :param pika.SelectConnection _unused_connection: The connection
        :param Exception err: The error

        """
        logger.error('Connection open failed, reopening in 5 seconds: %s', err)
        self._connection.ioloop.call_later(5, self._connection.ioloop.stop)

    def on_connection_closed(self, _unused_connection, reason):
        """This method is invoked by pika when the connection to RabbitMQ is
        closed unexpectedly. Since it is unexpected, we will reconnect to
        RabbitMQ if it disconnects.

        :param pika.connection.Connection connection: The closed connection obj
        :param Exception reason: exception representing reason for loss of
            connection.

        """
        self._channel = None
        if self._stopping:
            self._connection.ioloop.stop()
        else:
            logger.warning('Connection closed, reopening in 5 seconds: %s', reason)
            self._connection.ioloop.call_later(5, self._connection.ioloop.stop)

    def open_channel(self):
        """This method will open a new channel with RabbitMQ by issuing the
        Channel.Open RPC command. When RabbitMQ confirms the channel is open
        by sending the Channel.OpenOK RPC reply, the on_channel_open method
        will be invoked.

        """
        logger.info('Creating a new channel')
        self._connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        """This method is invoked by pika when the channel has been opened.
        The channel object is passed in so we can make use of it.

        Since the channel is now open, we'll declare the exchange to use.

        :param pika.channel.Channel channel: The channel object

        """
        logger.info('Channel opened')
        self._channel = channel
        self.add_on_channel_close_callback()
        self.setup_exchange(self._exchange)

    def add_on_channel_close_callback(self):
        """This method tells pika to call the on_channel_closed method if
        RabbitMQ unexpectedly closes the channel.

        """
        logger.info('Adding channel close callback')
        self._channel.add_on_close_callback(self.on_channel_closed)

    def on_channel_closed(self, channel, reason):
        """Invoked by pika when RabbitMQ unexpectedly closes the channel.
        Channels are usually closed if you attempt to do something that
        violates the protocol, such as re-declare an exchange or queue with
        different parameters. In this case, we'll close the connection
        to shutdown the object.

        :param pika.channel.Channel channel: The closed channel
        :param Exception reason: why the channel was closed

        """
        logger.warning('Channel %i was closed: %s', channel, reason)
        self._channel = None
        if not self._stopping or not EXIT.is_set():
            self.close_connection()

    def setup_exchange(self, exchange_name):
        """Setup the exchange on RabbitMQ by invoking the Exchange.Declare RPC
        command. When it is complete, the on_exchange_declareok method will
        be invoked by pika.

        :param str|unicode exchange_name: The name of the exchange to declare

        """
        logger.info('Declaring exchange %s', exchange_name)
        # Note: using functools.partial is not required, it is demonstrating
        # how arbitrary data can be passed to the callback when it is called
        cb = functools.partial(self.on_exchange_declareok, userdata=exchange_name)
        self._channel.exchange_declare(
            exchange=exchange_name, exchange_type=self._exchange_type, callback=cb
        )

    def on_exchange_declareok(self, _unused_frame, userdata):
        """Invoked by pika when RabbitMQ has finished the Exchange.Declare RPC
        command.

        :param pika.Frame.Method unused_frame: Exchange.DeclareOk response frame
        :param str|unicode userdata: Extra user data (exchange name)

        """
        logger.info('Exchange declared: %s', userdata)
        self.setup_queue(self._queue)

    def setup_queue(self, queue_name):
        """Setup the queue on RabbitMQ by invoking the Queue.Declare RPC
        command. When it is complete, the on_queue_declareok method will
        be invoked by pika.

        :param str|unicode queue_name: The name of the queue to declare.

        """
        logger.info('Declaring queue %s', queue_name)
        self._channel.queue_declare(queue=queue_name, callback=self.on_queue_declareok)

    def on_queue_declareok(self, _unused_frame):
        """Method invoked by pika when the Queue.Declare RPC call made in
        setup_queue has completed. In this method we will bind the queue
        and exchange together with the routing key by issuing the Queue.Bind
        RPC command. When this command is complete, the on_bindok method will
        be invoked by pika.

        :param pika.frame.Method method_frame: The Queue.DeclareOk frame

        """
        logger.info(
            'Binding %s to %s with %s', self._exchange, self._queue, self._routing_key
        )
        self._channel.queue_bind(
            self._queue,
            self._exchange,
            routing_key=self._routing_key,
            callback=self.on_bindok,
        )

    def on_bindok(self, _unused_frame):
        """This method is invoked by pika when it receives the Queue.BindOk
        response from RabbitMQ. Since we know we're now setup and bound, it's
        time to start publishing."""
        logger.info('Queue bound')
        self.start_publishing()

    def start_publishing(self):
        """This method will enable delivery confirmations and schedule the
        first message to be sent to RabbitMQ

        """
        logger.info('Issuing consumer related RPC commands')
        self.enable_delivery_confirmations()
        self.schedule_next_message()

    def enable_delivery_confirmations(self):
        """Send the Confirm.Select RPC method to RabbitMQ to enable delivery
        confirmations on the channel. The only way to turn this off is to close
        the channel and create a new one.

        When the message is confirmed from RabbitMQ, the
        on_delivery_confirmation method will be invoked passing in a Basic.Ack
        or Basic.Nack method from RabbitMQ that will indicate which messages it
        is confirming or rejecting.

        """
        logger.info('Issuing Confirm.Select RPC command')
        self._channel.confirm_delivery(self.on_delivery_confirmation)

    def on_delivery_confirmation(self, method_frame):
        """Invoked by pika when RabbitMQ responds to a Basic.Publish RPC
        command, passing in either a Basic.Ack or Basic.Nack frame with
        the delivery tag of the message that was published. The delivery tag
        is an integer counter indicating the message number that was sent
        on the channel via Basic.Publish. Here we're just doing house keeping
        to keep track of stats and remove message numbers that we expect
        a delivery confirmation of from the list used to keep track of messages
        that are pending confirmation.

        :param pika.frame.Method method_frame: Basic.Ack or Basic.Nack frame

        """
        confirmation_type = method_frame.method.NAME.split('.')[1].lower()
        ack_multiple = method_frame.method.multiple
        delivery_tag = method_frame.method.delivery_tag

        logger.info(
            'Received %s for delivery tag: %i (multiple: %s)',
            confirmation_type,
            delivery_tag,
            ack_multiple,
        )

        if confirmation_type == 'ack':
            self._acked += 1
        elif confirmation_type == 'nack':
            self._nacked += 1

        del self._deliveries[delivery_tag]

        if ack_multiple:
            for tmp_tag in list(self._deliveries.keys()):
                if tmp_tag <= delivery_tag:
                    self._acked += 1
                    del self._deliveries[tmp_tag]
        """
        NOTE: at some point you would check self._deliveries for stale
        entries and decide to attempt re-delivery
        """

        logger.info(
            'Published %i messages, %i have yet to be confirmed, '
            '%i were acked and %i were nacked',
            self._message_number,
            len(self._deliveries),
            self._acked,
            self._nacked,
        )

    def schedule_next_message(self):
        """If we are not closing our connection to RabbitMQ, schedule another
        message to be delivered in PUBLISH_INTERVAL seconds.

        """
        logger.info('Scheduling next message for %0.1f seconds', self._PUBLISH_INTERVAL)
        self._connection.ioloop.call_later(self._PUBLISH_INTERVAL, self.publish_message)

    def _get_message(self) -> Optional[str | bytes]:
        """Get message if any from internal msg queue"""
        try:
            message = self._MSG_QUEUE.get_nowait()
        except Empty:
            return None
        if not isinstance(message, (bytes, str)):
            message = json.dumps(message, ensure_ascii=False)
        return message

    def publish_message(self):
        """
        read a message from internal SimpleQueue and if there is message then deliver it to RabbitMQ.
        If there is no message in the queue, look for a message again in _PUBLISH_INTERVAL seconds.
        This ensures that we are always looking for a message in the internal queue as long as the connection is open
        """
        if self._channel is None or not self._channel.is_open:
            return

        properties = pika.BasicProperties(
            app_id=self._NAME, content_type='application/json', headers=self.msg_headers
        )
        message = self._get_message()
        if message:
            self._channel.basic_publish(
                self._exchange,
                self._routing_key,
                message,
                properties,
            )
            self._message_number += 1
            self._deliveries[self._message_number] = True
            logger.info('Published message # %i', self._message_number)
        else:
            logger.info('Nothing to publish, scheduling next message')
        self.schedule_next_message()

    def run(self):
        """Run the publisher by connecting and then starting the IOLoop."""
        self.set_running(True)
        while not self._stopping:
            # This is just an insurance to break from the ioloop when we need to exit
            if not EXIT.is_set():
                logger.warning(
                    'Received EXIT event, breaking from the RabbitMQ connection loop.'
                )
                break
            self._connection = None
            self._deliveries = {}
            self._acked = 0
            self._nacked = 0
            self._message_number = 0
            self._connection = self.connect()
            self._connection.ioloop.start()

        logger.info('Stopped')

    def stop(self):
        """Stop the publisher by closing the channel and connection. We
        set a flag here so that we stop scheduling new messages to be
        published. The IOLoop is started because this method is
        invoked by the Try/Catch below when KeyboardInterrupt is caught.
        Starting the IOLoop again will allow the publisher to cleanly
        disconnect from RabbitMQ.

        """
        logger.info('Stopping')
        self.set_running(False)
        self._stopping = True
        self.close_channel()
        self.close_connection()

    def close_channel(self):
        """Invoke this command to close the channel with RabbitMQ by sending
        the Channel.Close RPC command.

        """
        if self._channel is not None and self._channel.is_open:
            if self._channel.is_closing:
                logger.warning('Channel is already closing.')
            else:
                logger.info('Closing the channel')
                self._channel.close()

    def close_connection(self):
        """This method closes the connection to RabbitMQ."""
        if self._connection is not None and self._connection.is_open:
            if self._connection.is_closing:
                logger.warning('Connection is already closing.')
            else:
                logger.info('Closing connection')
                self._connection.close()

    @ensure_running
    def publish(self, msg: 'ReplicationMessage'):
        """Publish replication message"""
        self._MSG_QUEUE.put_nowait(msg.payload)
