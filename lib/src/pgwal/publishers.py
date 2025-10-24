"""Replication Message Publishers module"""
import abc
import json
import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from psycopg2.extras import ReplicationMessage
logger = logging.getLogger(__name__)


class BasePublisher(metaclass=abc.ABCMeta):
    """Base Publisher"""

    @abc.abstractmethod
    def publish(self, msg: 'ReplicationMessage'):
        """publish replication message to required destination"""
        raise NotImplementedError


class ShellPublisher(BasePublisher):
    """A Publisher that logs the replication message to shell"""

    def publish(self, msg: 'ReplicationMessage'):
        logger.info(
            'payload %s, send_time %s',
            json.dumps(msg.payload),
            msg.send_time,
        )


class RabbitPublisher(BasePublisher):
    """A Publisher that sends the replication message to RabbitMQ"""

    def publish(self, msg: 'ReplicationMessage'):
        ...
