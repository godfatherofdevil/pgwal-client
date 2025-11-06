"""Shell Publisher"""
# pylint: disable=W0107
import logging
from psycopg2._psycopg import ReplicationMessage
from .base import BasePublisher


logger = logging.getLogger(__name__)


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
