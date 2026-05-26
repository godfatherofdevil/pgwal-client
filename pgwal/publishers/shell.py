"""Shell Publisher"""
# pylint: disable=W0107
from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from .base import BasePublisher

if TYPE_CHECKING:
    from psycopg2.extras import ReplicationMessage


logger = logging.getLogger(__name__)


class ShellPublisher(BasePublisher):
    """A Publisher that logs the replication message to shell"""

    # this is always running
    _running = True

    def publish(self, msg: 'ReplicationMessage') -> None:
        logger.info(
            'payload %s, send_time %s',
            msg.payload,
            msg.send_time,
        )

    def run(self) -> None:
        """Run ShellPublisher"""
        pass

    def stop(self) -> None:
        """Stop ShellPublisher"""
        pass
