"""Shell publisher."""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from .base import BasePublisher, PublishResult, PublisherState

if TYPE_CHECKING:
    from psycopg2.extras import ReplicationMessage

logger = logging.getLogger(__name__)


class ShellPublisher(BasePublisher):
    """A publisher that logs the replication message to shell."""

    def __init__(self) -> None:
        super().__init__()
        self.set_state(PublisherState.RUNNING)

    @property
    def requires_worker(self) -> bool:
        """Shell publisher is synchronous."""
        return False

    def start(self) -> None:
        """No-op for synchronous publisher."""

    def publish(self, msg: 'ReplicationMessage') -> PublishResult:
        if self.state in {
            PublisherState.STOPPING,
            PublisherState.STOPPED,
            PublisherState.FAILED,
        }:
            return PublishResult(accepted=False, reason='publisher_unavailable')
        logger.info('payload %s, send_time %s', msg.payload, msg.send_time)
        self.mark_success()
        return PublishResult(accepted=True)

    def run(self) -> None:
        """No worker loop."""

    def stop(self, drain: bool = False) -> None:
        """Stop this publisher."""
        del drain
        self.set_state(PublisherState.STOPPED)
