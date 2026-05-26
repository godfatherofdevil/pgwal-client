from .base import BasePublisher as BasePublisher
from _typeshed import Incomplete as Incomplete
from psycopg2.extras import ReplicationMessage

logger: Incomplete

class ShellPublisher(BasePublisher):
    def publish(self, msg: ReplicationMessage) -> None: ...
    def run(self) -> None: ...
    def stop(self) -> None: ...
