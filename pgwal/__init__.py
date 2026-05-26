"""
Postgres WAL Python client package
"""
from __future__ import annotations

from .app import PGWAL
from .consumers import WALConsumer
from .interface import (
    WALReplicationValues,
    WALReplicationOpts,
)
from .publishers import (
    KafkaPublisher,
    RabbitPublisher,
    ShellPublisher,
)


def int_or_str(value: str) -> int | str:
    """int or string value"""
    try:
        return int(value)
    except ValueError:
        return value


__version__ = "0.0.1"

VERSION = tuple(map(int_or_str, __version__.split(".")))

__all__ = (
    'PGWAL',
    'WALConsumer',
    'WALReplicationValues',
    'WALReplicationOpts',
    'ShellPublisher',
    'RabbitPublisher',
    'KafkaPublisher',
)
