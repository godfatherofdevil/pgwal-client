"""
Postgres WAL Python client package
"""
from __future__ import annotations

from .app import AppState, ConsumerHandle, PGWAL
from .consumers import ConsumerState, WALConsumer
from .interface import (
    WALReplicationValues,
    WALReplicationOpts,
)
from .publishers import (
    KafkaPublisher,
    RabbitPublisher,
    ShellPublisher,
)
from .publishers.base import PublisherState


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
    'ConsumerHandle',
    'AppState',
    'WALConsumer',
    'ConsumerState',
    'WALReplicationValues',
    'WALReplicationOpts',
    'ShellPublisher',
    'RabbitPublisher',
    'KafkaPublisher',
    'PublisherState',
)
