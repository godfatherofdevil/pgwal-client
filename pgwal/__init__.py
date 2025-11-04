"""
Postgres WAL Python client package
"""
from .consumers import WALConsumer
from .interface import (
    WALReplicationValues,
    WALReplicationOpts,
)
from .publishers import (
    ShellPublisher,
    RabbitPublisher,
)


def int_or_str(value):
    """int or string value"""
    try:
        return int(value)
    except ValueError:
        return value


__version__ = "0.0.1"

VERSION = tuple(map(int_or_str, __version__.split(".")))

__all__ = (
    'WALConsumer',
    'WALReplicationValues',
    'WALReplicationOpts',
    'ShellPublisher',
    'RabbitPublisher',
)
