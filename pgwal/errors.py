"""Error Module"""
from pydantic import ValidationError


class InvalidReplicationAction(ValidationError):
    """Raised when invalid replication option is supplied"""
