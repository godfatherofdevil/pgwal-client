"""Internal interface models module"""
# pylint: disable=C0103
from enum import StrEnum
from typing import List, Union

from pydantic import (
    BaseModel,
    Field,
    field_validator,
)
from .errors import InvalidReplicationAction


class WALReplicationValues(StrEnum):
    """
    Possible replication option values,
    see WALReplicationOpts for complete list
    """

    false = '0'
    true = '1'
    empty = ''
    # actions
    insert = 'insert'
    update = 'update'
    delete = 'delete'
    truncate = 'truncate'


_WALReplicationActions = [
    WALReplicationValues.insert,
    WALReplicationValues.update,
    WALReplicationValues.delete,
    WALReplicationValues.truncate,
]


class WALReplicationOpts(BaseModel):
    """
    Supported replication options, see full list below
    https://github.com/eulerto/wal2json/tree/wal2json_2_6?tab=readme-ov-file#parameters
    """

    include_xids: WALReplicationValues | None = Field(
        WALReplicationValues.false,
        serialization_alias='include-xids',
    )
    include_timestamp: WALReplicationValues | None = Field(
        WALReplicationValues.false,
        serialization_alias='include-timestamp',
    )
    include_schemas: WALReplicationValues | None = Field(
        WALReplicationValues.true,
        serialization_alias='include-schemas',
    )
    include_types: WALReplicationValues | None = Field(
        WALReplicationValues.true,
        serialization_alias='include-types',
    )
    include_typmod: WALReplicationValues | None = Field(
        WALReplicationValues.true,
        serialization_alias='include-typmod',
    )
    include_type_oids: WALReplicationValues | None = Field(
        WALReplicationValues.false,
        serialization_alias='include-type-oids',
    )
    include_domain_data_type: WALReplicationValues | None = Field(
        WALReplicationValues.false,
        serialization_alias='include-domain-data-type',
    )
    include_column_positions: WALReplicationValues | None = Field(
        WALReplicationValues.false,
        serialization_alias='include-column-positions',
    )
    include_origin: WALReplicationValues | None = Field(
        WALReplicationValues.false,
        serialization_alias='include-origin',
    )
    include_not_null: WALReplicationValues | None = Field(
        WALReplicationValues.false,
        serialization_alias='include-not-null',
    )
    include_default: WALReplicationValues | None = Field(
        WALReplicationValues.false,
        serialization_alias='include-default',
    )
    include_pk: WALReplicationValues | None = Field(
        WALReplicationValues.false,
        serialization_alias='include-pk',
    )
    numeric_data_types_as_string: WALReplicationValues | None = Field(
        WALReplicationValues.false,
        serialization_alias='numeric-data-types-as-string',
    )
    pretty_print: WALReplicationValues | None = Field(
        WALReplicationValues.false,
        serialization_alias='pretty-print',
    )
    write_in_chunks: WALReplicationValues | None = Field(
        WALReplicationValues.false,
        serialization_alias='write-in-chunks',
    )
    include_lsn: WALReplicationValues | None = Field(
        WALReplicationValues.false,
        serialization_alias='include-lsn',
    )
    include_transaction: WALReplicationValues | None = Field(
        WALReplicationValues.true,
        serialization_alias='include-transaction',
    )
    filter_origins: WALReplicationValues | None = Field(
        WALReplicationValues.empty,
        serialization_alias='filter-origins',
    )
    filter_tables: WALReplicationValues | None = Field(
        WALReplicationValues.empty,
        serialization_alias='filter-tables',
    )
    add_tables: WALReplicationValues | None = Field(
        WALReplicationValues.empty,
        serialization_alias='add-tables',
    )
    filter_msg_prefixes: WALReplicationValues | None = Field(
        WALReplicationValues.empty,
        serialization_alias='filter-msg-prefixes',
    )
    add_msg_prefixes: WALReplicationValues | None = Field(
        WALReplicationValues.empty,
        serialization_alias='add-msg-prefixes',
    )
    format_version: WALReplicationValues | None = Field(
        WALReplicationValues.true,
        serialization_alias='format-version',
    )
    actions: List[WALReplicationValues] | str = Field(_WALReplicationActions)

    @field_validator('actions')
    @classmethod
    def validate_actions(cls, value: Union[List[WALReplicationValues], str]) -> str:
        """Validate and build the string format for the actions"""
        if isinstance(value, str):
            value = [WALReplicationValues(v.strip()) for v in value.strip().split(',')]
        if not all((action in _WALReplicationActions for action in value)):
            raise InvalidReplicationAction(
                f'Possible replication actions {_WALReplicationActions}, got {value}'
            )
        return ', '.join([action.value for action in value])
