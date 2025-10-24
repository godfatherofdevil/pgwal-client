"""Internal interface models module"""
# pylint: disable=C0103
from enum import StrEnum
from pydantic import BaseModel, Field


class DecodingParamsActions(StrEnum):
    """Possible values to wal2json decoding parameter actions"""

    insert = 'insert'
    update = 'update'
    delete = 'delete'
    truncate = 'truncate'
    all = 'insert, update, delete, truncate'


class DecodingParamsValues(StrEnum):
    """
    Possible values to wal2json decoding parameters,
    see Wal2JsonDecodingParams for complete list
    """

    false = '0'
    true = '1'
    empty = ''


class Wal2JsonDecodingParams(BaseModel):
    """
    Supported decoding parameters for wal2json decoding plugin.
    https://github.com/eulerto/wal2json/tree/wal2json_2_6?tab=readme-ov-file#parameters
    """

    include_xids: DecodingParamsValues | None = Field(
        DecodingParamsValues.false,
        serialization_alias='include-xids',
    )
    include_timestamp: DecodingParamsValues | None = Field(
        DecodingParamsValues.false,
        serialization_alias='include-timestamp',
    )
    include_schemas: DecodingParamsValues | None = Field(
        DecodingParamsValues.true,
        serialization_alias='include-schemas',
    )
    include_types: DecodingParamsValues | None = Field(
        DecodingParamsValues.true,
        serialization_alias='include-types',
    )
    include_typmod: DecodingParamsValues | None = Field(
        DecodingParamsValues.true,
        serialization_alias='include-typmod',
    )
    include_type_oids: DecodingParamsValues | None = Field(
        DecodingParamsValues.false,
        serialization_alias='include-type-oids',
    )
    include_domain_data_type: DecodingParamsValues | None = Field(
        DecodingParamsValues.false,
        serialization_alias='include-domain-data-type',
    )
    include_column_positions: DecodingParamsValues | None = Field(
        DecodingParamsValues.false,
        serialization_alias='include-column-positions',
    )
    include_origin: DecodingParamsValues | None = Field(
        DecodingParamsValues.false,
        serialization_alias='include-origin',
    )
    include_not_null: DecodingParamsValues | None = Field(
        DecodingParamsValues.false,
        serialization_alias='include-not-null',
    )
    include_default: DecodingParamsValues | None = Field(
        DecodingParamsValues.false,
        serialization_alias='include-default',
    )
    include_pk: DecodingParamsValues | None = Field(
        DecodingParamsValues.false,
        serialization_alias='include-pk',
    )
    numeric_data_types_as_string: DecodingParamsValues | None = Field(
        DecodingParamsValues.false,
        serialization_alias='numeric-data-types-as-string',
    )
    pretty_print: DecodingParamsValues | None = Field(
        DecodingParamsValues.false,
        serialization_alias='pretty-print',
    )
    write_in_chunks: DecodingParamsValues | None = Field(
        DecodingParamsValues.false,
        serialization_alias='write-in-chunks',
    )
    include_lsn: DecodingParamsValues | None = Field(
        DecodingParamsValues.false,
        serialization_alias='include-lsn',
    )
    include_transaction: DecodingParamsValues | None = Field(
        DecodingParamsValues.true,
        serialization_alias='include-transaction',
    )
    filter_origins: DecodingParamsValues | None = Field(
        DecodingParamsValues.empty,
        serialization_alias='filter-origins',
    )
    filter_tables: DecodingParamsValues | None = Field(
        DecodingParamsValues.empty,
        serialization_alias='filter-tables',
    )
    add_tables: DecodingParamsValues | None = Field(
        DecodingParamsValues.empty,
        serialization_alias='add-tables',
    )
    filter_msg_prefixes: DecodingParamsValues | None = Field(
        DecodingParamsValues.empty,
        serialization_alias='filter-msg-prefixes',
    )
    add_msg_prefixes: DecodingParamsValues | None = Field(
        DecodingParamsValues.empty,
        serialization_alias='add-msg-prefixes',
    )
    format_version: DecodingParamsValues | None = Field(
        DecodingParamsValues.true,
        serialization_alias='format-version',
    )
    actions: str | None = Field(DecodingParamsActions.all)
