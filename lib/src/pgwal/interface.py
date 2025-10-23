"""Internal interface models module"""
# pylint: disable=C0103
from enum import StrEnum
from typing import Optional
from pydantic import (
    BaseModel,
    Field,
)


class DecodingParamsActions(StrEnum):
    """Possible values to wal2json decoding parameter actions"""

    create = 'create'
    update = 'update'
    delete = 'delete'
    truncate = 'truncate'
    all = 'create, update, delete, truncate'


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

    include_xids: Optional[DecodingParamsValues] = Field(
        DecodingParamsValues.false, alias='include-xids'
    )
    include_timestamp: Optional[DecodingParamsValues] = Field(
        DecodingParamsValues.false, alias='include-timestamp'
    )
    include_schemas: Optional[DecodingParamsValues] = Field(
        DecodingParamsValues.true, alias='include-schemas'
    )
    include_types: Optional[DecodingParamsValues] = Field(
        DecodingParamsValues.true, alias='include-types'
    )
    include_typmod: Optional[DecodingParamsValues] = Field(
        DecodingParamsValues.true, alias='include-typmod'
    )
    include_type_oids: Optional[DecodingParamsValues] = Field(
        DecodingParamsValues.false, alias='include-type-oids'
    )
    include_domain_data_type: Optional[DecodingParamsValues] = Field(
        DecodingParamsValues.false, alias='include-domain-data-type'
    )
    include_column_positions: Optional[DecodingParamsValues] = Field(
        DecodingParamsValues.false, alias='include-column-positions'
    )
    include_origin: Optional[DecodingParamsValues] = Field(
        DecodingParamsValues.false, alias='include-origin'
    )
    include_not_null: Optional[DecodingParamsValues] = Field(
        DecodingParamsValues.false, alias='include-not-null'
    )
    include_default: Optional[DecodingParamsValues] = Field(
        DecodingParamsValues.false, alias='include-default'
    )
    include_pk: Optional[DecodingParamsValues] = Field(
        DecodingParamsValues.false, alias='include-pk'
    )
    numeric_data_types_as_string: Optional[DecodingParamsValues] = Field(
        DecodingParamsValues.false, alias='numeric-data-types-as-string'
    )
    pretty_print: Optional[DecodingParamsValues] = Field(
        DecodingParamsValues.false, alias='pretty-print'
    )
    write_in_chunks: Optional[DecodingParamsValues] = Field(
        DecodingParamsValues.false, alias='write-in-chunks'
    )
    include_lsn: Optional[DecodingParamsValues] = Field(
        DecodingParamsValues.false, alias='include-lsn'
    )
    include_transaction: Optional[DecodingParamsValues] = Field(
        DecodingParamsValues.true, alias='include-transaction'
    )
    # `include-unchanged-toast` (deprecated): Don't use it. It is deprecated.
    include_unchanged_toast: Optional[DecodingParamsValues] = Field(
        DecodingParamsValues.false, alias='include-unchanged-toast'
    )
    filter_origins: Optional[DecodingParamsValues] = Field(
        DecodingParamsValues.empty, alias='filter-origins'
    )
    filter_tables: Optional[DecodingParamsValues] = Field(
        DecodingParamsValues.empty, alias='filter-tables'
    )
    add_tables: Optional[DecodingParamsValues] = Field(
        DecodingParamsValues.empty, alias='add-tables'
    )
    filter_msg_prefixes: Optional[DecodingParamsValues] = Field(
        DecodingParamsValues.empty, alias='filter-msg-prefixes'
    )
    add_msg_prefixes: Optional[DecodingParamsValues] = Field(
        DecodingParamsValues.empty, alias='add-msg-prefixes'
    )
    format_version: Optional[DecodingParamsValues] = Field(
        DecodingParamsValues.true, alias='format-version'
    )
    actions: Optional[str] = Field(DecodingParamsActions.all)
