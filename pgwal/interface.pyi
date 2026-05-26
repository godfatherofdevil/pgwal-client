from enum import Enum
from pydantic import BaseModel

class WALReplicationValues(str, Enum):
    zero = '0'
    one = '1'
    two = '2'
    nil = ''
    insert = 'insert'
    update = 'update'
    delete = 'delete'
    truncate = 'truncate'

class WALReplicationOpts(BaseModel):
    include_xids: WALReplicationValues | None
    include_timestamp: WALReplicationValues | None
    include_schemas: WALReplicationValues | None
    include_types: WALReplicationValues | None
    include_typmod: WALReplicationValues | None
    include_type_oids: WALReplicationValues | None
    include_domain_data_type: WALReplicationValues | None
    include_column_positions: WALReplicationValues | None
    include_origin: WALReplicationValues | None
    include_not_null: WALReplicationValues | None
    include_default: WALReplicationValues | None
    include_pk: WALReplicationValues | None
    numeric_data_types_as_string: WALReplicationValues | None
    pretty_print: WALReplicationValues | None
    write_in_chunks: WALReplicationValues | None
    include_lsn: WALReplicationValues | None
    include_transaction: WALReplicationValues | None
    filter_origins: list[str] | str
    filter_tables: list[str] | str
    add_tables: list[str] | str
    filter_msg_prefixes: list[str] | str
    add_msg_prefixes: list[str] | str
    format_version: WALReplicationValues | None
    actions: list[WALReplicationValues] | str
    @classmethod
    def validate_actions(cls, value: list[WALReplicationValues] | str) -> str: ...
    @classmethod
    def validate_format_version(cls, value: str) -> str: ...
    @classmethod
    def validate_list_str(cls, value: str | list[str]) -> str: ...
