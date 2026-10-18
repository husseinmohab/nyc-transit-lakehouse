"""avro schemas and the local schema resolver"""

from nyc_transit.schemas.registry import (
    HEADER_BYTES,
    MAGIC,
    RECORD_TYPES,
    SchemaMismatchError,
    SchemaNotFoundError,
    SchemaRegistry,
    deserialize,
    load_schema,
    raw_schema_json,
    schema_fingerprint,
    serialize,
    serialize_json,
)

__all__ = [
    "HEADER_BYTES",
    "MAGIC",
    "RECORD_TYPES",
    "SchemaMismatchError",
    "SchemaNotFoundError",
    "SchemaRegistry",
    "deserialize",
    "load_schema",
    "raw_schema_json",
    "schema_fingerprint",
    "serialize",
    "serialize_json",
]
