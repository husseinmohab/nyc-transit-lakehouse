"""schema lookup and avro serialization

messages use avro single object encoding
the marker bytes 0xC3 0x01 then the 8 byte little endian CRC-64-AVRO fingerprint
of the schemas parsing canonical form then the schemaless body

not confluent schema registry
its framing needs a live registry to decode anything which means a fifth
container locally and a ci dependency on a running service
single object encoding gives the one property that matters here
a consumer can tell which schema wrote a message from the bytes alone
the fingerprint comes from the avsc files in this package so producer and
consumer agree by construction
tests/unit/test_schema_registry.py pins the framing against the avro spec null
test vector

moving to a real registry means replacing _frame and _unframe
"""

from __future__ import annotations

import io
import json
from functools import cache
from pathlib import Path
from typing import Any

import fastavro
from fastavro.schema import fingerprint, fullname, parse_schema, to_parsing_canonical_form

SCHEMA_DIR = Path(__file__).parent / "avro"

RECORD_TYPES: dict[str, str] = {
    "trip_update": "trip_update",
    "vehicle_position": "vehicle_position",
    "alert": "alert",
}

MAGIC = b"\xc3\x01"
FINGERPRINT_BYTES = 8
HEADER_BYTES = len(MAGIC) + FINGERPRINT_BYTES


class SchemaNotFoundError(KeyError):
    """no schema for that record_type"""


class SchemaMismatchError(ValueError):
    """message fingerprint did not match the local schema"""


@cache
def load_schema(record_type: str) -> dict[str, Any]:
    """load and parse the avro schema for a record type"""
    try:
        stem = RECORD_TYPES[record_type]
    except KeyError:
        raise SchemaNotFoundError(
            f"unknown record_type {record_type!r}; known: {sorted(RECORD_TYPES)}"
        ) from None
    raw = json.loads((SCHEMA_DIR / f"{stem}.avsc").read_text())
    return parse_schema(raw)


@cache
def raw_schema_json(record_type: str) -> str:
    """the avsc text as written on disk

    sparks from_avro wants plain avro json
    load_schema gives fastavros parsed form which carries bookkeeping keys spark
    rejects
    """
    try:
        stem = RECORD_TYPES[record_type]
    except KeyError:
        raise SchemaNotFoundError(record_type) from None
    return (SCHEMA_DIR / f"{stem}.avsc").read_text()


@cache
def schema_fingerprint(record_type: str) -> bytes:
    """the 8 little endian fingerprint bytes that go on the wire"""
    pcf = to_parsing_canonical_form(load_schema(record_type))
    return bytes.fromhex(fingerprint(pcf, "CRC-64-AVRO"))


def _frame(record_type: str, body: bytes) -> bytes:
    return MAGIC + schema_fingerprint(record_type) + body


def _unframe(payload: bytes, record_type: str) -> bytes:
    if len(payload) < HEADER_BYTES or payload[: len(MAGIC)] != MAGIC:
        raise SchemaMismatchError("payload is not Avro single-object encoded (bad marker bytes)")
    got = payload[len(MAGIC) : HEADER_BYTES]
    expected = schema_fingerprint(record_type)
    if got != expected:
        raise SchemaMismatchError(
            f"schema fingerprint mismatch for {record_type!r}: "
            f"message={got.hex()} local={expected.hex()}"
        )
    return payload[HEADER_BYTES:]


def serialize(record: dict[str, Any], *, record_type: str | None = None) -> bytes:
    """encode one record as single object encoded avro

    lets fastavro raise on a record that does not match its schema instead of
    coercing
    a type error at produce time is cheap and local
    a silently coerced null in bronze turns up three layers downstream
    """
    rt = record_type or record["record_type"]
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, load_schema(rt), record)
    return _frame(rt, buf.getvalue())


def deserialize(payload: bytes, *, record_type: str) -> dict[str, Any]:
    """single object encoded avro back to a dict

    the fingerprint is checked before decoding so a message written under an older
    schema fails loudly instead of being misparsed into plausible garbage
    """
    body = _unframe(payload, record_type)
    return fastavro.schemaless_reader(io.BytesIO(body), load_schema(record_type))


def serialize_json(record: dict[str, Any]) -> bytes:
    """json fallback for PRODUCER_SERIALIZATION=json

    handy for kcat debugging or a consumer that cannot hold the avsc files
    sort_keys keeps the bytes deterministic so a replay gives an identical payload
    """
    return json.dumps(record, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _field_map(schema: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {f["name"]: f for f in schema["fields"]}


class SchemaRegistry:
    """in process registry plus the compatibility rules"""

    def __init__(self) -> None:
        self._schemas = {rt: load_schema(rt) for rt in RECORD_TYPES}

    def get(self, record_type: str) -> dict[str, Any]:
        try:
            return self._schemas[record_type]
        except KeyError:
            raise SchemaNotFoundError(record_type) from None

    def fullnames(self) -> dict[str, str]:
        return {rt: fullname(s) for rt, s in self._schemas.items()}

    def fingerprints(self) -> dict[str, str]:
        return {rt: schema_fingerprint(rt).hex() for rt in self._schemas}

    @staticmethod
    def is_backward_compatible(
        writer: dict[str, Any], reader: dict[str, Any]
    ) -> tuple[bool, list[str]]:
        """can reader read data written by writer

        returns ok and reasons
        three rules and they are the ones that break a running consumer

        a field in both must not be retyped
        a field the reader adds must carry a default
        a field the writer has and the reader drops is fine since it is ignored on read
        """
        reasons: list[str] = []
        w, r = _field_map(writer), _field_map(reader)

        for name, reader_field in r.items():
            writer_field = w.get(name)
            if writer_field is None:
                if "default" not in reader_field:
                    reasons.append(f"reader adds required field {name!r} with no default")
                continue
            if json.dumps(writer_field["type"], sort_keys=True) != json.dumps(
                reader_field["type"], sort_keys=True
            ):
                reasons.append(
                    f"field {name!r} retyped: {writer_field['type']} -> {reader_field['type']}"
                )

        return (not reasons, reasons)
