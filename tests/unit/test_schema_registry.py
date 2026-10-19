"""avro schema wire format and compatibility tests"""

from __future__ import annotations

import json
import struct

import pytest
from fastavro.schema import fingerprint, to_parsing_canonical_form

from nyc_transit.schemas import (
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

# An independent CRC-64-AVRO so the framing is checked against the Avro spec
# not against the library that produced it
_EMPTY = 0xC15D213AA4D7A795
_MASK = (1 << 64) - 1


def _crc64_table() -> list[int]:
    table = []
    for i in range(256):
        value = i
        for _ in range(8):
            value = (value >> 1) ^ (_EMPTY & -(value & 1) & _MASK)
        table.append(value & _MASK)
    return table


_TABLE = _crc64_table()


def crc64_avro(data: bytes) -> int:
    value = _EMPTY
    for byte in data:
        value = ((value >> 8) ^ _TABLE[(value ^ byte) & 0xFF]) & _MASK
    return value


class TestWireFormat:
    def test_crc64_matches_the_avro_spec_test_vector(self):
        """The spec states fingerprint("null") == 0x63DD24E7CC258F8A

        Checking our implementation against the published vector first then
        checking fastavro against ours means the framing is verified against
        the spec rather than against fastavro's interpretation of it.
        """
        assert crc64_avro(b'"null"') == 0x63DD24E7CC258F8A

    def test_fingerprint_bytes_are_spec_little_endian(self):
        for record_type in RECORD_TYPES:
            pcf = to_parsing_canonical_form(load_schema(record_type))
            expected = struct.pack("<Q", crc64_avro(pcf.encode()))
            assert schema_fingerprint(record_type) == expected

    def test_fastavro_agrees_with_our_crc64(self):
        for record_type in RECORD_TYPES:
            pcf = to_parsing_canonical_form(load_schema(record_type))
            assert bytes.fromhex(fingerprint(pcf, "CRC-64-AVRO")) == struct.pack(
                "<Q", crc64_avro(pcf.encode())
            )

    def test_header_is_marker_plus_eight_fingerprint_bytes(self, fixture_records):
        payload = serialize(fixture_records[0])
        assert payload[:2] == MAGIC == b"\xc3\x01"
        assert HEADER_BYTES == 10
        assert payload[2:10] == schema_fingerprint(fixture_records[0]["record_type"])

    def test_each_record_type_has_a_distinct_fingerprint(self):
        fingerprints = {rt: schema_fingerprint(rt) for rt in RECORD_TYPES}
        assert len(set(fingerprints.values())) == len(RECORD_TYPES)


class TestRoundTrip:
    def test_every_fixture_record_round_trips_exactly(self, fixture_records):
        for record in fixture_records:
            decoded = deserialize(serialize(record), record_type=record["record_type"])
            assert decoded == record, f"{record['record_type']} did not round-trip"

    def test_all_three_record_types_are_covered(self, fixture_records):
        assert {r["record_type"] for r in fixture_records} == set(RECORD_TYPES)

    def test_nested_alert_arrays_survive(self, fixture_records):
        alert = next(r for r in fixture_records if r["record_type"] == "alert")
        decoded = deserialize(serialize(alert), record_type="alert")
        assert decoded["informed_entities"] == alert["informed_entities"]
        assert decoded["active_periods"] == alert["active_periods"]

    def test_json_serialization_is_deterministic(self, fixture_records):
        """A replay must produce identical bytes hence sort_keys"""
        record = fixture_records[0]
        assert serialize_json(record) == serialize_json(dict(reversed(list(record.items()))))

    def test_json_payload_is_parseable(self, fixture_records):
        assert json.loads(serialize_json(fixture_records[0]))["event_id"]


class TestFailureModes:
    def test_unknown_record_type_raises(self):
        with pytest.raises(SchemaNotFoundError):
            load_schema("not_a_record_type")

    def test_wrong_marker_bytes_raise(self):
        with pytest.raises(SchemaMismatchError, match="marker bytes"):
            deserialize(b"\x00\x01" + b"\x00" * 20, record_type="trip_update")

    def test_payload_shorter_than_the_header_raises(self):
        with pytest.raises(SchemaMismatchError):
            deserialize(MAGIC + b"\x00" * 3, record_type="trip_update")

    def test_fingerprint_mismatch_raises_rather_than_misparsing(self, fixture_records):
        """Decoding a vehicle_position as a trip_update must fail loudly

        Avro is a positional binary format so a mismatched schema does not
        error on its own it produces plausible-looking garbage The fingerprint
        check is what turns that into an exception
        """
        vehicle = next(r for r in fixture_records if r["record_type"] == "vehicle_position")
        with pytest.raises(SchemaMismatchError, match="fingerprint mismatch"):
            deserialize(serialize(vehicle), record_type="trip_update")

    def test_record_missing_a_required_field_fails_at_serialize_time(self, fixture_records):
        broken = dict(fixture_records[0])
        del broken["service_date"]  # non-nullable string
        with pytest.raises(Exception):  # noqa: B017 - fastavro's own error type
            serialize(broken, record_type="trip_update")

    def test_wrong_type_is_not_silently_coerced(self, fixture_records):
        broken = dict(fixture_records[0])
        broken["feed_timestamp_ms"] = "not-a-long"
        with pytest.raises(Exception):  # noqa: B017
            serialize(broken, record_type="trip_update")


class TestSchemaFiles:
    def test_every_record_type_has_an_avsc(self):
        for record_type in RECORD_TYPES:
            assert raw_schema_json(record_type).strip().startswith("{")

    def test_raw_json_has_no_fastavro_bookkeeping_keys(self):
        """Spark's from_avro rejects fastavro's parsed form"""
        for record_type in RECORD_TYPES:
            parsed = json.loads(raw_schema_json(record_type))
            assert "__fastavro_parsed" not in parsed
            assert "__named_schemas" not in parsed

    def test_every_field_is_documented_or_obviously_named(self):
        """The envelope and the non-obvious fields carry a doc string"""
        needs_doc = {
            "event_id",
            "service_date",
            "feed_timestamp_ms",
            "ingest_timestamp_ms",
            "payload_fingerprint",
        }
        for record_type in RECORD_TYPES:
            parsed = json.loads(raw_schema_json(record_type))
            assert parsed.get("doc"), f"{record_type} schema has no top-level doc"
            documented = {f["name"] for f in parsed["fields"] if f.get("doc")}
            present = {f["name"] for f in parsed["fields"]}
            assert (needs_doc & present) <= documented

    def test_namespaces_are_versioned(self):
        for record_type in RECORD_TYPES:
            assert json.loads(raw_schema_json(record_type))["namespace"].endswith(".v1")


class TestCompatibility:
    @staticmethod
    def _base() -> dict:
        return {
            "type": "record",
            "name": "R",
            "fields": [
                {"name": "a", "type": "string"},
                {"name": "b", "type": ["null", "long"], "default": None},
            ],
        }

    def test_identical_schemas_are_compatible(self):
        ok, reasons = SchemaRegistry.is_backward_compatible(self._base(), self._base())
        assert ok and reasons == []

    def test_adding_a_field_with_a_default_is_compatible(self):
        reader = self._base()
        reader["fields"].append({"name": "c", "type": ["null", "string"], "default": None})
        ok, _ = SchemaRegistry.is_backward_compatible(self._base(), reader)
        assert ok

    def test_adding_a_required_field_is_incompatible(self):
        reader = self._base()
        reader["fields"].append({"name": "c", "type": "string"})
        ok, reasons = SchemaRegistry.is_backward_compatible(self._base(), reader)
        assert not ok
        assert "no default" in reasons[0]

    def test_retyping_a_field_is_incompatible(self):
        reader = self._base()
        reader["fields"][0] = {"name": "a", "type": "long"}
        ok, reasons = SchemaRegistry.is_backward_compatible(self._base(), reader)
        assert not ok
        assert "retyped" in reasons[0]

    def test_dropping_a_field_is_compatible(self):
        """The reader ignores it"""
        reader = self._base()
        reader["fields"] = reader["fields"][:1]
        ok, _ = SchemaRegistry.is_backward_compatible(self._base(), reader)
        assert ok

    def test_live_schemas_are_self_compatible(self):
        registry = SchemaRegistry()
        for record_type in RECORD_TYPES:
            schema = json.loads(raw_schema_json(record_type))
            ok, reasons = registry.is_backward_compatible(schema, schema)
            assert ok, reasons

    def test_registry_reports_fullnames_and_fingerprints(self):
        registry = SchemaRegistry()
        assert set(registry.fullnames()) == set(RECORD_TYPES)
        assert all(len(f) == 16 for f in registry.fingerprints().values())
