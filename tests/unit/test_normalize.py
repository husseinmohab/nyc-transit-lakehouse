"""normalizer tests

the decode to canonical record contract
"""

from __future__ import annotations

import datetime as dt

import pytest
from google.transit import gtfs_realtime_pb2

from nyc_transit.gtfs.decoder import FeedDecodeError, decode_feed
from nyc_transit.gtfs.normalize import (
    AGENCY_TZ,
    SCHEMA_VERSION,
    make_event_id,
    normalize_feed_message,
    parse_gtfs_start_date,
    partition_key,
    service_date_from_epoch,
)

ENVELOPE_FIELDS = {
    "event_id",
    "record_type",
    "feed_id",
    "feed_timestamp_ms",
    "ingest_timestamp_ms",
    "producer_id",
    "schema_version",
}


class TestServiceDate:
    def test_uses_agency_local_calendar_not_utc(self):
        """01:30 ET on the 2nd is still the 1st's service day.

        The epoch below is 2026-11-02T05:30Z which is 01:30 ET A naive UTC
        date would say 2026-11-02 and split one night across two partitions
        """
        epoch = int(dt.datetime(2026, 11, 2, 5, 30, tzinfo=dt.UTC).timestamp())
        assert service_date_from_epoch(epoch) == "2026-11-01"

    def test_after_cutover_is_the_new_service_day(self):
        epoch = int(dt.datetime(2026, 11, 2, 12, 0, tzinfo=dt.UTC).timestamp())
        assert service_date_from_epoch(epoch) == "2026-11-02"

    def test_dst_transition_is_handled_by_the_tz_database(self):
        """2026-11-01 is the US DST end date so the offset changes mid-day

        Both instants are the same service date which only holds if the
        conversion goes through zoneinfo rather than a fixed offset
        """
        before = int(dt.datetime(2026, 11, 1, 5, 0, tzinfo=dt.UTC).timestamp())  # 01:00 EDT
        after = int(dt.datetime(2026, 11, 1, 7, 0, tzinfo=dt.UTC).timestamp())  # 02:00 EST
        assert service_date_from_epoch(before) == service_date_from_epoch(after)

    def test_cutover_hour_is_configurable(self):
        epoch = int(dt.datetime(2026, 11, 2, 5, 30, tzinfo=dt.UTC).timestamp())
        assert service_date_from_epoch(epoch, cutover_hour=0) == "2026-11-02"

    def test_agency_timezone_is_new_york(self):
        assert str(AGENCY_TZ) == "America/New_York"

    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ("20261101", "2026-11-01"),
            ("20260229", None),  # 2026 is not a leap year
            ("2026-11-01", None),  # already formatted, so the wrong length
            ("", None),
            ("abcdefgh", None),
            ("20261301", None),  # month 13
        ],
    )
    def test_parse_gtfs_start_date(self, raw, expected):
        assert parse_gtfs_start_date(raw) == expected


class TestEventId:
    def test_is_deterministic_across_calls(self):
        assert make_event_id("a", "b", 1) == make_event_id("a", "b", 1)

    def test_distinguishes_part_boundaries(self):
        """("A1","B") and ("A","1B") must not collide

        They would under a naive ''.join which is why the parts are joined on
        the unit separator
        """
        assert make_event_id("A1", "B") != make_event_id("A", "1B")

    def test_none_and_empty_string_differ_from_a_literal_marker(self):
        assert make_event_id(None, "x") == make_event_id("", "x")
        assert make_event_id(None, "x") != make_event_id("None", "x")

    def test_is_128_bit_hex(self):
        value = make_event_id("x")
        assert len(value) == 32
        int(value, 16)  # raises if not hex


class TestNormalizeFixture:
    def test_record_counts_match_the_fixture(self, fixture_records):
        by_type: dict[str, int] = {}
        for record in fixture_records:
            by_type[record["record_type"]] = by_type.get(record["record_type"], 0) + 1
        # 3 trips with 3-5 stops each 2 vehicles 1 alert
        assert by_type["vehicle_position"] == 2
        assert by_type["alert"] == 1
        assert 9 <= by_type["trip_update"] <= 15

    def test_every_record_carries_the_full_envelope(self, fixture_records):
        for record in fixture_records:
            assert record.keys() >= ENVELOPE_FIELDS
            assert record["schema_version"] == SCHEMA_VERSION
            assert record["feed_id"] == "nyct-ace"
            assert record["producer_id"] == "pytest"

    def test_event_ids_are_unique_within_a_feed_snapshot(self, fixture_records):
        ids = [r["event_id"] for r in fixture_records]
        assert len(ids) == len(set(ids))

    def test_normalization_is_pure(self, fixture_payload):
        """Same bytes and same ingest timestamp give identical records

        The whole idempotency argument rests on this if a re-poll of an
        unchanged feed produced different records the MERGE would insert
        duplicates instead of collapsing them
        """
        message_a = decode_feed(fixture_payload, feed_id="f")
        message_b = decode_feed(fixture_payload, feed_id="f")
        a = normalize_feed_message(message_a, feed_id="f", ingest_timestamp_ms=1, producer_id="p")
        b = normalize_feed_message(message_b, feed_id="f", ingest_timestamp_ms=1, producer_id="p")
        assert a == b

    def test_feed_id_is_part_of_the_identity(self, fixture_payload):
        """The same payload from two feeds must not collide on event_id"""
        message = decode_feed(fixture_payload, feed_id="x")
        a = normalize_feed_message(message, feed_id="feed-a", ingest_timestamp_ms=1)
        b = normalize_feed_message(message, feed_id="feed-b", ingest_timestamp_ms=1)
        assert {r["event_id"] for r in a}.isdisjoint({r["event_id"] for r in b})

    def test_trip_updates_carry_real_nyct_stop_ids(self, fixture_records):
        from nyc_transit.gtfs.static_reference import STOPS_BY_ID, parent_station

        stops = [r["stop_id"] for r in fixture_records if r["record_type"] == "trip_update"]
        assert stops, "fixture has no stop-level predictions"
        assert all(parent_station(s) in STOPS_BY_ID for s in stops)

    def test_stop_sequences_are_one_based_and_contiguous(self, fixture_records):
        by_trip: dict[str, list[int]] = {}
        for record in fixture_records:
            if record["record_type"] != "trip_update" or record["stop_sequence"] is None:
                continue
            by_trip.setdefault(record["trip_id"], []).append(record["stop_sequence"])
        assert by_trip
        for sequences in by_trip.values():
            assert sorted(sequences) == list(range(1, len(sequences) + 1))

    def test_absent_fields_become_none_not_zero(self, fixture_records):
        """The fixture publishes no arrival delay so it must be None

        A normalizer defaulting absent numerics to 0 would make every
        prediction look perfectly on time
        """
        trip_updates = [r for r in fixture_records if r["record_type"] == "trip_update"]
        assert all(r["arrival_delay_seconds"] is None for r in trip_updates)


class TestTripUpdateEdgeCases:
    @staticmethod
    def _message(*, timestamp: int | None = 1_793_536_000) -> gtfs_realtime_pb2.FeedMessage:
        message = gtfs_realtime_pb2.FeedMessage()
        message.header.gtfs_realtime_version = "2.0"
        if timestamp is not None:
            message.header.timestamp = timestamp
        return message

    def test_cancellation_yields_one_row_with_a_null_stop(self):
        """A cancelled trip carries no stop_time_updates but stays visible"""
        message = self._message()
        entity = message.entity.add()
        entity.id = "cancelled-1"
        tu = entity.trip_update
        tu.trip.trip_id = "123456_L..N01R"
        tu.trip.route_id = "L"
        tu.trip.start_date = "20261101"
        tu.trip.schedule_relationship = (
            gtfs_realtime_pb2.TripDescriptor.ScheduleRelationship.CANCELED
        )

        records = normalize_feed_message(message, feed_id="f", ingest_timestamp_ms=1)
        assert len(records) == 1
        assert records[0]["stop_id"] is None
        assert records[0]["stop_sequence"] is None
        assert records[0]["schedule_relationship"] == "CANCELED"
        assert records[0]["service_date"] == "2026-11-01"

    def test_missing_header_timestamp_falls_back_to_ingest_time(self):
        """And the fallback is detectable feed_timestamp == ingest_timestamp"""
        message = self._message(timestamp=None)
        entity = message.entity.add()
        entity.id = "no-ts"
        entity.trip_update.trip.trip_id = "t"
        entity.trip_update.trip.route_id = "L"

        records = normalize_feed_message(message, feed_id="f", ingest_timestamp_ms=777_000)
        assert records[0]["feed_timestamp_ms"] == 777_000
        assert records[0]["ingest_timestamp_ms"] == 777_000

    def test_absent_start_date_is_derived_but_start_date_column_stays_null(self):
        """service_date is derived start_date records only what the feed said"""
        message = self._message(timestamp=1_793_536_000)
        entity = message.entity.add()
        entity.id = "no-start-date"
        entity.trip_update.trip.trip_id = "t"
        entity.trip_update.trip.route_id = "L"

        records = normalize_feed_message(message, feed_id="f", ingest_timestamp_ms=1)
        assert records[0]["start_date"] is None
        assert records[0]["service_date"] == service_date_from_epoch(1_793_536_000)

    def test_zero_delay_is_preserved(self):
        """delay=0 means the feed said 0 and must survive normalization"""
        message = self._message()
        entity = message.entity.add()
        entity.id = "zero-delay"
        tu = entity.trip_update
        tu.trip.trip_id = "t"
        tu.trip.route_id = "L"
        stu = tu.stop_time_update.add()
        stu.stop_id = "L06N"
        stu.stop_sequence = 1
        stu.arrival.delay = 0
        stu.arrival.time = 1_793_536_500

        records = normalize_feed_message(message, feed_id="f", ingest_timestamp_ms=1)
        assert records[0]["arrival_delay_seconds"] == 0

    def test_skipped_stop_relationship_is_preserved(self):
        message = self._message()
        entity = message.entity.add()
        entity.id = "skip"
        tu = entity.trip_update
        tu.trip.trip_id = "t"
        tu.trip.route_id = "L"
        stu = tu.stop_time_update.add()
        stu.stop_id = "L06N"
        stu.stop_sequence = 3
        stu.schedule_relationship = (
            gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.ScheduleRelationship.SKIPPED
        )

        records = normalize_feed_message(message, feed_id="f", ingest_timestamp_ms=1)
        assert records[0]["stop_schedule_relationship"] == "SKIPPED"

    def test_entity_with_no_modelled_payload_is_dropped_without_raising(self):
        """An entity carrying none of trip_update vehicle or alert

        The spec allows entity kinds this pipeline does not model (shapes, trip
        modifications) They must be skipped rather than crash the poll which
        would take down ingestion for every other feed in the tick
        """
        message = self._message()
        entity = message.entity.add()
        entity.id = "unmodelled"

        assert normalize_feed_message(message, feed_id="f", ingest_timestamp_ms=1) == []


class TestVehiclePosition:
    def test_identity_uses_the_vehicle_clock_not_the_feed_clock(self):
        """An unchanged position republished twice collapses to one row

        The vehicle's own timestamp is the version. Using the feed timestamp
        would accumulate one row per poll for a stationary train
        """
        records = []
        for feed_ts in (1_793_536_000, 1_793_536_030):
            message = gtfs_realtime_pb2.FeedMessage()
            message.header.gtfs_realtime_version = "2.0"
            message.header.timestamp = feed_ts
            entity = message.entity.add()
            entity.id = "vp-1"
            vp = entity.vehicle
            vp.vehicle.id = "veh-L-001"
            vp.trip.trip_id = "t1"
            vp.trip.route_id = "L"
            vp.timestamp = 1_793_535_990  # unchanged across both polls
            vp.position.latitude = 40.7
            vp.position.longitude = -73.9
            records.extend(normalize_feed_message(message, feed_id="f", ingest_timestamp_ms=1))

        assert len({r["event_id"] for r in records}) == 1

    def test_coordinates_are_rounded_to_six_decimals(self):
        message = gtfs_realtime_pb2.FeedMessage()
        message.header.gtfs_realtime_version = "2.0"
        message.header.timestamp = 1_793_536_000
        entity = message.entity.add()
        entity.id = "vp-1"
        vp = entity.vehicle
        vp.vehicle.id = "v"
        vp.timestamp = 1_793_536_000
        vp.position.latitude = 40.712345678
        vp.position.longitude = -74.006789012

        record = normalize_feed_message(message, feed_id="f", ingest_timestamp_ms=1)[0]
        # About 11 cm More digits are protobuf float noise not signal
        assert record["latitude"] == pytest.approx(40.712346, abs=1e-6)

    def test_missing_position_yields_null_coordinates(self):
        message = gtfs_realtime_pb2.FeedMessage()
        message.header.gtfs_realtime_version = "2.0"
        message.header.timestamp = 1_793_536_000
        entity = message.entity.add()
        entity.id = "vp-1"
        entity.vehicle.vehicle.id = "v"
        entity.vehicle.timestamp = 1_793_536_000

        record = normalize_feed_message(message, feed_id="f", ingest_timestamp_ms=1)[0]
        assert record["latitude"] is None
        assert record["longitude"] is None


class TestAlert:
    def test_english_translation_is_preferred(self):
        message = gtfs_realtime_pb2.FeedMessage()
        message.header.gtfs_realtime_version = "2.0"
        message.header.timestamp = 1_793_536_000
        entity = message.entity.add()
        entity.id = "alert-1"
        for language, text in (("es", "Retrasos"), ("en", "Delays")):
            translation = entity.alert.header_text.translation.add()
            translation.language = language
            translation.text = text

        record = normalize_feed_message(message, feed_id="f", ingest_timestamp_ms=1)[0]
        assert record["header_text"] == "Delays"

    def test_informed_entities_stay_nested(self, fixture_records):
        alert = next(r for r in fixture_records if r["record_type"] == "alert")
        assert isinstance(alert["informed_entities"], list)
        assert alert["informed_entities"][0]["route_id"]

    def test_edited_text_produces_a_new_event_id(self):
        """The MTA edits alerts in place so an edit must be a new row"""
        ids = []
        for text in ("Delays on the L", "Severe delays on the L"):
            message = gtfs_realtime_pb2.FeedMessage()
            message.header.gtfs_realtime_version = "2.0"
            message.header.timestamp = 1_793_536_000
            entity = message.entity.add()
            entity.id = "alert-stable-id"
            translation = entity.alert.header_text.translation.add()
            translation.language = "en"
            translation.text = text
            ids.append(
                normalize_feed_message(message, feed_id="f", ingest_timestamp_ms=1)[0]["event_id"]
            )

        assert ids[0] != ids[1]

    def test_identical_republication_collapses(self):
        ids = set()
        for _ in range(2):
            message = gtfs_realtime_pb2.FeedMessage()
            message.header.gtfs_realtime_version = "2.0"
            message.header.timestamp = 1_793_536_000
            entity = message.entity.add()
            entity.id = "alert-stable-id"
            translation = entity.alert.header_text.translation.add()
            translation.language = "en"
            translation.text = "Delays"
            ids.add(
                normalize_feed_message(message, feed_id="f", ingest_timestamp_ms=1)[0]["event_id"]
            )

        assert len(ids) == 1


class TestPartitionKey:
    def test_trip_updates_key_on_trip_so_order_is_preserved(self, fixture_records):
        trip = next(r for r in fixture_records if r["record_type"] == "trip_update")
        assert partition_key(trip) == f"{trip['service_date']}|{trip['trip_id']}"

    def test_all_stops_of_one_trip_share_a_key(self, fixture_records):
        """Otherwise two predictions for one stop could commit out of order"""
        by_trip: dict[str, set[str]] = {}
        for record in fixture_records:
            if record["record_type"] == "trip_update":
                by_trip.setdefault(record["trip_id"], set()).add(partition_key(record))
        assert all(len(keys) == 1 for keys in by_trip.values())

    def test_vehicles_key_on_vehicle(self, fixture_records):
        vehicle = next(r for r in fixture_records if r["record_type"] == "vehicle_position")
        assert partition_key(vehicle).endswith(vehicle["vehicle_id"])


class TestDecoder:
    def test_empty_body_is_an_error_not_an_empty_feed(self):
        with pytest.raises(FeedDecodeError, match="empty response body"):
            decode_feed(b"", feed_id="f")

    def test_truncated_protobuf_raises(self, truncated_payload):
        with pytest.raises(FeedDecodeError):
            decode_feed(truncated_payload, feed_id="f")

    def test_html_error_page_raises(self):
        with pytest.raises(FeedDecodeError):
            decode_feed(b"<html><body>503 Service Unavailable</body></html>", feed_id="f")

    def test_unsupported_version_raises(self):
        message = gtfs_realtime_pb2.FeedMessage()
        message.header.gtfs_realtime_version = "3.0"
        message.header.timestamp = 1
        with pytest.raises(FeedDecodeError, match="unsupported gtfs_realtime_version"):
            decode_feed(message.SerializeToString(), feed_id="f")

    def test_empty_but_valid_feed_decodes_to_zero_records(self):
        """A quiet moment is not an error"""
        message = gtfs_realtime_pb2.FeedMessage()
        message.header.gtfs_realtime_version = "2.0"
        message.header.timestamp = 1_793_536_000
        decoded = decode_feed(message.SerializeToString(), feed_id="f")
        assert normalize_feed_message(decoded, feed_id="f", ingest_timestamp_ms=1) == []
