"""load generator and benchmark tests

the throughput asserts use an injected fake clock so they test the rate
controller and not how fast the ci box is
a test that asserts 2000 events/sec on this machine fails on a loaded runner and
gets deleted
"""

from __future__ import annotations

import dataclasses

import pytest

from nyc_transit.gtfs.synthetic import SyntheticFeedConfig, SyntheticFeedGenerator
from nyc_transit.producer.loadgen import build_parser, run_benchmark
from nyc_transit.producer.sink import MemorySink, NullSink


class FakeClock:
    """Advances by a fixed step on every read"""

    def __init__(self, step: float = 0.05) -> None:
        self.now = 0.0
        self.step = step

    def __call__(self) -> float:
        value = self.now
        self.now += self.step
        return value


SMALL = SyntheticFeedConfig(
    trips_per_message=3,
    stops_per_trip=(4, 6),
    vehicles_per_message=2,
    alerts_per_message=1,
    duplicate_fraction=0.0,
    cancellation_fraction=0.0,
    seed=1234,
)


class TestSyntheticConfig:
    def test_rejects_an_inverted_stop_range(self):
        with pytest.raises(ValueError, match="increasing positive range"):
            SyntheticFeedConfig(stops_per_trip=(10, 2))

    def test_rejects_a_zero_stop_minimum(self):
        with pytest.raises(ValueError, match="increasing positive range"):
            SyntheticFeedConfig(stops_per_trip=(0, 5))

    @pytest.mark.parametrize("value", [-0.1, 1.1])
    def test_rejects_out_of_range_fractions(self, value):
        with pytest.raises(ValueError, match="must be in \\[0, 1\\]"):
            SyntheticFeedConfig(duplicate_fraction=value)

    def test_rejects_an_empty_fleet(self):
        with pytest.raises(ValueError, match="at least 1"):
            SyntheticFeedConfig(fleet_size_per_route=0)

    def test_rejects_an_empty_route_list(self):
        with pytest.raises(ValueError, match="non-empty"):
            SyntheticFeedConfig(routes=())

    def test_mean_records_is_within_the_estimated_bounds(self):
        low, high = SMALL.expected_records()
        assert low <= SMALL.mean_records_per_message() <= high


def _train_id(trip):
    """the nyct train id off a trip descriptor

    the generator sends no gtfs vehicle descriptor because the real subway feed
    does not either, so the unit identity lives on the nyct extension
    """
    from nyc_transit.gtfs.proto import nyct_subway_pb2

    return trip.Extensions[nyct_subway_pb2.nyct_trip_descriptor].train_id


class TestSyntheticGenerator:
    def test_is_reproducible_for_a_fixed_seed(self):
        a = SyntheticFeedGenerator(SMALL).build_feed_message(feed_timestamp_epoch=1_793_536_000)
        b = SyntheticFeedGenerator(SMALL).build_feed_message(feed_timestamp_epoch=1_793_536_000)
        assert a.SerializeToString() == b.SerializeToString()

    def test_different_seeds_produce_different_feeds(self):
        other = dataclasses.replace(SMALL, seed=999)
        a = SyntheticFeedGenerator(SMALL).build_feed_message(feed_timestamp_epoch=1_793_536_000)
        b = SyntheticFeedGenerator(other).build_feed_message(feed_timestamp_epoch=1_793_536_000)
        assert a.SerializeToString() != b.SerializeToString()

    def test_duplicates_are_byte_identical_trips(self):
        """A duplicate that does not collapse is useless for testing dedup

        Regression guard for the bug the dbt grain test caught a re-emitted
        trip used to get freshly randomised stops so it was a different fact
        with a different event_id rather than a duplicate
        """
        from nyc_transit.gtfs.decoder import decode_feed
        from nyc_transit.gtfs.normalize import normalize_feed_message

        config = SyntheticFeedConfig(
            trips_per_message=10,
            stops_per_trip=(5, 5),
            vehicles_per_message=0,
            alerts_per_message=0,
            duplicate_fraction=0.5,
            cancellation_fraction=0.0,
            seed=7,
        )
        message = SyntheticFeedGenerator(config).build_feed_message(
            feed_timestamp_epoch=1_793_536_000, service_date_yyyymmdd="20261101"
        )
        records = normalize_feed_message(
            decode_feed(message.SerializeToString(), feed_id="f"),
            feed_id="f",
            ingest_timestamp_ms=1,
        )
        ids = [r["event_id"] for r in records]
        assert len(ids) > len(set(ids)), "no duplicates were generated"

        by_id: dict[str, list[dict]] = {}
        for record in records:
            by_id.setdefault(record["event_id"], []).append(record)
        for group in by_id.values():
            assert all(r == group[0] for r in group)

    def test_vehicles_appear_at_most_once_per_message(self):
        """A real feed reports each vehicle once per FeedMessage"""
        config = SyntheticFeedConfig(
            trips_per_message=0,
            vehicles_per_message=40,
            alerts_per_message=0,
            fleet_size_per_route=10,
            seed=3,
        )
        message = SyntheticFeedGenerator(config).build_feed_message(
            feed_timestamp_epoch=1_793_536_000
        )
        vehicle_ids = [_train_id(e.vehicle.trip) for e in message.entity if e.HasField("vehicle")]
        assert len(vehicle_ids) == len(set(vehicle_ids))

    def test_fleet_assignments_persist_across_messages(self):
        """Otherwise the SCD2 dimension has one version per vehicle and its
        history tests pass vacuously
        """
        generator = SyntheticFeedGenerator(
            SyntheticFeedConfig(
                trips_per_message=0,
                vehicles_per_message=10,
                alerts_per_message=0,
                fleet_size_per_route=4,
                reassignment_probability=0.0,
                unassigned_probability=0.0,
                routes=("L",),
                seed=11,
            )
        )
        assignments = []
        for poll in range(3):
            message = generator.build_feed_message(feed_timestamp_epoch=1_793_536_000 + poll * 30)
            assignments.append(
                {_train_id(e.vehicle.trip): e.vehicle.trip.trip_id for e in message.entity}
            )
        assert assignments[0] == assignments[1] == assignments[2]

    def test_reassignment_changes_the_trip(self):
        generator = SyntheticFeedGenerator(
            SyntheticFeedConfig(
                trips_per_message=0,
                vehicles_per_message=4,
                alerts_per_message=0,
                fleet_size_per_route=4,
                reassignment_probability=1.0,
                unassigned_probability=0.0,
                routes=("L",),
                seed=11,
            )
        )
        first = {
            e.vehicle.vehicle.id: e.vehicle.trip.trip_id
            for e in generator.build_feed_message(feed_timestamp_epoch=1_793_536_000).entity
        }
        second = {
            e.vehicle.vehicle.id: e.vehicle.trip.trip_id
            for e in generator.build_feed_message(feed_timestamp_epoch=1_793_536_030).entity
        }
        assert any(first[v] != second[v] for v in first.keys() & second.keys())

    def test_unassigned_vehicles_carry_a_route_but_no_trip(self):
        generator = SyntheticFeedGenerator(
            SyntheticFeedConfig(
                trips_per_message=0,
                vehicles_per_message=4,
                alerts_per_message=0,
                fleet_size_per_route=4,
                unassigned_probability=1.0,
                routes=("L",),
                seed=5,
            )
        )
        message = generator.build_feed_message(feed_timestamp_epoch=1_793_536_000)
        for entity in message.entity:
            assert entity.vehicle.trip.trip_id == ""
            assert entity.vehicle.trip.route_id == "L"

    def test_trip_paths_walk_real_stations_in_order(self):
        from nyc_transit.gtfs.static_reference import STOPS_BY_ID, parent_station

        message = SyntheticFeedGenerator(SMALL).build_feed_message(
            feed_timestamp_epoch=1_793_536_000
        )
        for entity in message.entity:
            if not entity.HasField("trip_update"):
                continue
            for stu in entity.trip_update.stop_time_update:
                assert parent_station(stu.stop_id) in STOPS_BY_ID


class TestBenchmark:
    def test_reports_the_records_it_produced(self):
        result = run_benchmark(
            NullSink(),
            duration_seconds=1.0,
            target_events_per_second=0,
            feed_config=SMALL,
            clock=FakeClock(0.2),
        )
        assert result.messages > 0
        assert result.records > 0
        assert result.events_per_second > 0
        assert result.records_failed == 0

    def test_exercises_the_full_production_path(self):
        """Protobuf serialize decode normalize Avro encode

        A benchmark looping over pre-built dicts would measure the wrong thing,
        so the sink's byte counter is the proof that encoding happened.
        """
        sink = NullSink()
        run_benchmark(
            sink,
            duration_seconds=1.0,
            target_events_per_second=0,
            feed_config=SMALL,
            clock=FakeClock(0.2),
        )
        assert sink.stats.bytes_out > 0
        assert set(sink.stats.by_record_type) == {"trip_update", "vehicle_position", "alert"}

    def test_throttled_run_tracks_the_target_rate(self):
        """Closed loop on records so the achieved rate lands near target

        With a fake clock this tests the controller not machine speed
        """
        result = run_benchmark(
            NullSink(),
            duration_seconds=60.0,
            target_events_per_second=2000.0,
            feed_config=SMALL,
            clock=FakeClock(0.001),
            sleeper=lambda _seconds: None,
        )
        assert result.achieved_fraction is not None
        assert 0.9 <= result.achieved_fraction <= 1.15

    def test_unthrottled_run_reports_no_achieved_fraction(self):
        result = run_benchmark(
            NullSink(),
            duration_seconds=1.0,
            target_events_per_second=0,
            feed_config=SMALL,
            clock=FakeClock(0.2),
        )
        assert result.achieved_fraction is None

    def test_memory_sink_target_retains_payloads(self):
        sink = MemorySink()
        run_benchmark(
            sink,
            duration_seconds=1.0,
            target_events_per_second=0,
            feed_config=SMALL,
            clock=FakeClock(0.2),
        )
        assert len(sink.messages) == sink.stats.records_in

    def test_result_renders_as_json(self):
        import json

        result = run_benchmark(
            NullSink(),
            duration_seconds=1.0,
            target_events_per_second=0,
            feed_config=SMALL,
            clock=FakeClock(0.3),
        )
        payload = json.loads(result.render())
        assert payload["records"] == result.records
        assert "mb_per_second" in payload


class TestCli:
    def test_defaults_target_2000_eps_with_no_broker(self):
        args = build_parser().parse_args([])
        assert args.target == "null"
        assert args.target_eps == 2000.0

    def test_kafka_target_is_available(self):
        args = build_parser().parse_args(["--target", "kafka", "--duration", "5"])
        assert args.target == "kafka"
        assert args.duration == 5.0

    def test_rejects_an_unknown_target(self):
        with pytest.raises(SystemExit):
            build_parser().parse_args(["--target", "s3"])
