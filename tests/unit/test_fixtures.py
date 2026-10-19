"""the committed fixture and seeds have to match their generators

these are what let the readme quote exact numbers
a fixture that can drift from the code that made it is a fixture nobody can
reason about
"""

from __future__ import annotations

import csv
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
FIXTURE = REPO_ROOT / "tests" / "fixtures" / "gtfs_rt_sample.pb"
SEED_DIR = REPO_ROOT / "dbt" / "nyc_transit" / "seeds"


class TestCommittedFixture:
    def test_is_byte_identical_to_the_generator_output(self):
        from nyc_transit.gtfs.synthetic import build_fixture_payload

        assert FIXTURE.read_bytes() == build_fixture_payload()

    def test_decodes_and_carries_all_three_entity_kinds(self, fixture_payload):
        from nyc_transit.gtfs.decoder import decode_feed

        message = decode_feed(fixture_payload, feed_id="nyct-ace")
        kinds = {
            (
                "trip_update"
                if e.HasField("trip_update")
                else "vehicle" if e.HasField("vehicle") else "alert"
            )
            for e in message.entity
        }
        assert kinds == {"trip_update", "vehicle", "alert"}

    def test_truncated_fixture_is_a_prefix_of_the_valid_one(self, truncated_payload):
        """Real truncated protobuf not random bytes"""
        assert FIXTURE.read_bytes().startswith(truncated_payload)
        assert len(truncated_payload) == 40

    def test_is_small_enough_to_commit(self):
        assert FIXTURE.stat().st_size < 10_000


class TestCommittedSeeds:
    SEEDS = (
        "gtfs_routes.csv",
        "gtfs_stops.csv",
        "silver_trip_stop_predictions.csv",
        "silver_vehicle_positions.csv",
        "silver_alert_informed_entities.csv",
    )

    def test_all_seeds_are_committed(self):
        for name in self.SEEDS:
            assert (SEED_DIR / name).exists(), name

    def test_regenerating_produces_identical_files(self, tmp_path):
        """The generator is deterministic CI asserts the same with git diff"""
        result = subprocess.run(
            [
                sys.executable,
                str(REPO_ROOT / "scripts" / "generate_dbt_seeds.py"),
                "--out",
                str(tmp_path),
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode == 0, result.stderr
        for name in self.SEEDS:
            assert (tmp_path / name).read_bytes() == (SEED_DIR / name).read_bytes(), name

    def test_route_seed_matches_the_static_reference(self):
        from nyc_transit.gtfs.static_reference import ROUTES

        rows = list(csv.DictReader((SEED_DIR / "gtfs_routes.csv").open()))
        assert len(rows) == len(ROUTES)
        assert {r["route_id"] for r in rows} == {r.route_id for r in ROUTES}

    def test_stop_seed_matches_the_static_reference(self):
        from nyc_transit.gtfs.static_reference import STOPS

        rows = list(csv.DictReader((SEED_DIR / "gtfs_stops.csv").open()))
        assert {r["stop_id"] for r in rows} == {s.stop_id for s in STOPS}

    def test_every_predicted_stop_joins_to_a_station(self):
        """Join coverage in the seeds must be total so the dbt coverage test's
        99% threshold is about production's full stops.txt rather than a hole in
        the local fixtures

        The seed carries the platform-level id as published ("101N") the station
        key is its parent which is the derivation the dbt staging layer does
        """
        from nyc_transit.gtfs.static_reference import STOPS_BY_ID, parent_station

        rows = list(csv.DictReader((SEED_DIR / "silver_trip_stop_predictions.csv").open()))
        unresolved = {
            r["stop_id"]
            for r in rows
            if r["stop_id"] and parent_station(r["stop_id"]) not in STOPS_BY_ID
        }
        assert unresolved == set()

    def test_seed_stop_ids_are_platform_level(self):
        """Not already-stripped parent ids the staging model needs real
        suffixed ids to exercise parent_station against
        """
        rows = list(csv.DictReader((SEED_DIR / "silver_trip_stop_predictions.csv").open()))
        suffixed = [r["stop_id"] for r in rows if r["stop_id"] and r["stop_id"][-1] in "NS"]
        assert len(suffixed) > 0.9 * sum(1 for r in rows if r["stop_id"])

    def test_seeds_span_two_service_dates(self):
        """So the incremental lookback window has more than one partition"""
        rows = list(csv.DictReader((SEED_DIR / "silver_trip_stop_predictions.csv").open()))
        assert len({r["service_date"] for r in rows}) == 2

    def test_seeds_include_cancellations_and_skipped_stops(self):
        """The nullability and SKIPPED branches must be exercised locally"""
        rows = list(csv.DictReader((SEED_DIR / "silver_trip_stop_predictions.csv").open()))
        assert any(r["is_cancelled_trip"] == "true" for r in rows)
        assert any(r["is_skipped"] == "true" for r in rows)

    def test_seeds_include_unassigned_vehicles(self):
        """So the SCD2 assigned to unassigned transition has real data"""
        rows = list(csv.DictReader((SEED_DIR / "silver_vehicle_positions.csv").open()))
        assert any(not r["trip_id"] for r in rows)

    def test_vehicle_seed_has_multi_observation_vehicles(self):
        """Otherwise every SCD2 history is one version long"""
        import collections

        rows = list(csv.DictReader((SEED_DIR / "silver_vehicle_positions.csv").open()))
        counts = collections.Counter(r["vehicle_id"] for r in rows)
        assert max(counts.values()) >= 3

    def test_null_convention_is_the_empty_string(self):
        """So both DuckDB and Spark read them back as NULL"""
        text = (SEED_DIR / "silver_trip_stop_predictions.csv").read_text()
        assert ",," in text
        assert "None" not in text
        assert "nan" not in text.lower()

    def test_booleans_are_lowercase_literals(self):
        rows = list(csv.DictReader((SEED_DIR / "silver_vehicle_positions.csv").open()))
        assert {r["position_is_plausible"] for r in rows} <= {"true", "false", ""}

    @pytest.mark.parametrize("name", SEEDS)
    def test_every_seed_has_a_header_and_rows(self, name):
        rows = list(csv.DictReader((SEED_DIR / name).open()))
        assert rows, name
        assert all(rows[0].keys() == r.keys() for r in rows)


class TestStaticReference:
    def test_every_generated_route_has_stops(self):
        """The generator draws from routes_with_stops() so a route with none
        would make join-coverage tests vacuous for it
        """
        from nyc_transit.gtfs.static_reference import routes_with_stops, stops_for_route

        assert [r for r in routes_with_stops() if not stops_for_route(r)] == []

    def test_only_the_express_variants_have_no_stops(self):
        """6X 7X and FX run over another routes stations so the published
        station list never names them anything else missing is a data problem
        """
        from nyc_transit.gtfs.static_reference import ROUTES, stops_for_route

        missing = [r.route_id for r in ROUTES if not stops_for_route(r.route_id)]
        assert sorted(missing) == ["6X", "7X", "FX"]

    def test_route_ids_are_unique(self):
        from nyc_transit.gtfs.static_reference import ROUTES

        ids = [r.route_id for r in ROUTES]
        assert len(ids) == len(set(ids))

    def test_stop_ids_are_unique(self):
        from nyc_transit.gtfs.static_reference import STOPS

        ids = [s.stop_id for s in STOPS]
        assert len(ids) == len(set(ids))

    def test_every_stop_references_known_routes(self):
        from nyc_transit.gtfs.static_reference import ROUTES_BY_ID, STOPS

        for stop in STOPS:
            unknown = set(stop.routes) - set(ROUTES_BY_ID)
            assert not unknown, f"{stop.stop_id} references unknown routes {unknown}"

    def test_every_stop_is_inside_the_nyc_bounding_box(self):
        from nyc_transit.gtfs.static_reference import STOPS

        for stop in STOPS:
            assert 40.4 <= stop.stop_lat <= 41.1, stop.stop_id
            assert -74.3 <= stop.stop_lon <= -73.6, stop.stop_id

    @pytest.mark.parametrize(
        ("platform", "expected"),
        [("127N", "127"), ("A31S", "A31"), ("L06N", "L06"), ("127", "127"), ("", "")],
    )
    def test_parent_station_strips_only_known_suffixes(self, platform, expected):
        from nyc_transit.gtfs.static_reference import parent_station

        assert parent_station(platform) == expected

    def test_platform_ids_are_two_per_station(self):
        from nyc_transit.gtfs.static_reference import platform_ids_for_route, stops_for_route

        for route in ("L", "A", "6"):
            assert len(platform_ids_for_route(route)) == 2 * len(stops_for_route(route))

    def test_sir_is_route_type_2_not_1(self):
        """It is operated as a subway but published as rail so anything that
        says subway must test route_type
        """
        from nyc_transit.gtfs.static_reference import ROUTES_BY_ID

        assert ROUTES_BY_ID["SI"].route_type == 2
