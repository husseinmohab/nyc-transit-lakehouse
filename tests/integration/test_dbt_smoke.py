"""end to end dbt smoke test over the committed seeds

runs the real dbt cli against duckdb in a temp dir then queries the warehouse
this is what proves the numbers in the readme
the model count the test count the scd2 behaviour

marked dbt so it skips when the dbt cli is not on PATH
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.dbt

REPO_ROOT = Path(__file__).resolve().parents[2]
DBT_PROJECT = REPO_ROOT / "dbt" / "nyc_transit"

# The README states this figure if it changes the README is wrong
EXPECTED_MIN_TESTS = 150


def _dbt_executable() -> str:
    candidate = Path(sys.executable).parent / "dbt"
    if candidate.exists():
        return str(candidate)
    found = shutil.which("dbt")
    if not found:
        pytest.skip("dbt CLI not available")
    return found


@pytest.fixture(scope="module")
def dbt_run(tmp_path_factory):
    """Run dbt deps seed and build once yield the manifest and db path"""
    dbt = _dbt_executable()
    workdir = tmp_path_factory.mktemp("dbt")
    duckdb_path = workdir / "nyc_transit.duckdb"

    env = {
        **os.environ,
        "DBT_PROFILES_DIR": str(DBT_PROJECT),
        "DBT_DUCKDB_PATH": str(duckdb_path),
        "DBT_LOG_PATH": str(workdir / "logs"),
    }

    def run(*args: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            [dbt, *args, "--project-dir", str(DBT_PROJECT), "--target", "duckdb"],
            capture_output=True,
            text=True,
            env=env,
            cwd=str(workdir),
            check=False,
        )

    deps = run("deps")
    if deps.returncode != 0:
        pytest.skip(f"dbt deps failed (no network for dbt_utils?): {deps.stdout[-500:]}")

    seed = run("seed")
    assert seed.returncode == 0, seed.stdout[-3000:]

    build = run("build")
    assert build.returncode == 0, build.stdout[-5000:]

    manifest = json.loads((DBT_PROJECT / "target" / "manifest.json").read_text())
    results = json.loads((DBT_PROJECT / "target" / "run_results.json").read_text())
    return {
        "manifest": manifest,
        "results": results,
        "duckdb": duckdb_path,
        "stdout": build.stdout,
    }


@pytest.fixture(scope="module")
def warehouse(dbt_run):
    import duckdb

    connection = duckdb.connect(str(dbt_run["duckdb"]), read_only=True)
    yield connection
    connection.close()


def q(warehouse, sql: str):
    return warehouse.execute(sql).fetchall()


class TestBuildSucceeded:
    def test_every_node_passed(self, dbt_run):
        failures = [
            r for r in dbt_run["results"]["results"] if r["status"] not in ("success", "pass")
        ]
        assert failures == [], [(f["unique_id"], f["status"], f.get("message")) for f in failures]

    def test_test_count_meets_the_readme_figure(self, dbt_run):
        tests = [n for n in dbt_run["manifest"]["nodes"].values() if n["resource_type"] == "test"]
        assert len(tests) >= EXPECTED_MIN_TESTS, (
            f"manifest has {len(tests)} tests; the README claims at least " f"{EXPECTED_MIN_TESTS}"
        )

    def test_has_both_generic_and_singular_tests(self, dbt_run):
        nodes = [n for n in dbt_run["manifest"]["nodes"].values() if n["resource_type"] == "test"]
        generic = [n for n in nodes if n.get("test_metadata")]
        singular = [n for n in nodes if not n.get("test_metadata")]
        assert len(generic) >= 100
        assert len(singular) >= 10

    def test_the_scd2_invariant_tests_exist_and_ran(self, dbt_run):
        names = {
            n["name"] for n in dbt_run["manifest"]["nodes"].values() if n["resource_type"] == "test"
        }
        for required in (
            "assert_scd2_no_overlapping_intervals",
            "assert_scd2_intervals_are_contiguous",
            "assert_scd2_one_current_row_per_vehicle",
        ):
            assert required in names

    def test_a_snapshot_was_built(self, dbt_run):
        snapshots = [
            n for n in dbt_run["manifest"]["nodes"].values() if n["resource_type"] == "snapshot"
        ]
        assert len(snapshots) == 1
        assert snapshots[0]["name"] == "snap_route_alert_status"


class TestModelsHaveRows:
    MODELS = (
        "main_staging.stg_trip_stop_predictions",
        "main_staging.stg_vehicle_positions",
        "main_staging.stg_alert_informed_entities",
        "main_staging.stg_gtfs_routes",
        "main_staging.stg_gtfs_stops",
        "main_marts.dim_route",
        "main_marts.dim_stop",
        "main_marts.dim_vehicle_assignment",
        "main_marts.route_alert_status",
        "main_marts.fct_trip_stop_prediction",
        "main_marts.fct_stop_arrival_estimate",
        "main_marts.agg_route_service_hourly",
        "main_marts.dq_pipeline_freshness",
        "main_marts.dq_prediction_quality_daily",
        "snapshots.snap_route_alert_status",
    )

    @pytest.mark.parametrize("relation", MODELS)
    def test_model_is_non_empty(self, warehouse, relation):
        assert q(warehouse, f"SELECT count(*) FROM {relation}")[0][0] > 0


class TestScd2Behaviour:
    def test_has_multi_version_histories(self, warehouse):
        """Otherwise the interval tests pass vacuously"""
        max_versions = q(
            warehouse, "SELECT max(scd_version) FROM main_marts.dim_vehicle_assignment"
        )[0][0]
        assert max_versions >= 3, f"deepest SCD2 history is only {max_versions} versions"

    def test_exactly_one_current_row_per_vehicle(self, warehouse):
        bad = q(
            warehouse,
            """
            SELECT vehicle_key, count(*) FROM main_marts.dim_vehicle_assignment
            WHERE is_current GROUP BY vehicle_key HAVING count(*) <> 1
            """,
        )
        assert bad == []

    def test_intervals_do_not_overlap(self, warehouse):
        bad = q(
            warehouse,
            """
            SELECT a.vehicle_key
            FROM main_marts.dim_vehicle_assignment a
            JOIN main_marts.dim_vehicle_assignment b
              ON a.vehicle_key = b.vehicle_key
             AND a.vehicle_assignment_sk <> b.vehicle_assignment_sk
             AND a.valid_from < b.valid_to
             AND b.valid_from < a.valid_to
            """,
        )
        assert bad == []

    def test_intervals_are_contiguous(self, warehouse):
        bad = q(
            warehouse,
            """
            WITH ordered AS (
              SELECT vehicle_key, valid_to,
                     lead(valid_from) OVER (PARTITION BY vehicle_key ORDER BY valid_from) AS nxt
              FROM main_marts.dim_vehicle_assignment
            )
            SELECT * FROM ordered WHERE nxt IS NOT NULL AND valid_to <> nxt
            """,
        )
        assert bad == []

    def test_captures_assigned_to_unassigned_transitions(self, warehouse):
        """NULL-safe change detection a plain a <> b would miss these"""
        transitions = q(
            warehouse,
            """
            WITH ordered AS (
              SELECT vehicle_key, is_assigned,
                     lag(is_assigned) OVER (PARTITION BY vehicle_key ORDER BY valid_from) AS prev
              FROM main_marts.dim_vehicle_assignment
            )
            SELECT count(*) FROM ordered WHERE prev IS NOT NULL AND prev <> is_assigned
            """,
        )[0][0]
        assert transitions > 0

    def test_unassigned_versions_have_no_trip(self, warehouse):
        bad = q(
            warehouse,
            """
            SELECT count(*) FROM main_marts.dim_vehicle_assignment
            WHERE (is_assigned AND trip_id IS NULL) OR (NOT is_assigned AND trip_id IS NOT NULL)
            """,
        )[0][0]
        assert bad == 0

    def test_surrogate_keys_are_unique(self, warehouse):
        total, distinct = q(
            warehouse,
            "SELECT count(*), count(DISTINCT vehicle_assignment_sk) "
            "FROM main_marts.dim_vehicle_assignment",
        )[0]
        assert total == distinct


class TestGrainAndJoins:
    def test_publication_grain_is_unique(self, warehouse):
        bad = q(
            warehouse,
            """
            SELECT service_date, trip_id, platform_stop_id, feed_timestamp_ms, count(*)
            FROM main_marts.fct_trip_stop_prediction
            WHERE is_cancelled_trip = false
            GROUP BY 1,2,3,4 HAVING count(*) > 1
            """,
        )
        assert bad == []

    def test_estimate_grain_is_one_row_per_trip_stop(self, warehouse):
        bad = q(
            warehouse,
            """
            SELECT service_date, trip_id, platform_stop_id, count(*)
            FROM main_marts.fct_stop_arrival_estimate
            GROUP BY 1,2,3 HAVING count(*) > 1
            """,
        )
        assert bad == []

    def test_stop_join_coverage_is_total_on_the_seeds(self, warehouse):
        """Every seeded stop must resolve so the dbt coverage test's 99%
        threshold is about production's full stops.txt and not a hole in the
        fixtures

        Scoped to rows that have a stop stop_resolved is NULL for a
        cancellation which carries no stop to resolve
        """
        resolved, total = q(
            warehouse,
            "SELECT sum(CASE WHEN stop_resolved THEN 1 ELSE 0 END), count(*) "
            "FROM main_marts.fct_trip_stop_prediction WHERE stop_id IS NOT NULL",
        )[0]
        assert resolved == total

    def test_cancellations_have_a_null_resolution_flag(self, warehouse):
        """Not false "did the stop resolve?" has no answer for a row with no
        stop and collapsing it to false depresses the coverage metric
        """
        bad = q(
            warehouse,
            "SELECT count(*) FROM main_marts.fct_trip_stop_prediction "
            "WHERE stop_id IS NULL AND stop_resolved IS NOT NULL",
        )[0][0]
        assert bad == 0

    def test_revision_counts_capture_churn(self, warehouse):
        """The estimate fact exists to measure churn If every trip-stop had one
        revision the model would be pointless
        """
        max_revisions = q(
            warehouse, "SELECT max(revision_count) FROM main_marts.fct_stop_arrival_estimate"
        )[0][0]
        assert max_revisions >= 2

    def test_hourly_rollup_never_exceeds_the_daily_summary(self, warehouse):
        bad = q(
            warehouse,
            """
            WITH hourly AS (
              SELECT service_date, route_id, sum(trip_stop_count) AS n
              FROM main_marts.agg_route_service_hourly GROUP BY 1,2
            ),
            daily AS (
              SELECT service_date, route_id, trip_stop_count AS n
              FROM main_marts.dq_prediction_quality_daily
            )
            SELECT h.service_date, h.route_id, h.n, d.n
            FROM hourly h JOIN daily d USING (service_date, route_id)
            WHERE h.n > d.n
            """,
        )
        assert bad == []

    def test_cancellations_are_present_and_have_no_stop(self, warehouse):
        cancelled = q(
            warehouse,
            "SELECT count(*) FROM main_marts.fct_trip_stop_prediction WHERE is_cancelled_trip",
        )[0][0]
        assert cancelled > 0
        with_stop = q(
            warehouse,
            "SELECT count(*) FROM main_marts.fct_trip_stop_prediction "
            "WHERE is_cancelled_trip AND stop_id IS NOT NULL",
        )[0][0]
        assert with_stop == 0

    def test_two_service_dates_are_present(self, warehouse):
        dates = q(
            warehouse,
            "SELECT count(DISTINCT service_date) FROM main_marts.fct_trip_stop_prediction",
        )[0][0]
        assert dates == 2

    def test_horizon_buckets_cover_more_than_one_bucket(self, warehouse):
        buckets = q(
            warehouse,
            "SELECT count(DISTINCT horizon_bucket) FROM main_marts.fct_trip_stop_prediction",
        )[0][0]
        assert buckets >= 2

    def test_freshness_table_covers_every_silver_source(self, warehouse):
        tables = {
            row[0]
            for row in q(warehouse, "SELECT table_name FROM main_marts.dq_pipeline_freshness")
        }
        assert tables == {
            "silver.trip_stop_predictions",
            "silver.vehicle_positions",
            "silver.alert_informed_entities",
        }

    def test_stability_score_is_in_range(self, warehouse):
        lo, hi = q(
            warehouse,
            "SELECT min(stability_score), max(stability_score) "
            "FROM main_marts.dq_prediction_quality_daily",
        )[0]
        assert 0 <= lo <= hi <= 100
