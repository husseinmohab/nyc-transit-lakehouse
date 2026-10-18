"""freshness sla and great expectations gate tests"""

from __future__ import annotations

import pytest

from nyc_transit.quality.expectations import (
    build_freshness_expectations,
    build_partition_expectations,
    observations_to_frame,
    validate_freshness,
    validate_partition,
)
from nyc_transit.quality.freshness import (
    FRESHNESS_SLAS,
    SLAS_BY_TABLE,
    FreshnessObservation,
    FreshnessSLA,
    Severity,
    evaluate_all,
    evaluate_freshness,
)
from nyc_transit.quality.probe import observation_sql

NOW = 1_793_600_000.0


def observation(
    *,
    table="silver.trip_stop_predictions",
    rows=50_000,
    event_epoch=NOW - 60,
    observed_at=NOW,
    partition="2026-11-01",
):
    return FreshnessObservation(
        table=table,
        partition_value=partition,
        row_count=rows,
        max_event_time_epoch_seconds=event_epoch,
        observed_at_epoch_seconds=observed_at,
    )


class TestSlaDefinitions:
    def test_every_sla_measures_an_event_time_column(self):
        """Never a load-time column

        Checking silver_loaded_at would report a stalled pipeline as fresh for
        as long as it kept rewriting the same stale rows
        """
        for sla in FRESHNESS_SLAS:
            assert "loaded_at" not in sla.event_time_column, sla.table

    def test_critical_slas_require_a_minimum_row_count(self):
        """Fresh but nearly empty is the signature of a partial outage"""
        for sla in FRESHNESS_SLAS:
            if sla.severity is Severity.CRITICAL:
                assert sla.min_rows_per_partition > 0, sla.table

    def test_alerts_sla_is_warning_only(self):
        """A quiet day produces no alerts"""
        sla = SLAS_BY_TABLE["bronze.alerts"]
        assert sla.severity is Severity.WARNING
        assert sla.max_lag_seconds >= 3600

    def test_silver_is_at_least_as_lenient_as_bronze(self):
        """Silver is merged after bronze so it cannot be fresher"""
        assert (
            SLAS_BY_TABLE["silver.trip_stop_predictions"].max_lag_seconds
            >= SLAS_BY_TABLE["bronze.trip_updates"].max_lag_seconds
        )

    def test_every_sla_has_a_rationale(self):
        for sla in FRESHNESS_SLAS:
            assert sla.description.strip(), sla.table

    def test_rejects_a_nonpositive_lag_budget(self):
        with pytest.raises(ValueError, match="must be positive"):
            FreshnessSLA(
                table="t", event_time_column="c", max_lag_seconds=0, min_rows_per_partition=1
            )

    def test_rejects_a_negative_row_minimum(self):
        with pytest.raises(ValueError, match="cannot be negative"):
            FreshnessSLA(
                table="t", event_time_column="c", max_lag_seconds=60, min_rows_per_partition=-1
            )


class TestEvaluateFreshness:
    SLA = SLAS_BY_TABLE["silver.trip_stop_predictions"]

    def test_a_healthy_observation_has_no_breaches(self):
        assert evaluate_freshness(observation(), self.SLA) == []

    def test_an_empty_partition_is_an_empty_breach(self):
        breaches = evaluate_freshness(observation(rows=0), self.SLA)
        assert [b.kind for b in breaches] == ["empty"]
        assert breaches[0].fails_dag

    def test_a_thin_partition_is_reported_separately_from_staleness(self):
        """Different cause different fix thin means some feeds are failing"""
        breaches = evaluate_freshness(observation(rows=5), self.SLA)
        assert "thin" in {b.kind for b in breaches}

    def test_stale_data_is_reported(self):
        breaches = evaluate_freshness(
            observation(event_epoch=NOW - self.SLA.max_lag_seconds - 60), self.SLA
        )
        assert [b.kind for b in breaches] == ["stale"]

    def test_lag_exactly_at_the_budget_is_not_a_breach(self):
        assert (
            evaluate_freshness(observation(event_epoch=NOW - self.SLA.max_lag_seconds), self.SLA)
            == []
        )

    def test_a_future_timestamp_is_reported_not_corrected(self):
        """A future event time makes every lag look healthy

        Without this check a feed publishing bad clocks would mask a real
        outage indefinitely
        """
        breaches = evaluate_freshness(observation(event_epoch=NOW + 3600), self.SLA)
        assert [b.kind for b in breaches] == ["future_timestamp"]

    def test_small_clock_skew_is_tolerated(self):
        assert evaluate_freshness(observation(event_epoch=NOW + 30), self.SLA) == []

    def test_rows_present_but_no_event_time_is_an_empty_breach(self):
        breaches = evaluate_freshness(observation(event_epoch=None), self.SLA)
        assert [b.kind for b in breaches] == ["empty"]

    def test_lag_is_none_for_an_empty_table(self):
        assert observation(event_epoch=None).lag_seconds is None

    def test_thin_and_stale_are_both_reported(self):
        breaches = evaluate_freshness(observation(rows=5, event_epoch=NOW - 10_000), self.SLA)
        assert {b.kind for b in breaches} == {"thin", "stale"}

    def test_warning_severity_does_not_fail_the_dag(self):
        sla = SLAS_BY_TABLE["bronze.alerts"]
        breaches = evaluate_freshness(
            observation(table="bronze.alerts", event_epoch=NOW - 999_999), sla
        )
        assert breaches and not any(b.fails_dag for b in breaches)


class TestEvaluateAll:
    def test_a_missing_table_is_a_breach(self):
        """A table with an SLA that was not observed means the probe failed"""
        breaches = evaluate_all([])
        assert len(breaches) == len(FRESHNESS_SLAS)
        assert all(b.kind == "empty" for b in breaches)

    def test_an_unmonitored_table_is_reported(self):
        """Silently skipping it is how a table stops being monitored"""
        breaches = evaluate_all(
            [observation(table="silver.mystery")],
            slas=(SLAS_BY_TABLE["silver.trip_stop_predictions"],),
        )
        assert "unmonitored" in " ".join(b.detail for b in breaches)

    def test_all_healthy_yields_no_breaches(self):
        observations = [
            observation(table=sla.table, rows=max(sla.min_rows_per_partition, 1) + 1)
            for sla in FRESHNESS_SLAS
        ]
        assert evaluate_all(observations) == []


class TestGreatExpectations:
    def _healthy(self):
        return [
            observation(table=sla.table, rows=max(sla.min_rows_per_partition, 1) + 1)
            for sla in FRESHNESS_SLAS
        ]

    def test_frame_has_one_row_per_observation(self):
        frame = observations_to_frame(self._healthy())
        assert len(frame) == len(FRESHNESS_SLAS)
        assert "lag_seconds" in frame.columns

    def test_empty_frame_has_the_right_columns(self):
        """So GE does not fail on a column-less frame"""
        frame = observations_to_frame([])
        assert frame.empty
        assert "lag_seconds" in frame.columns

    def test_one_expectation_pair_per_sla_plus_structural_checks(self):
        expectations = build_freshness_expectations()
        with_rows = sum(1 for s in FRESHNESS_SLAS if s.min_rows_per_partition > 0)
        assert len(expectations) == 4 + len(FRESHNESS_SLAS) + with_rows

    def test_healthy_observations_pass_the_ge_suite(self):
        outcome = validate_freshness(self._healthy())
        assert outcome.success
        assert outcome.breaches == []
        assert not outcome.should_fail_dag
        assert outcome.checks

    def test_stale_observation_fails_ge_and_the_dag(self):
        observations = self._healthy()
        observations[0] = observation(
            table=observations[0].table,
            rows=observations[0].row_count,
            event_epoch=NOW - 999_999,
        )
        outcome = validate_freshness(observations)
        assert not outcome.success
        assert outcome.should_fail_dag
        assert any(b.kind == "stale" for b in outcome.breaches)

    def test_warning_only_breach_does_not_fail_the_dag(self):
        """GE reports it the DAG keeps going Conflating the two means the
        loose alerts SLA could page someone at 3am
        """
        observations = [
            observation(table=sla.table, rows=max(sla.min_rows_per_partition, 1) + 1)
            for sla in FRESHNESS_SLAS
            if sla.table != "bronze.alerts"
        ]
        observations.append(observation(table="bronze.alerts", rows=5, event_epoch=NOW - 999_999))
        outcome = validate_freshness(observations)
        assert outcome.breaches
        assert not outcome.should_fail_dag

    def test_no_observations_at_all_fails(self):
        outcome = validate_freshness([])
        assert not outcome.success
        assert outcome.should_fail_dag

    def test_outcome_renders_observed_values(self):
        """on call needs to see lag was 612s against an sla of 420s"""
        rendered = validate_freshness(self._healthy()).render()
        assert "ge_success" in rendered
        assert "observed=" in rendered

    def test_expectations_carry_their_rationale_in_meta(self):
        metas = [
            e.meta
            for e in build_freshness_expectations()
            if getattr(e, "meta", None) and "sla_table" in e.meta
        ]
        assert metas
        assert any(m.get("rationale") for m in metas)


class TestPartitionGate:
    def _frame(self, rows=None):
        import pandas as pd

        rows = rows or [
            {
                "event_id": f"id-{i}",
                "service_date": "2026-11-01",
                "feed_timestamp_ms": 1_793_536_000_000 + i,
                "route_id": "L",
                "stop_sequence": (i % 30) + 1,
                "prediction_horizon_seconds": 300,
            }
            for i in range(50)
        ]
        return pd.DataFrame(rows)

    def test_a_clean_partition_passes(self):
        outcome = validate_partition(self._frame(), partition_value="2026-11-01")
        assert outcome.success
        assert not outcome.should_fail_dag

    def test_an_empty_partition_fails(self):
        import pandas as pd

        outcome = validate_partition(pd.DataFrame(), partition_value="2026-11-01")
        assert not outcome.success
        assert outcome.should_fail_dag

    def test_duplicate_event_ids_fail(self):
        """A duplicate here means the dedup logic regressed"""
        frame = self._frame()
        frame.loc[0, "event_id"] = frame.loc[1, "event_id"]
        outcome = validate_partition(frame, partition_value="2026-11-01")
        assert not outcome.success
        assert outcome.should_fail_dag

    def test_a_null_event_id_fails(self):
        frame = self._frame()
        frame.loc[0, "event_id"] = None
        assert not validate_partition(frame, partition_value="2026-11-01").success

    def test_an_absurd_prediction_horizon_fails(self):
        frame = self._frame()
        frame["prediction_horizon_seconds"] = 999_999
        assert not validate_partition(frame, partition_value="2026-11-01").success

    def test_partition_suite_asserts_event_id_uniqueness(self):
        types = {type(e).__name__ for e in build_partition_expectations()}
        assert "ExpectColumnValuesToBeUnique" in types


class TestProbeSql:
    def test_epoch_ms_columns_are_divided_by_1000(self):
        sql = observation_sql(
            catalog="cat", sla=SLAS_BY_TABLE["bronze.trip_updates"], partition_value=None
        )
        assert "max(feed_timestamp_ms) / 1000.0" in sql

    def test_timestamp_columns_use_unix_timestamp(self):
        sql = observation_sql(
            catalog="cat",
            sla=SLAS_BY_TABLE["silver.trip_stop_predictions"],
            partition_value=None,
        )
        assert "max(unix_timestamp(feed_timestamp))" in sql

    def test_partition_filter_enables_metadata_pruning(self):
        sql = observation_sql(
            catalog="cat",
            sla=SLAS_BY_TABLE["bronze.trip_updates"],
            partition_value="2026-11-01",
        )
        assert "WHERE service_date = DATE '2026-11-01'" in sql

    def test_no_filter_without_a_partition_value(self):
        sql = observation_sql(
            catalog="cat", sla=SLAS_BY_TABLE["bronze.trip_updates"], partition_value=None
        )
        assert "WHERE" not in sql

    @pytest.mark.parametrize("bad", ["2026/11/01", "'; DROP TABLE x --", "nov 1"])
    def test_rejects_a_non_iso_partition_value(self, bad):
        with pytest.raises(ValueError, match="not an ISO date"):
            observation_sql(
                catalog="cat", sla=SLAS_BY_TABLE["bronze.trip_updates"], partition_value=bad
            )
