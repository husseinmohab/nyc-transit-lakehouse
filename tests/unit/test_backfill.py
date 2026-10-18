"""backfill planning guard rails and sql generation"""

from __future__ import annotations

from datetime import date

import pytest

from nyc_transit.backfill.partitions import (
    REBUILDABLE_TABLES,
    BackfillPlan,
    PartitionWindow,
    UnsafeBackfillTarget,
    guard_target_table,
    plan_backfill,
    rebuild_partition_sql,
    run_backfill,
)


class TestPartitionWindow:
    def test_single_day_window(self):
        window = PartitionWindow(date(2026, 11, 1), date(2026, 11, 1))
        assert len(window) == 1
        assert window.iso_dates() == ["2026-11-01"]

    def test_multi_day_window_is_inclusive_on_both_ends(self):
        window = PartitionWindow(date(2026, 11, 1), date(2026, 11, 3))
        assert window.iso_dates() == ["2026-11-01", "2026-11-02", "2026-11-03"]

    def test_crosses_a_month_boundary(self):
        window = PartitionWindow(date(2026, 10, 31), date(2026, 11, 1))
        assert window.iso_dates() == ["2026-10-31", "2026-11-01"]

    def test_rejects_an_inverted_window(self):
        with pytest.raises(ValueError, match="precedes start"):
            PartitionWindow(date(2026, 11, 3), date(2026, 11, 1))


class TestGuardTargetTable:
    def test_allows_the_declared_silver_tables(self):
        for table in REBUILDABLE_TABLES:
            assert guard_target_table(table) == table

    @pytest.mark.parametrize(
        "forbidden",
        [
            "bronze.trip_updates",
            "bronze.vehicle_positions",
            "bronze.alerts",
            "bronze._stream_commits",
        ],
    )
    def test_refuses_every_bronze_table(self, forbidden):
        """Bronze is the replay substrate A backfill that can delete bronze
        turns a transform bug into permanent data loss
        """
        with pytest.raises(UnsafeBackfillTarget, match="not rebuildable"):
            guard_target_table(forbidden)

    def test_refuses_an_unknown_table(self):
        with pytest.raises(UnsafeBackfillTarget):
            guard_target_table("marts.fct_anything")

    def test_no_bronze_table_is_in_the_allowlist(self):
        assert not any(t.startswith("bronze") for t in REBUILDABLE_TABLES)

    def test_every_rebuildable_table_reads_from_bronze(self):
        assert all(src.startswith("bronze.") for src in REBUILDABLE_TABLES.values())


class TestRebuildSql:
    def test_returns_a_delete_then_an_insert(self):
        statements = rebuild_partition_sql(
            catalog="cat", table="silver.trip_stop_predictions", partition_value="2026-11-01"
        )
        assert len(statements) == 2
        assert statements[0].startswith("DELETE FROM cat.silver.trip_stop_predictions")
        assert statements[1].startswith("INSERT INTO cat.silver.trip_stop_predictions")

    def test_delete_is_scoped_to_the_partition(self):
        """Scoped to the partition column so Iceberg can drop whole files"""
        delete, _ = rebuild_partition_sql(
            catalog="cat", table="silver.vehicle_positions", partition_value="2026-11-02"
        )
        assert "WHERE service_date IN (DATE '2026-11-02')" in delete

    def test_insert_reads_the_same_partition_from_bronze(self):
        _, insert = rebuild_partition_sql(
            catalog="cat", table="silver.trip_stop_predictions", partition_value="2026-11-01"
        )
        assert "FROM cat.bronze.trip_updates" in insert
        assert "WHERE service_date = DATE '2026-11-01'" in insert

    def test_insert_lists_every_target_column_in_order(self):
        from nyc_transit.streaming import ddl

        _, insert = rebuild_partition_sql(
            catalog="cat", table="silver.trip_stop_predictions", partition_value="2026-11-01"
        )
        expected = [n for n, _, _ in ddl.SILVER_TRIP_STOP_PREDICTIONS.columns]
        column_list = insert.split("(", 1)[1].split(")", 1)[0]
        assert [c.strip() for c in column_list.split(",")] == expected

    def test_refuses_a_bronze_target(self):
        with pytest.raises(UnsafeBackfillTarget):
            rebuild_partition_sql(
                catalog="cat", table="bronze.trip_updates", partition_value="2026-11-01"
            )

    def test_rejects_a_malformed_partition_value(self):
        with pytest.raises(ValueError, match="not an ISO date"):
            rebuild_partition_sql(
                catalog="cat", table="silver.vehicle_positions", partition_value="2026/11/01"
            )

    def test_vehicle_select_prefers_the_vehicle_clock(self):
        _, insert = rebuild_partition_sql(
            catalog="cat", table="silver.vehicle_positions", partition_value="2026-11-01"
        )
        assert "coalesce(vehicle_timestamp_epoch * 1000, feed_timestamp_ms)" in insert


class TestPlanBackfill:
    def test_plans_two_statements_per_table_per_partition(self):
        plan = plan_backfill(
            catalog="cat", window=PartitionWindow(date(2026, 11, 1), date(2026, 11, 2))
        )
        assert plan.partition_count() == 2
        assert len(plan.statements) == 2 * len(REBUILDABLE_TABLES) * 2

    def test_orders_partition_by_partition(self):
        """So a failure on day 17 leaves days 1-16 correctly rebuilt"""
        plan = plan_backfill(
            catalog="cat", window=PartitionWindow(date(2026, 11, 1), date(2026, 11, 3))
        )
        dates = [d for d, _, _ in plan.statements]
        assert dates == sorted(dates)

    def test_enforces_the_blast_radius_limit(self):
        with pytest.raises(ValueError, match="limit is"):
            plan_backfill(
                catalog="cat",
                window=PartitionWindow(date(2026, 1, 1), date(2026, 12, 31)),
                max_partitions=92,
            )

    def test_limit_can_be_raised_deliberately(self):
        plan = plan_backfill(
            catalog="cat",
            window=PartitionWindow(date(2026, 1, 1), date(2026, 3, 1)),
            max_partitions=200,
        )
        assert plan.partition_count() == 60

    def test_refuses_a_bronze_target(self):
        with pytest.raises(UnsafeBackfillTarget):
            plan_backfill(
                catalog="cat",
                window=PartitionWindow(date(2026, 11, 1), date(2026, 11, 1)),
                targets=("bronze.trip_updates",),
            )

    def test_render_is_inspectable_before_running(self):
        plan = plan_backfill(
            catalog="cat", window=PartitionWindow(date(2026, 11, 1), date(2026, 11, 1))
        )
        rendered = plan.render()
        assert "2026-11-01" in rendered
        assert "2 partitions" not in rendered

    def test_plan_carries_the_catalog(self):
        plan = plan_backfill(
            catalog="mycat", window=PartitionWindow(date(2026, 11, 1), date(2026, 11, 1))
        )
        assert plan.catalog == "mycat"


class FakeSpark:
    """Records every statement returns a count for count(*) queries"""

    def __init__(self, count: int = 42) -> None:
        self.statements: list[str] = []
        self._count = count

    def sql(self, statement: str):
        self.statements.append(statement)
        return self

    def collect(self):
        return [{"n": self._count}]


class TestRunBackfill:
    def _plan(self) -> BackfillPlan:
        return plan_backfill(
            catalog="cat", window=PartitionWindow(date(2026, 11, 1), date(2026, 11, 1))
        )

    def test_dry_run_executes_nothing(self):
        spark = FakeSpark()
        result = run_backfill(spark, self._plan(), dry_run=True)
        assert spark.statements == []
        assert result["dry_run"] is True

    def test_executes_every_statement(self):
        spark = FakeSpark()
        plan = self._plan()
        run_backfill(spark, plan)
        executed = [s for s in spark.statements if not s.startswith("SELECT count(*)")]
        assert executed == [sql for _, _, sql in plan.statements]

    def test_delete_precedes_insert_for_each_table(self):
        spark = FakeSpark()
        run_backfill(spark, self._plan())
        for table in REBUILDABLE_TABLES:
            deletes = [
                i
                for i, s in enumerate(spark.statements)
                if s.startswith(f"DELETE FROM cat.{table}")
            ]
            inserts = [
                i
                for i, s in enumerate(spark.statements)
                if s.startswith(f"INSERT INTO cat.{table}")
            ]
            assert deletes and inserts
            assert deletes[0] < inserts[0]

    def test_reports_row_counts_per_partition(self):
        spark = FakeSpark(count=1234)
        result = run_backfill(spark, self._plan())
        for table in REBUILDABLE_TABLES:
            assert result["rows"][table]["2026-11-01"] == 1234

    def test_count_queries_use_the_plan_catalog(self):
        spark = FakeSpark()
        plan = plan_backfill(
            catalog="othercat", window=PartitionWindow(date(2026, 11, 1), date(2026, 11, 1))
        )
        run_backfill(spark, plan)
        counts = [s for s in spark.statements if s.startswith("SELECT count(*)")]
        assert counts
        assert all("othercat." in s for s in counts)

    def test_rerunning_issues_the_same_statements(self):
        """The plan is a pure function of the window so a retry replays
        exactly the same work
        """
        first, second = FakeSpark(), FakeSpark()
        run_backfill(first, self._plan())
        run_backfill(second, self._plan())
        assert first.statements == second.statements
