"""the exactly once sql generation and its guard rails"""

from __future__ import annotations

import re

import pytest

from nyc_transit.streaming.dedup import (
    BRONZE_MERGE_KEY,
    SILVER_ALERT_MERGE_KEY,
    SILVER_TRIP_MERGE_KEY,
    SILVER_VEHICLE_MERGE_KEY,
    MergeKey,
    dedup_within_batch_sql,
    merge_sql,
    partition_delete_sql,
)


class TestMergeKey:
    def test_requires_at_least_one_key_column(self):
        with pytest.raises(ValueError, match="at least one key column"):
            MergeKey(key_columns=(), version_column="v")

    def test_version_column_cannot_also_be_a_key(self):
        """A key is immutable a version changes One column cannot be both"""
        with pytest.raises(ValueError, match="cannot also be a key column"):
            MergeKey(key_columns=("id", "v"), version_column="v")

    def test_project_merge_keys_use_event_time_versions(self):
        """Never a processing-time version that would break idempotence

        If the version came from the clock replaying a batch would always win
        against the stored row and rewrite it so the result would depend on
        replay order
        """
        for key in (BRONZE_MERGE_KEY, SILVER_TRIP_MERGE_KEY, SILVER_ALERT_MERGE_KEY):
            assert key.version_column == "feed_timestamp_ms"
        assert SILVER_VEHICLE_MERGE_KEY.version_column == "reported_at_ms"

    def test_alert_key_includes_the_explode_ordinal(self):
        """Otherwise every informed entity of one alert collides"""
        assert "informed_ordinal" in SILVER_ALERT_MERGE_KEY.key_columns


class TestMergeSql:
    COLUMNS = ("event_id", "feed_timestamp_ms", "route_id", "stop_id")

    def _sql(self, **overrides):
        kwargs = {
            "target_table": "cat.bronze.trip_updates",
            "source_view": "batch_view",
            "merge_key": BRONZE_MERGE_KEY,
            "update_columns": ("feed_timestamp_ms", "route_id", "stop_id"),
            "insert_columns": self.COLUMNS,
        }
        kwargs.update(overrides)
        return merge_sql(**kwargs)

    def test_matches_on_the_key_columns(self):
        assert "ON t.event_id = s.event_id" in self._sql()

    def test_update_arm_is_guarded_by_a_strict_greater_than(self):
        """Strict > makes a replay free

        With >= a replayed batch would rewrite every matched row emitting a
        delete file plus a data file per micro-batch for no change at all
        """
        sql = self._sql()
        assert "s.feed_timestamp_ms > t.feed_timestamp_ms" in sql
        assert ">=" not in sql

    def test_has_an_insert_arm_for_new_keys(self):
        assert "WHEN NOT MATCHED THEN INSERT" in self._sql()

    def test_refuses_to_update_key_columns(self):
        """Updating a key in place silently changes a row's identity."""
        with pytest.raises(ValueError, match="refusing to UPDATE key columns"):
            self._sql(update_columns=("event_id", "route_id"))

    def test_refuses_an_insert_list_missing_a_key_column(self):
        with pytest.raises(ValueError, match="missing key columns"):
            self._sql(insert_columns=("route_id", "stop_id", "feed_timestamp_ms"))

    def test_composite_keys_are_anded(self):
        key = MergeKey(key_columns=("a", "b"), version_column="v")
        sql = merge_sql(
            target_table="t",
            source_view="s",
            merge_key=key,
            update_columns=("v",),
            insert_columns=("a", "b", "v"),
        )
        assert "ON t.a = s.a AND t.b = s.b" in sql

    def test_no_delete_arm(self):
        """A streaming MERGE must never delete

        Kafka can replay a batch and a WHEN NOT MATCHED BY SOURCE DELETE arm
        would wipe every row not in that one micro-batch
        """
        assert "DELETE" not in self._sql().upper()


class TestDedupWithinBatch:
    def test_keeps_the_highest_version_per_key(self):
        sql = dedup_within_batch_sql(
            source_view="v",
            merge_key=BRONZE_MERGE_KEY,
            select_columns=("event_id", "feed_timestamp_ms"),
        )
        assert "ROW_NUMBER() OVER" in sql
        assert "PARTITION BY event_id" in sql
        assert "ORDER BY feed_timestamp_ms DESC" in sql
        assert "WHERE _rn = 1" in sql

    def test_tiebreaks_deterministically_on_the_key(self):
        """Two rows with the same version must resolve the same way on replay"""
        sql = dedup_within_batch_sql(
            source_view="v",
            merge_key=BRONZE_MERGE_KEY,
            select_columns=("event_id", "feed_timestamp_ms"),
        )
        order_by = re.search(r"ORDER BY\s+([^\n]+)", sql).group(1)
        assert "feed_timestamp_ms DESC" in order_by
        assert "event_id" in order_by

    def test_partitions_by_every_key_column(self):
        sql = dedup_within_batch_sql(
            source_view="v",
            merge_key=SILVER_ALERT_MERGE_KEY,
            select_columns=("alert_event_id", "informed_ordinal", "feed_timestamp_ms"),
        )
        assert "PARTITION BY alert_event_id, informed_ordinal" in sql


class TestPartitionDelete:
    def test_builds_a_scoped_delete(self):
        sql = partition_delete_sql(
            target_table="cat.silver.trip_stop_predictions",
            partition_column="service_date",
            values=["2026-11-01"],
        )
        assert sql == (
            "DELETE FROM cat.silver.trip_stop_predictions "
            "WHERE service_date IN (DATE '2026-11-01')"
        )

    def test_refuses_an_empty_value_list(self):
        """An unscoped DELETE would truncate the table"""
        with pytest.raises(ValueError, match="no partition values"):
            partition_delete_sql(target_table="t", partition_column="d", values=[])

    @pytest.mark.parametrize(
        "bad",
        [
            "2026-11-1",
            "20261101",
            "2026/11/01",
            "' OR 1=1 --",
            "2026-11-01' OR '1'='1",
            "",
            "not-a-date",
        ],
    )
    def test_rejects_anything_that_is_not_an_iso_date(self, bad):
        """This builds a DELETE so a malformed value reaching the SQL could
        destroy data
        """
        with pytest.raises(ValueError, match="not an ISO date"):
            partition_delete_sql(target_table="t", partition_column="d", values=[bad])

    def test_rejects_a_bad_value_even_when_mixed_with_good_ones(self):
        with pytest.raises(ValueError):
            partition_delete_sql(
                target_table="t",
                partition_column="d",
                values=["2026-11-01", "'; DROP TABLE x; --"],
            )

    def test_accepts_multiple_dates(self):
        sql = partition_delete_sql(
            target_table="t", partition_column="d", values=["2026-11-01", "2026-11-02"]
        )
        assert "DATE '2026-11-01', DATE '2026-11-02'" in sql
