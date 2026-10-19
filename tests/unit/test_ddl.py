"""iceberg ddl generation"""

from __future__ import annotations

import pytest

from nyc_transit.streaming import ddl


class TestTableProperties:
    def test_format_version_is_2_for_row_level_deletes(self):
        """On v1 a MERGE rewrites whole data files"""
        assert ddl.COMMON_TABLE_PROPERTIES["format-version"] == "2"

    def test_writes_are_merge_on_read(self):
        for key in ("write.delete.mode", "write.update.mode", "write.merge.mode"):
            assert ddl.COMMON_TABLE_PROPERTIES[key] == "merge-on-read"

    def test_metadata_growth_is_bounded(self):
        """Without this the metadata.json chain grows once per micro-batch"""
        props = ddl.COMMON_TABLE_PROPERTIES
        assert props["write.metadata.delete-after-commit.enabled"] == "true"
        assert int(props["write.metadata.previous-versions-max"]) > 0

    def test_snapshot_retention_covers_a_week(self):
        """Snapshots are the backfill's rollback path."""
        assert int(ddl.COMMON_TABLE_PROPERTIES["history.expire.max-snapshot-age-ms"]) == (
            7 * 24 * 60 * 60 * 1000
        )

    def test_distribution_mode_is_hash(self):
        assert ddl.COMMON_TABLE_PROPERTIES["write.distribution-mode"] == "hash"


class TestSpecs:
    def test_every_spec_declares_identifier_fields(self):
        for spec in ddl.ALL_SPECS:
            assert spec.identifier_fields, f"{spec.name} has no identifier fields"

    def test_identifier_fields_exist_as_columns(self):
        for spec in ddl.ALL_SPECS:
            columns = {name for name, _, _ in spec.columns}
            missing = set(spec.identifier_fields) - columns
            assert not missing, f"{spec.name} identifies on missing columns {missing}"

    def test_partition_columns_exist_as_columns(self):
        for spec in ddl.ALL_SPECS:
            columns = {name for name, _, _ in spec.columns}
            for part in spec.partition_by:
                assert part in columns, f"{spec.name} partitions on unknown column {part}"

    def test_sort_order_columns_exist_as_columns(self):
        for spec in ddl.ALL_SPECS:
            columns = {name for name, _, _ in spec.columns}
            missing = set(spec.sort_order) - columns
            assert not missing, f"{spec.name} sorts on missing columns {missing}"

    def test_every_data_table_partitions_on_service_date(self):
        """Partition-scoped backfill depends on this"""
        for spec in ddl.ALL_SPECS:
            assert "service_date" in spec.partition_by, spec.name

    def test_every_column_has_a_comment(self):
        for spec in ddl.ALL_SPECS:
            for name, _, comment in spec.columns:
                assert comment.strip(), f"{spec.name}.{name} has no comment"

    def test_every_spec_has_a_table_comment(self):
        for spec in ddl.ALL_SPECS:
            assert spec.comment.strip()

    def test_silver_columns_are_a_subset_of_what_bronze_can_provide(self):
        """A silver column with no bronze origin means the backfill cannot
        rebuild it
        """
        bronze_columns = set()
        for spec in (ddl.BRONZE_TRIP_UPDATES, ddl.BRONZE_VEHICLE_POSITIONS, ddl.BRONZE_ALERTS):
            bronze_columns |= {name for name, _, _ in spec.columns}
        # Derived columns are computed not carried Listing them explicitly
        # means a new unexplained silver column fails this test
        derived = {
            "feed_timestamp",
            "predicted_arrival_at",
            "predicted_departure_at",
            "prediction_horizon_seconds",
            "is_skipped",
            "is_cancelled_trip",
            "is_added_trip",
            "ingest_timestamp",
            "silver_loaded_at",
            "reported_at",
            "reported_at_ms",
            "position_is_plausible",
            "alert_event_id",
            "informed_ordinal",
            "agency_id",
            "active_period_start",
            "active_period_end",
        }
        for spec in (
            ddl.SILVER_TRIP_STOP_PREDICTIONS,
            ddl.SILVER_VEHICLE_POSITIONS,
            ddl.SILVER_ALERT_INFORMED_ENTITIES,
        ):
            for name, _, _ in spec.columns:
                assert name in bronze_columns or name in derived, f"{spec.name}.{name}"


class TestRenderedSql:
    def test_create_table_is_idempotent(self):
        sql = ddl.create_table_sql("cat", ddl.BRONZE_TRIP_UPDATES)
        assert sql.startswith("CREATE TABLE IF NOT EXISTS cat.bronze.trip_updates")

    def test_uses_iceberg(self):
        assert "USING iceberg" in ddl.create_table_sql("cat", ddl.BRONZE_ALERTS)

    def test_partition_clause_is_rendered(self):
        sql = ddl.create_table_sql("cat", ddl.BRONZE_TRIP_UPDATES)
        assert "PARTITIONED BY (service_date, feed_id)" in sql

    def test_properties_are_rendered_and_sorted(self):
        sql = ddl.create_table_sql("cat", ddl.BRONZE_ALERTS)
        assert "'format-version' = '2'" in sql
        assert "'identifier-fields' = 'event_id'" in sql

    def test_comments_with_apostrophes_are_escaped(self):
        spec = ddl.TableSpec(
            name="t",
            namespace="bronze",
            columns=(("c", "string", "the MTA's own id"),),
            partition_by=(),
            identifier_fields=("c",),
            comment="it's fine",
        )
        sql = ddl.create_table_sql("cat", spec)
        # Backslash not a doubled quote Spark's parser does not accept ''.
        assert r"the MTA\'s own id" in sql
        assert r"it\'s fine" in sql

    def test_backslashes_are_escaped_before_quotes(self):
        from nyc_transit.streaming.ddl import _escape

        assert _escape(r"a\b") == r"a\\b"
        assert _escape("it's") == r"it\'s"

    def test_write_order_is_a_separate_statement(self):
        sql = ddl.write_order_sql("cat", ddl.BRONZE_TRIP_UPDATES)
        assert sql.startswith("ALTER TABLE cat.bronze.trip_updates WRITE ORDERED BY")

    def test_no_write_order_for_specs_without_one(self):
        spec = ddl.TableSpec(
            name="t",
            namespace="bronze",
            columns=(("c", "string", "x"),),
            partition_by=(),
            identifier_fields=("c",),
            comment="x",
        )
        assert ddl.write_order_sql("cat", spec) is None

    def test_all_ddl_creates_namespaces_before_tables(self):
        statements = ddl.all_ddl("cat")
        first_table = next(i for i, s in enumerate(statements) if s.startswith("CREATE TABLE"))
        namespaces = [s for s in statements[:first_table] if s.startswith("CREATE NAMESPACE")]
        assert {"bronze", "silver"} <= {s.rsplit(".", 1)[-1] for s in namespaces}

    def test_all_ddl_covers_every_spec(self):
        statements = ddl.all_ddl("cat")
        for spec in ddl.ALL_SPECS:
            assert any(f"cat.{spec.namespace}.{spec.name} (" in s for s in statements)

    @pytest.mark.parametrize("spec", ddl.ALL_SPECS, ids=lambda s: s.name)
    def test_every_spec_renders_without_error(self, spec):
        sql = ddl.create_table_sql("cat", spec)
        assert sql.count("(") >= 1
        assert "TBLPROPERTIES" in sql
