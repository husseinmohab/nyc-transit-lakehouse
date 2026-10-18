"""end to end iceberg tests

the exactly once claim actually exercised
these write to a real local iceberg warehouse through the real
IdempotentMergeWriter then replay the same micro batch and check the table did
not change
that is the whole delivery argument and it is the one claim here worth nothing
untested

marked iceberg so they skip when pyspark or the iceberg runtime jar is missing
"""

from __future__ import annotations

import pytest

pytestmark = [pytest.mark.spark, pytest.mark.iceberg]

_SPARK_TYPES = {
    "string": "StringType",
    "bigint": "LongType",
    "int": "IntegerType",
    "boolean": "BooleanType",
    "double": "DoubleType",
    "date": "StringType",
    "timestamp": "TimestampType",
}


def bronze_rows(fixture_records, *, record_type="trip_update", feed_timestamp_ms=None):
    """Fixture records projected onto a bronze column list"""
    from nyc_transit.streaming import ddl

    spec = {
        "trip_update": ddl.BRONZE_TRIP_UPDATES,
        "vehicle_position": ddl.BRONZE_VEHICLE_POSITIONS,
    }[record_type]
    columns = [n for n, _, _ in spec.columns if n != "bronze_loaded_at"]

    rows = []
    for record in fixture_records:
        if record["record_type"] != record_type:
            continue
        row = dict(record)
        row.update({"kafka_topic": "t", "kafka_partition": 0, "kafka_offset": 0})
        if feed_timestamp_ms is not None:
            row["feed_timestamp_ms"] = feed_timestamp_ms
        rows.append({c: row.get(c) for c in columns})
    return rows


def _frame(spark, rows):
    """Build a bronze-shaped DataFrame from plain dicts"""
    from pyspark.sql import types as T

    from nyc_transit.streaming import ddl

    fields = [
        T.StructField(name, getattr(T, _SPARK_TYPES[dtype])())
        for name, dtype, _ in ddl.BRONZE_TRIP_UPDATES.columns
        if name != "bronze_loaded_at"
    ]
    return spark.createDataFrame(rows, schema=T.StructType(fields))


@pytest.fixture
def trip_batch(spark, fixture_records):
    """A micro-batch DataFrame in the bronze shape"""
    return _frame(spark, bronze_rows(fixture_records))


def make_writer(spark, settings, *, with_ledger=True):
    from nyc_transit.streaming import ddl
    from nyc_transit.streaming.dedup import BRONZE_MERGE_KEY, SILVER_TRIP_MERGE_KEY
    from nyc_transit.streaming.transforms import to_bronze, to_silver_trip_predictions
    from nyc_transit.streaming.writer import IdempotentMergeWriter, MergeTarget

    bronze_columns = tuple(n for n, _, _ in ddl.BRONZE_TRIP_UPDATES.columns)
    silver_columns = tuple(n for n, _, _ in ddl.SILVER_TRIP_STOP_PREDICTIONS.columns)

    return IdempotentMergeWriter(
        spark=spark,
        query_name="test_bronze_trip_updates",
        targets=(
            MergeTarget(
                table=settings.bronze("trip_updates"),
                merge_key=BRONZE_MERGE_KEY,
                columns=bronze_columns,
                transform=to_bronze,
            ),
            MergeTarget(
                table=settings.silver("trip_stop_predictions"),
                merge_key=SILVER_TRIP_MERGE_KEY,
                columns=silver_columns,
                transform=lambda df: to_silver_trip_predictions(to_bronze(df)),
            ),
        ),
        ledger_table=settings.bronze("_stream_commits") if with_ledger else None,
    )


def count(spark, table: str) -> int:
    return spark.sql(f"SELECT count(*) AS n FROM {table}").collect()[0]["n"]


class TestDdlApplies:
    def test_every_table_is_created(self, spark, iceberg_tables):
        from nyc_transit.streaming import ddl

        catalog = iceberg_tables.catalog_name
        for spec in ddl.ALL_SPECS:
            assert count(spark, f"{catalog}.{spec.namespace}.{spec.name}") == 0

    def test_tables_are_format_version_2(self, spark, iceberg_tables):
        """v2 is what makes merge-on-read deletes possible"""
        table = iceberg_tables.bronze("trip_updates")
        properties = {
            r["key"]: r["value"] for r in spark.sql(f"SHOW TBLPROPERTIES {table}").collect()
        }
        assert properties["format-version"] == "2"
        assert properties["write.merge.mode"] == "merge-on-read"

    def test_partitioning_is_on_service_date(self, spark, iceberg_tables):
        table = iceberg_tables.bronze("trip_updates")
        text = " ".join(str(r) for r in spark.sql(f"DESCRIBE {table}").collect())
        assert "service_date" in text

    def test_ddl_is_idempotent(self, spark, iceberg_tables):
        """CREATE TABLE IF NOT EXISTS so the streaming job owns its own DDL"""
        from nyc_transit.streaming import ddl
        from nyc_transit.streaming.session import apply_ddl

        apply_ddl(spark, ddl.all_ddl(iceberg_tables.catalog_name))
        assert count(spark, iceberg_tables.bronze("trip_updates")) == 0


class TestExactlyOnce:
    def test_first_batch_inserts(self, spark, iceberg_tables, trip_batch):
        writer = make_writer(spark, iceberg_tables)
        writer(trip_batch, batch_id=0)

        expected = trip_batch.count()
        assert count(spark, iceberg_tables.bronze("trip_updates")) == expected
        assert count(spark, iceberg_tables.silver("trip_stop_predictions")) == expected

    def test_replaying_the_same_batch_changes_nothing(self, spark, iceberg_tables, trip_batch):
        """At-least-once delivery plus an idempotent MERGE gives exactly-once
        effects so a replay after a crash must be a no-op
        """
        writer = make_writer(spark, iceberg_tables)
        writer(trip_batch, batch_id=0)

        bronze = iceberg_tables.bronze("trip_updates")
        silver = iceberg_tables.silver("trip_stop_predictions")
        before_bronze, before_silver = count(spark, bronze), count(spark, silver)

        for batch_id in (0, 0, 0):
            writer(trip_batch, batch_id=batch_id)

        assert count(spark, bronze) == before_bronze
        assert count(spark, silver) == before_silver

    def test_replay_under_a_new_batch_id_is_also_a_no_op(self, spark, iceberg_tables, trip_batch):
        """The ledger is an optimisation not the guarantee

        Disable it and replay with different batch ids the MERGE alone must
        still be idempotent If this fails the correctness story depends on a
        non-atomic ledger write
        """
        writer = make_writer(spark, iceberg_tables, with_ledger=False)
        writer(trip_batch, batch_id=0)
        bronze = iceberg_tables.bronze("trip_updates")
        before = count(spark, bronze)

        writer(trip_batch, batch_id=1)
        writer(trip_batch, batch_id=2)

        assert count(spark, bronze) == before

    def test_event_ids_stay_unique_after_replay(self, spark, iceberg_tables, trip_batch):
        writer = make_writer(spark, iceberg_tables, with_ledger=False)
        for batch_id in range(3):
            writer(trip_batch, batch_id=batch_id)

        bronze = iceberg_tables.bronze("trip_updates")
        duplicates = spark.sql(
            f"SELECT event_id, count(*) AS n FROM {bronze} GROUP BY event_id HAVING count(*) > 1"
        )
        assert duplicates.count() == 0

    def test_a_later_publication_updates_in_place(self, spark, iceberg_tables, fixture_records):
        """Same business key newer feed timestamp one row newer value"""
        from pyspark.sql import functions as F

        writer = make_writer(spark, iceberg_tables, with_ledger=False)
        bronze = iceberg_tables.bronze("trip_updates")

        batch = _frame(spark, bronze_rows(fixture_records))
        writer(batch, batch_id=0)
        rows_before = count(spark, bronze)

        # A later publication of the same rows bump the version column while
        # keeping event_id which is what the WHEN MATCHED arm is for
        updated = batch.withColumn(
            "feed_timestamp_ms", F.col("feed_timestamp_ms") + F.lit(30_000)
        ).withColumn("arrival_delay_seconds", F.lit(120))
        writer(updated, batch_id=1)

        assert count(spark, bronze) == rows_before
        assert (
            spark.sql(f"SELECT max(arrival_delay_seconds) AS d FROM {bronze}").collect()[0]["d"]
            == 120
        )

    def test_an_older_publication_is_ignored(self, spark, iceberg_tables, fixture_records):
        """Out-of-order arrival must not overwrite newer data

        This is what makes the result independent of replay order and why the
        WHEN MATCHED arm uses a strict > on an event-time version
        """
        from pyspark.sql import functions as F

        writer = make_writer(spark, iceberg_tables, with_ledger=False)
        bronze = iceberg_tables.bronze("trip_updates")

        newer = _frame(spark, bronze_rows(fixture_records)).withColumn(
            "arrival_delay_seconds", F.lit(120)
        )
        writer(newer, batch_id=0)

        older = (
            _frame(spark, bronze_rows(fixture_records))
            .withColumn("feed_timestamp_ms", F.col("feed_timestamp_ms") - F.lit(60_000))
            .withColumn("arrival_delay_seconds", F.lit(-999))
        )
        writer(older, batch_id=1)

        assert (
            spark.sql(f"SELECT min(arrival_delay_seconds) AS d FROM {bronze}").collect()[0]["d"]
            == 120
        )

    def test_duplicates_within_one_batch_collapse(self, spark, iceberg_tables, fixture_records):
        """Iceberg's MERGE raises if two source rows match one target row, so
        the in-batch dedup is required not an optimisation
        """
        writer = make_writer(spark, iceberg_tables, with_ledger=False)
        rows = bronze_rows(fixture_records)
        writer(_frame(spark, rows + rows), batch_id=0)

        assert count(spark, iceberg_tables.bronze("trip_updates")) == len(rows)

    def test_in_batch_dedup_keeps_the_highest_version(self, spark, iceberg_tables, fixture_records):
        from pyspark.sql import functions as F

        writer = make_writer(spark, iceberg_tables, with_ledger=False)
        base = _frame(spark, bronze_rows(fixture_records)).withColumn(
            "arrival_delay_seconds", F.lit(10)
        )
        newer = base.withColumn(
            "feed_timestamp_ms", F.col("feed_timestamp_ms") + F.lit(30_000)
        ).withColumn("arrival_delay_seconds", F.lit(99))

        writer(base.unionByName(newer), batch_id=0)

        bronze = iceberg_tables.bronze("trip_updates")
        assert count(spark, bronze) == base.count()
        assert (
            spark.sql(f"SELECT max(arrival_delay_seconds) AS d FROM {bronze}").collect()[0]["d"]
            == 99
        )

    def test_empty_batch_is_a_no_op(self, spark, iceberg_tables, trip_batch):
        writer = make_writer(spark, iceberg_tables)
        writer(trip_batch.limit(0), batch_id=0)
        assert count(spark, iceberg_tables.bronze("trip_updates")) == 0


class TestCommitLedger:
    def test_records_a_row_per_committed_batch_and_target(self, spark, iceberg_tables, trip_batch):
        writer = make_writer(spark, iceberg_tables)
        writer(trip_batch, batch_id=0)

        ledger = iceberg_tables.bronze("_stream_commits")
        rows = spark.sql(f"SELECT * FROM {ledger} ORDER BY target_table").collect()
        assert len(rows) == 2  # bronze plus silver
        assert {r["query_name"] for r in rows} == {"test_bronze_trip_updates"}
        assert all(r["rows_written"] > 0 for r in rows)

    def test_a_replayed_batch_is_skipped_without_touching_the_table(
        self, spark, iceberg_tables, trip_batch
    ):
        writer = make_writer(spark, iceberg_tables)
        writer(trip_batch, batch_id=0)
        writer.history.clear()
        writer(trip_batch, batch_id=0)

        assert writer.history
        assert all(stat.skipped_as_replay for stat in writer.history)

    def test_a_new_batch_id_is_not_skipped(self, spark, iceberg_tables, trip_batch):
        writer = make_writer(spark, iceberg_tables)
        writer(trip_batch, batch_id=0)
        writer.history.clear()
        writer(trip_batch, batch_id=1)

        assert writer.history
        assert not any(stat.skipped_as_replay for stat in writer.history)

    def test_a_broken_ledger_degrades_to_merging_anyway(self, spark, iceberg_tables, trip_batch):
        """A missing ledger must not fail the write which would turn an
        optimisation into an outage The MERGE is idempotent regardless
        """
        from nyc_transit.streaming.writer import IdempotentMergeWriter

        writer = make_writer(spark, iceberg_tables)
        broken = IdempotentMergeWriter(
            spark=spark,
            query_name=writer.query_name,
            targets=writer.targets,
            ledger_table=f"{iceberg_tables.catalog_name}.bronze.does_not_exist",
        )
        broken(trip_batch, batch_id=0)
        assert count(spark, iceberg_tables.bronze("trip_updates")) == trip_batch.count()


class TestPartitionScopedBackfill:
    def test_backfill_reproduces_the_streaming_result(
        self, spark, iceberg_tables, trip_batch, fixture_records
    ):
        """The SQL backfill path and the PySpark streaming path must agree

        The projection is written twice (PySpark in transforms.py, SQL in
        backfill/partitions.py) and this is what keeps them honest
        """
        from nyc_transit.backfill.partitions import PartitionWindow, plan_backfill, run_backfill

        writer = make_writer(spark, iceberg_tables, with_ledger=False)
        writer(trip_batch, batch_id=0)

        silver = iceberg_tables.silver("trip_stop_predictions")
        streamed = {
            (r["event_id"], r["prediction_horizon_seconds"], r["is_skipped"])
            for r in spark.sql(
                f"SELECT event_id, prediction_horizon_seconds, is_skipped FROM {silver}"
            ).collect()
        }
        assert streamed

        service_date = spark.sql(f"SELECT min(service_date) AS d FROM {silver}").collect()[0]["d"]
        plan = plan_backfill(
            catalog=iceberg_tables.catalog_name,
            window=PartitionWindow(service_date, service_date),
            targets=("silver.trip_stop_predictions",),
        )
        run_backfill(spark, plan)

        rebuilt = {
            (r["event_id"], r["prediction_horizon_seconds"], r["is_skipped"])
            for r in spark.sql(
                f"SELECT event_id, prediction_horizon_seconds, is_skipped FROM {silver}"
            ).collect()
        }
        assert rebuilt == streamed

    def test_backfill_is_idempotent(self, spark, iceberg_tables, trip_batch):
        from nyc_transit.backfill.partitions import PartitionWindow, plan_backfill, run_backfill

        writer = make_writer(spark, iceberg_tables, with_ledger=False)
        writer(trip_batch, batch_id=0)

        silver = iceberg_tables.silver("trip_stop_predictions")
        service_date = spark.sql(f"SELECT min(service_date) AS d FROM {silver}").collect()[0]["d"]
        plan = plan_backfill(
            catalog=iceberg_tables.catalog_name,
            window=PartitionWindow(service_date, service_date),
            targets=("silver.trip_stop_predictions",),
        )

        run_backfill(spark, plan)
        after_first = count(spark, silver)
        run_backfill(spark, plan)
        run_backfill(spark, plan)

        assert count(spark, silver) == after_first

    def test_backfill_leaves_neighbouring_partitions_untouched(
        self, spark, iceberg_tables, fixture_records
    ):
        """Partition-scoped means scoped A backfill of day N must not disturb
        day N-1 which is what lets a DAG retry one date safely
        """
        from datetime import timedelta

        from pyspark.sql import functions as F

        from nyc_transit.backfill.partitions import PartitionWindow, plan_backfill, run_backfill

        writer = make_writer(spark, iceberg_tables, with_ledger=False)
        day_one = _frame(spark, bronze_rows(fixture_records))
        writer(day_one, batch_id=0)

        silver = iceberg_tables.silver("trip_stop_predictions")
        first_date = spark.sql(f"SELECT min(service_date) AS d FROM {silver}").collect()[0]["d"]
        second_date = first_date + timedelta(days=1)
        day_two = day_one.withColumn("service_date", F.lit(second_date.isoformat())).withColumn(
            "event_id", F.concat(F.col("event_id"), F.lit("-d2"))
        )
        writer(day_two, batch_id=1)

        before_day_one = spark.sql(
            f"SELECT count(*) AS n FROM {silver} WHERE service_date = DATE '{first_date}'"
        ).collect()[0]["n"]

        plan = plan_backfill(
            catalog=iceberg_tables.catalog_name,
            window=PartitionWindow(second_date, second_date),
            targets=("silver.trip_stop_predictions",),
        )
        run_backfill(spark, plan)

        after_day_one = spark.sql(
            f"SELECT count(*) AS n FROM {silver} WHERE service_date = DATE '{first_date}'"
        ).collect()[0]["n"]
        assert after_day_one == before_day_one

    def test_backfill_removes_rows_that_should_no_longer_exist(
        self, spark, iceberg_tables, trip_batch
    ):
        """Replacement not MERGE A MERGE-based backfill cannot remove a row
        that over-production put there yesterday so the bug would survive
        """
        from nyc_transit.backfill.partitions import PartitionWindow, plan_backfill, run_backfill
        from nyc_transit.streaming import ddl

        writer = make_writer(spark, iceberg_tables, with_ledger=False)
        writer(trip_batch, batch_id=0)

        silver = iceberg_tables.silver("trip_stop_predictions")
        service_date = spark.sql(f"SELECT min(service_date) AS d FROM {silver}").collect()[0]["d"]

        # Inject a bogus row bronze cannot justify clone a real row and give it
        # a new event_id cloning rather than writing a VALUES tuple because
        # Iceberg requires every column
        columns = [n for n, _, _ in ddl.SILVER_TRIP_STOP_PREDICTIONS.columns]
        projection = ", ".join("'bogus-row' AS event_id" if c == "event_id" else c for c in columns)
        spark.sql(f"INSERT INTO {silver} SELECT {projection} FROM {silver} LIMIT 1")
        assert (
            spark.sql(f"SELECT count(*) AS n FROM {silver} WHERE event_id = 'bogus-row'").collect()[
                0
            ]["n"]
            == 1
        )

        plan = plan_backfill(
            catalog=iceberg_tables.catalog_name,
            window=PartitionWindow(service_date, service_date),
            targets=("silver.trip_stop_predictions",),
        )
        run_backfill(spark, plan)

        assert (
            spark.sql(f"SELECT count(*) AS n FROM {silver} WHERE event_id = 'bogus-row'").collect()[
                0
            ]["n"]
            == 0
        )


class TestFreshnessProbeAgainstIceberg:
    def test_probe_reads_real_tables(self, spark, iceberg_tables, trip_batch):
        from nyc_transit.quality.freshness import SLAS_BY_TABLE
        from nyc_transit.quality.probe import observe_table

        writer = make_writer(spark, iceberg_tables, with_ledger=False)
        writer(trip_batch, batch_id=0)

        observation = observe_table(
            spark,
            catalog=iceberg_tables.catalog_name,
            sla=SLAS_BY_TABLE["bronze.trip_updates"],
        )
        assert observation.row_count == trip_batch.count()
        assert observation.max_event_time_epoch_seconds is not None

    def test_probe_on_a_missing_table_yields_an_empty_observation(self, spark, iceberg_tables):
        """Letting the exception escape would leave the other tables
        unchecked
        """
        from dataclasses import replace

        from nyc_transit.quality.freshness import SLAS_BY_TABLE
        from nyc_transit.quality.probe import observe_table

        sla = replace(SLAS_BY_TABLE["bronze.trip_updates"], table="bronze.nope")
        observation = observe_table(spark, catalog=iceberg_tables.catalog_name, sla=sla)
        assert observation.row_count == 0
        assert observation.max_event_time_epoch_seconds is None

    def test_partition_scoped_probe(self, spark, iceberg_tables, trip_batch):
        from nyc_transit.quality.freshness import SLAS_BY_TABLE
        from nyc_transit.quality.probe import observe_table

        writer = make_writer(spark, iceberg_tables, with_ledger=False)
        writer(trip_batch, batch_id=0)

        bronze = iceberg_tables.bronze("trip_updates")
        service_date = spark.sql(f"SELECT min(service_date) AS d FROM {bronze}").collect()[0]["d"]

        hit = observe_table(
            spark,
            catalog=iceberg_tables.catalog_name,
            sla=SLAS_BY_TABLE["bronze.trip_updates"],
            partition_value=service_date.isoformat(),
        )
        miss = observe_table(
            spark,
            catalog=iceberg_tables.catalog_name,
            sla=SLAS_BY_TABLE["bronze.trip_updates"],
            partition_value="1999-01-01",
        )
        assert hit.row_count == trip_batch.count()
        assert miss.row_count == 0
