"""spark transform tests and the differential test against the plain python
reference projection

marked spark so they skip when pyspark is missing
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.spark

# Format Spark's timestamps inside Spark, where the session timezone is UTC,
# rather than collecting datetimes and formatting them in Python py4j hands
# back naive datetimes converted to the driver's local zone, so a Python-side
# strftime would compare UTC against America/New_York
TS_FORMAT = "yyyy-MM-dd HH:mm:ss"


def collect_utc(df, timestamp_columns: tuple[str, ...], date_columns: tuple[str, ...] = ()):
    """Collect a DataFrame with timestamp and date columns as UTC strings"""
    from pyspark.sql import functions as F

    projected = df
    for column in timestamp_columns:
        projected = projected.withColumn(column, F.date_format(F.col(column), TS_FORMAT))
    for column in date_columns:
        projected = projected.withColumn(column, F.date_format(F.col(column), "yyyy-MM-dd"))
    return [r.asDict() for r in projected.collect()]


TRIP_TS_COLUMNS = (
    "feed_timestamp",
    "predicted_arrival_at",
    "predicted_departure_at",
    "ingest_timestamp",
)
VEHICLE_TS_COLUMNS = ("reported_at",)
ALERT_TS_COLUMNS = ("feed_timestamp", "active_period_start", "active_period_end")

_SPARK_TYPES = {
    "string": "StringType",
    "bigint": "LongType",
    "int": "IntegerType",
    "boolean": "BooleanType",
    "double": "DoubleType",
    "date": "StringType",  # to_bronze casts it
    "timestamp": "TimestampType",
}


def bronze_schema(spec):
    """A Spark schema for a bronze spec with service_date still a string"""
    from pyspark.sql import types as T

    return T.StructType(
        [
            T.StructField(name, getattr(T, _SPARK_TYPES[dtype])())
            for name, dtype, _ in spec.columns
            if name != "bronze_loaded_at"
        ]
    )


@pytest.fixture
def bronze_frame(spark, fixture_records):
    """The fixture records shaped exactly as the bronze table holds them"""
    from nyc_transit.streaming import ddl
    from nyc_transit.streaming.transforms import to_bronze

    spec = ddl.BRONZE_TRIP_UPDATES
    columns = [name for name, _, _ in spec.columns if name != "bronze_loaded_at"]
    rows = []
    for record in fixture_records:
        if record["record_type"] != "trip_update":
            continue
        row = dict(record)
        row.update(
            {"kafka_topic": "mta.gtfs.trip_updates.v1", "kafka_partition": 0, "kafka_offset": 0}
        )
        rows.append({c: row.get(c) for c in columns})

    return to_bronze(spark.createDataFrame(rows, schema=bronze_schema(spec)))


class TestToBronze:
    def test_service_date_becomes_a_date(self, bronze_frame):
        """A string partition column cannot be pruned on by Iceberg"""
        assert dict(bronze_frame.dtypes)["service_date"] == "date"

    def test_adds_a_load_timestamp(self, bronze_frame):
        assert "bronze_loaded_at" in bronze_frame.columns
        assert bronze_frame.filter("bronze_loaded_at IS NULL").count() == 0

    def test_preserves_the_wire_record_verbatim(self, bronze_frame, fixture_records):
        """Bronze is a faithful copy so a silver bug is fixable by replaying
        bronze rather than re-polling the MTA
        """
        trip_updates = [r for r in fixture_records if r["record_type"] == "trip_update"]
        rows = {r["event_id"]: r for r in bronze_frame.collect()}
        assert set(rows) == {r["event_id"] for r in trip_updates}
        for record in trip_updates:
            row = rows[record["event_id"]]
            assert row["trip_id"] == record["trip_id"]
            assert row["stop_id"] == record["stop_id"]
            assert row["arrival_time_epoch"] == record["arrival_time_epoch"]


class TestSilverTripPredictions:
    def test_matches_the_pure_python_reference_row_for_row(self, bronze_frame, fixture_records):
        """The differential test

        The projection is encoded three times PySpark for streaming SQL for
        the backfill Python as the oracle This asserts two of the three agree
        exactly the dbt test assert_prediction_horizon_matches_timestamps
        covers the third
        """
        from nyc_transit.streaming.reference import trip_prediction_row
        from nyc_transit.streaming.transforms import to_silver_trip_predictions

        spark_rows = {
            r["event_id"]: r
            for r in collect_utc(
                to_silver_trip_predictions(bronze_frame),
                TRIP_TS_COLUMNS,
                date_columns=("service_date",),
            )
        }
        trip_updates = [r for r in fixture_records if r["record_type"] == "trip_update"]
        assert len(spark_rows) == len(trip_updates)

        compared = 0
        for record in trip_updates:
            expected = trip_prediction_row(record)
            actual = spark_rows[record["event_id"]]
            for column, want in expected.items():
                assert (
                    actual[column] == want
                ), f"{column}: spark={actual[column]!r} reference={want!r}"
                compared += 1
        assert compared > 100, "differential test compared suspiciously few values"

    def test_prediction_horizon_is_arrival_minus_publication(self, bronze_frame):
        from nyc_transit.streaming.transforms import to_silver_trip_predictions

        silver = to_silver_trip_predictions(bronze_frame)
        mismatched = silver.filter(
            "predicted_arrival_at IS NOT NULL AND "
            "abs(prediction_horizon_seconds - "
            "(unix_timestamp(predicted_arrival_at) - unix_timestamp(feed_timestamp))) > 1"
        )
        assert mismatched.count() == 0

    def test_negative_horizons_are_kept_not_clamped(self, spark, bronze_frame):
        """The MTA publishes already-passed stops which is a quality signal"""
        from pyspark.sql import functions as F

        from nyc_transit.streaming.transforms import to_silver_trip_predictions

        past = bronze_frame.withColumn(
            "arrival_time_epoch", (F.col("feed_timestamp_ms") / 1000 - 300).cast("bigint")
        )
        horizons = [
            r["prediction_horizon_seconds"] for r in to_silver_trip_predictions(past).collect()
        ]
        assert horizons and all(h is not None and h < 0 for h in horizons)

    def test_schedule_relationship_becomes_booleans(self, spark, bronze_frame):
        from pyspark.sql import functions as F

        from nyc_transit.streaming.transforms import to_silver_trip_predictions

        cancelled = bronze_frame.withColumn("schedule_relationship", F.lit("CANCELED"))
        assert to_silver_trip_predictions(cancelled).filter("is_cancelled_trip").count() > 0

        added = bronze_frame.withColumn("schedule_relationship", F.lit("DUPLICATED"))
        assert to_silver_trip_predictions(added).filter("is_added_trip").count() > 0

    def test_output_columns_match_the_table_spec_exactly(self, bronze_frame):
        from nyc_transit.streaming import ddl
        from nyc_transit.streaming.transforms import to_silver_trip_predictions

        expected = [name for name, _, _ in ddl.SILVER_TRIP_STOP_PREDICTIONS.columns]
        assert to_silver_trip_predictions(bronze_frame).columns == expected


class TestSilverVehiclePositions:
    @pytest.fixture
    def vehicle_bronze(self, spark, fixture_records):
        from pyspark.sql import types as T

        from nyc_transit.streaming.transforms import to_bronze

        rows = [
            {
                "event_id": r["event_id"],
                "service_date": r["service_date"],
                "feed_id": r["feed_id"],
                "feed_timestamp_ms": r["feed_timestamp_ms"],
                "vehicle_id": r["vehicle_id"],
                "trip_id": r["trip_id"],
                "route_id": r["route_id"],
                "direction_id": r["direction_id"],
                "vehicle_timestamp_epoch": r["vehicle_timestamp_epoch"],
                "latitude": r["latitude"],
                "longitude": r["longitude"],
                "bearing": r["bearing"],
                "speed_mps": r["speed_mps"],
                "current_status": r["current_status"],
                "stop_id": r["stop_id"],
                "current_stop_sequence": r["current_stop_sequence"],
            }
            for r in fixture_records
            if r["record_type"] == "vehicle_position"
        ]
        schema = T.StructType(
            [
                T.StructField("event_id", T.StringType()),
                T.StructField("service_date", T.StringType()),
                T.StructField("feed_id", T.StringType()),
                T.StructField("feed_timestamp_ms", T.LongType()),
                T.StructField("vehicle_id", T.StringType()),
                T.StructField("trip_id", T.StringType()),
                T.StructField("route_id", T.StringType()),
                T.StructField("direction_id", T.IntegerType()),
                T.StructField("vehicle_timestamp_epoch", T.LongType()),
                T.StructField("latitude", T.DoubleType()),
                T.StructField("longitude", T.DoubleType()),
                T.StructField("bearing", T.DoubleType()),
                T.StructField("speed_mps", T.DoubleType()),
                T.StructField("current_status", T.StringType()),
                T.StructField("stop_id", T.StringType()),
                T.StructField("current_stop_sequence", T.IntegerType()),
            ]
        )
        return to_bronze(spark.createDataFrame(rows, schema=schema))

    def test_matches_the_pure_python_reference(self, vehicle_bronze, fixture_records):
        from nyc_transit.streaming.reference import vehicle_position_row
        from nyc_transit.streaming.transforms import to_silver_vehicle_positions

        actual = {
            r["event_id"]: r
            for r in collect_utc(
                to_silver_vehicle_positions(vehicle_bronze),
                VEHICLE_TS_COLUMNS,
                date_columns=("service_date",),
            )
        }
        compared = 0
        for record in (r for r in fixture_records if r["record_type"] == "vehicle_position"):
            expected = vehicle_position_row(record)
            row = actual[record["event_id"]]
            for column, want in expected.items():
                assert row[column] == want, f"{column}: spark={row[column]!r} reference={want!r}"
                compared += 1
        assert compared > 20

    def test_prefers_the_vehicle_clock_over_the_feed_clock(self, spark, vehicle_bronze):
        """Using the feed clock would make every position look fresh for as long
        as the feed kept republishing
        """
        from nyc_transit.streaming.transforms import to_silver_vehicle_positions

        rows = to_silver_vehicle_positions(vehicle_bronze).collect()
        source = {r["event_id"]: r for r in vehicle_bronze.collect()}
        for row in rows:
            assert (
                row["reported_at_ms"] == source[row["event_id"]]["vehicle_timestamp_epoch"] * 1000
            )

    def test_falls_back_to_the_feed_clock_when_the_vehicle_has_none(self, spark, vehicle_bronze):
        from pyspark.sql import functions as F

        from nyc_transit.streaming.transforms import to_silver_vehicle_positions

        no_clock = vehicle_bronze.withColumn("vehicle_timestamp_epoch", F.lit(None).cast("bigint"))
        for row in to_silver_vehicle_positions(no_clock).collect():
            assert row["reported_at_ms"] is not None

    def test_implausible_positions_are_flagged_not_dropped(self, spark, vehicle_bronze):
        """A vehicle reporting (0, 0) is real data about a broken feed"""
        from pyspark.sql import functions as F

        from nyc_transit.streaming.transforms import to_silver_vehicle_positions

        broken = vehicle_bronze.withColumn("latitude", F.lit(0.0)).withColumn(
            "longitude", F.lit(0.0)
        )
        silver = to_silver_vehicle_positions(broken)
        assert silver.count() == vehicle_bronze.count()
        assert silver.filter("position_is_plausible").count() == 0

    def test_output_columns_match_the_table_spec(self, vehicle_bronze):
        from nyc_transit.streaming import ddl
        from nyc_transit.streaming.transforms import to_silver_vehicle_positions

        expected = [name for name, _, _ in ddl.SILVER_VEHICLE_POSITIONS.columns]
        assert to_silver_vehicle_positions(vehicle_bronze).columns == expected


class TestSilverAlerts:
    @pytest.fixture
    def alert_bronze(self, spark, fixture_records):
        from nyc_transit.streaming.transforms import to_bronze

        rows = [
            {
                "event_id": r["event_id"],
                "service_date": r["service_date"],
                "feed_id": r["feed_id"],
                "feed_timestamp_ms": r["feed_timestamp_ms"],
                "entity_id": r["entity_id"],
                "alert_cause": r["alert_cause"],
                "alert_effect": r["alert_effect"],
                "header_text": r["header_text"],
                "active_periods": r["active_periods"],
                "informed_entities": r["informed_entities"],
            }
            for r in fixture_records
            if r["record_type"] == "alert"
        ]
        return to_bronze(spark.createDataFrame(rows))

    def test_explodes_to_one_row_per_informed_entity(self, alert_bronze, fixture_records):
        from nyc_transit.streaming.transforms import to_silver_alert_informed_entities

        alerts = [r for r in fixture_records if r["record_type"] == "alert"]
        expected = sum(max(len(a["informed_entities"]), 1) for a in alerts)
        assert to_silver_alert_informed_entities(alert_bronze).count() == expected

    def test_matches_the_pure_python_reference(self, alert_bronze, fixture_records):
        from nyc_transit.streaming.reference import alert_informed_entity_rows
        from nyc_transit.streaming.transforms import to_silver_alert_informed_entities

        actual = {
            (r["alert_event_id"], r["informed_ordinal"]): r
            for r in collect_utc(
                to_silver_alert_informed_entities(alert_bronze),
                ALERT_TS_COLUMNS,
                date_columns=("service_date",),
            )
        }
        compared = 0
        for record in (r for r in fixture_records if r["record_type"] == "alert"):
            for expected in alert_informed_entity_rows(record):
                row = actual[(expected["alert_event_id"], expected["informed_ordinal"])]
                for column, want in expected.items():
                    assert (
                        row[column] == want
                    ), f"{column}: spark={row[column]!r} reference={want!r}"
                    compared += 1
        assert compared > 10

    def test_an_alert_with_no_informed_entities_still_yields_a_row(self, spark, alert_bronze):
        """A system-wide alert The inner posexplode would drop it"""
        from pyspark.sql import functions as F

        from nyc_transit.streaming.transforms import to_silver_alert_informed_entities

        empty = alert_bronze.withColumn(
            "informed_entities", F.expr("filter(informed_entities, x -> false)")
        )
        rows = to_silver_alert_informed_entities(empty).collect()
        assert len(rows) == alert_bronze.count()
        assert all(r["route_id"] is None for r in rows)

    def test_output_columns_match_the_table_spec(self, alert_bronze):
        from nyc_transit.streaming import ddl
        from nyc_transit.streaming.transforms import to_silver_alert_informed_entities

        expected = [name for name, _, _ in ddl.SILVER_ALERT_INFORMED_ENTITIES.columns]
        assert to_silver_alert_informed_entities(alert_bronze).columns == expected


class TestKafkaAvroDecode:
    def test_round_trips_through_the_kafka_value_shape(self, spark, fixture_records):
        """Strip the 10-byte single-object header then from_avro

        This is where the wire format and Spark meet and getting the substring
        offset wrong produces a confusing Avro error rather than an obvious one
        """
        from pyspark.sql import types as T

        from nyc_transit.schemas import serialize
        from nyc_transit.streaming.transforms import decode_kafka_avro

        trip_updates = [r for r in fixture_records if r["record_type"] == "trip_update"]
        rows = [
            {
                "topic": "mta.gtfs.trip_updates.v1",
                "partition": 0,
                "offset": i,
                "timestamp": None,
                "value": serialize(record),
            }
            for i, record in enumerate(trip_updates)
        ]
        schema = T.StructType(
            [
                T.StructField("topic", T.StringType()),
                T.StructField("partition", T.IntegerType()),
                T.StructField("offset", T.LongType()),
                T.StructField("timestamp", T.TimestampType()),
                T.StructField("value", T.BinaryType()),
            ]
        )
        decoded = decode_kafka_avro(spark.createDataFrame(rows, schema=schema), "trip_update")
        collected = {r["event_id"]: r for r in decoded.collect()}

        assert set(collected) == {r["event_id"] for r in trip_updates}
        for record in trip_updates:
            row = collected[record["event_id"]]
            assert row["trip_id"] == record["trip_id"]
            assert row["stop_sequence"] == record["stop_sequence"]
            assert row["feed_timestamp_ms"] == record["feed_timestamp_ms"]
            assert row["kafka_offset"] is not None
            assert row["kafka_topic"] == "mta.gtfs.trip_updates.v1"
