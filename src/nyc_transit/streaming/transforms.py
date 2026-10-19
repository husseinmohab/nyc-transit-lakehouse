"""spark transforms

kafka value to bronze and bronze to silver
every function takes and returns a DataFrame and does no io so the whole
medallion shaping gets exercised by pytest -m spark against small in memory
frames
no kafka no iceberg no cluster
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro

from nyc_transit.schemas import HEADER_BYTES, raw_schema_json

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame

# NYC bounding box used only to flag implausible coordinates rows are never
# dropped on it a vehicle reporting (0, 0) is real data about a broken feed
NYC_BBOX = {"lat_min": 40.4, "lat_max": 41.1, "lon_min": -74.3, "lon_max": -73.6}


def decode_kafka_avro(kafka_df: DataFrame, record_type: str) -> DataFrame:
    """decode the kafka value column from avro single object encoding

    the 10 byte header is 2 marker bytes plus 8 fingerprint bytes and gets stripped
    before from_avro which wants a bare schemaless body
    substring on a binary column is 1 indexed hence HEADER_BYTES + 1

    mode=FAILFAST so a corrupt payload fails the micro batch and the offending offset
    shows up in the driver log
    PERMISSIVE would write a row of nulls into bronze and the problem would surface
    days later as an oddly empty partition
    """
    schema_json = raw_schema_json(record_type)
    body = F.expr(f"substring(value, {HEADER_BYTES + 1}, length(value) - {HEADER_BYTES})")
    return kafka_df.select(
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        from_avro(body, schema_json, {"mode": "FAILFAST"}).alias("r"),
    ).select("kafka_topic", "kafka_partition", "kafka_offset", "kafka_timestamp", "r.*")


def to_bronze(decoded_df: DataFrame) -> DataFrame:
    """shape a decoded batch for a bronze table

    two changes off the wire record
    service_date becomes a real date so iceberg can partition and prune on it and a
    bronze_loaded_at timestamp gets attached
    everything else is kept so a bug in silver is fixable by replaying bronze instead
    of re polling the mta
    """
    return decoded_df.withColumn("service_date", F.to_date("service_date")).withColumn(
        "bronze_loaded_at", F.current_timestamp()
    )


def to_silver_trip_predictions(bronze_df: DataFrame) -> DataFrame:
    """bronze.trip_updates to silver.trip_stop_predictions

    epoch columns become timestamps
    the schedule relationship enums become booleans at the grain people filter on
    prediction_horizon_seconds gets derived here since it feeds every how good are
    the predictions question and working it out ad hoc invites three definitions
    """
    feed_ts = F.col("feed_timestamp_ms") / F.lit(1000)
    return bronze_df.select(
        F.col("event_id"),
        F.col("service_date"),
        F.col("feed_id"),
        F.col("trip_id"),
        F.col("route_id"),
        F.col("direction_id"),
        F.col("vehicle_id"),
        F.col("stop_id"),
        F.col("stop_sequence"),
        F.timestamp_seconds(feed_ts).alias("feed_timestamp"),
        F.col("feed_timestamp_ms"),
        F.timestamp_seconds(F.col("arrival_time_epoch")).alias("predicted_arrival_at"),
        F.timestamp_seconds(F.col("departure_time_epoch")).alias("predicted_departure_at"),
        F.col("arrival_delay_seconds"),
        F.col("arrival_uncertainty_seconds"),
        # Negative horizons are kept not clamped the MTA does publish
        # predictions for stops whose time has already passed and that is a
        # measurable quality signal rather than bad data
        F.when(
            F.col("arrival_time_epoch").isNotNull(),
            (F.col("arrival_time_epoch") - feed_ts).cast("int"),
        ).alias("prediction_horizon_seconds"),
        (F.col("stop_schedule_relationship") == F.lit("SKIPPED")).alias("is_skipped"),
        (F.col("schedule_relationship") == F.lit("CANCELED")).alias("is_cancelled_trip"),
        F.col("schedule_relationship").isin("ADDED", "DUPLICATED").alias("is_added_trip"),
        F.timestamp_seconds(F.col("ingest_timestamp_ms") / F.lit(1000)).alias("ingest_timestamp"),
        F.current_timestamp().alias("silver_loaded_at"),
    )


def to_silver_vehicle_positions(bronze_df: DataFrame) -> DataFrame:
    """bronze.vehicle_positions to silver.vehicle_positions

    reported_at prefers the vehicles own clock and falls back to the feeds
    that order matters since the vehicle timestamp is what decides whether a position
    is stale and using the feed timestamp would make every position look fresh for as
    long as the feed kept republishing
    """
    reported_ms = F.coalesce(
        F.col("vehicle_timestamp_epoch") * F.lit(1000), F.col("feed_timestamp_ms")
    )
    return bronze_df.select(
        F.col("event_id"),
        F.col("service_date"),
        F.col("feed_id"),
        F.col("vehicle_id"),
        F.col("trip_id"),
        F.col("route_id"),
        F.col("nyct_train_id"),
        F.col("direction_id"),
        F.timestamp_seconds(reported_ms / F.lit(1000)).alias("reported_at"),
        reported_ms.cast("bigint").alias("reported_at_ms"),
        F.col("latitude"),
        F.col("longitude"),
        F.col("bearing"),
        F.col("speed_mps"),
        F.col("current_status"),
        F.col("stop_id"),
        F.col("current_stop_sequence"),
        (
            F.col("latitude").between(NYC_BBOX["lat_min"], NYC_BBOX["lat_max"])
            & F.col("longitude").between(NYC_BBOX["lon_min"], NYC_BBOX["lon_max"])
        ).alias("position_is_plausible"),
        F.current_timestamp().alias("silver_loaded_at"),
    )


def to_silver_alert_informed_entities(bronze_df: DataFrame) -> DataFrame:
    """bronze.alerts to silver.alert_informed_entities

    one row per informed entity
    posexplode_outer and not posexplode since an alert with an empty informed_entity
    array is a system wide alert and the inner variant would drop it
    the ordinal is part of the merge key so re merging the same alert overwrites row
    n instead of appending a copy

    active_period collapses to its min start and max end so an alert with disjoint
    windows loses the gaps
    the column answers when is this relevant at all and the full array stays in
    bronze
    """
    exploded = bronze_df.select(
        F.col("event_id").alias("alert_event_id"),
        F.col("service_date"),
        F.col("entity_id"),
        F.col("feed_timestamp_ms"),
        F.col("alert_cause"),
        F.col("alert_effect"),
        F.col("header_text"),
        F.expr(
            "aggregate(active_periods, cast(null as bigint), "
            "(acc, x) -> least(coalesce(acc, x.start_epoch), coalesce(x.start_epoch, acc)))"
        ).alias("active_period_start_epoch"),
        F.expr(
            "aggregate(active_periods, cast(null as bigint), "
            "(acc, x) -> greatest(coalesce(acc, x.end_epoch), coalesce(x.end_epoch, acc)))"
        ).alias("active_period_end_epoch"),
        F.posexplode_outer("informed_entities").alias("informed_ordinal", "ie"),
    )
    return exploded.select(
        "alert_event_id",
        "service_date",
        "entity_id",
        F.timestamp_seconds(F.col("feed_timestamp_ms") / F.lit(1000)).alias("feed_timestamp"),
        "feed_timestamp_ms",
        F.col("informed_ordinal").cast("int").alias("informed_ordinal"),
        F.col("ie.agency_id").alias("agency_id"),
        F.col("ie.route_id").alias("route_id"),
        F.col("ie.stop_id").alias("stop_id"),
        F.col("ie.trip_id").alias("trip_id"),
        F.col("ie.direction_id").alias("direction_id"),
        "alert_cause",
        "alert_effect",
        "header_text",
        F.timestamp_seconds(F.col("active_period_start_epoch")).alias("active_period_start"),
        F.timestamp_seconds(F.col("active_period_end_epoch")).alias("active_period_end"),
        F.current_timestamp().alias("silver_loaded_at"),
    )
