"""iceberg ddl

pure functions returning sql strings so the statements are testable with no
SparkSession and the ddl that runs in aws can be printed and read over before it
touches a glue catalog

table property notes

format-version 2 is needed for row level deletes
on v1 a merge rewrites whole data files on every micro batch
merge-on-read means a merge touching a few rows writes a small delete file
instead of rewriting a 128 mb parquet file
the read amplification gets paid back by the weekly rewrite_data_files
write.target-file-size-bytes is a compaction target and not what a micro batch
produces
distribution-mode hash gives one file per partition per batch instead of one per
partition and task
partitioning on service_date and not an ingest date since it is the column every
backfill freshness check and query filters on
"""

from __future__ import annotations

from dataclasses import dataclass

COMMON_TABLE_PROPERTIES: dict[str, str] = {
    "format-version": "2",
    "write.format.default": "parquet",
    "write.parquet.compression-codec": "zstd",
    "write.target-file-size-bytes": "134217728",
    "write.distribution-mode": "hash",
    "write.delete.mode": "merge-on-read",
    "write.update.mode": "merge-on-read",
    "write.merge.mode": "merge-on-read",
    # Without these the metadata.json chain grows by one file per micro-batch
    # and listing the table slows down over days
    "write.metadata.delete-after-commit.enabled": "true",
    "write.metadata.previous-versions-max": "100",
    # Snapshots are the backfill's rollback path. A week is enough to roll back
    # a bad batch without unbounded storage growth
    "history.expire.max-snapshot-age-ms": str(7 * 24 * 60 * 60 * 1000),
}


@dataclass(frozen=True)
class TableSpec:
    """everything needed to create one iceberg table"""

    name: str
    namespace: str
    columns: tuple[tuple[str, str, str], ...]  # (name, type, comment)
    partition_by: tuple[str, ...]
    identifier_fields: tuple[str, ...]
    comment: str
    sort_order: tuple[str, ...] = ()
    extra_properties: dict[str, str] | None = None

    def properties(self) -> dict[str, str]:
        props = dict(COMMON_TABLE_PROPERTIES)
        if self.extra_properties:
            props.update(self.extra_properties)
        if self.identifier_fields:
            # Iceberg's row-lineage support reads this, and it documents the
            # grain in the catalog itself where a reader looks first
            props["identifier-fields"] = ",".join(self.identifier_fields)
        return props


def _escape(text: str) -> str:
    """escape a string literal for spark sql

    backslash escaping and not the sql standard doubled quote
    sparks parser ends a literal at the first unescaped quote so vehicle''s clock
    parses as vehicle followed by a syntax error
    the backslash gets escaped first or an existing backslash would escape the quote
    just added
    """
    return text.replace("\\", "\\\\").replace("'", "\\'")


def create_namespace_sql(catalog: str, namespace: str) -> str:
    return f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{namespace}"


def create_table_sql(catalog: str, spec: TableSpec, namespace: str | None = None) -> str:
    """render CREATE TABLE IF NOT EXISTS for one spec"""
    cols = ",\n".join(
        f"  {name} {dtype} COMMENT '{_escape(comment)}'" for name, dtype, comment in spec.columns
    )
    parts = [
        f"CREATE TABLE IF NOT EXISTS {catalog}.{namespace or spec.namespace}.{spec.name} (",
        cols,
        ")",
        "USING iceberg",
    ]
    if spec.partition_by:
        parts.append(f"PARTITIONED BY ({', '.join(spec.partition_by)})")
    parts.append(f"COMMENT '{_escape(spec.comment)}'")
    props = ", ".join(f"'{k}' = '{v}'" for k, v in sorted(spec.properties().items()))
    parts.append(f"TBLPROPERTIES ({props})")
    return "\n".join(parts)


def write_order_sql(catalog: str, spec: TableSpec, namespace: str | None = None) -> str | None:
    """ALTER TABLE WRITE ORDERED BY for the tables sort order

    a separate statement because iceberg hangs the sort order off table metadata and
    not off the create
    sorting on the predicate columns makes the parquet min max stats selective which
    turns a WHERE route_id = 'A' scan from a full partition read into a few files
    """
    if not spec.sort_order:
        return None
    return (
        f"ALTER TABLE {catalog}.{namespace or spec.namespace}.{spec.name} "
        f"WRITE ORDERED BY {', '.join(spec.sort_order)}"
    )


_ENVELOPE: tuple[tuple[str, str, str], ...] = (
    ("event_id", "string", "blake2b-128 business-key hash; dedup key for MERGE"),
    ("record_type", "string", "trip_update | vehicle_position | alert"),
    ("feed_id", "string", "source GTFS-RT feed, e.g. nyct-ace"),
    ("feed_timestamp_ms", "bigint", "FeedHeader.timestamp in epoch ms"),
    ("ingest_timestamp_ms", "bigint", "producer wall clock at poll time, epoch ms"),
    ("producer_id", "string", "which producer instance emitted this"),
    ("schema_version", "int", "canonical record schema version"),
    ("entity_id", "string", "FeedEntity.id as published"),
    ("is_deleted", "boolean", "FeedEntity.is_deleted (incremental feeds)"),
    ("service_date", "date", "GTFS service date in America/New_York; partition column"),
)

_TRIP_COLS: tuple[tuple[str, str, str], ...] = (
    ("trip_id", "string", "GTFS trip id"),
    ("route_id", "string", "GTFS route id"),
    ("direction_id", "int", "0/1 as published"),
    ("start_date", "string", "feed-supplied trip start date YYYY-MM-DD, nullable"),
    ("start_time", "string", "GTFS HH:MM:SS, may exceed 24:00:00"),
    ("schedule_relationship", "string", "trip-level ScheduleRelationship name"),
    ("nyct_train_id", "string", "NYCT extension, nullable when uncompiled"),
    ("nyct_direction", "string", "NYCT extension, nullable"),
    ("nyct_is_assigned", "boolean", "NYCT extension, nullable"),
)

BRONZE_TRIP_UPDATES = TableSpec(
    name="trip_updates",
    namespace="bronze",
    columns=(
        *_ENVELOPE,
        ("vehicle_id", "string", "vehicle carrying the trip, nullable"),
        ("vehicle_label", "string", "human-readable vehicle label"),
        ("trip_timestamp_epoch", "bigint", "TripUpdate.timestamp, epoch seconds"),
        ("trip_delay_seconds", "int", "trip-level delay if published"),
        ("stop_id", "string", "GTFS stop id; null for a cancellation"),
        ("stop_sequence", "int", "1-based stop sequence"),
        ("stop_schedule_relationship", "string", "SCHEDULED|SKIPPED|NO_DATA|UNSCHEDULED"),
        ("arrival_time_epoch", "bigint", "predicted arrival, epoch seconds"),
        ("arrival_delay_seconds", "int", "predicted arrival delay"),
        ("arrival_uncertainty_seconds", "int", "StopTimeEvent.uncertainty"),
        ("departure_time_epoch", "bigint", "predicted departure, epoch seconds"),
        ("departure_delay_seconds", "int", "predicted departure delay"),
        ("departure_uncertainty_seconds", "int", "StopTimeEvent.uncertainty"),
        *_TRIP_COLS,
        ("kafka_topic", "string", "lineage: source Kafka topic"),
        ("kafka_partition", "int", "lineage: source Kafka partition"),
        ("kafka_offset", "bigint", "lineage: source Kafka offset"),
        ("bronze_loaded_at", "timestamp", "when the streaming job committed this row"),
    ),
    partition_by=("service_date", "feed_id"),
    identifier_fields=("event_id",),
    sort_order=("route_id", "stop_id", "feed_timestamp_ms"),
    comment="Raw stop-level GTFS-RT arrival/departure predictions, deduplicated on event_id.",
)

BRONZE_VEHICLE_POSITIONS = TableSpec(
    name="vehicle_positions",
    namespace="bronze",
    columns=(
        *_ENVELOPE,
        ("vehicle_id", "string", "GTFS vehicle id"),
        ("vehicle_label", "string", "human-readable vehicle label"),
        ("vehicle_timestamp_epoch", "bigint", "vehicle's own clock, epoch seconds"),
        ("latitude", "double", "WGS84 latitude"),
        ("longitude", "double", "WGS84 longitude"),
        ("bearing", "double", "degrees clockwise from true north"),
        ("speed_mps", "double", "metres per second"),
        ("current_stop_sequence", "int", "stop the vehicle is approaching"),
        ("stop_id", "string", "stop the vehicle is approaching"),
        ("current_status", "string", "INCOMING_AT|STOPPED_AT|IN_TRANSIT_TO"),
        ("congestion_level", "string", "CongestionLevel name"),
        ("occupancy_status", "string", "OccupancyStatus name"),
        *_TRIP_COLS,
        ("kafka_topic", "string", "lineage: source Kafka topic"),
        ("kafka_partition", "int", "lineage: source Kafka partition"),
        ("kafka_offset", "bigint", "lineage: source Kafka offset"),
        ("bronze_loaded_at", "timestamp", "when the streaming job committed this row"),
    ),
    partition_by=("service_date", "feed_id"),
    identifier_fields=("event_id",),
    sort_order=("route_id", "vehicle_id", "vehicle_timestamp_epoch"),
    comment="Raw GTFS-RT vehicle positions, deduplicated on event_id.",
)

BRONZE_ALERTS = TableSpec(
    name="alerts",
    namespace="bronze",
    columns=(
        *_ENVELOPE,
        ("alert_cause", "string", "Alert.Cause name"),
        ("alert_effect", "string", "Alert.Effect name"),
        ("header_text", "string", "English header translation"),
        ("description_text", "string", "English description translation"),
        ("url", "string", "alert URL"),
        (
            "active_periods",
            "array<struct<start_epoch: bigint, end_epoch: bigint>>",
            "windows during which the alert applies; empty means always",
        ),
        (
            "informed_entities",
            "array<struct<agency_id: string, route_id: string, route_type: int, "
            "direction_id: int, trip_id: string, stop_id: string>>",
            "routes/stops/trips affected; exploded downstream",
        ),
        ("payload_fingerprint", "string", "hash of alert text + informed entities"),
        ("kafka_topic", "string", "lineage: source Kafka topic"),
        ("kafka_partition", "int", "lineage: source Kafka partition"),
        ("kafka_offset", "bigint", "lineage: source Kafka offset"),
        ("bronze_loaded_at", "timestamp", "when the streaming job committed this row"),
    ),
    partition_by=("service_date",),
    identifier_fields=("event_id",),
    sort_order=("entity_id", "feed_timestamp_ms"),
    comment="Raw GTFS-RT service alerts; informed_entity kept nested so one change is one row.",
)

SILVER_TRIP_STOP_PREDICTIONS = TableSpec(
    name="trip_stop_predictions",
    namespace="silver",
    columns=(
        ("event_id", "string", "carried from bronze"),
        ("service_date", "date", "GTFS service date; partition column"),
        ("feed_id", "string", "source feed"),
        ("trip_id", "string", "GTFS trip id"),
        ("route_id", "string", "GTFS route id"),
        ("direction_id", "int", "0/1"),
        ("vehicle_id", "string", "assigned vehicle, nullable"),
        ("stop_id", "string", "GTFS stop id"),
        ("stop_sequence", "int", "1-based"),
        ("feed_timestamp", "timestamp", "when the MTA published this prediction"),
        ("feed_timestamp_ms", "bigint", "same instant in epoch ms; the version column"),
        ("predicted_arrival_at", "timestamp", "predicted arrival instant"),
        ("predicted_departure_at", "timestamp", "predicted departure instant"),
        ("arrival_delay_seconds", "int", "as published"),
        ("arrival_uncertainty_seconds", "int", "as published"),
        (
            "prediction_horizon_seconds",
            "int",
            "predicted_arrival_at - feed_timestamp: how far ahead the MTA was looking",
        ),
        ("is_skipped", "boolean", "stop_schedule_relationship = SKIPPED"),
        ("is_cancelled_trip", "boolean", "trip-level schedule_relationship = CANCELED"),
        ("is_added_trip", "boolean", "trip-level schedule_relationship in (ADDED, DUPLICATED)"),
        ("ingest_timestamp", "timestamp", "producer poll time"),
        ("silver_loaded_at", "timestamp", "when this row was last merged"),
    ),
    partition_by=("service_date",),
    identifier_fields=("event_id",),
    sort_order=("route_id", "stop_id", "feed_timestamp_ms"),
    comment=(
        "Typed, deduplicated stop-level predictions. Grain: one row per "
        "(trip, stop, feed publication). Last-writer-wins on feed_timestamp_ms."
    ),
)

SILVER_VEHICLE_POSITIONS = TableSpec(
    name="vehicle_positions",
    namespace="silver",
    columns=(
        ("event_id", "string", "carried from bronze"),
        ("service_date", "date", "GTFS service date; partition column"),
        ("feed_id", "string", "source feed"),
        ("vehicle_id", "string", "GTFS vehicle id"),
        ("trip_id", "string", "trip the vehicle is running, nullable when unassigned"),
        ("route_id", "string", "GTFS route id"),
        (
            "nyct_train_id",
            "string",
            "nyct physical train id, the only unit identity the subway feed gives",
        ),
        ("direction_id", "int", "0/1"),
        ("reported_at", "timestamp", "vehicle's own timestamp, falling back to the feed's"),
        ("reported_at_ms", "bigint", "same instant in epoch ms; the version column"),
        ("latitude", "double", "WGS84"),
        ("longitude", "double", "WGS84"),
        ("bearing", "double", "degrees"),
        ("speed_mps", "double", "metres per second"),
        ("current_status", "string", "INCOMING_AT|STOPPED_AT|IN_TRANSIT_TO"),
        ("stop_id", "string", "stop being approached"),
        ("current_stop_sequence", "int", "1-based"),
        ("position_is_plausible", "boolean", "inside the NYC bounding box"),
        ("silver_loaded_at", "timestamp", "when this row was last merged"),
    ),
    partition_by=("service_date",),
    identifier_fields=("event_id",),
    sort_order=("route_id", "vehicle_id", "reported_at_ms"),
    comment="Typed, deduplicated vehicle positions with a plausibility flag.",
)

SILVER_ALERT_INFORMED_ENTITIES = TableSpec(
    name="alert_informed_entities",
    namespace="silver",
    columns=(
        ("alert_event_id", "string", "event_id of the owning bronze.alerts row"),
        ("service_date", "date", "GTFS service date; partition column"),
        ("entity_id", "string", "FeedEntity.id of the alert"),
        ("feed_timestamp", "timestamp", "when the MTA published this alert version"),
        ("feed_timestamp_ms", "bigint", "same instant in epoch ms; the version column"),
        ("informed_ordinal", "int", "position within the alert's informed_entity array"),
        ("agency_id", "string", "affected agency"),
        ("route_id", "string", "affected route, nullable"),
        ("stop_id", "string", "affected stop, nullable"),
        ("trip_id", "string", "affected trip, nullable"),
        ("direction_id", "int", "affected direction, nullable"),
        ("alert_cause", "string", "Alert.Cause name"),
        ("alert_effect", "string", "Alert.Effect name"),
        ("header_text", "string", "English header translation"),
        ("active_period_start", "timestamp", "earliest active_period start, nullable"),
        ("active_period_end", "timestamp", "latest active_period end, nullable"),
        ("silver_loaded_at", "timestamp", "when this row was last merged"),
    ),
    partition_by=("service_date",),
    identifier_fields=("alert_event_id", "informed_ordinal"),
    sort_order=("route_id", "feed_timestamp_ms"),
    comment=(
        "Alerts exploded to one row per informed entity. The explode lives in "
        "silver, not dbt, so every downstream consumer (dbt included) sees a "
        "flat table and nobody re-implements the nested-array handling."
    ),
)

# Streaming commit ledger see nyc_transit.streaming.dedup for the contract
STREAM_COMMITS = TableSpec(
    name="_stream_commits",
    namespace="bronze",
    columns=(
        ("query_name", "string", "logical streaming query name"),
        ("batch_id", "bigint", "Structured Streaming micro-batch id"),
        ("target_table", "string", "table the batch was merged into"),
        ("rows_written", "bigint", "rows the batch merged"),
        ("committed_at", "timestamp", "when the ledger row was written"),
    ),
    partition_by=("query_name",),
    identifier_fields=("query_name", "batch_id", "target_table"),
    comment=(
        "Ledger of committed micro-batches. Lets a replayed batch be skipped "
        "cheaply; correctness does not depend on it (the MERGE is idempotent)."
    ),
)

ALL_SPECS: tuple[TableSpec, ...] = (
    STREAM_COMMITS,
    BRONZE_TRIP_UPDATES,
    BRONZE_VEHICLE_POSITIONS,
    BRONZE_ALERTS,
    SILVER_TRIP_STOP_PREDICTIONS,
    SILVER_VEHICLE_POSITIONS,
    SILVER_ALERT_INFORMED_ENTITIES,
)

SPECS_BY_FQN: dict[str, TableSpec] = {f"{s.namespace}.{s.name}": s for s in ALL_SPECS}


def all_ddl(
    catalog: str,
    specs: tuple[TableSpec, ...] = ALL_SPECS,
    namespaces: dict[str, str] | None = None,
) -> list[str]:
    """every statement needed to stand the lakehouse up in dependency order

    namespaces maps a spec's layer to the namespace it really lives in
    pass LakehouseSettings.namespaces() against a glue catalog where the
    database carries a prefix leaving it out keeps the layer name which is what
    the local hadoop catalog uses
    """
    resolved = {s.namespace: (namespaces or {}).get(s.namespace, s.namespace) for s in specs}
    statements: list[str] = []
    for namespace in dict.fromkeys(resolved[s.namespace] for s in specs):
        statements.append(create_namespace_sql(catalog, namespace))
    for spec in specs:
        ns = resolved[spec.namespace]
        statements.append(create_table_sql(catalog, spec, namespace=ns))
        order = write_order_sql(catalog, spec, namespace=ns)
        if order:
            statements.append(order)
    return statements
