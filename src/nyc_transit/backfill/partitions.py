"""partition scoped backfill

plan sql and runner

running the backfill for 2026-11-01 twice leaves the lakehouse where running it
once did and does not touch 2026-10-31 or 2026-11-02
the unit of work is a whole partition replacement

    DELETE FROM silver.x WHERE service_date = DATE '2026-11-01'
    INSERT INTO silver.x SELECT ... FROM bronze.y WHERE service_date = DATE '...'

inside one iceberg transaction per table so a reader sees either the old
partition or the new one

replace and not merge
the streaming path merges because it gets a trickle of rows whose keys may exist
a backfill owns the whole partition and replacing it is cheaper since a partition
aligned delete drops whole data files instead of writing position deletes
it is also stronger since a merge cannot remove a row that should no longer
exist so a bug that over produced yesterday would survive

bronze is never a target
it is the replay substrate and guard_target_table enforces that in code
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any

from nyc_transit.streaming import ddl
from nyc_transit.streaming.dedup import partition_delete_sql

log = logging.getLogger(__name__)

# Only these tables may be rebuilt Bronze is absent
REBUILDABLE_TABLES: dict[str, str] = {
    "silver.trip_stop_predictions": "bronze.trip_updates",
    "silver.vehicle_positions": "bronze.vehicle_positions",
}

# Projection from bronze to silver as SQL Mirrors streaming/transforms.py
# rebuild_partition_sql explains why there are two expressions of one transform
_SILVER_TRIP_SELECT = """
SELECT
    event_id,
    service_date,
    feed_id,
    trip_id,
    route_id,
    direction_id,
    vehicle_id,
    stop_id,
    stop_sequence,
    timestamp_seconds(feed_timestamp_ms / 1000) AS feed_timestamp,
    feed_timestamp_ms,
    timestamp_seconds(arrival_time_epoch) AS predicted_arrival_at,
    timestamp_seconds(departure_time_epoch) AS predicted_departure_at,
    arrival_delay_seconds,
    arrival_uncertainty_seconds,
    CAST(arrival_time_epoch - feed_timestamp_ms / 1000 AS INT) AS prediction_horizon_seconds,
    stop_schedule_relationship = 'SKIPPED' AS is_skipped,
    schedule_relationship = 'CANCELED' AS is_cancelled_trip,
    schedule_relationship IN ('ADDED', 'DUPLICATED') AS is_added_trip,
    timestamp_seconds(ingest_timestamp_ms / 1000) AS ingest_timestamp,
    current_timestamp AS silver_loaded_at
FROM {source}
WHERE service_date = DATE '{partition_value}'
"""

_SILVER_VEHICLE_SELECT = """
SELECT
    event_id,
    service_date,
    feed_id,
    vehicle_id,
    trip_id,
    route_id,
    direction_id,
    timestamp_seconds(
        coalesce(vehicle_timestamp_epoch * 1000, feed_timestamp_ms) / 1000
    ) AS reported_at,
    CAST(coalesce(vehicle_timestamp_epoch * 1000, feed_timestamp_ms) AS BIGINT) AS reported_at_ms,
    latitude,
    longitude,
    bearing,
    speed_mps,
    current_status,
    stop_id,
    current_stop_sequence,
    latitude BETWEEN 40.4 AND 41.1
        AND longitude BETWEEN -74.3 AND -73.6 AS position_is_plausible,
    current_timestamp AS silver_loaded_at
FROM {source}
WHERE service_date = DATE '{partition_value}'
"""

_SELECTS: dict[str, str] = {
    "silver.trip_stop_predictions": _SILVER_TRIP_SELECT,
    "silver.vehicle_positions": _SILVER_VEHICLE_SELECT,
}


class UnsafeBackfillTarget(ValueError):
    """backfill was asked to rewrite a table it must not touch"""


def guard_target_table(table: str) -> str:
    """refuse any target that is not an explicitly rebuildable silver table"""
    if table not in REBUILDABLE_TABLES:
        raise UnsafeBackfillTarget(
            f"{table!r} is not rebuildable. Allowed: {sorted(REBUILDABLE_TABLES)}. "
            "Bronze is the replay substrate and is never rewritten by a backfill."
        )
    return table


@dataclass(frozen=True)
class PartitionWindow:
    """an inclusive range of service dates"""

    start: date
    end: date

    def __post_init__(self) -> None:
        if self.end < self.start:
            raise ValueError(f"window end {self.end} precedes start {self.start}")

    def dates(self) -> list[date]:
        n = (self.end - self.start).days
        return [self.start + timedelta(days=i) for i in range(n + 1)]

    def iso_dates(self) -> list[str]:
        return [d.isoformat() for d in self.dates()]

    def __len__(self) -> int:
        return (self.end - self.start).days + 1


@dataclass
class BackfillPlan:
    """what a backfill will do before it does it

    --dry-run prints this
    a backfill nobody can look at first is a backfill nobody runs on a friday
    """

    catalog: str
    window: PartitionWindow
    targets: tuple[str, ...]
    max_partitions: int
    statements: list[tuple[str, str, str]] = field(default_factory=list)  # (date, table, sql)

    def partition_count(self) -> int:
        return len(self.window)

    def render(self) -> str:
        lines = [
            f"backfill {self.window.start} .. {self.window.end} "
            f"({self.partition_count()} partitions x {len(self.targets)} tables)",
        ]
        for partition_value, table, sql in self.statements:
            first = sql.strip().splitlines()[0]
            lines.append(f"  {partition_value} {table}: {first} ...")
        return "\n".join(lines)


def plan_backfill(
    *,
    catalog: str,
    window: PartitionWindow,
    targets: tuple[str, ...] = tuple(REBUILDABLE_TABLES),
    max_partitions: int = 92,
) -> BackfillPlan:
    """build the full statement list for a backfill window

    max_partitions is a blast radius guard and not a performance limit
    a fat fingered --start 2020-01-01 would otherwise queue 2000 partition rewrites
    92 is one quarter
    """
    if len(window) > max_partitions:
        raise ValueError(
            f"window spans {len(window)} partitions, limit is {max_partitions}. "
            "Split the backfill or raise --max-partitions."
        )
    for table in targets:
        guard_target_table(table)

    plan = BackfillPlan(
        catalog=catalog, window=window, targets=targets, max_partitions=max_partitions
    )
    for partition_value in window.iso_dates():
        for table in targets:
            plan.statements.extend(
                (partition_value, table, sql)
                for sql in rebuild_partition_sql(
                    catalog=catalog, table=table, partition_value=partition_value
                )
            )
    return plan


def rebuild_partition_sql(*, catalog: str, table: str, partition_value: str) -> list[str]:
    """the delete and insert that replace one partition of one table

    returned as strings instead of executed so the statements are printable and
    testable
    the projection lives twice in this repo
    pyspark in streaming/transforms.py for the stream and sql here for the batch path
    which has to run as pure sql so it can be read from a sql console with no python
    tests/integration/test_iceberg_exactly_once.py asserts the two give identical
    rows
    """
    guard_target_table(table)
    source = REBUILDABLE_TABLES[table]
    target_fqn = f"{catalog}.{table}"
    source_fqn = f"{catalog}.{source}"

    delete = partition_delete_sql(
        target_table=target_fqn,
        partition_column="service_date",
        values=[partition_value],
    )
    select = _SELECTS[table].format(source=source_fqn, partition_value=partition_value).strip()
    spec = ddl.SPECS_BY_FQN[table]
    columns = ", ".join(name for name, _, _ in spec.columns)
    insert = f"INSERT INTO {target_fqn} ({columns})\n{select}"
    return [delete, insert]


def run_backfill(
    spark: Any,
    plan: BackfillPlan,
    *,
    dry_run: bool = False,
) -> dict[str, Any]:
    """run a plan one partition at a time

    partition at a time and not all at once
    a 30 day backfill that dies on day 17 leaves days 1-16 rebuilt and day 17
    untouched since its delete and insert are one iceberg transaction
    re running the window redoes 1-16 as equivalent replacements and carries on from
    17
    one giant statement would start over
    """
    result: dict[str, Any] = {
        "partitions": plan.partition_count(),
        "targets": list(plan.targets),
        "dry_run": dry_run,
        "applied": [],
        "rows": {},
    }
    if dry_run:
        log.info("dry run:\n%s", plan.render())
        return result

    for partition_value, table, sql in plan.statements:
        log.info("%s %s: %s", partition_value, table, sql.splitlines()[0])
        spark.sql(sql)
        result["applied"].append({"partition": partition_value, "table": table})

    # Row counts after the fact the backfill's own report of what it produced,
    # and the input to the Great Expectations gate that runs next in the DAG
    for table in plan.targets:
        counts = {}
        for partition_value in plan.window.iso_dates():
            row = spark.sql(
                f"SELECT count(*) AS n FROM {plan.catalog}.{table} "
                f"WHERE service_date = DATE '{partition_value}'"
            ).collect()[0]
            counts[partition_value] = int(row["n"])
        result["rows"][table] = counts
    return result
