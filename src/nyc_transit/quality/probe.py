"""turn lakehouse tables into FreshnessObservations

kept apart from expectations.py so the sql that reads iceberg sits in one small
file
the expectations and the sla logic are then testable with hand built observations
and no spark
"""

from __future__ import annotations

import logging
import time
from typing import Any

from nyc_transit.quality.freshness import FRESHNESS_SLAS, FreshnessObservation, FreshnessSLA

log = logging.getLogger(__name__)

# Columns whose unit is epoch milliseconds rather than a timestamp type
_EPOCH_MS_COLUMNS = frozenset({"feed_timestamp_ms", "ingest_timestamp_ms", "reported_at_ms"})


def observation_sql(
    *,
    catalog: str,
    sla: FreshnessSLA,
    partition_column: str = "service_date",
    partition_value: str | None = None,
) -> str:
    """sql giving one row of row_count and max_event_time_epoch_seconds

    the event time column gets normalised to epoch seconds in sql and not in python
    so a timestamp column and an epoch ms column come out the same shape and the
    caller needs no per table branching

    the partition filter is what makes this cheap
    on a partitioned iceberg table it prunes to one partitions metadata so the probe
    is a metadata read and not a scan
    """
    column = sla.event_time_column
    if column in _EPOCH_MS_COLUMNS:
        event_expr = f"max({column}) / 1000.0"
    else:
        event_expr = f"max(unix_timestamp({column}))"

    where = ""
    if partition_value is not None:
        if len(partition_value) != 10 or partition_value[4] != "-":
            raise ValueError(f"partition_value {partition_value!r} is not an ISO date")
        where = f"WHERE {partition_column} = DATE '{partition_value}'"

    return (
        f"SELECT count(*) AS row_count, {event_expr} AS max_event_time_epoch_seconds "
        f"FROM {catalog}.{sla.table} {where}"
    ).strip()


def observe_table(
    spark: Any,
    *,
    catalog: str,
    sla: FreshnessSLA,
    partition_value: str | None = None,
    clock: Any = time.time,
) -> FreshnessObservation:
    """probe one table

    a missing table gives an empty observation instead of raising
    a table that does not exist is a real condition and evaluate_freshness reports it
    as empty so the dag fails on it
    letting the exception escape would abort the probe and leave the rest unchecked
    """
    sql = observation_sql(catalog=catalog, sla=sla, partition_value=partition_value)
    try:
        row = spark.sql(sql).collect()[0]
        row_count = int(row["row_count"])
        max_event = row["max_event_time_epoch_seconds"]
    except Exception as exc:  # noqa: BLE001
        log.warning("probe failed for %s: %s", sla.table, exc)
        row_count, max_event = 0, None

    return FreshnessObservation(
        table=sla.table,
        partition_value=partition_value,
        row_count=row_count,
        max_event_time_epoch_seconds=float(max_event) if max_event is not None else None,
        observed_at_epoch_seconds=float(clock()),
    )


def observe_all(
    spark: Any,
    *,
    catalog: str,
    slas: tuple[FreshnessSLA, ...] = FRESHNESS_SLAS,
    partition_value: str | None = None,
    clock: Any = time.time,
) -> list[FreshnessObservation]:
    return [
        observe_table(spark, catalog=catalog, sla=sla, partition_value=partition_value, clock=clock)
        for sla in slas
    ]
