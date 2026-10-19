"""python callables the dags use

kept in a plugin module, not inline in the dag files
the scheduler parses every dag file on a short interval so heavy imports like
pyspark and great_expectations must not happen at module scope
and a callable in a plain module is testable

so every pyspark and great_expectations import sits inside a function
"""

from __future__ import annotations

import logging
import os
from datetime import date, datetime
from typing import Any

log = logging.getLogger(__name__)


def resolve_service_date(logical_date: Any, *, lookback_days: int = 0) -> str:
    """airflow logical date to the gtfs service date to process

    for a daily dag logical_date is the start of the interval so a run at 02:00 on the
    2nd already has the 1st
    lookback_days is for the catch up dag
    comes back as an iso string since every consumer takes one
    """
    if isinstance(logical_date, str):
        parsed = datetime.fromisoformat(logical_date.replace("Z", "+00:00")).date()
    elif isinstance(logical_date, datetime):
        parsed = logical_date.date()
    elif isinstance(logical_date, date):
        parsed = logical_date
    else:
        raise TypeError(f"cannot resolve a service date from {type(logical_date)!r}")

    if lookback_days:
        from datetime import timedelta

        parsed = parsed - timedelta(days=lookback_days)
    return parsed.isoformat()


def check_streaming_queries(**_context: Any) -> dict[str, Any]:
    """supervision probe

    are the streaming queries alive and moving

    restarts nothing
    a dag that quietly restarts a crash looping stream turns a visible outage into an
    invisible one so this makes the state explicit and fails loudly and the restart
    is a human call

    health comes off each iceberg table's own snapshot history, which is durable
    and survives the driver dying, unlike the spark ui

    airflow hands the task context to every callable and this probe wants none of it
    hence **_context
    """
    import nyc_transit.streaming.jobs as jobs
    from nyc_transit.config import load_settings
    from nyc_transit.streaming.session import build_spark_session

    max_idle_minutes = int(os.environ.get("STREAM_MAX_IDLE_MINUTES", "15"))
    settings = load_settings()
    lake = settings.lakehouse

    spark = build_spark_session(lake, app_name="nyc-transit-supervisor", with_kafka=False)
    try:
        observed: dict[str, Any] = {}
        for job in jobs.JOBS:
            tables = (
                lake.bronze(job.bronze_spec.name),
                lake.silver(job.silver_spec.name),
            )
            for table in tables:
                row = spark.sql(f"""
                    SELECT committed_at,
                           CAST(summary['added-records'] AS BIGINT) AS added
                    FROM {table}.snapshots
                    ORDER BY committed_at DESC
                    LIMIT 1
                    """).collect()
                seen = observed.setdefault(job.name, {"last_commit_at": None, "added": 0})
                if not row:
                    seen["last_commit_at"] = None
                    continue
                committed_at = row[0]["committed_at"]
                seen["added"] = (seen["added"] or 0) + (row[0]["added"] or 0)
                # the staler of the two tables decides the job's health
                if seen["last_commit_at"] is None or committed_at < seen["last_commit_at"]:
                    seen["last_commit_at"] = committed_at
    finally:
        spark.stop()

    report: dict[str, Any] = {"max_idle_minutes": max_idle_minutes, "queries": {}}
    unhealthy: list[str] = []

    now = datetime.now(tz=None)
    for job in jobs.JOBS:
        row = observed.get(job.name)
        if row is None or row["last_commit_at"] is None:
            report["queries"][job.name] = {"status": "no_commits"}
            unhealthy.append(f"{job.name}: no commits recorded")
            continue
        idle_minutes = (now - row["last_commit_at"]).total_seconds() / 60
        status = "healthy" if idle_minutes <= max_idle_minutes else "stalled"
        report["queries"][job.name] = {
            "status": status,
            "idle_minutes": round(idle_minutes, 1),
            "rows_written": int(row["added"] or 0),
        }
        if status == "stalled":
            unhealthy.append(f"{job.name}: idle {idle_minutes:.0f}m")

    log.info("stream supervision report: %s", report)
    if unhealthy:
        raise RuntimeError("streaming queries unhealthy: " + "; ".join(unhealthy))
    return report


def run_freshness_gate(**context: Any) -> dict[str, Any]:
    """great expectations freshness gate

    fails the task on a CRITICAL breach
    WARNING breaches get logged and go into the xcom payload but do not fail
    the alerts table goes quiet for hours legitimately and a gate that pages on good
    news gets muted
    """
    from nyc_transit.config import load_settings
    from nyc_transit.quality.expectations import validate_freshness
    from nyc_transit.quality.probe import observe_all
    from nyc_transit.streaming.session import build_spark_session

    service_date = resolve_service_date(context["logical_date"])
    settings = load_settings()

    spark = build_spark_session(
        settings.lakehouse, app_name="nyc-transit-freshness", with_kafka=False
    )
    try:
        observations = observe_all(
            spark,
            catalog=settings.lakehouse.catalog_name,
            partition_value=service_date,
        )
    finally:
        spark.stop()

    outcome = validate_freshness(observations)
    log.info("freshness gate for %s:\n%s", service_date, outcome.render())

    payload = {
        "service_date": service_date,
        "ge_success": outcome.success,
        "checks": outcome.checks,
        "breaches": [
            {"table": b.table, "kind": b.kind, "severity": b.severity.value, "detail": b.detail}
            for b in outcome.breaches
        ],
    }
    if outcome.should_fail_dag:
        raise RuntimeError(
            "freshness SLA breached: "
            + "; ".join(f"{b.table} {b.kind}: {b.detail}" for b in outcome.critical_breaches)
        )
    return payload


def run_partition_backfill(**context: Any) -> dict[str, Any]:
    """rebuild one service dates silver partitions

    safe to re run
    one partition per dag run so an airflow retry redoes the same deterministic work
    and clearing one date cannot disturb its neighbours
    """
    from nyc_transit.backfill.partitions import PartitionWindow, plan_backfill, run_backfill
    from nyc_transit.config import load_settings
    from nyc_transit.streaming.session import build_spark_session

    lookback = int(context["params"].get("lookback_days", 0))
    service_date = resolve_service_date(context["logical_date"], lookback_days=lookback)
    target_date = date.fromisoformat(service_date)

    settings = load_settings()
    plan = plan_backfill(
        catalog=settings.lakehouse.catalog_name,
        window=PartitionWindow(target_date, target_date),
    )
    log.info("backfill plan:\n%s", plan.render())

    spark = build_spark_session(
        settings.lakehouse, app_name=f"nyc-transit-backfill-{service_date}", with_kafka=False
    )
    try:
        result = run_backfill(spark, plan, dry_run=bool(context["params"].get("dry_run", False)))
    finally:
        spark.stop()

    log.info("backfill result: %s", result)
    return result


def validate_backfilled_partition(**context: Any) -> dict[str, Any]:
    """row level ge gate on the partition just rebuilt"""
    from nyc_transit.config import load_settings
    from nyc_transit.quality.expectations import validate_partition
    from nyc_transit.streaming.session import build_spark_session

    lookback = int(context["params"].get("lookback_days", 0))
    service_date = resolve_service_date(context["logical_date"], lookback_days=lookback)
    settings = load_settings()
    table = settings.lakehouse.silver("trip_stop_predictions")

    spark = build_spark_session(
        settings.lakehouse, app_name="nyc-transit-partition-gate", with_kafka=False
    )
    try:
        # Sampled not the whole partition a day of predictions is millions of
        # rows and these expectations are distributional so a bounded sample
        # answers the question at a fraction of the driver memory the sample is
        # ordered by event_id so a failure is reproducible
        frame = spark.sql(
            f"SELECT * FROM {table} WHERE service_date = DATE '{service_date}' "
            f"ORDER BY event_id LIMIT 200000"
        ).toPandas()
    finally:
        spark.stop()

    outcome = validate_partition(frame, partition_value=service_date)
    log.info("partition gate for %s:\n%s", service_date, outcome.render())
    if outcome.should_fail_dag:
        raise RuntimeError(f"partition {service_date} failed integrity expectations")
    return {"service_date": service_date, "rows_validated": len(frame), "success": outcome.success}
