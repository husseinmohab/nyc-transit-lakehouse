"""plain python version of the silver projections

transforms.py is pyspark and backfill/partitions.py is sql
both encode the same projection and two encodings drift
this is a third trivial encoding that earns its place twice

tests/integration/test_spark_transforms.py runs the pyspark transform and these
functions over the same fixture and asserts the rows match so editing one and not
the other fails a test

scripts/generate_dbt_seeds.py runs the fixture through it to make the csvs that
let dbt build run with no spark kafka or aws

not used in production
three encodings is already one more than ideal and this is the slowest
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

NYC_BBOX = {"lat_min": 40.4, "lat_max": 41.1, "lon_min": -74.3, "lon_max": -73.6}


def _ts(epoch_seconds: float | None) -> str | None:
    """format an epoch as a utc YYYY-MM-DD HH:MM:SS string

    seconds precision to match sparks timestamp_seconds on an integer epoch so the
    differential test compares like with like
    """
    if epoch_seconds is None:
        return None
    return datetime.fromtimestamp(epoch_seconds, tz=UTC).strftime("%Y-%m-%d %H:%M:%S")


def trip_prediction_row(record: dict[str, Any]) -> dict[str, Any]:
    """one canonical trip_update record to one silver row"""
    feed_seconds = record["feed_timestamp_ms"] / 1000
    arrival = record.get("arrival_time_epoch")
    return {
        "event_id": record["event_id"],
        "service_date": record["service_date"],
        "feed_id": record["feed_id"],
        "trip_id": record.get("trip_id"),
        "route_id": record.get("route_id"),
        "direction_id": record.get("direction_id"),
        "vehicle_id": record.get("vehicle_id"),
        "stop_id": record.get("stop_id"),
        "stop_sequence": record.get("stop_sequence"),
        "feed_timestamp": _ts(feed_seconds),
        "feed_timestamp_ms": record["feed_timestamp_ms"],
        "predicted_arrival_at": _ts(arrival),
        "predicted_departure_at": _ts(record.get("departure_time_epoch")),
        "arrival_delay_seconds": record.get("arrival_delay_seconds"),
        "arrival_uncertainty_seconds": record.get("arrival_uncertainty_seconds"),
        # int() truncates toward zero matching Spark's CAST(... AS INT).
        "prediction_horizon_seconds": (
            int(arrival - feed_seconds) if arrival is not None else None
        ),
        "is_skipped": record.get("stop_schedule_relationship") == "SKIPPED",
        "is_cancelled_trip": record.get("schedule_relationship") == "CANCELED",
        "is_added_trip": record.get("schedule_relationship") in ("ADDED", "DUPLICATED"),
        "ingest_timestamp": _ts(record["ingest_timestamp_ms"] / 1000),
    }


def vehicle_position_row(record: dict[str, Any]) -> dict[str, Any]:
    """one canonical vehicle_position record to one silver row"""
    vehicle_epoch = record.get("vehicle_timestamp_epoch")
    reported_ms = vehicle_epoch * 1000 if vehicle_epoch is not None else record["feed_timestamp_ms"]
    lat, lon = record.get("latitude"), record.get("longitude")
    plausible = (
        lat is not None
        and lon is not None
        and NYC_BBOX["lat_min"] <= lat <= NYC_BBOX["lat_max"]
        and NYC_BBOX["lon_min"] <= lon <= NYC_BBOX["lon_max"]
    )
    return {
        "event_id": record["event_id"],
        "service_date": record["service_date"],
        "feed_id": record["feed_id"],
        "vehicle_id": record.get("vehicle_id"),
        "trip_id": record.get("trip_id"),
        "route_id": record.get("route_id"),
        "direction_id": record.get("direction_id"),
        "reported_at": _ts(reported_ms / 1000),
        "reported_at_ms": int(reported_ms),
        "latitude": lat,
        "longitude": lon,
        "bearing": record.get("bearing"),
        "speed_mps": record.get("speed_mps"),
        "current_status": record.get("current_status"),
        "stop_id": record.get("stop_id"),
        "current_stop_sequence": record.get("current_stop_sequence"),
        "position_is_plausible": bool(plausible),
    }


def alert_informed_entity_rows(record: dict[str, Any]) -> list[dict[str, Any]]:
    """one canonical alert record to one row per informed entity

    an alert with no informed entities gives one all null row to match
    posexplode_outer
    """
    periods = record.get("active_periods") or []
    starts = [p["start_epoch"] for p in periods if p.get("start_epoch") is not None]
    ends = [p["end_epoch"] for p in periods if p.get("end_epoch") is not None]

    base = {
        "alert_event_id": record["event_id"],
        "service_date": record["service_date"],
        "entity_id": record.get("entity_id"),
        "feed_timestamp": _ts(record["feed_timestamp_ms"] / 1000),
        "feed_timestamp_ms": record["feed_timestamp_ms"],
        "alert_cause": record.get("alert_cause"),
        "alert_effect": record.get("alert_effect"),
        "header_text": record.get("header_text"),
        "active_period_start": _ts(min(starts)) if starts else None,
        "active_period_end": _ts(max(ends)) if ends else None,
    }

    informed = record.get("informed_entities") or []
    if not informed:
        return [
            {
                **base,
                "informed_ordinal": None,
                "agency_id": None,
                "route_id": None,
                "stop_id": None,
                "trip_id": None,
                "direction_id": None,
            }
        ]
    return [
        {
            **base,
            "informed_ordinal": ordinal,
            "agency_id": ie.get("agency_id"),
            "route_id": ie.get("route_id"),
            "stop_id": ie.get("stop_id"),
            "trip_id": ie.get("trip_id"),
            "direction_id": ie.get("direction_id"),
        }
        for ordinal, ie in enumerate(informed)
    ]


def project_records(records: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    """project a mixed list of canonical records into the three silver shapes

    duplicates collapse the way the merge does
    highest version per key and ties broken on the key so the seeds and the lakehouse
    agree on which publication survived
    """
    trips: dict[str, dict[str, Any]] = {}
    vehicles: dict[str, dict[str, Any]] = {}
    alerts: dict[tuple[str, Any], dict[str, Any]] = {}

    for record in records:
        kind = record["record_type"]
        if kind == "trip_update":
            row = trip_prediction_row(record)
            key = row["event_id"]
            existing = trips.get(key)
            if existing is None or row["feed_timestamp_ms"] > existing["feed_timestamp_ms"]:
                trips[key] = row
        elif kind == "vehicle_position":
            row = vehicle_position_row(record)
            key = row["event_id"]
            existing = vehicles.get(key)
            if existing is None or row["reported_at_ms"] > existing["reported_at_ms"]:
                vehicles[key] = row
        elif kind == "alert":
            for row in alert_informed_entity_rows(record):
                akey = (row["alert_event_id"], row["informed_ordinal"])
                existing = alerts.get(akey)
                if existing is None or row["feed_timestamp_ms"] > existing["feed_timestamp_ms"]:
                    alerts[akey] = row

    return {
        "trip_stop_predictions": sorted(trips.values(), key=lambda r: r["event_id"]),
        "vehicle_positions": sorted(vehicles.values(), key=lambda r: r["event_id"]),
        "alert_informed_entities": sorted(
            alerts.values(),
            key=lambda r: (r["alert_event_id"], r["informed_ordinal"] or 0),
        ),
    }
