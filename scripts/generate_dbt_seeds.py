#!/usr/bin/env python
"""make the dbt seed csvs that let dbt build run locally

the seeds stand in for the iceberg silver tables when dbt runs on duckdb
they come out of running the committed gtfs rt fixture through the real decoder
the real normalizer and the plain python reference projection so the local dbt
run sees data with the same shape nullability and quirks as production including
skipped stops cancelled trips and duplicate publications

deterministic with a fixed seed and fixed timestamps
make seeds regenerates them and the result has to be byte identical which ci
checks with git diff --exit-code

run with python scripts/generate_dbt_seeds.py
"""

from __future__ import annotations

import argparse
import csv
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from nyc_transit.gtfs.decoder import decode_feed  # noqa: E402
from nyc_transit.gtfs.feeds import FEEDS_BY_ID  # noqa: E402
from nyc_transit.gtfs.normalize import normalize_feed_message  # noqa: E402
from nyc_transit.gtfs.static_reference import ROUTES, STOPS, routes_with_stops  # noqa: E402
from nyc_transit.gtfs.synthetic import SyntheticFeedConfig, SyntheticFeedGenerator  # noqa: E402
from nyc_transit.streaming.reference import project_records  # noqa: E402

SEED_DIR = REPO_ROOT / "dbt" / "nyc_transit" / "seeds"

# Two service dates so the incremental models' lookback window and the
# partition-scoped tests have more than one partition to reason about
BASE_EPOCH = 1_793_536_000  # 2026-11-01T13:46:40Z
SERVICE_DATES = ("20261101", "20261102")

# Several feed_ids matter some tests assert a grain is unique within a feed and
# that cross-feed trip ids do not collide
SEED_FEEDS = ("nyct-ace", "nyct-numeric", "nyct-l")


def routes_for_feed(feed_id: str) -> tuple[str, ...]:
    """routes a feed carries limited to ones with curated stops

    real feeds carry a fixed set of routes since nyct-l only carries the L
    honouring that keeps the seeded feed_id to route_id relationship realistic which
    several dbt tests lean on
    """
    hint = FEEDS_BY_ID[feed_id].route_hint
    known = set(routes_with_stops())
    routes = tuple(r for r in hint if r in known)
    return routes or routes_with_stops()


def build_records() -> list[dict]:
    """decode and normalize the synthetic feeds for every feed and service date"""
    records: list[dict] = []
    for day_offset, service_date in enumerate(SERVICE_DATES):
        day_epoch = BASE_EPOCH + day_offset * 86_400
        for feed_index, feed_id in enumerate(SEED_FEEDS):
            generator = SyntheticFeedGenerator(
                SyntheticFeedConfig(
                    trips_per_message=14,
                    stops_per_trip=(6, 14),
                    vehicles_per_message=10,
                    alerts_per_message=2,
                    duplicate_fraction=0.10,
                    cancellation_fraction=0.05,
                    # Varies per (day, feed) so the two days differ but fixed
                    # so the output is reproducible
                    seed=98765 + day_offset * 100 + feed_index,
                    routes=routes_for_feed(feed_id),
                )
            )
            # Three polls per feed 30s apart this produces same-key
            # republications across micro-batches which is what the dedup and
            # SCD2 logic downstream has to cope with
            for poll in range(3):
                feed_epoch = day_epoch + poll * 30
                message = generator.build_feed_message(
                    feed_timestamp_epoch=feed_epoch,
                    service_date_yyyymmdd=service_date,
                )
                records.extend(
                    normalize_feed_message(
                        decode_feed(message.SerializeToString(), feed_id=feed_id),
                        feed_id=feed_id,
                        ingest_timestamp_ms=(feed_epoch + 2) * 1000,
                        producer_id="seed-generator",
                    )
                )
    return records


def _render(value: object) -> str:
    """csv rendering with an explicit null convention

    None becomes the empty string and booleans become true or false
    duckdb and spark both read those back as NULL and BOOLEAN so one seed file works
    against either dbt target
    """
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def write_csv(path: Path, rows: list[dict], columns: list[str]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow({c: _render(row.get(c)) for c in columns})
    return len(rows)


TRIP_COLUMNS = [
    "event_id",
    "service_date",
    "feed_id",
    "trip_id",
    "route_id",
    "direction_id",
    "vehicle_id",
    "stop_id",
    "stop_sequence",
    "feed_timestamp",
    "feed_timestamp_ms",
    "predicted_arrival_at",
    "predicted_departure_at",
    "arrival_delay_seconds",
    "arrival_uncertainty_seconds",
    "prediction_horizon_seconds",
    "is_skipped",
    "is_cancelled_trip",
    "is_added_trip",
    "ingest_timestamp",
]

VEHICLE_COLUMNS = [
    "event_id",
    "service_date",
    "feed_id",
    "vehicle_id",
    "trip_id",
    "route_id",
    "direction_id",
    "reported_at",
    "reported_at_ms",
    "latitude",
    "longitude",
    "bearing",
    "speed_mps",
    "current_status",
    "stop_id",
    "current_stop_sequence",
    "position_is_plausible",
]

ROUTE_COLUMNS = [
    "route_id",
    "route_short_name",
    "route_long_name",
    "route_color",
    "route_type",
    "division",
]

STOP_COLUMNS = [
    "stop_id",
    "stop_name",
    "stop_lat",
    "stop_lon",
    "borough",
    "served_by_routes",
]

ALERT_COLUMNS = [
    "alert_event_id",
    "service_date",
    "entity_id",
    "feed_timestamp",
    "feed_timestamp_ms",
    "informed_ordinal",
    "agency_id",
    "route_id",
    "stop_id",
    "trip_id",
    "direction_id",
    "alert_cause",
    "alert_effect",
    "header_text",
    "active_period_start",
    "active_period_end",
]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", default=str(SEED_DIR), help="seed directory")
    args = parser.parse_args(argv)
    out = Path(args.out)

    records = build_records()
    projected = project_records(records)

    counts = {
        # Static reference dimensions written from the Python literals in
        # static_reference so the seeds cannot drift from the stop universe the
        # generator draws from
        "gtfs_routes.csv": write_csv(
            out / "gtfs_routes.csv",
            [
                {
                    "route_id": r.route_id,
                    "route_short_name": r.route_short_name,
                    "route_long_name": r.route_long_name,
                    "route_color": r.route_color,
                    "route_type": r.route_type,
                    "division": r.division,
                }
                for r in ROUTES
            ],
            ROUTE_COLUMNS,
        ),
        "gtfs_stops.csv": write_csv(
            out / "gtfs_stops.csv",
            [
                {
                    "stop_id": s.stop_id,
                    "stop_name": s.stop_name,
                    "stop_lat": s.stop_lat,
                    "stop_lon": s.stop_lon,
                    "borough": s.borough,
                    # Pipe-separated rather than comma the file is a CSV and
                    # quoting a nested comma list is a needless foot-gun
                    "served_by_routes": "|".join(s.routes),
                }
                for s in STOPS
            ],
            STOP_COLUMNS,
        ),
        "silver_trip_stop_predictions.csv": write_csv(
            out / "silver_trip_stop_predictions.csv",
            projected["trip_stop_predictions"],
            TRIP_COLUMNS,
        ),
        "silver_vehicle_positions.csv": write_csv(
            out / "silver_vehicle_positions.csv",
            projected["vehicle_positions"],
            VEHICLE_COLUMNS,
        ),
        "silver_alert_informed_entities.csv": write_csv(
            out / "silver_alert_informed_entities.csv",
            projected["alert_informed_entities"],
            ALERT_COLUMNS,
        ),
    }

    span = datetime.fromtimestamp(BASE_EPOCH, tz=UTC)
    print(f"generated from {len(records)} canonical records (base {span:%Y-%m-%d})")
    for name, n in counts.items():
        print(f"  {name}: {n} rows")
    print(f"  window: {span:%Y-%m-%d} .. {span + timedelta(days=len(SERVICE_DATES) - 1):%Y-%m-%d}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
