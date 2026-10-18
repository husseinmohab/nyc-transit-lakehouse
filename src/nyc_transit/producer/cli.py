"""gtfs-producer entry point

runs the polling service
"""

from __future__ import annotations

import argparse
import json
import logging
import sys

from nyc_transit.config import load_settings
from nyc_transit.gtfs.feeds import FEEDS_BY_ID, feeds_for_groups
from nyc_transit.producer.poller import GtfsRealtimePoller
from nyc_transit.producer.sink import build_sink

log = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="gtfs-producer",
        description="Poll MTA GTFS-Realtime feeds and produce normalized records to Kafka.",
    )
    parser.add_argument(
        "--groups",
        default=None,
        help="comma-separated feed groups (subway,bus,lirr,mnr,alerts); "
        "defaults to GTFS_ENABLED_FEEDS.",
    )
    parser.add_argument(
        "--feed-id",
        action="append",
        default=None,
        help="poll specific feed ids instead of whole groups; repeatable.",
    )
    parser.add_argument("--target", choices=["kafka", "memory", "null"], default="kafka")
    parser.add_argument(
        "--max-ticks",
        type=int,
        default=None,
        help="stop after N poll cycles (used by the Airflow health probe).",
    )
    parser.add_argument("--dry-run", action="store_true", help="list selected feeds and exit.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    settings = load_settings()
    logging.basicConfig(
        level=settings.log_level, format="%(asctime)s %(levelname)s %(name)s %(message)s"
    )

    if args.feed_id:
        missing = [f for f in args.feed_id if f not in FEEDS_BY_ID]
        if missing:
            log.error("unknown feed ids: %s", ", ".join(missing))
            return 2
        feeds = [FEEDS_BY_ID[f] for f in args.feed_id]
    else:
        groups = (
            [g.strip() for g in args.groups.split(",") if g.strip()]
            if args.groups
            else settings.feeds.enabled()
        )
        feeds = feeds_for_groups(groups, bus_api_key=settings.feeds.bus_api_key)

    if not feeds:
        log.error("no feeds selected. Bus feeds need GTFS_BUS_API_KEY; subway feeds need no key.")
        return 2

    if args.dry_run:
        print(
            json.dumps(
                [
                    {"feed_id": f.feed_id, "group": f.group, "entities": sorted(f.entity_types)}
                    for f in feeds
                ],
                indent=2,
            )
        )
        return 0

    sink = build_sink(settings.kafka, settings.producer, target=args.target)
    poller = GtfsRealtimePoller(
        feeds,
        sink,
        settings.feeds,
        producer_id=settings.producer.producer_id,
    )
    log.info("polling %d feeds every %.0fs", len(feeds), settings.feeds.poll_interval_seconds)
    metrics = poller.run(max_ticks=args.max_ticks)
    print(json.dumps(metrics.summary(), indent=2))
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
