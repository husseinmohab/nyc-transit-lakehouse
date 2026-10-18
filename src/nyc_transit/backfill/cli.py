"""python -m nyc_transit.backfill

run or plan a partition backfill
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date, datetime

from nyc_transit.backfill.partitions import (
    REBUILDABLE_TABLES,
    PartitionWindow,
    plan_backfill,
    run_backfill,
)
from nyc_transit.config import load_settings
from nyc_transit.quality.expectations import validate_partition
from nyc_transit.streaming.session import build_spark_session

log = logging.getLogger(__name__)


def _parse_date(raw: str) -> date:
    try:
        return datetime.strptime(raw, "%Y-%m-%d").date()
    except ValueError:
        raise argparse.ArgumentTypeError(f"{raw!r} is not a YYYY-MM-DD date") from None


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="nyc-transit-backfill",
        description="Rebuild silver partitions from bronze. Idempotent and partition-scoped.",
    )
    parser.add_argument("--start", type=_parse_date, required=True, help="first service date")
    parser.add_argument(
        "--end", type=_parse_date, default=None, help="last service date (default: --start)"
    )
    parser.add_argument(
        "--table",
        action="append",
        choices=sorted(REBUILDABLE_TABLES),
        default=None,
        help="repeatable; defaults to every rebuildable silver table.",
    )
    parser.add_argument("--max-partitions", type=int, default=92)
    parser.add_argument("--dry-run", action="store_true", help="print the SQL and exit.")
    parser.add_argument(
        "--validate",
        action="store_true",
        help="run the Great Expectations partition suite on the result.",
    )
    parser.add_argument("--local-threads", default=None)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    settings = load_settings()
    catalog = settings.lakehouse.catalog_name
    window = PartitionWindow(args.start, args.end or args.start)
    targets = tuple(args.table) if args.table else tuple(REBUILDABLE_TABLES)

    plan = plan_backfill(
        catalog=catalog,
        window=window,
        targets=targets,
        max_partitions=args.max_partitions,
    )

    if args.dry_run:
        print(plan.render())
        print()
        for partition_value, table, sql in plan.statements:
            print(f"-- {partition_value} {table}\n{sql};\n")
        return 0

    spark = build_spark_session(
        settings.lakehouse,
        app_name="nyc-transit-backfill",
        with_kafka=False,
        local_threads=args.local_threads,
    )
    result = run_backfill(spark, plan)
    print(json.dumps(result, indent=2, default=str))

    if args.validate and "silver.trip_stop_predictions" in targets:
        table = f"{catalog}.silver.trip_stop_predictions"
        for partition_value in window.iso_dates():
            frame = (
                spark.sql(f"SELECT * FROM {table} WHERE service_date = DATE '{partition_value}'")
                .limit(500_000)
                .toPandas()
            )
            outcome = validate_partition(frame, partition_value=partition_value)
            print(outcome.render())
            if outcome.should_fail_dag:
                log.error("partition %s failed validation", partition_value)
                return 1
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
