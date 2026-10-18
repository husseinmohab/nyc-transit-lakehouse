"""synthetic load generator for the throughput benchmark

runs the production path
build a FeedMessage serialize to protobuf decode normalize avro encode hand to a
sink
so the reported events/sec is the rate this code turns feed bytes into kafka
ready records

three targets

--target null decodes normalizes and encodes with no broker
--target memory does the same and keeps the payloads
--target kafka goes end to end into a broker

rate control is closed loop on records
after every message the loop asks when record n was due at the target rate and
sleeps until then
an open loop cadence off an estimated records per message drifts because a
cancelled trip gives one row instead of 8-24
--target-eps 0 runs flat out
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from dataclasses import dataclass
from typing import Any

from nyc_transit.config import load_settings
from nyc_transit.gtfs.decoder import decode_feed
from nyc_transit.gtfs.normalize import normalize_feed_message
from nyc_transit.gtfs.synthetic import SyntheticFeedConfig, SyntheticFeedGenerator
from nyc_transit.producer.sink import RecordSink, build_sink

log = logging.getLogger(__name__)


@dataclass
class BenchmarkResult:
    target: str
    duration_seconds: float
    messages: int
    records: int
    bytes_out: int
    events_per_second: float
    bytes_per_second: float
    target_events_per_second: float
    achieved_fraction: float | None
    records_failed: int
    by_record_type: dict[str, int]
    serialization: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "target": self.target,
            "serialization": self.serialization,
            "duration_seconds": round(self.duration_seconds, 3),
            "messages": self.messages,
            "records": self.records,
            "bytes_out": self.bytes_out,
            "events_per_second": round(self.events_per_second, 1),
            "mb_per_second": round(self.bytes_per_second / 1_048_576, 3),
            "target_events_per_second": self.target_events_per_second,
            "achieved_fraction": (
                round(self.achieved_fraction, 4) if self.achieved_fraction is not None else None
            ),
            "records_failed": self.records_failed,
            "by_record_type": self.by_record_type,
        }

    def render(self) -> str:
        return json.dumps(self.as_dict(), indent=2, sort_keys=True)


def run_benchmark(
    sink: RecordSink,
    *,
    duration_seconds: float,
    target_events_per_second: float = 2000.0,
    feed_config: SyntheticFeedConfig | None = None,
    serialization: str = "avro",
    feed_id: str = "synthetic-loadgen",
    producer_id: str = "loadgen-1",
    clock: Any = time.monotonic,
    sleeper: Any = time.sleep,
) -> BenchmarkResult:
    """drive sink at roughly target_events_per_second

    a target of 0 or less runs unthrottled which is how you find the ceiling
    clock and sleeper go in together for the unit test
    a fake clock with a real time.sleep would sleep for the simulated duration
    """
    cfg = feed_config or SyntheticFeedConfig(seed=None)
    generator = SyntheticFeedGenerator(cfg)

    throttled = target_events_per_second > 0

    start = clock()
    deadline = start + duration_seconds
    messages = 0
    records = 0

    while True:
        now = clock()
        if now >= deadline:
            break
        if throttled:
            # Record N is due at start + N / target_eps because records is the
            # true count a feed shape yielding fewer rows than estimated simply
            # produces messages sooner
            due_at = start + records / target_events_per_second
            gap = due_at - now
            if gap > 0:
                # Sub-millisecond sleeps cost more in syscall overhead than they
                # save so below that threshold we spin instead
                if gap > 0.001:
                    sleeper(min(gap, deadline - now))
                continue

        # Full production path including the protobuf round trip
        message = generator.build_feed_message(feed_timestamp_epoch=int(time.time()))
        payload = message.SerializeToString()
        decoded = decode_feed(payload, feed_id=feed_id)
        normalized = normalize_feed_message(
            decoded,
            feed_id=feed_id,
            ingest_timestamp_ms=int(time.time() * 1000),
            producer_id=producer_id,
        )
        for record in normalized:
            sink.send(record)

        messages += 1
        records += len(normalized)

    in_flight = sink.flush(timeout_seconds=60.0)
    if in_flight:
        log.warning("%d records still in flight after flush timeout", in_flight)
    elapsed = max(clock() - start, 1e-9)
    stats = sink.stats

    eps = records / elapsed
    return BenchmarkResult(
        target=type(sink).__name__,
        duration_seconds=elapsed,
        messages=messages,
        records=records,
        bytes_out=stats.bytes_out,
        events_per_second=eps,
        bytes_per_second=stats.bytes_out / elapsed,
        target_events_per_second=target_events_per_second,
        achieved_fraction=(eps / target_events_per_second) if throttled else None,
        records_failed=stats.records_failed + in_flight,
        by_record_type=dict(stats.by_record_type),
        serialization=serialization,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="gtfs-loadgen",
        description="Reproduce the ~2k events/sec GTFS-RT ingestion benchmark.",
    )
    parser.add_argument(
        "--target",
        choices=["null", "memory", "kafka"],
        default="null",
        help="null and memory need no broker; kafka requires docker compose up.",
    )
    parser.add_argument("--duration", type=float, default=30.0, help="seconds (default 30)")
    parser.add_argument(
        "--target-eps",
        type=float,
        default=2000.0,
        help="target events/sec; 0 runs unthrottled to find the ceiling.",
    )
    parser.add_argument("--trips-per-message", type=int, default=20)
    parser.add_argument("--vehicles-per-message", type=int, default=40)
    parser.add_argument("--duplicate-fraction", type=float, default=0.05)
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument(
        "--serialization",
        choices=["avro", "json"],
        default=None,
        help="defaults to PRODUCER_SERIALIZATION (avro).",
    )
    parser.add_argument("--json-out", default=None, help="write the result JSON to this path")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    settings = load_settings()
    if args.serialization:
        settings.producer.serialization = args.serialization

    feed_config = SyntheticFeedConfig(
        trips_per_message=args.trips_per_message,
        vehicles_per_message=args.vehicles_per_message,
        duplicate_fraction=args.duplicate_fraction,
        seed=args.seed,
    )
    sink = build_sink(settings.kafka, settings.producer, target=args.target)

    log.info(
        "benchmark: target=%s duration=%.1fs target_eps=%.0f mean_records_per_message=%.1f",
        args.target,
        args.duration,
        args.target_eps,
        feed_config.mean_records_per_message(),
    )
    try:
        result = run_benchmark(
            sink,
            duration_seconds=args.duration,
            target_events_per_second=args.target_eps,
            feed_config=feed_config,
            serialization=settings.producer.serialization,
        )
    finally:
        sink.close()

    print(result.render())
    if args.json_out:
        from pathlib import Path

        Path(args.json_out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.json_out).write_text(result.render() + "\n")

    # Non-zero exit when a throttled run could not keep up so the Makefile
    # target asserts the claim rather than just printing a number
    if result.achieved_fraction is not None and result.achieved_fraction < 0.95:
        log.error(
            "achieved only %.1f%% of the %.0f eps target",
            result.achieved_fraction * 100,
            result.target_events_per_second,
        )
        return 1
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
