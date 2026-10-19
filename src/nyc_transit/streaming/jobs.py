"""streaming jobs and the cli

one streaming query per kafka topic
each one keeps both its bronze table and its silver projection inside a single
foreachBatch instead of chaining a second stream off bronze

icebergs streaming read cannot see merge output without being told to skip
overwrite snapshots which would drop the merged rows
reading bronze as a stream is only safe for an append only table and bronze is
not one

one checkpoint means one replay unit
a crash between the bronze merge and the silver merge replays the whole batch and
both merges are idempotent so the tables converge
two queries means two checkpoints and silver can sit arbitrarily far behind

the cost is that silver follows the bronze jobs cadence
backfill/partitions.py rebuilds silver off bronze on its own so a silver only bug
is fixable without re polling the mta
"""

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass
from typing import Any

from nyc_transit.config import Settings, load_settings
from nyc_transit.streaming import ddl
from nyc_transit.streaming.dedup import (
    BRONZE_MERGE_KEY,
    SILVER_ALERT_MERGE_KEY,
    SILVER_TRIP_MERGE_KEY,
    SILVER_VEHICLE_MERGE_KEY,
)
from nyc_transit.streaming.session import apply_ddl, build_spark_session
from nyc_transit.streaming.transforms import (
    decode_kafka_avro,
    to_bronze,
    to_silver_alert_informed_entities,
    to_silver_trip_predictions,
    to_silver_vehicle_positions,
)
from nyc_transit.streaming.writer import IdempotentMergeWriter, MergeTarget

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class JobSpec:
    """one streaming query

    a topic a bronze table and maybe silver
    """

    name: str
    record_type: str
    bronze_spec: ddl.TableSpec
    silver_spec: ddl.TableSpec | None
    silver_transform: Any = None
    silver_merge_key: Any = None

    def topic(self, settings: Settings) -> str:
        return settings.kafka.topic_for(self.record_type)


JOBS: tuple[JobSpec, ...] = (
    JobSpec(
        name="bronze_trip_updates",
        record_type="trip_update",
        bronze_spec=ddl.BRONZE_TRIP_UPDATES,
        silver_spec=ddl.SILVER_TRIP_STOP_PREDICTIONS,
        silver_transform=to_silver_trip_predictions,
        silver_merge_key=SILVER_TRIP_MERGE_KEY,
    ),
    JobSpec(
        name="bronze_vehicle_positions",
        record_type="vehicle_position",
        bronze_spec=ddl.BRONZE_VEHICLE_POSITIONS,
        silver_spec=ddl.SILVER_VEHICLE_POSITIONS,
        silver_transform=to_silver_vehicle_positions,
        silver_merge_key=SILVER_VEHICLE_MERGE_KEY,
    ),
    JobSpec(
        name="bronze_alerts",
        record_type="alert",
        bronze_spec=ddl.BRONZE_ALERTS,
        silver_spec=ddl.SILVER_ALERT_INFORMED_ENTITIES,
        silver_transform=to_silver_alert_informed_entities,
        silver_merge_key=SILVER_ALERT_MERGE_KEY,
    ),
)

JOBS_BY_NAME: dict[str, JobSpec] = {j.name: j for j in JOBS}


def bronze_columns(spec: ddl.TableSpec) -> tuple[str, ...]:
    return tuple(name for name, _, _ in spec.columns)


def build_merge_targets(job: JobSpec, settings: Settings) -> tuple[MergeTarget, ...]:
    """bronze target then silver

    order matters since silver reads bronzes shape
    """
    lake = settings.lakehouse
    targets = [
        MergeTarget(
            table=lake.bronze(job.bronze_spec.name),
            merge_key=BRONZE_MERGE_KEY,
            columns=bronze_columns(job.bronze_spec),
            transform=to_bronze,
        )
    ]
    if job.silver_spec is not None:
        targets.append(
            MergeTarget(
                table=lake.silver(job.silver_spec.name),
                merge_key=job.silver_merge_key,
                columns=bronze_columns(job.silver_spec),
                # Silver derives from the bronze shape so to_bronze runs first:
                # service_date must already be a date not a string
                transform=lambda df, _t=job.silver_transform: _t(to_bronze(df)),
            )
        )
    return tuple(targets)


def kafka_source_options(
    settings: Settings, job: JobSpec, *, starting_offsets: str
) -> dict[str, str]:
    """kafka source options with the correctness ones called out"""
    return {
        "kafka.bootstrap.servers": settings.kafka.bootstrap_servers,
        "kafka.security.protocol": settings.kafka.security_protocol,
        "subscribe": job.topic(settings),
        "startingOffsets": starting_offsets,
        # True not false if Kafka retention has expired offsets the stream
        # still needs that is data loss and the job must stop so a human
        # decides whether to backfill set explicitly so nobody "fixes" a crash
        # loop by flipping it
        "failOnDataLoss": "true",
        # Bound the micro-batch Unbounded the first batch after a long outage
        # tries to process the whole backlog and OOMs so the catch-up never
        # completes 200k records is about a 100s backlog at 2k events/sec
        "maxOffsetsPerTrigger": "200000",
        "kafka.group.id": f"nyc-transit-{job.name}",
        **msk_iam_options(settings),
    }


def msk_iam_options(settings: Settings) -> dict[str, str]:
    """the three options the java kafka client needs for msk sasl iam

    empty for PLAINTEXT so the local path is unchanged
    the jar holding these classes is added by session.iceberg_packages
    the java client calls the mechanism AWS_MSK_IAM where librdkafka calls the
    same exchange OAUTHBEARER
    """
    if not settings.kafka.uses_msk_iam:
        return {}
    return {
        # msk only listens for iam on tls and the default is PLAINTEXT so
        # without this the client speaks plaintext at port 9098 and the only
        # symptom is describeTopics timing out on a node assignment which
        # looks like a network or security group problem
        # librdkafka_config forces the same thing for the producer
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "AWS_MSK_IAM",
        "kafka.sasl.jaas.config": ("software.amazon.msk.auth.iam.IAMLoginModule required;"),
        "kafka.sasl.client.callback.handler.class": (
            "software.amazon.msk.auth.iam.IAMClientCallbackHandler"
        ),
    }


def start_query(
    spark: Any,
    settings: Settings,
    job: JobSpec,
    *,
    starting_offsets: str = "latest",
    trigger_seconds: int = 30,
    once: bool = False,
) -> Any:
    """start one streaming query and hand back the StreamingQuery"""
    source = spark.readStream.format("kafka")
    for key, value in kafka_source_options(
        settings, job, starting_offsets=starting_offsets
    ).items():
        source = source.option(key, value)

    decoded = decode_kafka_avro(source.load(), job.record_type)
    writer = IdempotentMergeWriter(
        spark=spark,
        query_name=job.name,
        targets=build_merge_targets(job, settings),
    )

    stream = (
        decoded.writeStream.queryName(job.name)
        .foreachBatch(writer)
        .option("checkpointLocation", settings.lakehouse.checkpoint_for(job.name))
    )
    if once:
        # availableNow processes everything currently in the topic and stops
        # Used by CI and by the Airflow catch-up task it keeps the same
        # checkpoint so a later continuous run resumes where this left off
        stream = stream.trigger(availableNow=True)
    else:
        stream = stream.trigger(processingTime=f"{trigger_seconds} seconds")
    return stream.start()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="nyc-transit-streaming",
        description="Run the Kafka to Iceberg Structured Streaming jobs.",
    )
    parser.add_argument(
        "--job",
        action="append",
        choices=sorted(JOBS_BY_NAME),
        default=None,
        help="repeatable; defaults to all jobs.",
    )
    parser.add_argument("--create-tables", action="store_true", help="apply the Iceberg DDL first.")
    parser.add_argument(
        "--load-static",
        action="store_true",
        help="write the static GTFS reference tables and exit (dbt seeds cannot "
        "load them on spark; see streaming/static_tables.py).",
    )
    parser.add_argument("--show-ddl", action="store_true", help="print the DDL and exit.")
    parser.add_argument("--starting-offsets", default="latest", choices=["latest", "earliest"])
    parser.add_argument("--trigger-seconds", type=int, default=30)
    parser.add_argument(
        "--once",
        action="store_true",
        help="trigger(availableNow): drain the topic and exit (CI or catch-up).",
    )
    parser.add_argument("--local-threads", default=None, help="force master=local[N].")
    parser.add_argument(
        "--await-seconds",
        type=float,
        default=None,
        help="stop after N seconds instead of running until terminated.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    settings = load_settings()
    catalog = settings.lakehouse.catalog_name

    if args.show_ddl:
        for statement in ddl.all_ddl(catalog, namespaces=settings.lakehouse.namespaces()):
            print(statement + ";\n")
        return 0

    jobs = [JOBS_BY_NAME[n] for n in (args.job or sorted(JOBS_BY_NAME))]
    spark = build_spark_session(
        settings.lakehouse,
        app_name="nyc-transit-streaming",
        with_kafka=True,
        with_msk_iam=settings.kafka.uses_msk_iam,
        local_threads=args.local_threads,
    )

    if args.create_tables:
        apply_ddl(spark, ddl.all_ddl(catalog, namespaces=settings.lakehouse.namespaces()))

    if args.load_static:
        from nyc_transit.streaming.static_tables import load_static_reference

        for table, rows in load_static_reference(spark, settings.lakehouse).items():
            log.info("static reference: %s <- %d rows", table, rows)
        return 0

    queries = [
        start_query(
            spark,
            settings,
            job,
            starting_offsets=args.starting_offsets,
            trigger_seconds=args.trigger_seconds,
            once=args.once,
        )
        for job in jobs
    ]
    log.info("started %d streaming queries: %s", len(queries), [q.name for q in queries])

    try:
        if args.await_seconds is not None:
            for q in queries:
                q.awaitTermination(args.await_seconds)
            for q in queries:
                q.stop()
        else:
            for q in queries:
                q.awaitTermination()
    except KeyboardInterrupt:  # pragma: no cover
        log.info("interrupted; stopping queries")
        for q in queries:
            q.stop()
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
