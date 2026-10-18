"""shared fixtures

the spark session is module scoped and not function scoped
starting one costs 10-20 seconds between the jvm and the iceberg jar so a
function scoped fixture would eat the whole suite
tests drop and recreate their tables instead which gives isolation for free
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path

import pytest

FIXTURE_DIR = Path(__file__).parent / "fixtures"

# Ingest timestamp used throughout so normalized records are byte-comparable
FIXED_INGEST_MS = 1_793_536_002_000
# Feed timestamp baked into the committed fixture
FIXTURE_FEED_EPOCH = 1_793_536_000


@pytest.fixture(scope="session")
def fixture_payload() -> bytes:
    """The committed GTFS-Realtime fixture bytes"""
    return (FIXTURE_DIR / "gtfs_rt_sample.pb").read_bytes()


@pytest.fixture(scope="session")
def truncated_payload() -> bytes:
    return (FIXTURE_DIR / "truncated.pb").read_bytes()


@pytest.fixture(scope="session")
def fixture_records(fixture_payload: bytes) -> list[dict]:
    """Normalized canonical records from the committed fixture"""
    from nyc_transit.gtfs.decoder import decode_feed
    from nyc_transit.gtfs.normalize import normalize_feed_message

    message = decode_feed(fixture_payload, feed_id="nyct-ace")
    return normalize_feed_message(
        message,
        feed_id="nyct-ace",
        ingest_timestamp_ms=FIXED_INGEST_MS,
        producer_id="pytest",
    )


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    """A local SparkSession with the Iceberg extensions and a temp warehouse

    Skips rather than fails when pyspark is absent or the Iceberg jar cannot be
    fetched so the pure-Python half of the suite stays runnable on a machine
    with no JVM and no network
    """
    pytest.importorskip("pyspark", reason="pyspark not installed")

    from nyc_transit.config import LakehouseSettings
    from nyc_transit.streaming.session import build_spark_session

    warehouse = tmp_path_factory.mktemp("warehouse")
    checkpoints = tmp_path_factory.mktemp("checkpoints")
    settings = LakehouseSettings(
        catalog_name="test_lakehouse",
        catalog_type="hadoop",
        warehouse=str(warehouse),
        checkpoint_root=str(checkpoints),
    )

    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    try:
        session = build_spark_session(
            settings,
            app_name="nyc-transit-tests",
            with_kafka=False,
            local_threads="2",
            extra_conf={
                "spark.ui.enabled": "false",
                "spark.sql.shuffle.partitions": "2",
                "spark.driver.memory": "1g",
            },
        )
    except Exception as exc:  # pragma: no cover
        pytest.skip(f"could not start SparkSession (Iceberg jars unavailable?): {exc}")

    session._nyc_settings = settings  # type: ignore[attr-defined]
    yield session
    session.stop()
    shutil.rmtree(warehouse, ignore_errors=True)
    shutil.rmtree(checkpoints, ignore_errors=True)


@pytest.fixture
def lake_settings(spark):
    """The LakehouseSettings the module-scoped Spark session was built with"""
    return spark._nyc_settings


@pytest.fixture
def iceberg_tables(spark, lake_settings):
    """Create every Iceberg table for one test then drop them

    Namespaces are shared across the module-scoped session so each test drops
    and recreates its tables That gives per-test isolation without paying for a
    new SparkSession
    """
    from nyc_transit.streaming import ddl
    from nyc_transit.streaming.session import apply_ddl

    catalog = lake_settings.catalog_name
    apply_ddl(spark, ddl.all_ddl(catalog))
    yield lake_settings
    for spec in reversed(ddl.ALL_SPECS):
        spark.sql(f"DROP TABLE IF EXISTS {catalog}.{spec.namespace}.{spec.name} PURGE")
