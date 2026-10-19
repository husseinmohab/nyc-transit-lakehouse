"""SparkSession for the three environments this runs in

local with catalog_type=hadoop is a path based iceberg catalog on the local
filesystem
no glue no s3 no hive metastore
pytest -m iceberg and make stream-local use this

minio is the same catalog with the warehouse on s3a against docker compose minio
which exercises the s3a code path with no aws account

aws with catalog_type=glue is the glue data catalog and an s3 warehouse with
credentials off the instance or task role terraform creates
never off static keys

jars come through spark.jars.packages so maven at submit time
the coordinate gets built from config so the iceberg version is bumped in one
place
the spark minor version is part of the coordinate which is why
requirements/spark.txt pins pyspark to the 3.5 line
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

from nyc_transit.config import LakehouseSettings

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import SparkSession

log = logging.getLogger(__name__)

# Needed alongside the Iceberg runtime spark-avro for from_avro(),
# spark-sql-kafka for the Kafka source hadoop-aws plus the AWS SDK for s3a://
_AWS_BUNDLE_VERSION = "2.29.52"
_HADOOP_AWS_VERSION = "3.3.4"


# Pinned like the other coordinates. The 2.x line is the one that ships the
# IAMClientCallbackHandler the kafka options reference.
_MSK_IAM_AUTH_VERSION = "2.2.0"


def _pyspark_version() -> str:
    try:
        import pyspark

        return pyspark.__version__
    except ImportError:  # pragma: no cover
        return "3.5.6"


def iceberg_packages(
    settings: LakehouseSettings,
    *,
    with_kafka: bool,
    with_s3: bool,
    with_msk_iam: bool = False,
) -> list[str]:
    """maven coordinates for spark.jars.packages"""
    spark_version = _pyspark_version()
    scala = settings.scala_version
    packages = [
        settings.iceberg_runtime_coordinate(),
        f"org.apache.spark:spark-avro_{scala}:{spark_version}",
    ]
    if with_kafka:
        packages.append(f"org.apache.spark:spark-sql-kafka-0-10_{scala}:{spark_version}")
        if with_msk_iam:
            # the login module and callback handler jobs.py names in the kafka
            # options live in this jar without it the driver fails with
            # ClassNotFoundException on IAMLoginModule and not with an auth error
            packages.append(f"software.amazon.msk:aws-msk-iam-auth:{_MSK_IAM_AUTH_VERSION}")
    if with_s3:
        packages.append(f"org.apache.hadoop:hadoop-aws:{_HADOOP_AWS_VERSION}")
        packages.append(f"software.amazon.awssdk:bundle:{_AWS_BUNDLE_VERSION}")
        if settings.catalog_type == "glue":
            packages.append(f"org.apache.iceberg:iceberg-aws-bundle:{settings.iceberg_version}")
    return packages


def catalog_conf(settings: LakehouseSettings) -> dict[str, str]:
    """spark.sql.catalog config for the iceberg catalog"""
    prefix = f"spark.sql.catalog.{settings.catalog_name}"
    conf: dict[str, str] = {
        prefix: "org.apache.iceberg.spark.SparkCatalog",
        f"{prefix}.warehouse": settings.warehouse,
    }

    if settings.catalog_type == "hadoop":
        conf[f"{prefix}.type"] = "hadoop"
    elif settings.catalog_type == "glue":
        conf[f"{prefix}.catalog-impl"] = "org.apache.iceberg.aws.glue.GlueCatalog"
        conf[f"{prefix}.io-impl"] = "org.apache.iceberg.aws.s3.S3FileIO"
    elif settings.catalog_type == "rest":
        conf[f"{prefix}.type"] = "rest"

    # The Iceberg SQL extensions provide MERGE INTO CALL for the maintenance
    # procedures and ALTER TABLE .. WRITE ORDERED BY Without them the DDL in
    # ddl.py and the MERGE in dedup.py both fail to parse
    conf["spark.sql.extensions"] = (
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )
    conf["spark.sql.defaultCatalog"] = settings.catalog_name

    if settings.s3_endpoint:
        # MinIO path-style addressing explicit endpoint keys from env
        conf["spark.hadoop.fs.s3a.endpoint"] = settings.s3_endpoint
        conf["spark.hadoop.fs.s3a.path.style.access"] = str(settings.s3_path_style_access).lower()
        conf["spark.hadoop.fs.s3a.impl"] = "org.apache.hadoop.fs.s3a.S3AFileSystem"
        conf["spark.hadoop.fs.s3a.connection.ssl.enabled"] = str(
            settings.s3_endpoint.startswith("https")
        ).lower()
        if settings.s3_access_key and settings.s3_secret_key:
            conf["spark.hadoop.fs.s3a.access.key"] = settings.s3_access_key
            conf["spark.hadoop.fs.s3a.secret.key"] = settings.s3_secret_key
            conf["spark.hadoop.fs.s3a.aws.credentials.provider"] = (
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
            )
    elif settings.warehouse.startswith("s3"):
        # Real AWS resolve credentials from the task or instance role Static
        # keys are never read here which is why infra/modules/iam exists
        conf["spark.hadoop.fs.s3a.aws.credentials.provider"] = (
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
        )
        # Iceberg reads and writes table data through S3FileIO which talks to
        # the aws sdk directly and is happy with s3://
        # the streaming checkpoint is not a table it goes through the hadoop
        # filesystem api and hadoop only registers s3a:// so a checkpoint on
        # s3:// dies with UnsupportedFileSystemException no FileSystem for
        # scheme s3 the moment the query starts
        # mapping the scheme means one s3:// path works for both
        for scheme in ("s3", "s3n"):
            conf[f"spark.hadoop.fs.{scheme}.impl"] = "org.apache.hadoop.fs.s3a.S3AFileSystem"

    return conf


def streaming_conf() -> dict[str, str]:
    """session settings that matter for a long running stream"""
    return {
        # The default of 200 shuffle partitions means 200 tiny files per
        # micro-batch on a laptop streaming batches are small the production
        # job overrides this on submit
        "spark.sql.shuffle.partitions": "8",
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        # A duplicated speculative task must not double-write
        "spark.speculation": "false",
        "spark.sql.streaming.metricsEnabled": "true",
        "spark.sql.streaming.stopActiveRunOnRestart": "true",
        "spark.sql.session.timeZone": "UTC",
    }


def _pin_worker_interpreter() -> None:
    """point the spark workers at the drivers interpreter

    spark launches python workers with whatever python3 is first on PATH which inside
    a virtualenv is often a different minor version from the driver and then every
    task dies with PYTHON_VERSION_MISMATCH
    only set when absent so a deployment override still wins
    """
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


def build_spark_session(
    settings: LakehouseSettings,
    *,
    app_name: str = "nyc-transit-lakehouse",
    with_kafka: bool = True,
    with_msk_iam: bool = False,
    local_threads: str | None = None,
    extra_conf: dict[str, str] | None = None,
) -> SparkSession:
    """build the SparkSession

    local_threads forces a local[n] master which the iceberg tests and
    make stream-local use
    left as None the master comes off spark-submit or SPARK_MASTER so the same code
    runs on emr or standalone unchanged
    """
    from pyspark.sql import SparkSession

    _pin_worker_interpreter()

    with_s3 = bool(settings.s3_endpoint) or settings.warehouse.startswith("s3")
    packages = iceberg_packages(
        settings, with_kafka=with_kafka, with_s3=with_s3, with_msk_iam=with_msk_iam
    )

    builder = SparkSession.builder.appName(app_name)
    if local_threads:
        builder = builder.master(f"local[{local_threads}]")
    builder = builder.config("spark.jars.packages", ",".join(packages))

    for key, value in {**catalog_conf(settings), **streaming_conf()}.items():
        builder = builder.config(key, value)
    for key, value in (extra_conf or {}).items():
        builder = builder.config(key, value)

    if settings.catalog_type == "hadoop" and not settings.warehouse.startswith("s3"):
        Path(settings.warehouse).mkdir(parents=True, exist_ok=True)
        Path(settings.checkpoint_root).mkdir(parents=True, exist_ok=True)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    log.info(
        "SparkSession ready: catalog=%s type=%s warehouse=%s",
        settings.catalog_name,
        settings.catalog_type,
        settings.warehouse,
    )
    return spark


def apply_ddl(spark: Any, statements: list[str]) -> None:
    """run ddl statements in order and log each one"""
    for statement in statements:
        log.info("DDL: %s", statement.splitlines()[0])
        spark.sql(statement)
