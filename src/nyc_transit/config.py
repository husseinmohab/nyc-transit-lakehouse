"""env driven config

anything that differs between a laptop and aws lives here so no module hardcodes
an endpoint
the local defaults actually work so an empty env runs against docker compose
"""

from __future__ import annotations

from pathlib import Path

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

REPO_ROOT = Path(__file__).resolve().parents[2]


class KafkaSettings(BaseSettings):
    """librdkafka config shared by the producer and the load generator"""

    model_config = SettingsConfigDict(env_prefix="KAFKA_", extra="ignore")

    bootstrap_servers: str = "localhost:9092"
    security_protocol: str = "PLAINTEXT"
    # AWS_MSK_IAM is the name the java client uses and the one to set in env
    # librdkafka calls the same thing OAUTHBEARER and librdkafka_config
    # translates it below
    sasl_mechanism: str | None = None
    # only read when the mechanism is AWS_MSK_IAM the signer needs a region to
    # build the presigned url it uses as the token
    aws_region: str | None = None

    # Topic names are versioned so a breaking schema change is a new topic
    # rather than a poison pill on the existing one
    topic_trip_updates: str = "mta.gtfs.trip_updates.v1"
    topic_vehicle_positions: str = "mta.gtfs.vehicle_positions.v1"
    topic_alerts: str = "mta.gtfs.alerts.v1"

    # acks=all plus the idempotent producer is what makes the Kafka leg
    # exactly once per produce attempt
    # streaming/dedup.py has the rest of the delivery argument
    acks: str = "all"
    enable_idempotence: bool = True
    compression_type: str = "lz4"
    linger_ms: int = 20
    batch_num_messages: int = 10_000
    queue_buffering_max_messages: int = 1_000_000
    message_timeout_ms: int = 120_000

    @property
    def uses_msk_iam(self) -> bool:
        """true when the broker expects msk sasl iam"""
        return (self.sasl_mechanism or "").upper() == "AWS_MSK_IAM"

    def librdkafka_config(self) -> dict[str, object]:
        """a dict confluent_kafka.Producer takes as is

        msk iam needs an oauth_cb on top of this which is not serialisable so
        producer/msk_iam.py adds it see with_oauth_callback
        """
        cfg: dict[str, object] = {
            "bootstrap.servers": self.bootstrap_servers,
            "security.protocol": self.security_protocol,
            "acks": self.acks,
            "enable.idempotence": self.enable_idempotence,
            "compression.type": self.compression_type,
            "linger.ms": self.linger_ms,
            "batch.num.messages": self.batch_num_messages,
            "queue.buffering.max.messages": self.queue_buffering_max_messages,
            "message.timeout.ms": self.message_timeout_ms,
        }
        if self.uses_msk_iam:
            # librdkafka has no AWS_MSK_IAM mechanism it does the same thing
            # under OAUTHBEARER with a callback that mints the token
            cfg["sasl.mechanism"] = "OAUTHBEARER"
            # msk only listens for iam on tls so a PLAINTEXT default here is
            # always a misconfiguration and fails at connect with no useful error
            cfg["security.protocol"] = "SASL_SSL"
        elif self.sasl_mechanism:
            # librdkafka rejects an empty sasl.mechanism so only set it when present
            cfg["sasl.mechanism"] = self.sasl_mechanism
        return cfg

    def topic_for(self, record_type: str) -> str:
        try:
            return {
                "trip_update": self.topic_trip_updates,
                "vehicle_position": self.topic_vehicle_positions,
                "alert": self.topic_alerts,
            }[record_type]
        except KeyError:
            raise ValueError(f"no topic configured for record type {record_type!r}") from None


class FeedSettings(BaseSettings):
    """gtfs realtime polling behaviour"""

    model_config = SettingsConfigDict(env_prefix="GTFS_", extra="ignore")

    # Subway feeds have been key-less since the MTA retired its API gateway in
    # 2023 the bus feeds still need a OneBusAway key without one they are
    # skipped rather than faked
    bus_api_key: str | None = None
    poll_interval_seconds: float = 15.0
    http_timeout_seconds: float = 10.0
    max_retries: int = 4
    enabled_feeds: str = "subway"  # comma-separated: subway,bus,lirr,mnr,alerts

    @field_validator("poll_interval_seconds")
    @classmethod
    def _sane_interval(cls, v: float) -> float:
        # The MTA publishes every 15-30s Polling faster wastes requests and
        # risks rate limiting without producing new information
        if v < 5.0:
            raise ValueError("poll_interval_seconds < 5 is abusive to the MTA endpoint")
        return v

    def enabled(self) -> list[str]:
        return [g.strip() for g in self.enabled_feeds.split(",") if g.strip()]


class LakehouseSettings(BaseSettings):
    """iceberg catalog and warehouse location"""

    model_config = SettingsConfigDict(env_prefix="LAKEHOUSE_", extra="ignore")

    catalog_name: str = "lakehouse"
    # hadoop = path-based catalog (local dev, no Glue) glue = AWS Glue
    catalog_type: str = "hadoop"
    warehouse: str = str(REPO_ROOT / "warehouse")
    checkpoint_root: str = str(REPO_ROOT / "checkpoints")

    bronze_namespace: str = "bronze"
    silver_namespace: str = "silver"
    # where the static gtfs reference tables are written see
    # streaming/static_tables.py dbt reads them through the gtfs_static source
    static_namespace: str = "staging"

    # Set for MinIO in docker-compose unset on AWS so the default S3 endpoint wins
    s3_endpoint: str | None = None
    s3_access_key: str | None = None
    s3_secret_key: str | None = None
    s3_path_style_access: bool = True

    # 1.7.1 crashes the jvm on a vectorized parquet read, a SIGSEGV inside
    # the jvm's own symbol tables that reproduces on both corretto and temurin
    # 17, so the fault is in the iceberg reader
    # 1.10.2 reads the same tables with vectorization left on
    iceberg_version: str = "1.10.2"
    spark_major_minor: str = "3.5"
    scala_version: str = "2.12"

    @field_validator("catalog_type")
    @classmethod
    def _known_catalog(cls, v: str) -> str:
        if v not in {"hadoop", "glue", "rest"}:
            raise ValueError(f"unsupported catalog_type {v!r}")
        return v

    def table(self, namespace: str, name: str) -> str:
        return f"{self.catalog_name}.{namespace}.{name}"

    def bronze(self, name: str) -> str:
        return self.table(self.bronze_namespace, name)

    def silver(self, name: str) -> str:
        return self.table(self.silver_namespace, name)

    def namespaces(self) -> dict[str, str]:
        """layer name to the namespace it actually lives in

        the specs in streaming/ddl.py and the rebuildable tables in
        backfill/partitions.py name their layer bronze or silver
        against a glue catalog the namespace is the glue database which carries
        a prefix so the two are not the same string and anything building a
        table name needs this mapping or the ddl and the writers disagree
        """
        return {"bronze": self.bronze_namespace, "silver": self.silver_namespace}

    def qualify(self, layer_qualified: str) -> str:
        """bronze.trip_updates to the full catalog namespace name"""
        layer, _, name = layer_qualified.partition(".")
        return self.table(self.namespaces().get(layer, layer), name)

    def checkpoint_for(self, job_name: str) -> str:
        """checkpoint dir for a named streaming query

        one dir per query never shared
        structured streaming keeps its offset log and commit log here so two queries in
        one dir would corrupt each others progress
        """
        return f"{self.checkpoint_root.rstrip('/')}/{job_name}"

    def iceberg_runtime_coordinate(self) -> str:
        return (
            f"org.apache.iceberg:iceberg-spark-runtime-"
            f"{self.spark_major_minor}_{self.scala_version}:{self.iceberg_version}"
        )


class ProducerSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="PRODUCER_", extra="ignore")

    producer_id: str = "local-dev-1"
    serialization: str = "avro"  # "avro" | "json"
    flush_every: int = 50_000
    stats_interval_seconds: float = 5.0

    @field_validator("serialization")
    @classmethod
    def _known_serialization(cls, v: str) -> str:
        if v not in {"avro", "json"}:
            raise ValueError("serialization must be 'avro' or 'json'")
        return v


class Settings(BaseSettings):
    """top level handle

    build one per process
    """

    model_config = SettingsConfigDict(extra="ignore")

    kafka: KafkaSettings = Field(default_factory=KafkaSettings)
    feeds: FeedSettings = Field(default_factory=FeedSettings)
    lakehouse: LakehouseSettings = Field(default_factory=LakehouseSettings)
    producer: ProducerSettings = Field(default_factory=ProducerSettings)
    log_level: str = "INFO"


def load_settings() -> Settings:
    return Settings()
