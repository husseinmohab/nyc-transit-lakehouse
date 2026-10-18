"""record sinks

the poller and the load generator both write through a narrow RecordSink
protocol so the same ingestion path can target a real kafka cluster an in memory
list or dev null
without that split the throughput benchmark could not tell our transforms are
slow from the broker is the bottleneck which is the first thing anyone asks
about a 2k events/sec claim
"""

from __future__ import annotations

import time
from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Protocol

from nyc_transit.config import KafkaSettings, ProducerSettings
from nyc_transit.gtfs.normalize import partition_key
from nyc_transit.producer.msk_iam import with_oauth_callback
from nyc_transit.schemas import serialize, serialize_json


@dataclass
class SinkStats:
    """counters every sink keeps

    the benchmark reads these
    """

    records_in: int = 0
    records_delivered: int = 0
    records_failed: int = 0
    bytes_out: int = 0
    by_record_type: Counter[str] = field(default_factory=Counter)
    first_send_monotonic: float | None = None
    last_send_monotonic: float | None = None

    def observe(self, record_type: str, payload_size: int) -> None:
        self.records_in += 1
        self.bytes_out += payload_size
        self.by_record_type[record_type] += 1
        now = time.monotonic()
        if self.first_send_monotonic is None:
            self.first_send_monotonic = now
        self.last_send_monotonic = now

    @property
    def elapsed_seconds(self) -> float:
        if self.first_send_monotonic is None or self.last_send_monotonic is None:
            return 0.0
        return max(self.last_send_monotonic - self.first_send_monotonic, 1e-9)

    @property
    def events_per_second(self) -> float:
        return self.records_in / self.elapsed_seconds if self.records_in else 0.0

    def as_dict(self) -> dict[str, Any]:
        return {
            "records_in": self.records_in,
            "records_delivered": self.records_delivered,
            "records_failed": self.records_failed,
            "bytes_out": self.bytes_out,
            "by_record_type": dict(self.by_record_type),
            "elapsed_seconds": round(self.elapsed_seconds, 4),
            "events_per_second": round(self.events_per_second, 1),
        }


class RecordSink(Protocol):
    """where normalized records go"""

    stats: SinkStats

    def send(self, record: dict[str, Any]) -> None: ...

    def flush(self, timeout_seconds: float = 30.0) -> int:
        """block until outstanding records are acked

        returns how many are still in flight so 0 on a clean flush
        """
        ...

    def close(self) -> None: ...


def encode(record: dict[str, Any], serialization: str) -> bytes:
    return serialize(record) if serialization == "avro" else serialize_json(record)


class MemorySink:
    """keeps records in a list

    used by the tests
    """

    def __init__(self, serialization: str = "avro", *, keep_payloads: bool = True) -> None:
        self.serialization = serialization
        self.keep_payloads = keep_payloads
        self.messages: list[tuple[str, str, bytes]] = []
        self.stats = SinkStats()

    def send(self, record: dict[str, Any]) -> None:
        payload = encode(record, self.serialization)
        self.stats.observe(record["record_type"], len(payload))
        self.stats.records_delivered += 1
        if self.keep_payloads:
            self.messages.append((record["record_type"], partition_key(record), payload))

    def flush(self, timeout_seconds: float = 30.0) -> int:  # noqa: ARG002 - protocol signature
        return 0

    def close(self) -> None:
        return None


class NullSink:
    """serializes then throws away

    splits transform and serialize throughput off broker throughput so the benchmark
    can report both numbers
    see docs/benchmark.md
    """

    def __init__(self, serialization: str = "avro") -> None:
        self.serialization = serialization
        self.stats = SinkStats()

    def send(self, record: dict[str, Any]) -> None:
        payload = encode(record, self.serialization)
        self.stats.observe(record["record_type"], len(payload))
        self.stats.records_delivered += 1

    def flush(self, timeout_seconds: float = 30.0) -> int:  # noqa: ARG002 - protocol signature
        return 0

    def close(self) -> None:
        return None


class KafkaSink:
    """confluent-kafka producer with idempotent delivery and keyed partitioning

    enable.idempotence plus acks=all gives exactly once produce
    librdkafka attaches a producer id and a sequence number per partition so a retry
    after a network timeout cannot duplicate the record in the log
    that covers broker side duplication but not the producer dying between polling a
    feed and producing it which the deterministic event_id handles instead

    the delivery report callback counts permanent failures instead of raising
    one unroutable record should not take down a service doing thousands per second
    but a non zero records_failed fails the airflow freshness gate
    """

    def __init__(
        self,
        kafka: KafkaSettings,
        producer_cfg: ProducerSettings,
        *,
        producer_factory: Any = None,
    ) -> None:
        self.kafka = kafka
        self.producer_cfg = producer_cfg
        self.stats = SinkStats()
        self._delivery_errors: list[str] = []

        if producer_factory is None:  # pragma: no cover - needs a broker
            from confluent_kafka import Producer

            producer_factory = Producer
        self._producer = producer_factory(with_oauth_callback(kafka.librdkafka_config(), kafka))
        self._since_flush = 0

    def _on_delivery(self, err: Any, _msg: Any) -> None:
        """librdkafka delivery report

        called positionally hence _msg
        """
        if err is None:
            self.stats.records_delivered += 1
            return
        self.stats.records_failed += 1
        if len(self._delivery_errors) < 50:  # bounded, to avoid unbounded growth
            self._delivery_errors.append(str(err))

    def send(self, record: dict[str, Any]) -> None:
        payload = encode(record, self.producer_cfg.serialization)
        topic = self.kafka.topic_for(record["record_type"])
        key = partition_key(record)
        self.stats.observe(record["record_type"], len(payload))

        while True:
            try:
                self._producer.produce(
                    topic=topic,
                    key=key.encode("utf-8"),
                    value=payload,
                    on_delivery=self._on_delivery,
                    # Carrying the ids in headers lets an operator trace a row
                    # in Iceberg back to the Kafka message with kcat
                    headers={
                        "event_id": record["event_id"],
                        "feed_id": record["feed_id"],
                        "schema_version": str(record["schema_version"]),
                    },
                )
                break
            except BufferError:
                # Local queue full Serve delivery callbacks and retry dropping
                # here instead would silently lose data under backpressure
                self._producer.poll(0.5)

        self._since_flush += 1
        if self._since_flush >= self.producer_cfg.flush_every:
            self._producer.poll(0)
            self._since_flush = 0

    def flush(self, timeout_seconds: float = 30.0) -> int:
        remaining = self._producer.flush(timeout_seconds)
        self._since_flush = 0
        return int(remaining)

    def close(self) -> None:
        self.flush()

    @property
    def delivery_errors(self) -> list[str]:
        return list(self._delivery_errors)


def build_sink(
    kafka: KafkaSettings,
    producer_cfg: ProducerSettings,
    *,
    target: str = "kafka",
) -> RecordSink:
    """factory both clis use

    target is kafka or memory or null
    """
    if target == "kafka":
        return KafkaSink(kafka, producer_cfg)
    if target == "memory":
        return MemorySink(producer_cfg.serialization)
    if target == "null":
        return NullSink(producer_cfg.serialization)
    raise ValueError(f"unknown sink target {target!r}")
