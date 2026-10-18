"""producer tests

the pollers http behaviour and the sinks
"""

from __future__ import annotations

import httpx
import pytest

from nyc_transit.config import FeedSettings, KafkaSettings, ProducerSettings
from nyc_transit.gtfs.feeds import FEEDS, FEEDS_BY_ID, feeds_for_groups
from nyc_transit.producer.poller import GtfsRealtimePoller
from nyc_transit.producer.sink import KafkaSink, MemorySink, NullSink, build_sink

FEED = FEEDS_BY_ID["nyct-ace"]


def make_poller(handler, *, clock=None, feeds=None, settings=None):
    transport = httpx.MockTransport(handler)
    client = httpx.Client(transport=transport)
    sink = MemorySink()
    poller = GtfsRealtimePoller(
        feeds or [FEED],
        sink,
        settings or FeedSettings(poll_interval_seconds=5.0, max_retries=2),
        producer_id="pytest",
        client=client,
        clock=clock or (lambda: 1_793_536_002.0),
    )
    return poller, sink


class TestFeedCatalog:
    def test_feed_ids_are_unique(self):
        ids = [f.feed_id for f in FEEDS]
        assert len(ids) == len(set(ids))

    def test_subway_feeds_need_no_key(self):
        """Key-less since the MTA retired its gateway in 2023"""
        assert all(not f.requires_key for f in FEEDS if f.group == "subway")

    def test_bus_feeds_require_a_key(self):
        assert all(f.requires_key for f in FEEDS if f.group == "bus")

    def test_key_gated_feeds_are_skipped_rather_than_faked(self):
        """The pipeline never substitutes synthetic data for an unreachable
        feed which would corrupt the freshness metrics
        """
        assert feeds_for_groups(["bus"], bus_api_key=None) == []

    def test_key_is_appended_when_present(self):
        assert FEEDS_BY_ID["bus-trip-updates"].with_key("abc123").endswith("key=abc123")

    def test_with_key_raises_when_a_required_key_is_missing(self):
        with pytest.raises(ValueError, match="requires an API key"):
            FEEDS_BY_ID["bus-trip-updates"].with_key(None)

    def test_keyless_feed_ignores_a_key(self):
        assert FEED.with_key("abc") == FEED.url

    def test_every_feed_declares_entity_types(self):
        assert all(f.entity_types for f in FEEDS)

    def test_alerts_feed_only_carries_alerts(self):
        assert FEEDS_BY_ID["nyct-subway-alerts"].entity_types == frozenset({"alert"})


class TestPollerHappyPath:
    def test_decodes_and_sends_records(self, fixture_payload):
        poller, sink = make_poller(lambda _request: httpx.Response(200, content=fixture_payload))
        result = poller.poll_feed(FEED)
        assert result.status == "ok"
        assert result.records == len(sink.messages) > 0

    def test_captures_the_etag_for_conditional_requests(self, fixture_payload):
        poller, _ = make_poller(
            lambda _request: httpx.Response(
                200, content=fixture_payload, headers={"ETag": 'W/"abc"'}
            )
        )
        poller.poll_feed(FEED)
        assert poller.state[FEED.feed_id].etag == 'W/"abc"'

    def test_sends_if_none_match_on_the_second_poll(self, fixture_payload):
        seen: list[dict] = []

        def handler(request):
            seen.append(dict(request.headers))
            return httpx.Response(200, content=fixture_payload, headers={"ETag": '"v1"'})

        poller, _ = make_poller(handler)
        poller.poll_feed(FEED)
        poller.poll_feed(FEED)
        assert "if-none-match" not in seen[0]
        assert seen[1]["if-none-match"] == '"v1"'

    def test_304_produces_no_records_and_is_not_a_failure(self, fixture_payload):
        responses = iter(
            [
                httpx.Response(200, content=fixture_payload, headers={"ETag": '"v1"'}),
                httpx.Response(304),
            ]
        )
        poller, sink = make_poller(lambda _request: next(responses))
        poller.poll_feed(FEED)
        before = len(sink.messages)
        result = poller.poll_feed(FEED)
        assert result.status == "not_modified"
        assert len(sink.messages) == before
        assert poller.state[FEED.feed_id].consecutive_failures == 0

    def test_identical_snapshot_is_reported_stale_but_still_produced(self, fixture_payload):
        """The records carry the same event_ids so the lake dedups them The
        stale count is what tells an operator the upstream went quiet
        """
        poller, sink = make_poller(lambda _request: httpx.Response(200, content=fixture_payload))
        first = poller.poll_feed(FEED)
        second = poller.poll_feed(FEED)
        assert first.status == "ok"
        assert second.status == "stale"
        assert second.records == first.records
        assert len(sink.messages) == 2 * first.records

    def test_records_carry_the_producer_id(self, fixture_payload):
        poller, sink = make_poller(lambda _request: httpx.Response(200, content=fixture_payload))
        poller.poll_feed(FEED)
        assert poller.producer_id == "pytest"
        assert sink.stats.records_in > 0


class TestPollerFailures:
    def test_http_500_is_counted_not_raised(self, fixture_payload):
        poller, sink = make_poller(lambda _request: httpx.Response(500, text="boom"))
        result = poller.poll_feed(FEED)
        assert result.status == "http_error"
        assert "HTTP 500" in result.detail
        assert sink.messages == []

    def test_html_error_page_is_a_decode_error_not_an_empty_feed(self):
        """An empty feed is a quiet moment an HTML page is an incident"""
        poller, _ = make_poller(lambda _request: httpx.Response(200, content=b"<html>503</html>"))
        result = poller.poll_feed(FEED)
        assert result.status == "decode_error"
        assert poller.state[FEED.feed_id].decode_errors == 1

    def test_transport_error_is_retried_then_reported(self):
        attempts = {"n": 0}

        def handler(_request):
            attempts["n"] += 1
            raise httpx.ConnectError("refused")

        poller, _ = make_poller(handler)
        result = poller.poll_feed(FEED)
        assert result.status == "http_error"
        assert attempts["n"] == 2  # max_retries

    def test_a_4xx_is_not_retried(self):
        """A 403 means the key is wrong so retrying is pointless"""
        attempts = {"n": 0}

        def handler(_request):
            attempts["n"] += 1
            return httpx.Response(403)

        poller, _ = make_poller(handler)
        poller.poll_feed(FEED)
        assert attempts["n"] == 1

    def test_one_failing_feed_does_not_stop_the_others(self, fixture_payload):
        """Otherwise one bad feed takes down ingestion for all eight"""
        good = FEEDS_BY_ID["nyct-l"]

        def handler(request):
            if "gtfs-l" in str(request.url):
                return httpx.Response(200, content=fixture_payload)
            return httpx.Response(500)

        poller, sink = make_poller(handler, feeds=[FEED, good])
        results = poller.tick()
        statuses = {r.feed_id: r.status for r in results}
        assert statuses[FEED.feed_id] == "http_error"
        assert statuses[good.feed_id] == "ok"
        assert sink.messages

    def test_consecutive_failures_are_tracked_and_reset(self, fixture_payload):
        responses = iter(
            [
                httpx.Response(500),
                httpx.Response(500),
                httpx.Response(200, content=fixture_payload),
            ]
        )
        poller, _ = make_poller(lambda _request: next(responses))
        poller.poll_feed(FEED)
        poller.poll_feed(FEED)
        assert poller.state[FEED.feed_id].consecutive_failures == 2
        poller.poll_feed(FEED)
        assert poller.state[FEED.feed_id].consecutive_failures == 0

    def test_poller_requires_at_least_one_feed(self):
        with pytest.raises(ValueError, match="at least one feed"):
            GtfsRealtimePoller([], MemorySink(), FeedSettings())


class TestPollerRunLoop:
    def test_run_stops_after_max_ticks(self, fixture_payload):
        poller, _ = make_poller(lambda _request: httpx.Response(200, content=fixture_payload))
        slept: list[float] = []
        metrics = poller.run(max_ticks=3, sleep=slept.append)
        assert metrics.ticks == 3
        # No sleep after the final tick
        assert len(slept) == 2

    def test_metrics_summary_counts_by_status(self, fixture_payload):
        responses = iter([httpx.Response(200, content=fixture_payload), httpx.Response(500)])
        poller, _ = make_poller(lambda _request: next(responses))
        metrics = poller.run(max_ticks=2, sleep=lambda _: None)
        summary = metrics.summary()
        assert summary["ticks"] == 2
        assert summary["by_status"]["http_error"] == 1


class TestSinks:
    def test_memory_sink_keeps_keys_and_payloads(self, fixture_records):
        sink = MemorySink()
        for record in fixture_records:
            sink.send(record)
        assert len(sink.messages) == len(fixture_records)
        assert sink.stats.records_in == len(fixture_records)
        assert sink.stats.bytes_out > 0

    def test_null_sink_still_serializes(self, fixture_records):
        """So the benchmark measures encoding not an empty loop"""
        sink = NullSink()
        for record in fixture_records:
            sink.send(record)
        assert sink.stats.bytes_out > 0

    def test_json_serialization_produces_different_bytes(self, fixture_records):
        avro, json_sink = MemorySink("avro"), MemorySink("json")
        avro.send(fixture_records[0])
        json_sink.send(fixture_records[0])
        assert avro.messages[0][2] != json_sink.messages[0][2]

    def test_stats_report_an_events_per_second_figure(self, fixture_records):
        sink = MemorySink()
        for record in fixture_records:
            sink.send(record)
        assert sink.stats.events_per_second > 0
        assert set(sink.stats.as_dict()) >= {"records_in", "events_per_second", "bytes_out"}

    def test_build_sink_dispatches(self):
        kafka, producer = KafkaSettings(), ProducerSettings()
        assert isinstance(build_sink(kafka, producer, target="memory"), MemorySink)
        assert isinstance(build_sink(kafka, producer, target="null"), NullSink)
        with pytest.raises(ValueError, match="unknown sink target"):
            build_sink(kafka, producer, target="s3")


class FakeKafkaProducer:
    """Stands in for confluent_kafka.Producer"""

    def __init__(self, config, *, fail_every: int | None = None, buffer_full_once: bool = False):
        self.config = config
        self.produced: list[dict] = []
        self.flushed = 0
        self.polls = 0
        self._fail_every = fail_every
        self._buffer_full_once = buffer_full_once
        self._pending: list = []

    def produce(self, *, topic, key, value, on_delivery, headers=None):
        if self._buffer_full_once:
            self._buffer_full_once = False
            raise BufferError("queue full")
        self.produced.append({"topic": topic, "key": key, "value": value, "headers": headers})
        err = (
            "delivery failed"
            if self._fail_every and len(self.produced) % self._fail_every == 0
            else None
        )
        self._pending.append((on_delivery, err))

    def poll(self, timeout):
        self.polls += 1
        for callback, err in self._pending:
            callback(err, None)
        self._pending.clear()
        return 0

    def flush(self, timeout):
        self.flushed += 1
        self.poll(0)
        return 0


class TestKafkaSink:
    def _sink(self, **kwargs):
        producer_cfg = ProducerSettings(flush_every=1)
        return KafkaSink(
            KafkaSettings(),
            producer_cfg,
            producer_factory=lambda config: FakeKafkaProducer(config, **kwargs),
        )

    def test_passes_the_idempotence_config_to_librdkafka(self):
        sink = self._sink()
        assert sink._producer.config["enable.idempotence"] is True
        assert sink._producer.config["acks"] == "all"

    def test_routes_each_record_type_to_its_topic(self, fixture_records):
        sink = self._sink()
        for record in fixture_records:
            sink.send(record)
        topics = {m["topic"] for m in sink._producer.produced}
        assert topics == {
            "mta.gtfs.trip_updates.v1",
            "mta.gtfs.vehicle_positions.v1",
            "mta.gtfs.alerts.v1",
        }

    def test_keys_all_stops_of_a_trip_identically(self, fixture_records):
        """Ordering per trip depends on them landing on one partition"""
        sink = self._sink()
        trip_updates = [r for r in fixture_records if r["record_type"] == "trip_update"]
        for record in trip_updates:
            sink.send(record)
        by_trip: dict[str, set[bytes]] = {}
        for record, message in zip(trip_updates, sink._producer.produced, strict=True):
            by_trip.setdefault(record["trip_id"], set()).add(message["key"])
        assert all(len(keys) == 1 for keys in by_trip.values())

    def test_attaches_tracing_headers(self, fixture_records):
        sink = self._sink()
        sink.send(fixture_records[0])
        headers = sink._producer.produced[0]["headers"]
        assert headers["event_id"] == fixture_records[0]["event_id"]
        assert headers["feed_id"] == fixture_records[0]["feed_id"]

    def test_counts_delivery_failures_without_raising(self, fixture_records):
        """One unroutable record must not take down a service ingesting
        thousands per second
        """
        sink = self._sink(fail_every=2)
        for record in fixture_records:
            sink.send(record)
        sink.flush()
        assert sink.stats.records_failed > 0
        assert sink.delivery_errors

    def test_buffer_full_retries_instead_of_dropping(self, fixture_records):
        """Dropping under backpressure is silent data loss"""
        sink = self._sink(buffer_full_once=True)
        sink.send(fixture_records[0])
        assert len(sink._producer.produced) == 1
        assert sink._producer.polls >= 1

    def test_flush_is_called_on_close(self, fixture_records):
        sink = self._sink()
        sink.send(fixture_records[0])
        sink.close()
        assert sink._producer.flushed >= 1
