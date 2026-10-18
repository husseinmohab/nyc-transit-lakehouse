"""the gtfs realtime polling service

one loop n feeds
every tick fetches each enabled feed decodes normalizes and hands records to a
RecordSink

conditional requests
the mta serves etag and last modified so a 304 costs one round trip and no
records
validators are in memory only so the first poll after a restart is unconditional

per feed isolation
one feed returning 500 or html must not stall the other seven so each failure is
counted and the loop carries on

one ingest_timestamp_ms per tick so records from one poll group together and the
normalizer stays pure

a failed feed produces no records which is what lets the freshness sla notice
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any

import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from nyc_transit.config import FeedSettings
from nyc_transit.gtfs.decoder import FeedDecodeError, decode_feed
from nyc_transit.gtfs.feeds import Feed
from nyc_transit.gtfs.normalize import normalize_feed_message
from nyc_transit.producer.sink import RecordSink

log = logging.getLogger(__name__)


@dataclass
class FeedState:
    """per feed polling state

    http validators and health counters
    """

    etag: str | None = None
    last_modified: str | None = None
    last_feed_timestamp_ms: int | None = None
    consecutive_failures: int = 0
    polls: int = 0
    not_modified: int = 0
    decode_errors: int = 0
    http_errors: int = 0
    records_emitted: int = 0

    def conditional_headers(self) -> dict[str, str]:
        headers: dict[str, str] = {}
        if self.etag:
            headers["If-None-Match"] = self.etag
        if self.last_modified:
            headers["If-Modified-Since"] = self.last_modified
        return headers


@dataclass
class PollResult:
    feed_id: str
    status: str  # ok | not_modified | http_error | decode_error | stale
    records: int = 0
    feed_timestamp_ms: int | None = None
    detail: str | None = None


@dataclass
class PollerMetrics:
    ticks: int = 0
    records: int = 0
    results: list[PollResult] = field(default_factory=list)

    def summary(self) -> dict[str, Any]:
        by_status: dict[str, int] = {}
        for r in self.results:
            by_status[r.status] = by_status.get(r.status, 0) + 1
        return {"ticks": self.ticks, "records": self.records, "by_status": by_status}


class GtfsRealtimePoller:
    """polls a fixed list of feeds and writes normalized records to a sink"""

    def __init__(
        self,
        feeds: list[Feed],
        sink: RecordSink,
        settings: FeedSettings,
        *,
        producer_id: str = "poller-1",
        client: httpx.Client | None = None,
        clock: Any = None,
    ) -> None:
        if not feeds:
            raise ValueError("poller requires at least one feed")
        self.feeds = feeds
        self.sink = sink
        self.settings = settings
        self.producer_id = producer_id
        # Injected so the tests can drive a MockTransport and a fake clock
        self._client = client or httpx.Client(
            timeout=settings.http_timeout_seconds,
            headers={"User-Agent": "nyc-transit-lakehouse/0.3 (+github.com/husseinmohab)"},
            follow_redirects=True,
        )
        self._clock = clock or (lambda: time.time())
        self.state: dict[str, FeedState] = {f.feed_id: FeedState() for f in feeds}
        self.metrics = PollerMetrics()

    def _fetch(self, feed: Feed) -> httpx.Response:
        """get one feed with bounded exponential backoff on transport errors

        transport failures get retried and nothing else
        a 4xx or 5xx comes back to the caller since a 403 means the key is wrong so
        retrying is pointless and a 5xx gets handled by the next tick which is cheaper
        than holding the loop open
        """
        url = feed.with_key(self.settings.bus_api_key)
        headers = self.state[feed.feed_id].conditional_headers()

        @retry(
            retry=retry_if_exception_type(httpx.TransportError),
            stop=stop_after_attempt(self.settings.max_retries),
            wait=wait_exponential(multiplier=0.5, max=8),
            reraise=True,
        )
        def _do() -> httpx.Response:
            return self._client.get(url, headers=headers)

        return _do()

    def poll_feed(self, feed: Feed) -> PollResult:
        """poll one feed once

        never raises and always returns a PollResult
        """
        state = self.state[feed.feed_id]
        state.polls += 1
        ingest_ms = int(self._clock() * 1000)

        try:
            response = self._fetch(feed)
        except httpx.TransportError as exc:
            state.consecutive_failures += 1
            state.http_errors += 1
            return PollResult(feed.feed_id, "http_error", detail=f"transport: {exc}")

        if response.status_code == 304:
            state.consecutive_failures = 0
            state.not_modified += 1
            return PollResult(feed.feed_id, "not_modified")

        if response.status_code != 200:
            state.consecutive_failures += 1
            state.http_errors += 1
            return PollResult(feed.feed_id, "http_error", detail=f"HTTP {response.status_code}")

        try:
            message = decode_feed(response.content, feed_id=feed.feed_id)
        except FeedDecodeError as exc:
            state.consecutive_failures += 1
            state.decode_errors += 1
            return PollResult(feed.feed_id, "decode_error", detail=str(exc))

        state.consecutive_failures = 0
        state.etag = response.headers.get("ETag") or state.etag
        state.last_modified = response.headers.get("Last-Modified") or state.last_modified

        records = normalize_feed_message(
            message,
            feed_id=feed.feed_id,
            ingest_timestamp_ms=ingest_ms,
            producer_id=self.producer_id,
        )
        feed_ts = records[0]["feed_timestamp_ms"] if records else None

        # A feed re-serving an identical snapshot is reported as stale but still
        # produced the records carry the same event_ids so the lakehouse dedups
        # them and the count tells an operator the upstream has gone quiet
        status = "ok"
        if feed_ts is not None and feed_ts == state.last_feed_timestamp_ms:
            status = "stale"
        state.last_feed_timestamp_ms = feed_ts

        for record in records:
            self.sink.send(record)
        state.records_emitted += len(records)

        return PollResult(feed.feed_id, status, records=len(records), feed_timestamp_ms=feed_ts)

    def tick(self) -> list[PollResult]:
        """poll every feed once"""
        results = [self.poll_feed(feed) for feed in self.feeds]
        self.metrics.ticks += 1
        self.metrics.records += sum(r.records for r in results)
        self.metrics.results.extend(results)
        for r in results:
            if r.status in {"http_error", "decode_error"}:
                log.warning("feed %s: %s (%s)", r.feed_id, r.status, r.detail)
            else:
                log.info("feed %s: %s records=%d", r.feed_id, r.status, r.records)
        return results

    def run(self, *, max_ticks: int | None = None, sleep: Any = time.sleep) -> PollerMetrics:
        """run the poll loop

        max_ticks bounds it for the tests and for the airflow health probe
        None runs until interrupted
        """
        tick_no = 0
        try:
            while max_ticks is None or tick_no < max_ticks:
                started = time.monotonic()
                self.tick()
                self.sink.flush(timeout_seconds=self.settings.poll_interval_seconds)
                tick_no += 1
                if max_ticks is not None and tick_no >= max_ticks:
                    break
                # Fixed cadence rather than fixed sleep so a slow tick does not
                # drift the schedule and a tick slower than the interval does
                # not sleep at all
                remaining = self.settings.poll_interval_seconds - (time.monotonic() - started)
                if remaining > 0:
                    sleep(remaining)
        except KeyboardInterrupt:  # pragma: no cover
            log.info("interrupted; flushing")
        finally:
            self.sink.close()
        return self.metrics
