"""the idempotent foreachBatch writer

one class used by the streaming jobs and by the backfill so the write path is the
same in both
dedup.py has the delivery argument this implements
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field
from typing import Any

from nyc_transit.streaming.dedup import (
    MergeKey,
    dedup_within_batch_sql,
    ledger_contains_sql,
    merge_sql,
)

log = logging.getLogger(__name__)


@dataclass
class MergeTarget:
    """a table a micro batch gets merged into

    transform shapes the incoming batch for this table
    defining it per target lets one streaming query keep both a bronze table and its
    silver projection off a single kafka read
    one checkpoint one replay unit two idempotent merges
    """

    table: str
    merge_key: MergeKey
    columns: tuple[str, ...]
    transform: Any = None  # Callable[[DataFrame], DataFrame]

    def update_columns(self) -> tuple[str, ...]:
        return tuple(c for c in self.columns if c not in self.merge_key.key_columns)


@dataclass
class BatchStats:
    batch_id: int
    target: str
    rows_in: int
    rows_after_dedup: int
    skipped_as_replay: bool = False


@dataclass
class IdempotentMergeWriter:
    """foreachBatch callable that merges one micro batch into n targets

    query_name is the ledgers first key column and has to be stable across restarts
    or replay detection is useless
    ledger_table can be None and the merges stay idempotent without it
    """

    spark: Any
    query_name: str
    targets: tuple[MergeTarget, ...]
    ledger_table: str | None = None
    history: list[BatchStats] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not self.targets:
            raise ValueError("writer needs at least one merge target")
        # A unique suffix keeps temp view names from colliding when two queries
        # share a SparkSession which both the e2e test and the supervisor DAG do
        self._view_suffix = uuid.uuid4().hex[:8]

    def _already_committed(self, batch_id: int, target: str) -> bool:
        if not self.ledger_table:
            return False
        sql = ledger_contains_sql(
            ledger_table=self.ledger_table,
            query_name=self.query_name,
            batch_id=batch_id,
            target=target,
        )
        try:
            return bool(self.spark.sql(sql).collect()[0]["n"])
        except Exception as exc:  # noqa: BLE001 - the ledger must never break the write
            # A missing or unreadable ledger degrades to "merge anyway" which
            # is safe because the MERGE is idempotent failing the batch here
            # would turn an optimisation into an outage
            log.warning("ledger lookup failed (%s); merging without replay check", exc)
            return False

    def _record_commit(self, batch_id: int, target: str, rows: int) -> None:
        if not self.ledger_table:
            return
        try:
            self.spark.sql(
                f"INSERT INTO {self.ledger_table} "
                f"(query_name, batch_id, target_table, rows_written, committed_at) "
                f"VALUES ('{self.query_name}', {int(batch_id)}, '{target}', "
                f"{int(rows)}, current_timestamp())"
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("ledger insert failed for batch %s (%s)", batch_id, exc)

    def __call__(self, batch_df: Any, batch_id: int) -> None:
        """merge one micro batch

        signature foreachBatch wants
        """
        # Cache the batch is read once per target and without a cache each
        # read re-executes the Kafka scan and the Avro decode
        batch_df = batch_df.persist()
        try:
            for target in self.targets:
                self._merge_one(batch_df, batch_id, target)
        finally:
            batch_df.unpersist()

    def _merge_one(self, batch_df: Any, batch_id: int, target: MergeTarget) -> None:
        if self._already_committed(batch_id, target.table):
            log.info("batch %s already applied to %s; skipping", batch_id, target.table)
            self.history.append(BatchStats(batch_id, target.table, 0, 0, skipped_as_replay=True))
            return

        shaped = target.transform(batch_df) if target.transform else batch_df
        shaped = shaped.select(*target.columns)

        # foreachBatch hands over a DataFrame belonging to a CLONED session, so
        # a temp view registered on it is invisible to the session this writer
        # was built with. Running the MERGE off self.spark fails with
        # TABLE_OR_VIEW_NOT_FOUND on the batch view. Real tables resolve from
        # either session, which is why only the temp view path breaks and why
        # calling _merge_one directly in a test never shows it.
        session = shaped.sparkSession

        raw_view = f"_batch_{self._view_suffix}_{abs(hash(target.table)) % 10**6}"
        shaped.createOrReplaceTempView(raw_view)

        deduped_view = f"{raw_view}_dedup"
        session.sql(
            dedup_within_batch_sql(
                source_view=raw_view,
                merge_key=target.merge_key,
                select_columns=target.columns,
            )
        ).createOrReplaceTempView(deduped_view)

        rows_in = shaped.count()
        rows_after = session.table(deduped_view).count()
        if rows_after == 0:
            log.debug("batch %s: nothing to merge into %s", batch_id, target.table)
            return

        session.sql(
            merge_sql(
                target_table=target.table,
                source_view=deduped_view,
                merge_key=target.merge_key,
                update_columns=target.update_columns(),
                insert_columns=target.columns,
            )
        )
        self._record_commit(batch_id, target.table, rows_after)
        self.history.append(BatchStats(batch_id, target.table, rows_in, rows_after))
        log.info(
            "batch %s -> %s: %d rows in, %d after in-batch dedup",
            batch_id,
            target.table,
            rows_in,
            rows_after,
        )
