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


@dataclass
class IdempotentMergeWriter:
    """foreachBatch callable that merges one micro batch into n targets

    a replayed batch merges again on the same keys and lands the same rows so the
    writer keeps no record of what it has already done
    """

    spark: Any
    query_name: str
    targets: tuple[MergeTarget, ...]
    history: list[BatchStats] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not self.targets:
            raise ValueError("writer needs at least one merge target")
        # A unique suffix keeps temp view names from colliding when two queries
        # share a SparkSession which both the e2e test and the supervisor DAG do
        self._view_suffix = uuid.uuid4().hex[:8]

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
        shaped = target.transform(batch_df) if target.transform else batch_df
        shaped = shaped.select(*target.columns)

        # foreachBatch hands over a DataFrame belonging to a cloned session. A
        # temp view registered on it is invisible to self.spark, so the MERGE
        # has to run off the batch's own session or it fails with
        # TABLE_OR_VIEW_NOT_FOUND on the batch view.
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
        self.history.append(BatchStats(batch_id, target.table, rows_in, rows_after))
        log.info(
            "%s batch %s -> %s: %d rows in, %d after in-batch dedup",
            self.query_name,
            batch_id,
            target.table,
            rows_in,
            rows_after,
        )
