# Delivery guarantees

The accurate description is **at-least-once delivery with idempotent, keyed
application**, which yields **exactly-once effects** in the lakehouse. That is
the same guarantee Flink's two-phase-commit sinks and Kafka Streams give you:
neither can make a stateless HTTP endpoint replay deterministically, and neither
can prevent a duplicate it cannot recognise. What makes this pipeline
exactly-once in effect is that every record carries a deterministic identity, so
a duplicate is always recognisable.

## The three links

### 1. MTA feed to Kafka

The GTFS-Realtime endpoints are pull-based and stateless. A producer crash
between polling and producing means the next process may re-poll a snapshot it
already published, and there is no transaction to join.

What makes that safe is that records are not assigned random ids:

```
event_id = blake2b(record_type ∥ feed_id ∥ trip_id ∥ service_date ∥ stop_id ∥ stop_sequence ∥ feed_timestamp_ms)
```

`nyc_transit.gtfs.normalize.make_event_id` is a pure function of the business
key, so re-polling the same snapshot produces byte-identical records and the
duplicate is detectable downstream.
`tests/unit/test_normalize.py::TestNormalizeFixture::test_normalization_is_pure`
pins this.

Three details matter:

- The parts are joined on `\x1f` (unit separator), so `("A1","B")` and
  `("A","1B")` cannot collide.
- blake2b, not Python's `hash()` (salted per process) and not `uuid4()`
  (random). The id has to be reproducible across processes and replays.
- `feed_id` is part of the key, so the same payload served by two feeds does not
  collide.

Within Kafka, `enable.idempotence=true` plus `acks=all`
(`nyc_transit.config.KafkaSettings`) means librdkafka attaches a producer id and
per-partition sequence number, so a retry after a network timeout cannot
duplicate the record in the log. Broker-side, `min.insync.replicas=2` with
`unclean.leader.election.enable=false` (`infra/modules/msk`) means an
acknowledged write survives a single broker loss.

Message keys are `service_date|trip_id` (or `|vehicle_id`), so every update for
one trip lands on one partition and arrives in publication order. Without that,
two predictions for the same stop could be committed out of order across
partitions.

### 2. Kafka to Spark

The Kafka source writes the offset range of each micro-batch to the checkpoint's
offset log before processing it, and to the commit log after. On restart an
uncommitted batch is replayed with byte-identical content.

- One checkpoint directory per query, never shared
  (`LakehouseSettings.checkpoint_for`). Two queries sharing one would corrupt
  each other's progress.
- `failOnDataLoss=true`. If Kafka retention has expired offsets the stream still
  needs, that is data loss, and the job stops so a human decides whether to
  backfill. Set explicitly so nobody "fixes" a crash loop by flipping it.
- `maxOffsetsPerTrigger=200000` bounds the first batch after an outage.
  Unbounded, the catch-up OOMs and never completes.
- `spark.speculation=false`, so a duplicated speculative task cannot
  double-write.

So Spark gives at-least-once with deterministic replay.

### 3. Spark to Iceberg

Writes go through `foreachBatch` and an idempotent `MERGE INTO`
(`nyc_transit.streaming.dedup`, `nyc_transit.streaming.writer`):

```sql
MERGE INTO lakehouse.bronze.trip_updates AS t
USING batch_dedup AS s
ON t.event_id = s.event_id
WHEN MATCHED AND s.feed_timestamp_ms > t.feed_timestamp_ms THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT (...) VALUES (...)
```

A first delivery inserts. A later publication for the same key updates in place.
A replay matches with an equal version, so both arms are skipped and nothing is
written. Replaying any batch, any number of times, in any order, converges on
the same table state.

Four non-obvious choices:

| Choice | Why |
|---|---|
| Strict `>`, not `>=` | With `>=` a replay rewrites every matched row, so a delete file plus a data file per micro-batch for no change. Strict `>` makes a replay free. |
| The version column is event time (`feed_timestamp_ms`), never processing time | A processing-time version always wins on replay, so the result would depend on replay order, which is what idempotence rules out. |
| In-batch dedup before the MERGE | Iceberg raises if two source rows match one target row. One micro-batch routinely holds several publications of one key, because the producer polls faster than the feed changes. `ROW_NUMBER()` keeps the highest version, tie-broken on the key so a replay picks the same winner. |
| No `DELETE` arm | Kafka can replay a batch, and a `WHEN NOT MATCHED BY SOURCE DELETE` arm would wipe every row absent from that one micro-batch. |

### Why foreachBatch rather than the Iceberg streaming sink

`.format("iceberg").outputMode("append")` appends. An append sink replaying a
batch duplicates rows unless it can prove the batch was already committed, and
proving that means reasoning about the connector's internal epoch bookkeeping.
`foreachBatch` plus MERGE puts the dedup key in our schema and our SQL, where it
is reviewable and testable, and it lets the backfill reuse the same write path.

## The commit ledger is not the guarantee

`bronze._stream_commits` records `(query_name, batch_id, target_table)` so a
replayed batch can be skipped without touching the data table.

This is a cost optimisation, not the correctness argument. The ledger write is
not atomic with the MERGE, and treating a non-atomic ledger as the guarantee is
a common mistake: a crash between MERGE and ledger-insert would then "lose" a
batch. Here that window is harmless, because the batch replays, the MERGE is a
no-op, and the ledger row is written on the second attempt.

`tests/integration/test_iceberg_exactly_once.py::TestExactlyOnce::test_replay_under_a_new_batch_id_is_also_a_no_op`
disables the ledger entirely and replays under different batch ids. If that test
ever fails, the correctness story was resting on the ledger.

A missing or unreadable ledger degrades to "merge anyway"
(`test_a_broken_ledger_degrades_to_merging_anyway`), because failing the batch
there would convert an optimisation into an outage.

## Backfill: replacement, not merge

The streaming path merges because it receives a trickle of rows whose keys may
already exist. A backfill knows it owns the whole partition, so it replaces it:

```sql
DELETE FROM lakehouse.silver.trip_stop_predictions WHERE service_date IN (DATE '2026-11-01');
INSERT INTO lakehouse.silver.trip_stop_predictions (...)
SELECT ... FROM lakehouse.bronze.trip_updates WHERE service_date = DATE '2026-11-01';
```

Replacement is cheaper (a partition-aligned `DELETE` drops whole data files from
metadata instead of writing position deletes) and stronger: a MERGE-based
backfill cannot remove a row that should no longer exist, so a bug that
over-produced rows yesterday would survive the backfill.
`test_backfill_removes_rows_that_should_no_longer_exist` injects a row bronze
cannot justify and asserts the backfill removes it.

Bronze is never a backfill target. It is the replay substrate: once a bad silver
transform ships, bronze is what makes the fix a ten-minute backfill instead of
data that is gone. `nyc_transit.backfill.partitions.guard_target_table` enforces
that in code, and `partition_delete_sql` validates every partition value as an
ISO date before it reaches the SQL, because that function builds a `DELETE`.

## What is tested

`tests/integration/test_iceberg_exactly_once.py` writes to a real local Iceberg
warehouse through the real writer. 24 tests:

| Claim | Test |
|---|---|
| A replay changes nothing | `test_replaying_the_same_batch_changes_nothing` |
| Correctness does not need the ledger | `test_replay_under_a_new_batch_id_is_also_a_no_op` |
| No duplicate keys after replay | `test_event_ids_stay_unique_after_replay` |
| A later publication updates in place | `test_a_later_publication_updates_in_place` |
| An out-of-order publication is ignored | `test_an_older_publication_is_ignored` |
| In-batch duplicates collapse, highest version wins | `test_duplicates_within_one_batch_collapse`, `test_in_batch_dedup_keeps_the_highest_version` |
| The SQL backfill reproduces the PySpark stream | `test_backfill_reproduces_the_streaming_result` |
| The backfill is idempotent | `test_backfill_is_idempotent` |
| A backfill of day N leaves day N-1 alone | `test_backfill_leaves_neighbouring_partitions_untouched` |
| A backfill removes rows bronze cannot justify | `test_backfill_removes_rows_that_should_no_longer_exist` |

## What is not guaranteed

- If the MTA published a logically different fact under an identical business
  key and timestamp, the two would collapse to one row. The feed does not do
  this, but nothing here would detect it if it started.
- Ordering across record types. Trip updates, vehicle positions and alerts are
  separate topics with separate partitioning, so a trip update and the vehicle
  position for the same train can be applied in either order. No model depends
  on that ordering.
- Alert edit history is identity-based, not temporal. An MTA in-place edit
  becomes a new row because the alert text is hashed into `event_id`. If the MTA
  reverted an edit, the reverted version would collide with the original and the
  intermediate version would be an extra row, which is not a corruption but not
  a clean temporal history either. `snap_route_alert_status` provides the
  temporal view.
- Nothing here has run against MSK or a real multi-node Spark cluster. See
  [limitations.md](limitations.md).
