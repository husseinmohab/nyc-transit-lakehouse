# Data model

## Grain

| Table | Grain | Key |
|---|---|---|
| `bronze.trip_updates` | one arrival/departure prediction, per stop, per feed publication | `event_id` |
| `bronze.vehicle_positions` | one vehicle position report | `event_id` |
| `bronze.alerts` | one service alert version, `informed_entity` nested | `event_id` |
| `silver.trip_stop_predictions` | typed and deduplicated bronze trip updates | `event_id` |
| `silver.vehicle_positions` | typed and deduplicated bronze vehicle positions | `event_id` |
| `silver.alert_informed_entities` | one row per (alert version, informed entity) | `(alert_event_id, informed_ordinal)` |
| `dim_route` | one row per GTFS route (SCD1) | `route_id` |
| `dim_stop` | one row per parent station (SCD1) | `stop_id` |
| `dim_vehicle_assignment` | SCD2: one row per (vehicle, continuous run of identical attributes) | `vehicle_assignment_sk` |
| `snap_route_alert_status` | dbt snapshot (SCD2): per-route service status over time | `dbt_scd_id` |
| `fct_trip_stop_prediction` | one row per (trip, stop, feed publication) | `event_id` |
| `fct_stop_arrival_estimate` | one row per (service_date, trip, platform) | `trip_stop_sk` |
| `agg_route_service_hourly` | route x service_date x arrival hour x direction | composite |
| `dq_prediction_quality_daily` | route x service_date | composite |
| `dq_pipeline_freshness` | one row per monitored table | `table_name` |

## Lineage

```
MTA GTFS-RT (8 subway feeds + alerts)
        |  protobuf
        v
  normalize ---> 3 record types (Avro, single-object encoded)
        |
        v  Kafka, keyed on service_date|trip_id (or |vehicle_id)
        |
        v  Spark Structured Streaming, foreachBatch + MERGE
  +-----------------------------+
  | bronze.trip_updates         |--+
  | bronze.vehicle_positions    |  | same micro-batch,
  | bronze.alerts               |  | second idempotent MERGE
  +-----------------------------+  |
        |                          v
        |            +----------------------------------+
        |            | silver.trip_stop_predictions     |
        |            | silver.vehicle_positions         |
        |            | silver.alert_informed_entities   |
        |            +----------------------------------+
        | replay substrate                  |
        | (backfill reads bronze,           | dbt
        |  never writes it)                 v
        |                      staging (5 views)
        |                                   |
        |                +------------------+--------------------+
        |                v                  v                    v
        |           dim_route          dim_vehicle_assignment   dim_stop
        |           dim_stop            (SCD2, hand-rolled)
        |                |                  |
        |                v                  |
        |      fct_trip_stop_prediction <---+
        |                |
        |                v
        |      fct_stop_arrival_estimate
        |                |
        |        +-------+--------+
        |        v                v
        | agg_route_service   dq_prediction
        |     _hourly          _quality_daily
        |
        +--> route_alert_status --> snap_route_alert_status (dbt snapshot, SCD2)
```

## The two SCD2 implementations

### dim_vehicle_assignment: hand-rolled incremental SCD2

Tracks which trip and route a vehicle is working, including the assigned and
unassigned transition.

Hand-rolled because the source is an event stream that already carries the event
time (`reported_at`), so the real transition instants are in the data. A dbt
snapshot would record "dbt noticed this at 03:00", and the history would be an
artefact of the schedule rather than of the railway.

Interval convention: `[valid_from, valid_to)`, inclusive start and exclusive end,
closed with a far-future sentinel rather than `NULL`. A point-in-time lookup is
then `valid_from <= t and t < valid_to` with no `coalesce`, and an accidental
inner join cannot silently drop current rows. A closed version's `valid_to`
equals its successor's `valid_from` exactly: no gaps, no overlaps.

How it is built:

1. collapse to one observation per (vehicle, instant), because the same vehicle
   appears once per poll in every feed carrying its route and two feeds can
   publish an identical timestamp;
2. `lag()` the four tracked attributes (`trip_id`, `route_id`, `direction_id`,
   `is_assigned`);
3. flag a change with a NULL-safe comparison, because `a <> b` is NULL when
   either side is NULL and a plain inequality would miss exactly the
   assigned/unassigned transition that matters most;
4. a running `sum()` of the change flag gives a version group;
5. collapse each group, and take `valid_to` from the next group's `valid_from`
   via `lead()`, so intervals abut;
6. `is_current` is derived from `valid_to`, never stored independently, because
   two sources of truth for "current" is how SCD2 tables end up with two current
   rows.

Four tests in `dbt/nyc_transit/tests/singular/` enforce it: no overlapping
intervals, contiguous intervals, exactly one current row per vehicle, and every
observed vehicle present. The first two are both needed, because a history can
be gap-free and overlapping, or disjoint and gapped.

On the committed seeds the deepest history is 5 versions, including assigned to
unassigned to reassigned.

### snap_route_alert_status: dbt snapshot

Tracks each route's service status (`normal`, `degraded`, `disrupted`,
`notice`).

A snapshot is right here because the MTA edits alerts in place and overwrites the
old text. The source is current-state with no history of its own, and the only
available transition time is when we observed it.

- `strategy = 'check'`, not `timestamp`: `alert_published_at` changes whenever
  any alert on the route is republished, so a timestamp strategy would open a new
  version on every run.
- `check_cols` excludes `header_text`. The MTA rewords alerts constantly, and
  snapshotting on the prose would produce a version per typo fix and bury the
  real transitions.
- `invalidate_hard_deletes = True`, so a route leaving the static feed closes its
  interval instead of leaving a dangling current row.

## Design decisions

**Partitioning on `service_date`, not an ingest date.** The GTFS service date is
in America/New_York with a 3 a.m. cutover (a 01:30 trip belongs to the previous
service day). Partitioning on a UTC date would split every night's service
across two partitions. It is also the column every backfill, freshness check and
analytical query filters on, which is what makes a backfill partition-scoped and
re-runnable.

**Platform against parent station.** GTFS-RT publishes platform-level stop ids
(`127N` is Times Sq northbound); static `stops.txt` is keyed on the parent
(`127`). The staging layer derives the parent once, so every downstream join uses
one definition. The fact keeps both: `platform_stop_id` for directional analysis,
`stop_id` for the dimension join. `dim_stop` is at parent-station grain, because
a northbound and a southbound platform are the same place and duplicating the
station would double every count joined through it.

**Cancellations keep a row with a NULL stop.** A cancelled trip carries no
`stop_time_update`s. The normalizer still emits one row for it so cancellations
stay countable; `stop_id` and `stop_sequence` are NULL, and `stop_resolved` is
NULL rather than false, because "did the stop resolve?" has no answer for a row
with no stop and collapsing that to false would make every cancellation look like
a broken join.

**Two facts at two grains.** `fct_trip_stop_prediction` keeps publication grain
because prediction churn is the signal: a stop whose ETA moved eight times in
four minutes is a different operational story from one predicted once and hit.
`fct_stop_arrival_estimate` collapses to the final answer and carries
`revision_count`, `distinct_eta_count` and `prediction_churn_seconds` so the
churn survives the collapse.

**The hourly rollup is built on trip-stop grain, not publication grain.**
Otherwise a route whose predictions are republished more often looks busier than
one republished less often. Counting publications instead of trip-stops is the
easiest way to produce a plausible-looking but wrong transit metric, and
`assert_skipped_stops_are_not_double_counted.sql` cross-checks the two rollups.

**Alerts are exploded in silver, not in dbt.** `informed_entity` stays nested in
bronze (one service change is one row, so `count(distinct alert)` is not a join
away), and silver explodes it once with `posexplode_outer`. Outer, because an
alert with an empty array is a system-wide alert that the inner variant would
drop. The ordinal is part of the merge key, which makes the explode idempotent.
Every downstream consumer then sees a flat table.

**`prediction_horizon_seconds` keeps negatives.** The MTA does publish
predictions for stops whose time has already passed (a train sitting at a
platform). Clamping would destroy a measurable quality signal, and
`assert_no_prediction_precedes_its_publication.sql` bounds it at minus one hour,
which is what would catch a seconds-against-milliseconds unit bug.

## Iceberg table properties

| Property | Value | Why |
|---|---|---|
| `format-version` | `2` | Row-level deletes. On v1 a MERGE rewrites whole data files every micro-batch, which is unusable at streaming cadence. |
| `write.{delete,update,merge}.mode` | `merge-on-read` | A MERGE touching a few rows writes a small delete file instead of rewriting a 128 MB Parquet file. The cost is read amplification, repaid by the weekly compaction DAG. |
| `write.target-file-size-bytes` | `134217728` | 128 MB, a compaction target rather than what a micro-batch produces. `iceberg_maintenance` is what makes it true. |
| `write.distribution-mode` | `hash` | One file per partition per batch, instead of one per (partition x task). |
| `write.metadata.delete-after-commit.enabled` | `true` | Without it the `metadata.json` chain grows by one file per micro-batch and listing the table slows down over days. |
| `history.expire.max-snapshot-age-ms` | 7 days | Snapshots are the backfill's rollback path; beyond a week they are pure storage cost. |
| `identifier-fields` | the merge key | Documents the grain in the catalog itself, where a reader looks before they read the code. |

## Weekly maintenance is required

Streaming writes with merge-on-read deletes accrue three kinds of debt, and
without `iceberg_maintenance` the tables get slower every day until queries time
out:

- `rewrite_data_files` compacts the small files a 30-second cadence produces
  (`min-input-files = 5`, so a run does not rewrite every partition every week);
- `rewrite_position_delete_files` folds delete files into the data, since read
  cost grows with their number;
- `expire_snapshots` with 7-day retention and `retain_last => 10`;
- `remove_orphan_files` with `older_than` of 3 days, not hours, because deleting
  a live in-flight file is the one maintenance action that destroys data.
