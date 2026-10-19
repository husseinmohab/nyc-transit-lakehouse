# data model

## grain

| table | grain | key |
|---|---|---|
| `bronze.trip_updates` | one arrival/departure prediction per stop per feed publication | `event_id` |
| `bronze.vehicle_positions` | one vehicle position report | `event_id` |
| `bronze.alerts` | one alert version, `informed_entity` nested | `event_id` |
| `silver.trip_stop_predictions` | typed and deduplicated bronze trip updates | `event_id` |
| `silver.vehicle_positions` | typed and deduplicated bronze positions | `event_id` |
| `silver.alert_informed_entities` | one row per (alert version, informed entity) | `(alert_event_id, informed_ordinal)` |
| `dim_route` | one row per GTFS route (SCD1) | `route_id` |
| `dim_stop` | one row per parent station (SCD1) | `stop_id` |
| `dim_vehicle_assignment` | SCD2, one row per (vehicle, continuous run of identical attributes) | `vehicle_assignment_sk` |
| `snap_route_alert_status` | dbt snapshot SCD2, per route service status over time | `dbt_scd_id` |
| `fct_trip_stop_prediction` | one row per (trip, stop, feed publication) | `event_id` |
| `fct_stop_arrival_estimate` | one row per (service_date, trip, platform) | `trip_stop_sk` |
| `agg_route_service_hourly` | route x service_date x arrival hour x direction | composite |
| `dq_prediction_quality_daily` | route x service_date | composite |
| `dq_pipeline_freshness` | one row per monitored table | `table_name` |

## lineage

```
MTA GTFS-RT (8 subway feeds + alerts)
        |  protobuf
        v
  normalize ---> 3 record types (avro, single object encoded)
        |
        v  Kafka, keyed on service_date|trip_id (or |vehicle_id)
        |
        v  Spark Structured Streaming, foreachBatch + MERGE
  +-----------------------------+
  | bronze.trip_updates         |--+
  | bronze.vehicle_positions    |  | same micro batch
  | bronze.alerts               |  | second idempotent MERGE
  +-----------------------------+  |
        |                          v
        |            +----------------------------------+
        |            | silver.trip_stop_predictions     |
        |            | silver.vehicle_positions         |
        |            | silver.alert_informed_entities   |
        |            +----------------------------------+
        | replay substrate                  |
        | backfill reads bronze             | dbt
        | never writes it                   v
        |                              staging
        |                                   |
        |                +------------------+--------------------+
        |                v                  v                    v
        |           dim_route          dim_vehicle_assignment   dim_stop
        |                |              (SCD2, hand rolled)
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
        +--> route_alert_status --> snap_route_alert_status (snapshot, SCD2)
```

## two SCD2 implementations

### dim_vehicle_assignment, hand rolled

tracks which trip and route a vehicle is working including the assigned to
unassigned transition

hand rolled because the source already carries the event time (`reported_at`)
a dbt snapshot would record when dbt noticed, making the history an artefact of
the schedule not the railway

intervals are `[valid_from, valid_to)` closed with a far future sentinel not NULL
a point in time lookup needs no coalesce and an inner join cannot silently drop
current rows

built by
1. collapse to one observation per (vehicle, instant)
   the same vehicle appears once per poll in every feed carrying its route
2. `lag()` the 4 tracked attributes `trip_id` `route_id` `direction_id`
   `is_assigned`
3. flag a change with a NULL safe comparison
   `a <> b` is NULL when either side is NULL and would miss exactly the assigned
   to unassigned transition that matters most
4. running `sum()` of the flag gives a version group
5. `valid_to` from the next group's `valid_from` via `lead()` so intervals abut
6. `is_current` derived from `valid_to` never stored
   two sources of truth for current is how SCD2 tables get two current rows

4 singular tests enforce it
no overlaps, contiguous, one current row per vehicle, every observed vehicle
present
the first two are both needed since a history can be gap free and overlapping or
disjoint and gapped

rebuilt whole not partition scoped
it was incremental first and those tests rejected it
a run boundary depends on the observation before it and is closed by the one
after so version numbering depends on the vehicle's whole history
reading one service date renumbers every run, changes the surrogate key so old
rows are never replaced, and reopens the day's last run to the sentinel
that is an overlap plus a second current row
merging on the surrogate key does not help since a run that splits orphans the
old key
facts are row grained so they partition cleanly, this does not

against the live nyct subway feed this dimension is empty
measured on the wire 0 of 423 VehiclePosition entities carried a vehicle
descriptor and 0 carried a position
the feed sends only trip, stop_id, timestamp, current_status and
current_stop_sequence
the nyct trip extension would give a train id but
`google.transit.nyct_subway_pb2` is not in the gtfs-realtime-bindings wheel so
`nyct_train_id` is null too
the scd2 logic is exercised by the synthetic feed which does assign vehicle ids

### snap_route_alert_status, dbt snapshot

tracks each route status `normal` `degraded` `disrupted` `notice`

a snapshot fits because the MTA edits alerts in place
the source is current state with no history and the only transition time
available is when we observed it

- `strategy = 'check'` not `timestamp`
  `alert_published_at` moves whenever any alert on the route is republished
- `check_cols` excludes `header_text`
  the MTA rewords constantly and snapshotting prose gives a version per typo fix
- `invalidate_hard_deletes = True` so a route leaving the static feed closes its
  interval

the live alerts feed never sets the GTFS `effect` or `cause` enum
the category is in the hand written header so `service_status` matches the
wording the MTA actually publishes
151 distinct live headers sorted 31 disrupted, 84 degraded, 36 notice

## design decisions

**partition on `service_date`**
the GTFS service date is America/New_York with a 3am cutover so a 01:30 trip
belongs to the previous service day
a UTC date would split every night across two partitions
it is also the column every backfill, freshness check and query filters on

**platform against parent station**
GTFS-RT publishes platform ids (`127N`), static `stops.txt` is keyed on the
parent (`127`)
staging derives the parent once so every join uses one definition
the fact keeps both, `dim_stop` is parent grain since two platforms are the same
place and duplicating it would double every count joined through it

**rows with no stop keep a NULL stop**
a cancelled trip carries no `stop_time_update`s and neither does a scheduled trip
the MTA is not predicting yet, the Rockaway Park shuttle does it most of the day
the normalizer emits one row so both stay countable
`stop_resolved` is NULL not false since the question has no answer for a row with
no stop

**`stop_sequence` is mostly absent**
nyct leaves it unset on about 94% of stop_time_updates
anything keyed on it covers 6% of production rows so invariants key on the stop
id

**two facts at two grains**
`fct_trip_stop_prediction` keeps publication grain because churn is the signal
a stop whose ETA moved 8 times in 4 minutes is a different story from one
predicted once and hit
`fct_stop_arrival_estimate` collapses to the final answer and carries
`revision_count` `distinct_eta_count` `prediction_churn_seconds`

**the hourly rollup is trip stop grain**
counting publications makes a route republished more often look busier
`assert_skipped_stops_are_not_double_counted.sql` cross checks the two

**alerts explode in silver not dbt**
nested in bronze so one service change is one row
silver explodes once with `posexplode_outer`, outer because an empty array is a
system wide alert the inner variant would drop
the ordinal is in the merge key which makes the explode idempotent

**`prediction_horizon_seconds` keeps negatives**
the MTA publishes predictions for stops already passed
clamping would destroy a quality signal
`assert_no_prediction_precedes_its_publication.sql` bounds it at minus one hour
which catches a seconds against milliseconds bug

## iceberg table properties

| property | value | why |
|---|---|---|
| `format-version` | `2` | row level deletes. on v1 a MERGE rewrites whole data files every micro batch which is unusable at streaming cadence |
| `write.{delete,update,merge}.mode` | `merge-on-read` | a MERGE touching a few rows writes a small delete file instead of rewriting a 128 MB parquet file. cost is read amplification repaid by weekly compaction |
| `write.target-file-size-bytes` | `134217728` | 128 MB, a compaction target not what a micro batch produces |
| `write.distribution-mode` | `hash` | one file per partition per batch instead of one per (partition x task) |
| `write.metadata.delete-after-commit.enabled` | `true` | without it the `metadata.json` chain grows one file per micro batch and listing slows over days |
| `history.expire.max-snapshot-age-ms` | 7 days | snapshots are the backfill rollback path, beyond a week they are storage cost |
| `identifier-fields` | the merge key | documents the grain in the catalog where a reader looks before the code |

## weekly maintenance is required

streaming writes with merge-on-read deletes accrue debt
without `iceberg_maintenance` the tables get slower every day until queries time
out

- `rewrite_data_files` compacts the small files a 30 second cadence produces
  (`min-input-files = 5` so a run does not rewrite every partition every week)
- `rewrite_position_delete_files` folds delete files into the data since read
  cost grows with their number
- `expire_snapshots` 7 day retention, `retain_last => 10`
- `remove_orphan_files` `older_than` 3 days not hours
  deleting a live in flight file is the one maintenance action that destroys data

## glue catalog notes

iceberg maps a namespace onto a glue database and has no prefix property of its
own
the databases are `<prefix>_bronze` `<prefix>_silver` `<prefix>_marts`
`<prefix>_snapshots` `<prefix>_staging`
so `LAKEHOUSE_BRONZE_NAMESPACE` must be the real database name not `bronze`

dbt gets the same mapping from `macros/generate_schema_name.sql` plus
`glue_db_prefix`
staging materializes as tables there not views since iceberg's SparkCatalog
cannot hold a view

static reference tables are written by `streaming/static_tables.py` not
`dbt seed`
dbt-spark builds one INSERT INTO VALUES per seed and does not escape a single
quote which `Prince's Bay` and `E 143 St-St Mary's St` both contain
so do not run `dbt build` on a spark target, it runs seeds and a failing seed
drops the table the loader wrote
run `dbt run`, `dbt test` and `dbt snapshot` instead

casts are `as string` not `as varchar`
duckdb takes either, spark rejects a bare varchar with DATATYPE_MISSING_SIZE

the iceberg runtime is pinned at 1.10.2
1.7.1 crashes the jvm on a vectorized parquet read, a SIGSEGV inside the jvm's
own symbol tables that reproduces on corretto and temurin 17 alike, so it is
the iceberg reader and not the jvm
1.10.2 reads the same tables with vectorization left on

spark local dirs default to `/tmp` which is a 3.9G tmpfs on amazon linux 2023
that is RAM backed so a shuffle fills memory and the run dies
point `SPARK_LOCAL_DIRS` at the EBS volume

### the snapshot runs on athena and not on spark

`snap_route_alert_status` builds on duckdb and on athena
it fails on a glue backed iceberg catalog through dbt-spark with
"Replacing a view is not supported by catalog"

dbt-spark builds the snapshot staging relation as a view
it already knows an iceberg catalog cannot hold one and drops the catalog and
schema to put it in the spark session catalog instead, but it decides that from
`target_relation.is_iceberg` which it parses out of `show table extended`
looking for `Provider: iceberg`
a datasource v2 catalog does not report that line so the branch is never taken
forcing the branch does not help either, an unqualified name still resolves
through `spark.sql.defaultCatalog` back to the iceberg catalog and dbt-spark
refuses a relation with `database` set

athena supports views natively so none of that applies
this is the main reason the transform layer runs on athena
