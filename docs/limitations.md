# Limitations: what is verified, what is not

This file exists because the README makes concrete claims, and a claim without a
stated scope is a claim nobody should trust. Everything below is a known gap.

## Verified by running it

| Claim | How | Result |
|---|---|---|
| Python test suite passes locally | `pytest` | 411 passed |
| Includes real Spark and Iceberg execution | `pytest -m "spark or iceberg"` | 42 tests boot a local SparkSession and write a real Iceberg warehouse |
| Exactly-once effects under replay | `tests/integration/test_iceberg_exactly_once.py` | 24 tests, including a ledger-disabled replay |
| dbt project builds and all tests pass | `dbt seed && dbt build` (DuckDB) | 188 nodes: 14 models, 1 snapshot, 5 seeds, 168 tests, 0 failures |
| 40+ dbt tests | `make dbt-count-tests` | 168 (153 generic, 15 singular) |
| Incremental models are idempotent | same partition run twice, then `dbt test` | identical row counts, 168 tests still pass |
| ~2,000 events/sec is reproducible | `make bench` | 2,003 events/sec (100.2% of target); ceiling 56,704/s |
| Terraform is syntactically valid | `fmt -check -recursive`, `init -backend=false`, `validate` (1.9.8) | clean; each module also validates standalone |
| docker-compose is valid | `docker compose config` for the default stack and all four profiles | clean |
| Lint | `ruff check`, `black --check` | clean |
| Airflow DAGs import | `python scripts/check_dags.py` against Airflow 2.10.5 | 3 DAGs, 12 tasks, no import errors |

## Not verified: needs an AWS account

Nothing here has been applied to a cloud account. That is a constraint, not an
oversight.

- **`terraform apply` has never run.** `validate` checks syntax, types, provider
  schemas and module wiring. It does not check that AWS accepts the
  configuration: service quotas, MSK broker/subnet arithmetic, IAM policy
  evaluation, Glue and Lake Formation interaction. Expect to iterate on a first
  real apply.
- **MSK.** Never provisioned. The SASL/IAM configuration, the broker sizing and
  the `min.insync.replicas=2` durability argument are reasoned from the measured
  throughput (0.43 MB/s at 2,000 events/sec) and the Kafka docs, not from an
  observed cluster.
- **Glue Data Catalog.** The Iceberg `GlueCatalog` path is configured but never
  exercised; local and CI runs use the path-based `hadoop` catalog. If the
  account has Lake Formation enforcement enabled, the IAM-only grants in
  `infra/modules/iam` are insufficient and `aws_lakeformation_permissions`
  resources would be needed. Not guessed at here.
- **S3.** The `s3a://` path is configured for MinIO, but the local test runs use
  the filesystem warehouse. S3-specific behaviour (listing consistency,
  request-rate throttling on a hot prefix, multipart tuning) is untested.
- **IAM policies have not been evaluated.** The least-privilege split (producer
  writes Kafka only; streaming reads Kafka and writes S3 and Glue; dbt writes
  silver and marts with bronze read-only behind an explicit Deny) is the intended
  design. Whether every action an Iceberg commit needs is present can only be
  learned from a real apply, so expect to add one or two.
- **`dbt --target spark` has never run.** All dbt verification is against
  DuckDB. The models are written to compile against both and the dialect
  differences are isolated in `macros/cross_adapter.sql`, but the Spark/Thrift
  target is unexercised.
- **Airflow has been import-checked, not run.** `scripts/check_dags.py` proves
  every DAG parses and that the expected `dag_id`s exist with tasks. No DAG run
  has executed, because the task bodies need Spark, Iceberg and MSK.

## Not verified: needs the live MTA feeds

- **No real GTFS-Realtime payload has been processed.** Every fixture and seed is
  synthetic, generated from `nyc_transit.gtfs.synthetic` with real NYCT route ids
  and real platform-level stop ids. It matches production in shape, nullability,
  id format and string length, but not in the MTA's own quirks: unusual enum
  values, occasional malformed entities, feed-specific oddities.
- **The NYCT protobuf extension is not decoded.** `nyct_subway.proto` (field
  1001) carries the physical train id, track-level direction and `is_assigned`.
  The `gtfs-realtime-bindings` wheel does not ship a compiled `nyct_subway_pb2`,
  so `nyc_transit.gtfs.nyct` reads it only if one happens to be importable and
  otherwise writes nulls. `nyct_train_id` is a nullable enrichment column, never
  a join key, so silver and the marts are identical with or without it.
- **The bus feeds are untested.** They require a OneBusAway key, which is not in
  this repository. Without one they are skipped, never faked, because
  substituting synthetic data for an unreachable feed would corrupt the freshness
  metrics.
- **Feed-level failure modes are tested against mocks**, not real MTA behaviour.
  `httpx.MockTransport` covers 500s, HTML error pages, truncated protobuf, 304s
  and transport errors (`tests/unit/test_producer.py`).

## Scope decisions

These are choices, not gaps.

- **`gtfs_stops.csv` is a curated subset**: 26 real routes and about 60 real
  stations, not the full ~1,500-row `stops.txt`. Committing all of it would bloat
  the repo without strengthening any test. The join-coverage test therefore
  asserts a 99% threshold rather than 100%, so it stays meaningful against
  production's full file. On the seeds, coverage is 100%
  (`test_stop_join_coverage_is_total_on_the_seeds`).
- **Station coordinates are rounded to 4 decimals** (about 11 m). Enough for a
  bounding-box check and a map, and honest about not being surveyed data.
- **No prediction accuracy metric.** `dq_prediction_quality_daily` measures
  stability and coverage: churn, revision count, uncertainty, missing-ETA rate.
  Accuracy needs a ground-truth arrival time, which this pipeline does not have.
  An actual arrival could be derived from the last prediction before a
  `STOPPED_AT` vehicle position, but that is an estimate, and labelling an
  estimate as actual is how a data product starts lying.
- **The silver projection is written three times**: PySpark
  (`streaming/transforms.py`), SQL (`backfill/partitions.py`) and plain Python
  (`streaming/reference.py`). That duplication is a real cost, paid because the
  batch path must be pure SQL so it can run with no Python dependency and be
  audited from a SQL console, and the Python version generates the dbt seeds.
  Three differential tests keep them honest.
- **No Confluent Schema Registry.** The wire format is Avro single-object
  encoding (`0xC3 0x01` plus an 8-byte CRC-64-AVRO fingerprint), which gives the
  property that matters, that a consumer can tell which schema wrote a message
  from the bytes alone, with no service and no network. The framing is pinned
  against the Avro spec's own `"null"` test vector in
  `tests/unit/test_schema_registry.py`. Migrating to a real registry means
  replacing two functions.
- **Bronze and silver are maintained by one streaming query**, not two chained
  ones. Iceberg's streaming read cannot see the output of a MERGE without being
  told to skip overwrite snapshots, which would drop the merged rows, so reading
  bronze as a stream is only safe for an append-only table and bronze is not one.
  One checkpoint also means one replay unit.
- **`dq_pipeline_freshness.sla_breached` is always true against the committed
  seeds**, because the seeds are a fixed historical window and lag therefore
  grows forever. That is correct on static data, and it is why the dbt tests
  assert the table's shape rather than that lag is within SLA. A test that can
  only pass on live data fails CI every day and gets deleted.
- **`make` could not be executed on the authoring machine**: the system `make` is
  gated behind an unaccepted Xcode license and `sudo` was out of scope. The
  Makefile was validated statically (34 targets, all `.PHONY`, all documented,
  tab-indented recipes) and every recipe's underlying command was run directly.
  It works normally on Linux CI and on a machine with the Xcode licence accepted.

## Known rough edges

- `dbt build` alone, on a fresh target, runs source-level tests before the seeds
  exist. Run `dbt seed` first, which is what the Makefile and CI do.
- `scripts/check_dags.py` needs Airflow installed, which `requirements/dev.txt`
  does not include: Airflow only resolves against its own constraints file, and
  mixing that with pyspark, dbt and great-expectations in one resolution produces
  an unbootable scheduler. Install `requirements/airflow.txt` in a separate venv.
- `requirements/*.txt` use lower-bound pins rather than a lockfile. For a
  reproducible deploy, generate one (`uv pip compile`) per image.
