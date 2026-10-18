# Reproducing the ~2,000 events/sec claim

## What is measured

The load generator runs the production code path end to end:

```
build FeedMessage -> protobuf serialize -> protobuf decode -> normalize -> Avro encode -> sink
```

It is not a loop over pre-built dicts. The protobuf round trip is included
because decoding is a real share of the cost, and a benchmark that skipped it
would measure the wrong thing.
`tests/unit/test_loadgen.py::TestBenchmark::test_exercises_the_full_production_path`
asserts the Avro encoder actually ran.

## Three sinks, three numbers

| `--target` | What it isolates |
|---|---|
| `null` | decode, normalize and Avro encode. The pipeline's own ceiling, no broker. |
| `memory` | as above, retaining payloads, which shows allocation cost. |
| `kafka` | end to end into a real broker. The number the README quotes. |

Separating them matters: without it the benchmark cannot distinguish "our
transforms are slow" from "the broker is the bottleneck", which is the first
question anyone asks about a throughput claim.

## Commands

```bash
make bench          # 2,000 events/sec for 30s, no broker, no AWS, no MTA key
make bench-ceiling  # unthrottled, this machine's limit
make bench-kafka    # end to end into the local broker (run `make up` first)
```

Or directly:

```bash
python -m nyc_transit.producer.loadgen --target null --duration 30 --target-eps 2000
```

The process exits non-zero if a throttled run achieves less than 95% of target,
so `make bench` is an assertion rather than a number to eyeball.

## Measured results

Apple M4, 10 cores, 16 GB, macOS 26.6.2, Python 3.12.13, single process. JSON
written to `bench-results/` by `make bench`.

| Run | Target | Achieved | Records | Throughput | Notes |
|---|---|---|---|---|---|
| `--target null --target-eps 2000` | 2,000/s | 2,003.3/s (100.2%) | 60,104 in 30.0s | 0.43 MB/s | Avro |
| `--target null --target-eps 0` | unthrottled | 56,704/s | 1,701,205 in 30.0s | 12.1 MB/s | Avro |
| `--target null --target-eps 0 --serialization json` | unthrottled | 83,985/s | 1,259,931 in 15.0s | 68.5 MB/s | JSON |

Reading those numbers:

- The 2,000/s target is met with roughly 28x headroom on one process. The claim
  is about sustaining the MTA's real feed volume, not about finding a limit.
- Avro produces about 215 bytes per record (12.1 MB/s over 56,704/s) against
  JSON's 855. JSON encodes about 1.5x faster per record (`json.dumps` in C
  against fastavro's schema walk) but costs 4x the bytes, which is paid again on
  every Kafka write, every replication hop and every S3 read. Avro is the
  default for that reason, and `PRODUCER_SERIALIZATION=json` exists for `kcat`
  debugging.
- These are single-process numbers. The deployed producer runs one process per
  feed group.

### What 2,000 events/sec corresponds to

A record is one stop-level arrival or departure prediction, which is the grain
the lakehouse stores. One poll of one subway feed expands to roughly 2,000 to
6,000 of these, because each trip carries 8 to 24 downstream stop predictions.
Eight subway feeds on a 15-second cadence is therefore of this order, so the
benchmark reproduces the real shape rather than a convenient one.

### Why the throttled run lands within 0.2%

Rate control is closed-loop on records, not open-loop on messages: after every
message the controller asks when record N was due at the target rate and sleeps
until then.

An open-loop cadence derived from an estimated records-per-message drifts by
however wrong the estimate is, and the estimate is wrong, because a cancelled
trip emits one row instead of 8 to 24. The first version of this benchmark used
the estimate and consistently reported about 97.5% of target; the error was in
the controller, not the pipeline.

`tests/unit/test_loadgen.py::TestBenchmark::test_throttled_run_tracks_the_target_rate`
asserts the controller against an injected fake clock, so it tests the
controller rather than the speed of whatever machine CI landed on.

## Kafka end to end

```bash
make up           # Kafka (KRaft) + MinIO + topics
make bench-kafka  # 2,000 events/sec for 60s into the broker
```

This exercises `KafkaSink`: `acks=all`, `enable.idempotence=true`,
`compression.type=lz4`, keyed partitioning, and a delivery-report callback that
counts permanent failures. `records_failed` includes anything still in flight
after the flush timeout, so a partial delivery cannot be reported as a success.

Not measured here: the number from a real MSK cluster across an AZ boundary,
which is the configuration `infra/` describes. See [limitations.md](limitations.md).

## What the benchmark does not prove

- It does not prove Spark keeps up at 2,000 events/sec. The streaming job's
  throughput depends on the cluster, and the local `make stream-local` run is a
  correctness check, not a capacity test.
- It does not prove the MTA endpoints will serve 2,000 events/sec of genuinely
  new data. They publish every 15 to 30 seconds, so sustained real throughput is
  bounded by the feed.
- It is a synthetic feed. Real payloads vary in size, and the generator's
  distribution is a reasonable approximation rather than a measurement of the
  real one.
