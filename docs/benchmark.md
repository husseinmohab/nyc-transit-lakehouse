# benchmark

## what is measured

```
build FeedMessage -> protobuf serialize -> protobuf decode -> normalize -> avro encode -> sink
```

the protobuf round trip is included because decoding is a real share of the
cost

`tests/unit/test_loadgen.py::TestBenchmark::test_exercises_the_full_production_path`
asserts the avro encoder actually ran

## sinks

| `--target` | isolates |
|---|---|
| `null` | decode, normalize, avro encode. pipeline ceiling, no broker |
| `memory` | same but retains payloads, shows allocation cost |
| `kafka` | end to end into a real broker |

separating them is what tells a slow transform apart from a slow broker

## commands

```bash
make bench          # 2,000 events/sec for 30s, no broker, no AWS, no MTA key
make bench-ceiling  # unthrottled
make bench-kafka    # end to end into the local broker, run `make up` first
```

or directly

```bash
python -m nyc_transit.producer.loadgen --target null --duration 30 --target-eps 2000
```

exits non zero under 95% of target so `make bench` asserts

## results

Apple M4, 10 cores, 16 GB, macOS 26.6.2, Python 3.12.13, single process
JSON written to `bench-results/`

| run | target | achieved | records | throughput |
|---|---|---|---|---|
| `--target null --target-eps 2000` | 2,000/s | 2,003.3/s (100.2%) | 60,104 in 30.0s | 0.43 MB/s |
| `--target null --target-eps 0` | unthrottled | 56,704/s | 1,701,205 in 30.0s | 12.1 MB/s |
| `--target null --target-eps 0 --serialization json` | unthrottled | 83,985/s | 1,259,931 in 15.0s | 68.5 MB/s |

notes
- 2,000/s met with ~28x headroom on one process
- the claim is sustaining real feed volume not finding a limit
- avro ~215 bytes per record against JSON 855
- JSON encodes ~1.5x faster per record (`json.dumps` in C against fastavro
  schema walk) and costs 4x the bytes
- those bytes are paid again on every Kafka write, replication hop and S3 read
- `PRODUCER_SERIALIZATION=json` exists for `kcat` debugging
- single process numbers, the deployed producer runs one per feed group

## what 2,000 events/sec is

a record is one stop level arrival or departure prediction
one poll of one subway feed expands to roughly 2,000 to 6,000 of them since each
trip carries 8 to 24 downstream stop predictions
8 subway feeds on a 15 second cadence is that order

## why the throttled run lands within 0.2%

rate control is closed loop on records not open loop on messages
after every message the controller asks when record N was due and sleeps

an open loop cadence from an estimated records per message drifts by however
wrong the estimate is, and it is wrong because a cancelled trip emits one row
instead of 8 to 24
the first version used the estimate and reported ~97.5% of target
the error was in the controller not the pipeline

`tests/unit/test_loadgen.py::TestBenchmark::test_throttled_run_tracks_the_target_rate`
asserts the controller against an injected fake clock so it tests the controller
not the speed of whatever machine CI landed on

## kafka end to end

```bash
make up
make bench-kafka   # 2,000 events/sec for 60s into the broker
```

exercises `KafkaSink`
`acks=all` `enable.idempotence=true` `compression.type=lz4` keyed partitioning
delivery report callback counting permanent failures
`records_failed` includes anything still in flight after the flush timeout so a
partial delivery cannot be reported as success

