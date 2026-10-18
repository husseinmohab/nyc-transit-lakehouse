# Test fixtures

| File | What it is |
|---|---|
| `gtfs_rt_sample.pb` | A serialized GTFS-Realtime `FeedMessage` (3 trip updates, 2 vehicle positions, 1 alert) produced by `nyc_transit.gtfs.synthetic.build_fixture_payload()` with a fixed seed. |
| `truncated.pb` | The first 40 bytes of a valid `FeedMessage`, so the decoder's failure path is tested against real truncated protobuf rather than random bytes. |

`gtfs_rt_sample.pb` is synthetic, not captured from the MTA:

- a captured payload would be a snapshot of one moment's live service and could
  not be regenerated, so a change to the normalizer could never be checked
  against a reconstructable input;
- `tests/unit/test_fixtures.py` regenerates it from the generator and
  byte-compares, so the fixture cannot silently drift from the code;
- it contains real NYCT route ids and real platform-level stop ids (from
  `nyc_transit.gtfs.static_reference`), so shapes, string lengths and id formats
  match production.

What it does not give you is the MTA's own quirks: the NYCT protobuf extension,
unusual enum values, the occasional malformed entity. See
`docs/limitations.md`.
