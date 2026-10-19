# proto

vendored protobuf sources, compiled by `make proto`

| file | upstream |
|---|---|
| `gtfs-realtime.proto` | https://raw.githubusercontent.com/google/transit/master/gtfs-realtime/proto/gtfs-realtime.proto |
| `nyct-subway.proto` | https://api.mta.info/nyct-subway.proto.txt |

`gtfs-realtime.proto` is byte for byte upstream
`nyct-subway.proto` has one line changed, its import points at the vendored
path above so protoc emits a package relative import

both are compiled here, not taken from the `gtfs-realtime-bindings`
wheel because that wheel ships no compiled nyct extension, so `nyct_train_id`
`nyct_is_assigned` and `nyct_direction` were always null without this
