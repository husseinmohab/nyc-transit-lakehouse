"""protobuf decoding with loud failures

a truncated body or an html error page must not look like an empty feed
an empty FeedMessage is a quiet moment and a decode failure is an incident
keeping them apart lets the producer count them separately and lets the
freshness sla fire on the second but not the first
"""

from __future__ import annotations

from google.protobuf.message import DecodeError

from nyc_transit.gtfs.proto import gtfs_realtime_pb2

SUPPORTED_GTFS_RT_VERSIONS = frozenset({"1.0", "2.0"})


class FeedDecodeError(RuntimeError):
    """body was not a parseable FeedMessage"""

    def __init__(self, feed_id: str, reason: str, payload_size: int) -> None:
        super().__init__(f"feed {feed_id}: {reason} (payload {payload_size} bytes)")
        self.feed_id = feed_id
        self.reason = reason
        self.payload_size = payload_size


def decode_feed(payload: bytes, *, feed_id: str = "unknown") -> gtfs_realtime_pb2.FeedMessage:
    """parse a FeedMessage from raw bytes

    raises FeedDecodeError on an empty body a bad parse or an unsupported
    gtfs_realtime_version
    """
    if not payload:
        raise FeedDecodeError(feed_id, "empty response body", 0)

    message = gtfs_realtime_pb2.FeedMessage()
    try:
        message.ParseFromString(payload)
    except DecodeError as exc:
        raise FeedDecodeError(feed_id, f"protobuf decode failed: {exc}", len(payload)) from exc

    if not message.HasField("header"):
        raise FeedDecodeError(feed_id, "FeedMessage has no header", len(payload))

    version = message.header.gtfs_realtime_version
    if version and version not in SUPPORTED_GTFS_RT_VERSIONS:
        raise FeedDecodeError(
            feed_id, f"unsupported gtfs_realtime_version {version!r}", len(payload)
        )
    return message


def enum_name(descriptor_enum: object, value: int, default: str) -> str:
    """protobuf enum int to its symbolic name

    reads the name off the descriptor instead of a hand written dict so a value
    added to the spec later lands in the lake as its real name
    """
    try:
        return descriptor_enum.Name(value)  # type: ignore[attr-defined]
    except (ValueError, AttributeError):
        return default
