"""nyct feed extension

the nyct subway feeds carry a vendor extension with the physical train id the
track level direction and an is_assigned flag
the gtfs-realtime-bindings wheel ships no compiled nyct_subway_pb2 so this only
reads it when one happens to be importable and writes nulls otherwise

nyct_train_id is a nullable enrichment column and never a join key so silver and
the marts look the same with or without it
see docs/limitations.md
"""

from __future__ import annotations

from typing import Any

try:  # pragma: no cover - depends on an optional compiled protobuf module
    from google.transit import nyct_subway_pb2  # type: ignore[attr-defined]

    NYCT_EXTENSION_AVAILABLE = True
except ImportError:  # pragma: no cover
    nyct_subway_pb2 = None  # type: ignore[assignment]
    NYCT_EXTENSION_AVAILABLE = False

_DIRECTION_NAMES = {1: "NORTH", 2: "EAST", 3: "SOUTH", 4: "WEST"}


def trip_extension(trip_descriptor: Any) -> dict[str, Any]:
    """nyct trip extension fields or all nulls when unavailable"""
    empty: dict[str, Any] = {
        "nyct_train_id": None,
        "nyct_direction": None,
        "nyct_is_assigned": None,
    }
    if not NYCT_EXTENSION_AVAILABLE:
        return empty
    try:  # pragma: no cover - exercised only with the compiled extension
        ext = trip_descriptor.Extensions[nyct_subway_pb2.nyct_trip_descriptor]
    except Exception:
        return empty
    return {
        "nyct_train_id": ext.train_id or None,
        "nyct_direction": _DIRECTION_NAMES.get(ext.direction),
        "nyct_is_assigned": bool(ext.is_assigned) if ext.HasField("is_assigned") else None,
    }
