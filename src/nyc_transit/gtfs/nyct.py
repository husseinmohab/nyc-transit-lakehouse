"""nyct feed extension

the nyct subway feeds carry a vendor extension with the physical train id the
track level direction and an is_assigned flag

the extension is compiled from the vendored proto by `make proto` so these
fields are populated
they used to be null on every row because the gtfs-realtime-bindings wheel
ships no compiled nyct extension and this module silently fell back to nulls
that is why dim_vehicle_assignment was empty against the live feed
"""

from __future__ import annotations

from typing import Any

from nyc_transit.gtfs.proto import nyct_subway_pb2

_DIRECTION_NAMES = {1: "NORTH", 2: "EAST", 3: "SOUTH", 4: "WEST"}


def trip_extension(trip_descriptor: Any) -> dict[str, Any]:
    """nyct trip extension fields

    a feed that carries no extension gives nulls which is the case for the
    commuter rail and bus feeds
    """
    if not trip_descriptor.HasExtension(nyct_subway_pb2.nyct_trip_descriptor):
        return {"nyct_train_id": None, "nyct_direction": None, "nyct_is_assigned": None}
    ext = trip_descriptor.Extensions[nyct_subway_pb2.nyct_trip_descriptor]
    return {
        "nyct_train_id": ext.train_id or None,
        "nyct_direction": _DIRECTION_NAMES.get(ext.direction),
        "nyct_is_assigned": bool(ext.is_assigned) if ext.HasField("is_assigned") else None,
    }


def stop_time_extension(stop_time_update: Any) -> dict[str, Any]:
    """nyct stop time extension fields

    scheduled_track is the track the trip is booked on and actual_track is
    where it is actually running
    a difference between them is a reroute the rider sees
    """
    if not stop_time_update.HasExtension(nyct_subway_pb2.nyct_stop_time_update):
        return {"nyct_scheduled_track": None, "nyct_actual_track": None}
    ext = stop_time_update.Extensions[nyct_subway_pb2.nyct_stop_time_update]
    return {
        "nyct_scheduled_track": ext.scheduled_track or None,
        "nyct_actual_track": ext.actual_track or None,
    }
