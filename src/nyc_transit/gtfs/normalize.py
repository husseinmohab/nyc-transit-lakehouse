"""gtfs realtime to canonical records

one FeedMessage gives three record types

trip_update is one row per trip and stop_time_update
flattening at stop level gives the grain fct_trip_stop_prediction needs and one
subway poll blows up to roughly 2-6k stop predictions
vehicle_position is one row per vehicle entity
alert is one row per alert with informed_entity left as an array so one service
change does not get multiplied across routes

every record carries an envelope
event_id feed_id feed_timestamp ingest_timestamp
streaming/dedup.py explains what the envelope is for

three rules

normalize_feed_message is pure in FeedMessage feed_id and ingest_timestamp_ms so
replaying a payload gives byte identical records

a missing protobuf field becomes None and never 0 and never now
a delay of 0 means the feed said 0

service_date is a gtfs service date in new york time and not utc
a utc date would split one night of service across two partitions
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from nyc_transit.gtfs.decoder import enum_name
from nyc_transit.gtfs.nyct import trip_extension
from nyc_transit.gtfs.proto import gtfs_realtime_pb2

# MTA service dates are expressed in agency-local time
AGENCY_TZ = ZoneInfo("America/New_York")

# Stamped on every record Bump on a breaking field change
SCHEMA_VERSION = 1

# A trip departing at 01:30 belongs to the previous service day Used only when
# the feed omits trip.start_date a feed-supplied start_date always wins
SERVICE_DAY_CUTOVER_HOUR = 3

_TRIP_REL = gtfs_realtime_pb2.TripDescriptor.ScheduleRelationship
_STU_REL = gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.ScheduleRelationship
_STOP_STATUS = gtfs_realtime_pb2.VehiclePosition.VehicleStopStatus
_CONGESTION = gtfs_realtime_pb2.VehiclePosition.CongestionLevel
_OCCUPANCY = gtfs_realtime_pb2.VehiclePosition.OccupancyStatus
_ALERT_CAUSE = gtfs_realtime_pb2.Alert.Cause
_ALERT_EFFECT = gtfs_realtime_pb2.Alert.Effect


def service_date_from_epoch(
    epoch_seconds: int, *, cutover_hour: int = SERVICE_DAY_CUTOVER_HOUR
) -> str:
    """gtfs service date from a utc epoch

    converts to new york time first so the tz db handles dst
    then shifts back by cutover_hour so late night service lands on the prior day
    """
    local = datetime.fromtimestamp(epoch_seconds, tz=UTC).astimezone(AGENCY_TZ)
    return (local - timedelta(hours=cutover_hour)).date().isoformat()


def parse_gtfs_start_date(raw: str) -> str | None:
    """gtfs YYYYMMDD start_date to YYYY-MM-DD

    anything malformed comes back as None
    one bad trip should not poison a whole poll and service_date has a fallback
    """
    if len(raw) != 8 or not raw.isdigit():
        return None
    try:
        return datetime.strptime(raw, "%Y%m%d").date().isoformat()
    except ValueError:
        return None


def make_event_id(*parts: Any) -> str:
    """deterministic 128 bit hex id over a records business key

    blake2b not hash() which is salted per process and not uuid4 which is random
    this id is the merge dedup key so it has to be reproducible across processes and
    replays

    parts join on the unit separator so A1 B and A 1B cannot collide
    """
    joined = "\x1f".join("" if p is None else str(p) for p in parts)
    return hashlib.blake2b(joined.encode("utf-8"), digest_size=16).hexdigest()


def _envelope(
    *,
    event_id: str,
    record_type: str,
    feed_id: str,
    feed_timestamp_ms: int,
    ingest_timestamp_ms: int,
    producer_id: str,
) -> dict[str, Any]:
    return {
        "event_id": event_id,
        "record_type": record_type,
        "feed_id": feed_id,
        "feed_timestamp_ms": feed_timestamp_ms,
        "ingest_timestamp_ms": ingest_timestamp_ms,
        "producer_id": producer_id,
        "schema_version": SCHEMA_VERSION,
    }


def _trip_fields(trip: Any) -> dict[str, Any]:
    """TripDescriptor fields shared by trip updates and vehicle positions"""
    start_date = parse_gtfs_start_date(trip.start_date) if trip.start_date else None
    fields = {
        "trip_id": trip.trip_id or None,
        "route_id": trip.route_id or None,
        "direction_id": trip.direction_id if trip.HasField("direction_id") else None,
        "start_date": start_date,
        "start_time": trip.start_time or None,
        "schedule_relationship": (
            enum_name(_TRIP_REL, trip.schedule_relationship, "SCHEDULED")
            if trip.HasField("schedule_relationship")
            else "SCHEDULED"
        ),
    }
    fields.update(trip_extension(trip))
    return fields


def _stop_time_event(event: Any) -> dict[str, Any]:
    """project a StopTimeEvent

    gtfs rt allows time or delay or both and neither is derived from the other
    working out delay needs the static schedule which this path never loads so a
    missing delay stays null
    """
    return {
        "time_epoch": event.time if event.HasField("time") else None,
        "delay_seconds": event.delay if event.HasField("delay") else None,
        "uncertainty_seconds": event.uncertainty if event.HasField("uncertainty") else None,
    }


_NULL_STOP_TIME_EVENT = {"time_epoch": None, "delay_seconds": None, "uncertainty_seconds": None}


def normalize_trip_update(
    entity: Any,
    *,
    feed_id: str,
    feed_timestamp_ms: int,
    ingest_timestamp_ms: int,
    producer_id: str,
) -> Iterator[dict[str, Any]]:
    """one record per stop_time_update on this trip update

    a trip update with no stop_time_updates still gives one row with a null
    stop_sequence
    a cancellation carries no stops and dropping it would make cancellations
    invisible downstream
    """
    tu = entity.trip_update
    trip = _trip_fields(tu.trip)
    vehicle_id = tu.vehicle.id or None if tu.HasField("vehicle") else None
    vehicle_label = tu.vehicle.label or None if tu.HasField("vehicle") else None
    trip_ts = tu.timestamp if tu.HasField("timestamp") else None
    trip_delay = tu.delay if tu.HasField("delay") else None

    service_date = trip["start_date"] or service_date_from_epoch(feed_timestamp_ms // 1000)

    stop_updates = list(tu.stop_time_update) or [None]
    for stu in stop_updates:
        if stu is None:
            stop_id: str | None = None
            stop_sequence: int | None = None
            arrival = dict(_NULL_STOP_TIME_EVENT)
            departure = dict(_NULL_STOP_TIME_EVENT)
            stu_relationship = None
        else:
            stop_id = stu.stop_id or None
            stop_sequence = stu.stop_sequence if stu.HasField("stop_sequence") else None
            arrival = (
                _stop_time_event(stu.arrival)
                if stu.HasField("arrival")
                else dict(_NULL_STOP_TIME_EVENT)
            )
            departure = (
                _stop_time_event(stu.departure)
                if stu.HasField("departure")
                else dict(_NULL_STOP_TIME_EVENT)
            )
            stu_relationship = (
                enum_name(_STU_REL, stu.schedule_relationship, "SCHEDULED")
                if stu.HasField("schedule_relationship")
                else "SCHEDULED"
            )

        # Business key which stop of which trip on which service date as of
        # which feed timestamp two polls returning an unchanged prediction
        # collide on event_id and the MERGE drops the duplicate
        event_id = make_event_id(
            "trip_update",
            feed_id,
            trip["trip_id"],
            service_date,
            stop_id,
            stop_sequence,
            feed_timestamp_ms,
        )
        record = _envelope(
            event_id=event_id,
            record_type="trip_update",
            feed_id=feed_id,
            feed_timestamp_ms=feed_timestamp_ms,
            ingest_timestamp_ms=ingest_timestamp_ms,
            producer_id=producer_id,
        )
        record.update(
            {
                "entity_id": entity.id or None,
                "is_deleted": bool(entity.is_deleted) if entity.HasField("is_deleted") else False,
                "service_date": service_date,
                "vehicle_id": vehicle_id,
                "vehicle_label": vehicle_label,
                "trip_timestamp_epoch": trip_ts,
                "trip_delay_seconds": trip_delay,
                "stop_id": stop_id,
                "stop_sequence": stop_sequence,
                "stop_schedule_relationship": stu_relationship,
                "arrival_time_epoch": arrival["time_epoch"],
                "arrival_delay_seconds": arrival["delay_seconds"],
                "arrival_uncertainty_seconds": arrival["uncertainty_seconds"],
                "departure_time_epoch": departure["time_epoch"],
                "departure_delay_seconds": departure["delay_seconds"],
                "departure_uncertainty_seconds": departure["uncertainty_seconds"],
                **trip,
            }
        )
        yield record


def normalize_vehicle_position(
    entity: Any,
    *,
    feed_id: str,
    feed_timestamp_ms: int,
    ingest_timestamp_ms: int,
    producer_id: str,
) -> Iterator[dict[str, Any]]:
    """one record for a vehicle position entity"""
    vp = entity.vehicle
    trip = (
        _trip_fields(vp.trip)
        if vp.HasField("trip")
        else {
            "trip_id": None,
            "route_id": None,
            "direction_id": None,
            "start_date": None,
            "start_time": None,
            "schedule_relationship": None,
            "nyct_train_id": None,
            "nyct_direction": None,
            "nyct_is_assigned": None,
        }
    )
    vehicle_ts = vp.timestamp if vp.HasField("timestamp") else None
    service_date = trip["start_date"] or service_date_from_epoch(
        vehicle_ts or feed_timestamp_ms // 1000
    )
    vehicle_id = vp.vehicle.id or None if vp.HasField("vehicle") else None

    # A vehicle reports at most one position per timestamp so vehicle id plus
    # its own timestamp is the natural key not the feed timestamp a stale
    # position republished across polls should collapse to one row
    event_id = make_event_id(
        "vehicle_position", feed_id, vehicle_id, trip["trip_id"], vehicle_ts or feed_timestamp_ms
    )
    record = _envelope(
        event_id=event_id,
        record_type="vehicle_position",
        feed_id=feed_id,
        feed_timestamp_ms=feed_timestamp_ms,
        ingest_timestamp_ms=ingest_timestamp_ms,
        producer_id=producer_id,
    )
    position = vp.position if vp.HasField("position") else None
    record.update(
        {
            "entity_id": entity.id or None,
            "is_deleted": bool(entity.is_deleted) if entity.HasField("is_deleted") else False,
            "service_date": service_date,
            "vehicle_id": vehicle_id,
            "vehicle_label": vp.vehicle.label or None if vp.HasField("vehicle") else None,
            "vehicle_timestamp_epoch": vehicle_ts,
            "latitude": round(position.latitude, 6) if position is not None else None,
            "longitude": round(position.longitude, 6) if position is not None else None,
            "bearing": (
                position.bearing if position is not None and position.HasField("bearing") else None
            ),
            "speed_mps": (
                position.speed if position is not None and position.HasField("speed") else None
            ),
            "current_stop_sequence": (
                vp.current_stop_sequence if vp.HasField("current_stop_sequence") else None
            ),
            "stop_id": vp.stop_id or None,
            "current_status": (
                enum_name(_STOP_STATUS, vp.current_status, "IN_TRANSIT_TO")
                if vp.HasField("current_status")
                else None
            ),
            "congestion_level": (
                enum_name(_CONGESTION, vp.congestion_level, "UNKNOWN_CONGESTION_LEVEL")
                if vp.HasField("congestion_level")
                else None
            ),
            "occupancy_status": (
                enum_name(_OCCUPANCY, vp.occupancy_status, "NO_DATA_AVAILABLE")
                if vp.HasField("occupancy_status")
                else None
            ),
            **trip,
        }
    )
    yield record


def _translated_string(ts: Any) -> str | None:
    """english translation off a TranslatedString else the first one"""
    if ts is None or not ts.translation:
        return None
    for tr in ts.translation:
        if tr.language.lower().startswith("en"):
            return tr.text or None
    return ts.translation[0].text or None


def normalize_alert(
    entity: Any,
    *,
    feed_id: str,
    feed_timestamp_ms: int,
    ingest_timestamp_ms: int,
    producer_id: str,
) -> Iterator[dict[str, Any]]:
    """one record for a service alert

    informed_entity stays nested
    exploding here would multiply a weekend shuttle alert across every informed stop
    and put count(distinct alert) behind a join
    silver explodes it once
    """
    alert = entity.alert
    header = _translated_string(alert.header_text if alert.HasField("header_text") else None)
    description = _translated_string(
        alert.description_text if alert.HasField("description_text") else None
    )

    periods = [
        {
            "start_epoch": p.start if p.HasField("start") else None,
            "end_epoch": p.end if p.HasField("end") else None,
        }
        for p in alert.active_period
    ]
    informed = [
        {
            "agency_id": ie.agency_id or None,
            "route_id": ie.route_id or None,
            "route_type": ie.route_type if ie.HasField("route_type") else None,
            "direction_id": ie.direction_id if ie.HasField("direction_id") else None,
            "trip_id": ie.trip.trip_id or None if ie.HasField("trip") else None,
            "stop_id": ie.stop_id or None,
        }
        for ie in alert.informed_entity
    ]

    # The MTA edits alerts in place Hashing the payload into the key makes an
    # edit a new row while a republication of identical text dedups away
    payload_fingerprint = make_event_id(header, description, tuple(sorted(map(str, informed))))
    event_id = make_event_id("alert", feed_id, entity.id, payload_fingerprint)

    record = _envelope(
        event_id=event_id,
        record_type="alert",
        feed_id=feed_id,
        feed_timestamp_ms=feed_timestamp_ms,
        ingest_timestamp_ms=ingest_timestamp_ms,
        producer_id=producer_id,
    )
    record.update(
        {
            "entity_id": entity.id or None,
            "is_deleted": bool(entity.is_deleted) if entity.HasField("is_deleted") else False,
            "service_date": service_date_from_epoch(feed_timestamp_ms // 1000),
            "alert_cause": (
                enum_name(_ALERT_CAUSE, alert.cause, "UNKNOWN_CAUSE")
                if alert.HasField("cause")
                else None
            ),
            "alert_effect": (
                enum_name(_ALERT_EFFECT, alert.effect, "UNKNOWN_EFFECT")
                if alert.HasField("effect")
                else None
            ),
            "header_text": header,
            "description_text": description,
            "url": _translated_string(alert.url if alert.HasField("url") else None),
            "active_periods": periods,
            "informed_entities": informed,
            "payload_fingerprint": payload_fingerprint,
        }
    )
    yield record


def normalize_feed_message(
    message: Any,
    *,
    feed_id: str,
    ingest_timestamp_ms: int,
    producer_id: str = "unknown",
) -> list[dict[str, Any]]:
    """whole FeedMessage to canonical records

    ingest_timestamp_ms is passed in rather than read off the clock so this stays
    pure and tests can assert byte exact output

    a header with no timestamp falls back to ingest_timestamp_ms which you can spot
    on the row as feed_timestamp_ms == ingest_timestamp_ms
    """
    header = message.header
    feed_timestamp_ms = (
        header.timestamp * 1000 if header.HasField("timestamp") else ingest_timestamp_ms
    )

    common = {
        "feed_id": feed_id,
        "feed_timestamp_ms": feed_timestamp_ms,
        "ingest_timestamp_ms": ingest_timestamp_ms,
        "producer_id": producer_id,
    }

    records: list[dict[str, Any]] = []
    for entity in message.entity:
        if entity.HasField("trip_update"):
            records.extend(normalize_trip_update(entity, **common))
        elif entity.HasField("vehicle"):
            records.extend(normalize_vehicle_position(entity, **common))
        elif entity.HasField("alert"):
            records.extend(normalize_alert(entity, **common))
        # Other entity kinds (shapes, trip modifications) are skipped the
        # producer compares record count against entity count and logs a
        # divergence
    return records


def partition_key(record: dict[str, Any]) -> str:
    """kafka message key

    keyed on whatever owns the ordering invariant so every update for one trip or
    vehicle or alert lands on one partition and arrives in publication order
    without it two predictions for one stop could commit out of order
    """
    record_type = record["record_type"]
    if record_type == "trip_update":
        return f"{record['service_date']}|{record.get('trip_id') or record.get('entity_id')}"
    if record_type == "vehicle_position":
        # the unit identity in preference order
        # gtfs vehicle id, then the nyct train id which is the only one the
        # subway feed gives, then the feed's entity id as a last resort
        # entity_id works but is assigned by the feed and not the unit so it is
        # the weakest of the three for an ordering key
        unit = record.get("vehicle_id") or record.get("nyct_train_id") or record.get("entity_id")
        return f"{record['service_date']}|{unit}"
    return f"alert|{record.get('entity_id')}"
