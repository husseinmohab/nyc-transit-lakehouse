"""synthetic gtfs realtime feeds

producer/loadgen.py uses this for the throughput benchmark so there is no need
for an mta key and no load on a public endpoint
the tests use it for deterministic fixtures

the output is a real FeedMessage so the load generator runs the same decode
normalize and encode path as production traffic

what matters for realism

trip ids follow the nyct pattern so key cardinality and string lengths look like
real payload sizes
stop ids are real platform level nyct ids from static_reference walked in route
order in one direction so every stop joins to the station dimension
each trip carries 8-24 downstream stops which is what turns one poll into
thousands of stop level records
a share of trips repeat an unchanged prediction so the dedup path sees a
realistic duplicate rate
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import Any

from nyc_transit.gtfs.proto import gtfs_realtime_pb2, nyct_subway_pb2
from nyc_transit.gtfs.static_reference import (
    PLATFORM_SUFFIXES,
    routes_with_stops,
    stops_for_route,
)

# Header wording paired with the effect it stands for the mta writes these by
# hand so the text is what route_alert_status has to classify
ALERT_KINDS: tuple[tuple[int, str], ...] = (
    (gtfs_realtime_pb2.Alert.Effect.SIGNIFICANT_DELAYS, "trains are running with delays"),
    (gtfs_realtime_pb2.Alert.Effect.NO_SERVICE, "no service between two stations"),
    (gtfs_realtime_pb2.Alert.Effect.REDUCED_SERVICE, "service is suspended in one direction"),
    (gtfs_realtime_pb2.Alert.Effect.DETOUR, "service is rerouted in both directions"),
    (gtfs_realtime_pb2.Alert.Effect.MODIFIED_SERVICE, "runs local in both directions"),
    (gtfs_realtime_pb2.Alert.Effect.OTHER_EFFECT, "trains board from the express track"),
)


@dataclass(slots=True)
class SyntheticFeedConfig:
    """shape of the generated feed

    duplicate_fraction re emits trips with an identical prediction and feed timestamp
    so they collapse in silver
    cancellation_fraction emits trips as CANCELED with no stop_time_updates

    trips_per_route and fleet_size_per_route keep the trip roster and the vehicle
    fleet stable across polls
    drawing fresh ids every poll made every trip and stop a one off so
    revision_count and churn came out constant and every scd2 history was one
    version long

    reassignment_probability and unassigned_probability drive the vehicle transitions
    the scd2 interval tests need including assigned to unassigned which is the case
    null safe change detection exists for

    a fixed seed makes the feed byte reproducible so tests can assert exact counts
    """

    trips_per_message: int = 20
    stops_per_trip: tuple[int, int] = (8, 24)
    vehicles_per_message: int = 40
    alerts_per_message: int = 2
    duplicate_fraction: float = 0.05
    cancellation_fraction: float = 0.02
    no_prediction_fraction: float = 0.03
    stop_sequence_fraction: float = 0.06
    trips_per_route: int = 24
    fleet_size_per_route: int = 10
    reassignment_probability: float = 0.15
    unassigned_probability: float = 0.08
    seed: int | None = None
    routes: tuple[str, ...] = field(default_factory=routes_with_stops)

    def __post_init__(self) -> None:
        lo, hi = self.stops_per_trip
        if lo < 1 or hi < lo:
            raise ValueError(f"stops_per_trip must be an increasing positive range, got {(lo, hi)}")
        if not 0.0 <= self.duplicate_fraction <= 1.0:
            raise ValueError("duplicate_fraction must be in [0, 1]")
        if not 0.0 <= self.cancellation_fraction <= 1.0:
            raise ValueError("cancellation_fraction must be in [0, 1]")
        if not 0.0 <= self.no_prediction_fraction <= 1.0:
            raise ValueError("no_prediction_fraction must be in [0, 1]")
        if not 0.0 <= self.stop_sequence_fraction <= 1.0:
            raise ValueError("stop_sequence_fraction must be in [0, 1]")
        if self.fleet_size_per_route < 1:
            raise ValueError("fleet_size_per_route must be at least 1")
        if self.trips_per_route < 1:
            raise ValueError("trips_per_route must be at least 1")
        for name in ("reassignment_probability", "unassigned_probability"):
            value = getattr(self, name)
            if not 0.0 <= value <= 1.0:
                raise ValueError(f"{name} must be in [0, 1]")
        if not self.routes:
            raise ValueError("routes must be non-empty")

    def expected_records(self) -> tuple[int, int]:
        """rough min and max records per FeedMessage

        rough and not a bound since a cancelled trip gives one row instead of
        stops_per_trip rows
        only used to log an order of magnitude
        rate control measures what actually came out
        """
        lo, hi = self.stops_per_trip
        dup = int(self.trips_per_message * self.duplicate_fraction)
        total_trips = self.trips_per_message + dup
        fixed = self.vehicles_per_message + self.alerts_per_message
        return (total_trips * lo + fixed, total_trips * hi + fixed)

    def mean_records_per_message(self) -> float:
        lo, hi = self.expected_records()
        return (lo + hi) / 2


class SyntheticFeedGenerator:
    """builds FeedMessage payloads that look like an nyct subway feed"""

    def __init__(self, config: SyntheticFeedConfig | None = None) -> None:
        self.config = config or SyntheticFeedConfig()
        self._rng = random.Random(self.config.seed)
        # Per-route platform id lists in route order one list per direction
        # Built once stops_for_route is a linear scan and a trip update needs
        # it for every entity
        self._platforms: dict[tuple[str, str], list[str]] = {
            (route, suffix): [f"{stop.stop_id}{suffix}" for stop in stops_for_route(route)]
            for route in self.config.routes
            for suffix in PLATFORM_SUFFIXES
        }
        self._all_platforms: list[str] = sorted(
            {pid for ids in self._platforms.values() for pid in ids}
        )
        # Mutable state carried across build_feed_message calls so consecutive
        # polls tell a coherent story about the same trains
        self._fleet: dict[str, str | None] = {}
        self._roster: dict[str, list[str]] = {}

    def _trip_path(
        self, route: str, direction: str, n_stops: int, *, rng: random.Random | None = None
    ) -> list[str]:
        """a contiguous run of stops along one route in one direction

        in route order and not a random sample
        a trip teleporting between boroughs would break anything that reasons about
        stop_sequence
        rng is the per trip generator so the path is reproducible from the trip id
        """
        rng = rng or self._rng
        platforms = self._platforms.get((route, direction)) or self._all_platforms
        n_stops = min(n_stops, len(platforms))
        start = rng.randrange(max(len(platforms) - n_stops, 1))
        return platforms[start : start + n_stops]

    def _trip_roster(self, route: str) -> list[str]:
        """the routes stable set of trip ids built once off the seeded stream"""
        if route not in self._roster:
            self._roster[route] = [
                self._trip_id(route, self._rng.randint(0, 14_399))
                for _ in range(self.config.trips_per_route)
            ]
        return self._roster[route]

    def _trip_id(self, route: str, origin_decisec: int) -> str:
        """nyct trip id like 071150_A..N08R"""
        path = f"{self._rng.choice('NS')}{self._rng.randint(1, 99):02d}{self._rng.choice('RX')}"
        return f"{origin_decisec:06d}_{route}..{path}"

    def build_feed_message(
        self,
        *,
        feed_timestamp_epoch: int,
        service_date_yyyymmdd: str | None = None,
    ) -> gtfs_realtime_pb2.FeedMessage:
        """build one complete FeedMessage"""
        cfg = self.config
        message = gtfs_realtime_pb2.FeedMessage()
        message.header.gtfs_realtime_version = "2.0"
        message.header.incrementality = gtfs_realtime_pb2.FeedHeader.FULL_DATASET
        message.header.timestamp = feed_timestamp_epoch

        # Draw this poll's trips from the stable roster, without replacement: a
        # real feed publishes each trip at most once per message
        roster = [(trip_id, route) for route in cfg.routes for trip_id in self._trip_roster(route)]
        trip_specs = self._rng.sample(roster, min(cfg.trips_per_message, len(roster)))

        # Duplicates repeat earlier trips verbatim giving an identical event_id
        n_dupes = int(len(trip_specs) * cfg.duplicate_fraction)
        trip_specs.extend(trip_specs[i % len(trip_specs)] for i in range(n_dupes))

        for trip_id, route in trip_specs:
            self._add_trip_update(
                message,
                trip_id=trip_id,
                route=route,
                feed_timestamp_epoch=feed_timestamp_epoch,
                service_date_yyyymmdd=service_date_yyyymmdd,
            )

        # Sample the fleet without replacement a real feed reports each vehicle
        # at most once per FeedMessage Sampling with replacement produced two
        # positions for one vehicle at one instant which broke the declared
        # (vehicle_id, reported_at_ms, feed_id) grain
        fleet_pool = [
            (route, index) for route in cfg.routes for index in range(cfg.fleet_size_per_route)
        ]
        for route, vehicle_index in self._rng.sample(
            fleet_pool, min(cfg.vehicles_per_message, len(fleet_pool))
        ):
            self._add_vehicle(
                message,
                route=route,
                vehicle_index=vehicle_index,
                feed_timestamp_epoch=feed_timestamp_epoch,
                service_date_yyyymmdd=service_date_yyyymmdd,
            )

        for _ in range(cfg.alerts_per_message):
            self._add_alert(message, feed_timestamp_epoch=feed_timestamp_epoch)

        return message

    def _add_trip_update(
        self,
        message: gtfs_realtime_pb2.FeedMessage,
        *,
        trip_id: str,
        route: str,
        feed_timestamp_epoch: int,
        service_date_yyyymmdd: str | None,
    ) -> None:
        """append one trip update

        two seeded rngs
        trip_rng is seeded on trip_id alone and fixes the trips identity
        stop path direction and whether it is cancelled
        a real trip does not change which stations it calls at between refreshes and when
        every poll re rolled the path revision_count came out 1 for every row

        publication_rng is seeded on trip_id plus the feed timestamp and fixes this
        publications state
        etas uncertainty skipped stops
        so predictions churn across polls while a trip emitted twice in one message is
        byte identical and collapses to one event_id
        """
        entity = message.entity.add()
        # Deterministic from the trip not from its position in the message A
        # real feed keeps an entity id stable across polls and an index-based
        # id made a re-emitted trip differ in entity_id while sharing its
        # event_id so it was not actually a duplicate
        entity.id = f"tu-{trip_id}"
        tu = entity.trip_update
        tu.trip.trip_id = trip_id
        tu.trip.route_id = route
        tu.trip.direction_id = 0 if trip_id.rsplit("..", 1)[-1].startswith("N") else 1
        if service_date_yyyymmdd:
            tu.trip.start_date = service_date_yyyymmdd
        tu.timestamp = feed_timestamp_epoch

        trip_rng = random.Random(trip_id)
        publication_rng = random.Random(f"{trip_id}|{feed_timestamp_epoch}")
        self._set_trip_extension(
            tu.trip,
            self._train_id(route, trip_rng.randint(1, 99)),
            "N" if tu.trip.direction_id == 0 else "S",
        )

        if trip_rng.random() < self.config.cancellation_fraction:
            # A cancellation carries no stop_time_updates the normalizer still
            # emits one row for it which is the behaviour under test
            tu.trip.schedule_relationship = (
                gtfs_realtime_pb2.TripDescriptor.ScheduleRelationship.CANCELED
            )
            return

        tu.trip.schedule_relationship = (
            gtfs_realtime_pb2.TripDescriptor.ScheduleRelationship.SCHEDULED
        )
        if trip_rng.random() < self.config.no_prediction_fraction:
            # Real feeds publish scheduled trips with no stop_time_updates at all
            # the rockaway park shuttle does it constantly
            # the normalizer still emits one row and stop_id stays null so anything
            # asserting that only cancellations lack a stop is wrong about the feed
            return

        lo, hi = self.config.stops_per_trip
        direction = "N" if tu.trip.direction_id == 0 else "S"
        path = self._trip_path(route, direction, trip_rng.randint(lo, hi), rng=trip_rng)
        eta = feed_timestamp_epoch + publication_rng.randint(30, 240)

        # stop_sequence is optional in GTFS-RT and nyct leaves it unset on most
        # stop_time_updates it publishes so most trips here leave it unset too
        # anything that needs a stop has to key on stop_id
        with_sequence = trip_rng.random() < self.config.stop_sequence_fraction

        for offset, platform_id in enumerate(path):
            stu = tu.stop_time_update.add()
            stu.stop_id = platform_id
            if with_sequence:
                stu.stop_sequence = offset + 1
            eta += publication_rng.randint(60, 180)
            stu.arrival.time = eta
            stu.arrival.uncertainty = publication_rng.choice([0, 30, 60])
            stu.departure.time = eta + publication_rng.randint(20, 45)
            if publication_rng.random() < 0.02:
                stu.schedule_relationship = (
                    gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.ScheduleRelationship.SKIPPED
                )

    def _train_id(self, route: str, index: int) -> str:
        """an nyct style physical train id

        the real feed sends things like "1A 1715+ 207/LEF" and no gtfs vehicle
        descriptor at all so this mirrors that
        """
        return f"{index:02d}{route} {1500 + index:04d}+ SYN/SYN"

    def _set_trip_extension(self, trip: Any, train_id: str, direction: str) -> None:
        """attach the nyct trip extension

        compiled from the vendored proto by `make proto` so the generator can
        produce the same enrichment columns the live feed carries
        """
        ext = trip.Extensions[nyct_subway_pb2.nyct_trip_descriptor]
        ext.train_id = train_id
        ext.direction = (
            nyct_subway_pb2.NyctTripDescriptor.Direction.NORTH
            if direction == "N"
            else nyct_subway_pb2.NyctTripDescriptor.Direction.SOUTH
        )

    def _current_assignment(self, train_id: str, route: str) -> str | None:
        """the trip this vehicle is working after evolving it

        per poll unassigned_probability makes it unassigned
        reassignment_probability gives it a new trip
        otherwise it keeps what it had
        the result is a run of observations with occasional transitions which is the
        shape scd2 models
        """
        if self._rng.random() < self.config.unassigned_probability:
            self._fleet[train_id] = None
        elif (
            self._rng.random() < self.config.reassignment_probability
            or train_id not in self._fleet
            or self._fleet[train_id] is None
        ):
            self._fleet[train_id] = self._trip_id(route, self._rng.randint(0, 14_399))
        return self._fleet[train_id]

    def _add_vehicle(
        self,
        message: gtfs_realtime_pb2.FeedMessage,
        *,
        route: str,
        vehicle_index: int,
        feed_timestamp_epoch: int,
        service_date_yyyymmdd: str | None,
    ) -> None:
        entity = message.entity.add()
        vp = entity.vehicle

        train_id = self._train_id(route, vehicle_index)
        entity.id = f"vp-{train_id}"
        trip_id = self._current_assignment(train_id, route)

        if trip_id is not None:
            vp.trip.trip_id = trip_id
            vp.trip.route_id = route
            if service_date_yyyymmdd:
                vp.trip.start_date = service_date_yyyymmdd
        else:
            # An unassigned vehicle still reports a route (it is sitting on a
            # line) but no trip that is what the real feeds do and it is the
            # case silver's is_assigned flag and the SCD2 change detection both
            # have to handle
            vp.trip.route_id = route

        # No vehicle descriptor, which is what the nyct subway feeds do
        # measured on the wire 0 of 419 vehicle positions carried one
        # the identity comes from the nyct extension instead and vehicle_key in
        # staging coalesces the two
        self._set_trip_extension(vp.trip, train_id, self._rng.choice(PLATFORM_SUFFIXES))
        vp.timestamp = feed_timestamp_epoch - self._rng.randint(0, 30)
        # Positions are kept even though the subway feeds send none, because the
        # bus and commuter rail feeds do and position_is_plausible needs data
        # Roughly the NYC bounding box Plausible coordinates not real ones
        vp.position.latitude = 40.577 + self._rng.random() * 0.335
        vp.position.longitude = -74.041 + self._rng.random() * 0.316
        vp.position.bearing = self._rng.random() * 360.0
        vp.current_stop_sequence = self._rng.randint(1, 40)
        route_platforms = self._platforms.get((route, self._rng.choice(PLATFORM_SUFFIXES)))
        vp.stop_id = self._rng.choice(route_platforms or self._all_platforms)
        vp.current_status = self._rng.choice(
            [
                gtfs_realtime_pb2.VehiclePosition.VehicleStopStatus.INCOMING_AT,
                gtfs_realtime_pb2.VehiclePosition.VehicleStopStatus.STOPPED_AT,
                gtfs_realtime_pb2.VehiclePosition.VehicleStopStatus.IN_TRANSIT_TO,
            ]
        )

    def _add_alert(
        self, message: gtfs_realtime_pb2.FeedMessage, *, feed_timestamp_epoch: int
    ) -> None:
        entity = message.entity.add()
        entity.id = f"alert-{self._rng.randint(100000, 999999)}"
        alert = entity.alert
        period = alert.active_period.add()
        period.start = feed_timestamp_epoch
        period.end = feed_timestamp_epoch + 7200
        route = self._rng.choice(self.config.routes)
        effect, text = self._rng.choice(ALERT_KINDS)

        # The live subway alerts feed never sets effect or cause it puts the
        # category in the header and leaves the enums at their default
        # most alerts here look like that so route_alert_status gets exercised
        # on the path production actually takes
        if self._rng.random() < 0.25:
            alert.cause = gtfs_realtime_pb2.Alert.Cause.MAINTENANCE
            alert.effect = effect

        header = alert.header_text.translation.add()
        header.language = "en"
        header.text = f"[{route}] {text}"
        body = alert.description_text.translation.add()
        body.language = "en"
        body.text = f"[{route}] service is affected in both directions."
        informed = alert.informed_entity.add()
        informed.agency_id = "MTA NYCT"
        informed.route_id = route


def build_fixture_payload(
    *,
    feed_timestamp_epoch: int = 1_793_536_000,
    seed: int = 20260218,
    trips: int = 3,
    vehicles: int = 2,
    alerts: int = 1,
    service_date_yyyymmdd: str | None = "20261101",
) -> bytes:
    """serialized FeedMessage bytes for a small deterministic fixture

    tests/fixtures/gtfs_rt_sample.pb is this functions output
    the tests regenerate and byte compare it so the fixture cannot drift
    """
    generator = SyntheticFeedGenerator(
        SyntheticFeedConfig(
            trips_per_message=trips,
            stops_per_trip=(3, 5),
            vehicles_per_message=vehicles,
            alerts_per_message=alerts,
            duplicate_fraction=0.0,
            cancellation_fraction=0.0,
            seed=seed,
        )
    )
    message = generator.build_feed_message(
        feed_timestamp_epoch=feed_timestamp_epoch,
        service_date_yyyymmdd=service_date_yyyymmdd,
    )
    return message.SerializeToString()
