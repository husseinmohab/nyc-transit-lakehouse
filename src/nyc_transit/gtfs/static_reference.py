"""static gtfs reference data

ROUTES and STOPS are a snapshot of the mta published subway files
routes.csv comes from routes.txt and stops.csv is every parent station in
stops.txt with the borough and the daytime route list joined on from the
MTA Subway Stations dataset on data.ny.gov

the snapshot is bundled instead of fetched so a checkout works offline and the
seeds and fixtures stay byte stable across machines
production can point the same loader at the live files in s3

the synthetic generator draws its stop ids from here so fixtures seeds and the
dimension tables all share one stop universe
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from functools import cache
from importlib import resources


@dataclass(frozen=True, slots=True)
class Route:
    route_id: str
    route_short_name: str
    route_long_name: str
    route_color: str
    route_type: int  # 1 is subway and 2 is rail
    division: str  # IRT BMT IND or SIR used by dim_route


@dataclass(frozen=True, slots=True)
class Stop:
    stop_id: str  # parent station id as published in stops.txt
    stop_name: str
    stop_lat: float
    stop_lon: float
    borough: str
    routes: tuple[str, ...]


def _read(name: str) -> list[dict[str, str]]:
    text = resources.files(__package__).joinpath("data", name).read_text(encoding="utf-8")
    return list(csv.DictReader(text.splitlines()))


def _load_routes() -> tuple[Route, ...]:
    return tuple(
        Route(
            r["route_id"],
            r["route_short_name"],
            r["route_long_name"],
            r["route_color"],
            int(r["route_type"]),
            r["division"],
        )
        for r in _read("routes.csv")
    )


def _load_stops() -> tuple[Stop, ...]:
    return tuple(
        Stop(
            s["stop_id"],
            s["stop_name"],
            float(s["stop_lat"]),
            float(s["stop_lon"]),
            s["borough"],
            tuple(s["routes"].split()),
        )
        for s in _read("stops.csv")
    )


# Station order follows the MTA Station ID which runs along each line so a
# contiguous slice of stops_for_route is a route a train could actually work
ROUTES: tuple[Route, ...] = _load_routes()
STOPS: tuple[Stop, ...] = _load_stops()

ROUTES_BY_ID: dict[str, Route] = {r.route_id: r for r in ROUTES}
STOPS_BY_ID: dict[str, Stop] = {s.stop_id: s for s in STOPS}

# Directional platform suffixes NYCT appends to the parent station id in the
# realtime feeds so 127N is Times Sq northbound
# GTFS-RT stop ids are platform level while static stops.txt rows are parents
# so the dbt staging layer strips the suffix to join
# see stg_trip_stop_predictions.sql
PLATFORM_SUFFIXES: tuple[str, ...] = ("N", "S")


@cache
def stops_for_route(route_id: str) -> tuple[Stop, ...]:
    """stops a route serves in line order"""
    return tuple(s for s in STOPS if route_id in s.routes)


@cache
def platform_ids_for_route(route_id: str) -> tuple[str, ...]:
    """platform level stop ids for a route like 127N and 127S"""
    return tuple(
        f"{stop.stop_id}{suffix}"
        for stop in stops_for_route(route_id)
        for suffix in PLATFORM_SUFFIXES
    )


def parent_station(platform_stop_id: str) -> str:
    """strip the direction suffix off a realtime stop id

    127N becomes 127
    an id without a known suffix comes back unchanged since the mta does publish a
    few unsuffixed ones and truncating those breaks the join
    """
    if platform_stop_id and platform_stop_id[-1] in PLATFORM_SUFFIXES:
        return platform_stop_id[:-1]
    return platform_stop_id


def routes_with_stops() -> tuple[str, ...]:
    """routes with at least one station in the snapshot

    the express variants 6X 7X FX run over another routes stations so the
    published station list never names them and the generator skips them
    """
    return tuple(r.route_id for r in ROUTES if stops_for_route(r.route_id))
