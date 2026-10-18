"""mta gtfs realtime endpoints

the subway and commuter rail feeds have needed no key since the mta retired its
gateway in 2023
the bus feeds still sit behind a onebusaway key and get skipped when none is set
nothing substitutes fake data for a feed it could not reach because that would
quietly corrupt the freshness numbers
"""

from __future__ import annotations

from dataclasses import dataclass

_MTA_BASE = "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds"
_OBANYC_BASE = "https://gtfsrt.prod.obanyc.com"


@dataclass(frozen=True, slots=True)
class Feed:
    """one pollable gtfs realtime endpoint

    feed_id is the kafka key prefix and lands on every bronze row so a row can be
    traced back to its feed
    group is what GTFS_ENABLED_FEEDS selects on
    route_hint is for logging and the load generator and never for filtering since
    the mta moves a route between feeds now and then
    """

    feed_id: str
    group: str
    url: str
    route_hint: tuple[str, ...]
    entity_types: frozenset[str]
    requires_key: bool = False

    def with_key(self, key: str | None) -> str:
        """the pollable url with the onebusaway key appended when needed"""
        if not self.requires_key:
            return self.url
        if not key:
            raise ValueError(f"feed {self.feed_id} requires an API key")
        sep = "&" if "?" in self.url else "?"
        return f"{self.url}{sep}key={key}"


_TU_VP = frozenset({"trip_update", "vehicle_position"})
_ALL = frozenset({"trip_update", "vehicle_position", "alert"})

FEEDS: tuple[Feed, ...] = (
    Feed("nyct-ace", "subway", f"{_MTA_BASE}/nyct%2Fgtfs-ace", ("A", "C", "E", "H", "FS"), _TU_VP),
    Feed("nyct-bdfm", "subway", f"{_MTA_BASE}/nyct%2Fgtfs-bdfm", ("B", "D", "F", "M"), _TU_VP),
    Feed("nyct-g", "subway", f"{_MTA_BASE}/nyct%2Fgtfs-g", ("G",), _TU_VP),
    Feed("nyct-jz", "subway", f"{_MTA_BASE}/nyct%2Fgtfs-jz", ("J", "Z"), _TU_VP),
    Feed("nyct-nqrw", "subway", f"{_MTA_BASE}/nyct%2Fgtfs-nqrw", ("N", "Q", "R", "W"), _TU_VP),
    Feed("nyct-l", "subway", f"{_MTA_BASE}/nyct%2Fgtfs-l", ("L",), _TU_VP),
    Feed("nyct-si", "subway", f"{_MTA_BASE}/nyct%2Fgtfs-si", ("SI",), _TU_VP),
    Feed(
        "nyct-numeric",
        "subway",
        f"{_MTA_BASE}/nyct%2Fgtfs",
        ("1", "2", "3", "4", "5", "6", "7", "GS"),
        _TU_VP,
    ),
    Feed(
        "nyct-subway-alerts",
        "alerts",
        f"{_MTA_BASE}/camsys%2Fsubway-alerts",
        (),
        frozenset({"alert"}),
    ),
    Feed("lirr", "lirr", f"{_MTA_BASE}/lirr%2Fgtfs-lirr", (), _ALL),
    Feed("mnr", "mnr", f"{_MTA_BASE}/mnr%2Fgtfs-mnr", (), _ALL),
    Feed(
        "bus-trip-updates",
        "bus",
        f"{_OBANYC_BASE}/tripUpdates",
        (),
        frozenset({"trip_update"}),
        requires_key=True,
    ),
    Feed(
        "bus-vehicle-positions",
        "bus",
        f"{_OBANYC_BASE}/vehiclePositions",
        (),
        frozenset({"vehicle_position"}),
        requires_key=True,
    ),
)

FEEDS_BY_ID: dict[str, Feed] = {f.feed_id: f for f in FEEDS}


def feeds_for_groups(groups: list[str], *, bus_api_key: str | None = None) -> list[Feed]:
    """pick feeds by group and drop key gated ones when no key is set"""
    wanted = set(groups)
    selected = [f for f in FEEDS if f.group in wanted]
    if bus_api_key is None:
        selected = [f for f in selected if not f.requires_key]
    return selected


def subway_route_universe() -> tuple[str, ...]:
    """every subway route id across the subway feeds"""
    routes: list[str] = []
    for feed in FEEDS:
        if feed.group == "subway":
            routes.extend(feed.route_hint)
    return tuple(dict.fromkeys(routes))
