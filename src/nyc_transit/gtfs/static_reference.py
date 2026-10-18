"""static gtfs reference data

ROUTES is the full nyct subway route list with real ids names colours and types
STOPS is a curated subset of stops.txt
about sixty real stations across every line with their real parent stop_id
coordinates are rounded to 4 decimals which is roughly 11 m

the full stops.txt is around 1500 rows counting every directional platform so
production reads it from s3 and the local build uses this subset
that is also why the join coverage test asserts a threshold and not 100 percent
see docs/limitations.md

the synthetic generator draws its stop ids from here so fixtures seeds and the
dimension tables all share one stop universe and local joins resolve
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Route:
    route_id: str
    route_short_name: str
    route_long_name: str
    route_color: str
    route_type: int  # GTFS: 1 = subway/metro, 2 = rail
    division: str  # IRT (numbered) / BMT / IND, used by dim_route


@dataclass(frozen=True, slots=True)
class Stop:
    stop_id: str  # parent station id, as published in stops.txt
    stop_name: str
    stop_lat: float
    stop_lon: float
    borough: str
    routes: tuple[str, ...]


ROUTES: tuple[Route, ...] = (
    Route("1", "1", "Broadway - 7 Avenue Local", "EE352E", 1, "IRT"),
    Route("2", "2", "7 Avenue Express", "EE352E", 1, "IRT"),
    Route("3", "3", "7 Avenue Express", "EE352E", 1, "IRT"),
    Route("4", "4", "Lexington Avenue Express", "00933C", 1, "IRT"),
    Route("5", "5", "Lexington Avenue Express", "00933C", 1, "IRT"),
    Route("6", "6", "Lexington Avenue Local", "00933C", 1, "IRT"),
    Route("7", "7", "Flushing Local", "B933AD", 1, "IRT"),
    Route("GS", "S", "42 Street Shuttle", "808183", 1, "IRT"),
    Route("A", "A", "8 Avenue Express", "2850AD", 1, "IND"),
    Route("C", "C", "8 Avenue Local", "2850AD", 1, "IND"),
    Route("E", "E", "8 Avenue Local", "2850AD", 1, "IND"),
    Route("H", "S", "Rockaway Park Shuttle", "808183", 1, "IND"),
    Route("FS", "S", "Franklin Avenue Shuttle", "808183", 1, "BMT"),
    Route("B", "B", "6 Avenue Express", "FF6319", 1, "IND"),
    Route("D", "D", "6 Avenue Express", "FF6319", 1, "IND"),
    Route("F", "F", "Queens Blvd Express / 6 Av Local", "FF6319", 1, "IND"),
    Route("M", "M", "QNS BLVD-6 AVE / MYRTLE Local", "FF6319", 1, "IND"),
    Route("G", "G", "Brooklyn - Queens Crosstown", "6CBE45", 1, "IND"),
    Route("J", "J", "Nassau Street Local", "996633", 1, "BMT"),
    Route("Z", "Z", "Nassau Street Express", "996633", 1, "BMT"),
    Route("L", "L", "14 St - Canarsie Local", "A7A9AC", 1, "BMT"),
    Route("N", "N", "Broadway Express", "FCCC0A", 1, "BMT"),
    Route("Q", "Q", "Broadway Express", "FCCC0A", 1, "BMT"),
    Route("R", "R", "Broadway Local", "FCCC0A", 1, "BMT"),
    Route("W", "W", "Broadway Local", "FCCC0A", 1, "BMT"),
    Route("SI", "SIR", "Staten Island Railway", "0039A6", 2, "SIR"),
)

ROUTES_BY_ID: dict[str, Route] = {r.route_id: r for r in ROUTES}

STOPS: tuple[Stop, ...] = (
    Stop("101", "Van Cortlandt Park-242 St", 40.8896, -73.8987, "Bronx", ("1",)),
    Stop("120", "96 St", 40.7939, -73.9723, "Manhattan", ("1", "2", "3")),
    Stop(
        "127",
        "Times Sq-42 St",
        40.7550,
        -73.9871,
        "Manhattan",
        ("1", "2", "3", "7", "N", "Q", "R", "W", "GS"),
    ),
    Stop("132", "14 St", 40.7377, -74.0004, "Manhattan", ("1", "2", "3", "F", "M", "L")),
    Stop("142", "South Ferry", 40.7020, -74.0135, "Manhattan", ("1",)),
    Stop("201", "Wakefield-241 St", 40.9031, -73.8506, "Bronx", ("2",)),
    Stop("228", "Nostrand Av", 40.6699, -73.9502, "Brooklyn", ("2", "5")),
    Stop("247", "Flatbush Av-Brooklyn College", 40.6325, -73.9476, "Brooklyn", ("2", "5")),
    Stop(
        "250",
        "Atlantic Av-Barclays Ctr",
        40.6842,
        -73.9775,
        "Brooklyn",
        ("2", "3", "4", "5", "B", "D", "N", "Q", "R", "W"),
    ),
    Stop("401", "Woodlawn", 40.8862, -73.8784, "Bronx", ("4",)),
    Stop("418", "125 St", 40.8044, -73.9375, "Manhattan", ("4", "5", "6")),
    Stop("423", "86 St", 40.7794, -73.9557, "Manhattan", ("4", "5", "6")),
    Stop("631", "Grand Central-42 St", 40.7519, -73.9767, "Manhattan", ("4", "5", "6", "7", "GS")),
    Stop(
        "635",
        "14 St-Union Sq",
        40.7349,
        -73.9897,
        "Manhattan",
        ("4", "5", "6", "L", "N", "Q", "R", "W"),
    ),
    Stop("640", "Brooklyn Bridge-City Hall", 40.7132, -74.0041, "Manhattan", ("4", "5", "6")),
    Stop("601", "Pelham Bay Park", 40.8526, -73.8281, "Bronx", ("6",)),
    Stop("701", "Flushing-Main St", 40.7596, -73.8301, "Queens", ("7",)),
    Stop("710", "Queensboro Plaza", 40.7509, -73.9400, "Queens", ("7", "N", "W")),
    Stop("726", "34 St-Hudson Yards", 40.7557, -74.0021, "Manhattan", ("7",)),
    Stop("A02", "Inwood-207 St", 40.8683, -73.9199, "Manhattan", ("A",)),
    Stop("A15", "125 St", 40.8113, -73.9524, "Manhattan", ("A", "B", "C", "D")),
    Stop("A24", "59 St-Columbus Circle", 40.7682, -73.9819, "Manhattan", ("A", "B", "C", "D", "1")),
    Stop("A27", "42 St-Port Authority Bus Term", 40.7570, -73.9897, "Manhattan", ("A", "C", "E")),
    Stop("A31", "14 St", 40.7404, -74.0020, "Manhattan", ("A", "C", "E", "L")),
    Stop(
        "A32", "W 4 St-Wash Sq", 40.7323, -74.0005, "Manhattan", ("A", "B", "C", "D", "E", "F", "M")
    ),
    Stop("A41", "Jay St-MetroTech", 40.6923, -73.9873, "Brooklyn", ("A", "C", "F", "R")),
    Stop("A65", "Far Rockaway-Mott Av", 40.6033, -73.7553, "Queens", ("A",)),
    Stop("H11", "Rockaway Park-Beach 116 St", 40.5781, -73.8358, "Queens", ("H",)),
    Stop("E01", "World Trade Center", 40.7126, -74.0099, "Manhattan", ("E",)),
    Stop("D01", "Norwood-205 St", 40.8748, -73.8788, "Bronx", ("D",)),
    Stop("D14", "47-50 Sts-Rockefeller Ctr", 40.7587, -73.9811, "Manhattan", ("B", "D", "F", "M")),
    Stop("D21", "Grand St", 40.7183, -73.9937, "Manhattan", ("B", "D")),
    Stop("D43", "Coney Island-Stillwell Av", 40.5775, -73.9812, "Brooklyn", ("D", "F", "N", "Q")),
    Stop("B06", "21 St-Queensbridge", 40.7542, -73.9421, "Queens", ("F",)),
    Stop("F01", "Jamaica-179 St", 40.7124, -73.7834, "Queens", ("F",)),
    Stop("F14", "Roosevelt Av-Jackson Hts", 40.7465, -73.8913, "Queens", ("E", "F", "M", "R", "7")),
    Stop("F25", "Bergen St", 40.6861, -73.9906, "Brooklyn", ("F", "G")),
    Stop("M01", "Middle Village-Metropolitan Av", 40.7116, -73.8894, "Queens", ("M",)),
    Stop("M11", "Myrtle Av", 40.6975, -73.9354, "Brooklyn", ("M", "J", "Z")),
    Stop("G22", "Court Sq", 40.7470, -73.9456, "Queens", ("G", "E", "M", "7")),
    Stop("G29", "Nassau Av", 40.7245, -73.9511, "Brooklyn", ("G",)),
    Stop("G36", "Church Av", 40.6441, -73.9796, "Brooklyn", ("G", "F")),
    Stop("J12", "Jamaica Center-Parsons/Archer", 40.7021, -73.8014, "Queens", ("J", "Z", "E")),
    Stop("J27", "Marcy Av", 40.7084, -73.9579, "Brooklyn", ("J", "M", "Z")),
    Stop("M23", "Essex St", 40.7184, -73.9879, "Manhattan", ("J", "M", "Z")),
    Stop("L01", "8 Av", 40.7398, -74.0025, "Manhattan", ("L",)),
    Stop("L06", "1 Av", 40.7309, -73.9817, "Manhattan", ("L",)),
    Stop("L08", "Bedford Av", 40.7171, -73.9568, "Brooklyn", ("L",)),
    Stop("L17", "Myrtle-Wyckoff Avs", 40.6996, -73.9119, "Brooklyn", ("L", "M")),
    Stop("L29", "Canarsie-Rockaway Pkwy", 40.6464, -73.9020, "Brooklyn", ("L",)),
    Stop("R01", "Astoria-Ditmars Blvd", 40.7753, -73.9121, "Queens", ("N", "W")),
    Stop(
        "R11", "Lexington Av/59 St", 40.7625, -73.9676, "Manhattan", ("N", "R", "W", "4", "5", "6")
    ),
    Stop("R16", "Times Sq-42 St", 40.7556, -73.9870, "Manhattan", ("N", "Q", "R", "W")),
    Stop("R23", "Canal St", 40.7186, -74.0003, "Manhattan", ("N", "Q", "R", "W")),
    Stop("R31", "Atlantic Av-Barclays Ctr", 40.6840, -73.9777, "Brooklyn", ("N", "Q", "R", "W")),
    Stop("R45", "Bay Ridge-95 St", 40.6161, -74.0309, "Brooklyn", ("R",)),
    Stop("Q05", "Brighton Beach", 40.5779, -73.9613, "Brooklyn", ("Q", "B")),
    Stop("S01", "Franklin Av", 40.6809, -73.9558, "Brooklyn", ("FS", "C")),
    Stop("S31", "St George", 40.6435, -74.0736, "Staten Island", ("SI",)),
    Stop("S09", "Tottenville", 40.5125, -74.2516, "Staten Island", ("SI",)),
)

STOPS_BY_ID: dict[str, Stop] = {s.stop_id: s for s in STOPS}

# Directional platform suffixes NYCT appends to the parent station id in the
# realtime feeds ("127N" is Times Sq northbound) GTFS-RT stop ids are
# platform-level while static stops.txt rows are parents so the dbt staging
# layer strips the suffix to join see stg_trip_stop_predictions.sql
PLATFORM_SUFFIXES: tuple[str, ...] = ("N", "S")


def stops_for_route(route_id: str) -> tuple[Stop, ...]:
    """stops a route serves in the order declared above"""
    return tuple(s for s in STOPS if route_id in s.routes)


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
    """routes with at least one curated stop

    the generator draws from these so every generated stop id resolves to a station
    and the join coverage test is not vacuous
    """
    return tuple(r.route_id for r in ROUTES if stops_for_route(r.route_id))
