"""the static loader and the dbt seeds must carry identical data

dbt loads gtfs_routes and gtfs_stops from the seed CSVs on duckdb and from
streaming/static_tables.py on spark, because dbt-spark cannot insert a station
name with an apostrophe in it

if the two drift then the same model gives different answers per target, which
is exactly the kind of difference nobody notices until production
"""

from __future__ import annotations

import csv
from pathlib import Path

from nyc_transit.streaming.static_tables import (
    ROUTE_COLUMNS,
    STOP_COLUMNS,
    route_rows,
    stop_rows,
)

SEED_DIR = Path(__file__).resolve().parents[2] / "dbt" / "nyc_transit" / "seeds"


def _seed(name: str) -> list[dict[str, str]]:
    with (SEED_DIR / name).open() as fh:
        return list(csv.DictReader(fh))


class TestColumnsMatchTheSeeds:
    def test_route_columns(self):
        assert tuple(_seed("gtfs_routes.csv")[0]) == ROUTE_COLUMNS

    def test_stop_columns(self):
        assert tuple(_seed("gtfs_stops.csv")[0]) == STOP_COLUMNS


class TestRowsMatchTheSeeds:
    def test_route_rows(self):
        seed = _seed("gtfs_routes.csv")
        rows = route_rows()
        assert len(rows) == len(seed)
        for want, got in zip(seed, rows, strict=True):
            assert want == {k: str(v) for k, v in got.items()}

    def test_stop_rows(self):
        seed = _seed("gtfs_stops.csv")
        rows = stop_rows()
        assert len(rows) == len(seed)
        for want, got in zip(seed, rows, strict=True):
            assert want == {k: str(v) for k, v in got.items()}


class TestTheApostropheCase:
    def test_station_names_with_apostrophes_survive(self):
        """the reason this module exists

        these two go through the dataframe api
        """
        names = {s["stop_name"] for s in stop_rows() if "'" in s["stop_name"]}
        assert names == {"E 143 St-St Mary's St", "Prince's Bay"}
