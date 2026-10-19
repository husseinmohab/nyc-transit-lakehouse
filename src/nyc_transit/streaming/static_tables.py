"""write the static gtfs reference into iceberg

dbt seeds load these two tables on duckdb but not on spark dbt-spark builds one
INSERT INTO VALUES per seed and does not escape a single quote so
Prince's Bay and E 143 St-St Mary's St both fail the parse
real station names have apostrophes in them so that path is not usable here

the rows come from the same snapshot static_reference reads and the columns are
the same as the committed seeds so dbt sees identical data either way
tests/unit/test_static_tables.py asserts that against the seed files
"""

from __future__ import annotations

from typing import Any

from nyc_transit.config import LakehouseSettings
from nyc_transit.gtfs.static_reference import ROUTES, STOPS

ROUTE_COLUMNS = (
    "route_id",
    "route_short_name",
    "route_long_name",
    "route_color",
    "route_type",
    "division",
)

STOP_COLUMNS = (
    "stop_id",
    "stop_name",
    "stop_lat",
    "stop_lon",
    "borough",
    "served_by_routes",
)


def route_rows() -> list[dict[str, Any]]:
    """one dict per route in the order the snapshot declares them"""
    return [
        {
            "route_id": r.route_id,
            "route_short_name": r.route_short_name,
            "route_long_name": r.route_long_name,
            "route_color": r.route_color,
            "route_type": r.route_type,
            "division": r.division,
        }
        for r in ROUTES
    ]


def stop_rows() -> list[dict[str, Any]]:
    """one dict per parent station

    served_by_routes is pipe separated to match the seed where a nested comma
    list would need quoting
    """
    return [
        {
            "stop_id": s.stop_id,
            "stop_name": s.stop_name,
            "stop_lat": s.stop_lat,
            "stop_lon": s.stop_lon,
            "borough": s.borough,
            "served_by_routes": "|".join(s.routes),
        }
        for s in STOPS
    ]


def _schemas() -> tuple[Any, Any]:
    """declared not inferred so a column never silently becomes the wrong type

    route_id looks numeric for the 1 train and an inferred bigint breaks every
    join against the realtime feeds where it arrives as a string
    """
    from pyspark.sql.types import (
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
    )

    routes = StructType(
        [
            StructField("route_id", StringType(), nullable=False),
            StructField("route_short_name", StringType()),
            StructField("route_long_name", StringType()),
            StructField("route_color", StringType()),
            StructField("route_type", IntegerType()),
            StructField("division", StringType()),
        ]
    )
    stops = StructType(
        [
            StructField("stop_id", StringType(), nullable=False),
            StructField("stop_name", StringType()),
            StructField("stop_lat", DoubleType()),
            StructField("stop_lon", DoubleType()),
            StructField("borough", StringType()),
            StructField("served_by_routes", StringType()),
        ]
    )
    return routes, stops


def load_static_reference(spark: Any, settings: LakehouseSettings) -> dict[str, int]:
    """create or replace the two static tables and hand back the row counts

    createOrReplace, so running it twice lands the same two tables. they hold a
    snapshot of the published files, not a history
    """
    route_schema, stop_schema = _schemas()
    namespace = f"{settings.catalog_name}.{settings.static_namespace}"
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")

    written: dict[str, int] = {}
    for name, rows, schema in (
        ("gtfs_routes", route_rows(), route_schema),
        ("gtfs_stops", stop_rows(), stop_schema),
    ):
        df = spark.createDataFrame(rows, schema=schema)
        df.writeTo(f"{namespace}.{name}").using("iceberg").createOrReplace()
        written[f"{namespace}.{name}"] = len(rows)
    return written
