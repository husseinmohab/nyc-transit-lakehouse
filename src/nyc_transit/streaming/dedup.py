"""idempotent merge statements

the pipeline is at least once delivery with idempotent keyed application which
gives exactly once effects in the lakehouse
kafka replays a micro batch with identical content and the merge below applies it
by event_id so a replay lands the same rows
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class MergeKey:
    """dedup key and version column for one target table

    key_columns identify a row so equality means same fact
    version_column decides which of two rows for the same key wins and has to come
    off the source data like a feed timestamp
    a processing time version makes the result depend on replay order which breaks
    idempotence
    """

    key_columns: tuple[str, ...]
    version_column: str

    def __post_init__(self) -> None:
        if not self.key_columns:
            raise ValueError("a merge key needs at least one key column")
        if self.version_column in self.key_columns:
            raise ValueError(
                f"version column {self.version_column!r} cannot also be a key column: "
                "a key is immutable, a version changes"
            )


def _qualified_eq(key_columns: tuple[str, ...]) -> str:
    return " AND ".join(f"t.{c} = s.{c}" for c in key_columns)


def merge_sql(
    *,
    target_table: str,
    source_view: str,
    merge_key: MergeKey,
    update_columns: tuple[str, ...],
    insert_columns: tuple[str, ...],
) -> str:
    """render the idempotent MERGE INTO

    update_columns leaves out the key columns since updating a key in place would
    change a rows identity

    the WHEN MATCHED arm uses a strict > on the version column and not >=
    with >= a replay rewrites every matched row so you get a delete file plus a data
    file per micro batch for no change
    """
    bad = set(update_columns) & set(merge_key.key_columns)
    if bad:
        raise ValueError(f"refusing to UPDATE key columns {sorted(bad)}")
    missing = set(merge_key.key_columns) - set(insert_columns)
    if missing:
        raise ValueError(f"insert_columns is missing key columns {sorted(missing)}")

    set_clause = ",\n    ".join(f"t.{c} = s.{c}" for c in update_columns)
    insert_cols = ", ".join(insert_columns)
    insert_vals = ", ".join(f"s.{c}" for c in insert_columns)

    return (
        f"MERGE INTO {target_table} AS t\n"
        f"USING {source_view} AS s\n"
        f"ON {_qualified_eq(merge_key.key_columns)}\n"
        f"WHEN MATCHED AND s.{merge_key.version_column} > "
        f"t.{merge_key.version_column} THEN UPDATE SET\n"
        f"    {set_clause}\n"
        f"WHEN NOT MATCHED THEN INSERT ({insert_cols})\n"
        f"VALUES ({insert_vals})"
    )


def dedup_within_batch_sql(
    *,
    source_view: str,
    merge_key: MergeKey,
    select_columns: tuple[str, ...],
) -> str:
    """collapse duplicates inside one micro batch before merging

    iceberg raises if two source rows match the same target row
    one micro batch routinely holds several publications of one key because the
    producer polls faster than the feed changes

    ROW_NUMBER keeps the highest version row per key tie broken on the key so a
    replay picks the same winner
    """
    cols = ", ".join(select_columns)
    order_by = ", ".join([f"{merge_key.version_column} DESC", *merge_key.key_columns])
    partition_by = ", ".join(merge_key.key_columns)
    return (
        f"SELECT {cols} FROM (\n"
        f"  SELECT {cols},\n"
        f"         ROW_NUMBER() OVER (\n"
        f"           PARTITION BY {partition_by}\n"
        f"           ORDER BY {order_by}\n"
        f"         ) AS _rn\n"
        f"  FROM {source_view}\n"
        f") WHERE _rn = 1"
    )


def partition_delete_sql(*, target_table: str, partition_column: str, values: list[str]) -> str:
    """delete scoped to whole partitions

    the backfills first step
    it is re runnable because it replaces a partition instead of merging into it
    scoping the delete to the partition column lets iceberg satisfy it by dropping
    whole data files instead of writing position deletes

    values get validated as YYYY-MM-DD since this builds a delete and a malformed
    value reaching the sql could destroy data
    """
    if not values:
        raise ValueError("refusing to build a partition DELETE with no partition values")
    for v in values:
        if len(v) != 10 or v[4] != "-" or v[7] != "-":
            raise ValueError(f"partition value {v!r} is not an ISO date (YYYY-MM-DD)")
        if not (v[:4] + v[5:7] + v[8:]).isdigit():
            raise ValueError(f"partition value {v!r} is not an ISO date (YYYY-MM-DD)")
    in_list = ", ".join(f"DATE '{v}'" for v in values)
    return f"DELETE FROM {target_table} WHERE {partition_column} IN ({in_list})"


# Bronze dedups on event_id the version is the feed publication time so a
# later publication of the same business key replaces the row
BRONZE_MERGE_KEY = MergeKey(key_columns=("event_id",), version_column="feed_timestamp_ms")

SILVER_TRIP_MERGE_KEY = MergeKey(key_columns=("event_id",), version_column="feed_timestamp_ms")

SILVER_VEHICLE_MERGE_KEY = MergeKey(key_columns=("event_id",), version_column="reported_at_ms")

# Alerts explode to one row per informed entity so the ordinal is part of the
# key without it every informed entity of one alert collides on alert_event_id
# and the MERGE keeps only the last
SILVER_ALERT_MERGE_KEY = MergeKey(
    key_columns=("alert_event_id", "informed_ordinal"), version_column="feed_timestamp_ms"
)
