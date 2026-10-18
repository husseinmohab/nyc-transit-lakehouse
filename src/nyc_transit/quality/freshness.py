"""freshness slas and how they get evaluated

plain dataclasses and pure functions
expectations.py wraps them in great expectations to get validation results and a
pass or fail the airflow task asserts on but what fresh means lives here so a
threshold change is a reviewable diff instead of a line buried in a ge suite

picking the thresholds
the mta publishes most subway feeds every 15-30s and the streaming trigger is 30s
so a healthy silver.trip_stop_predictions is at most around 90s stale once you
add feed interval trigger and merge
the thresholds are roughly 3x that so one slow micro batch does not page anyone
but a stalled producer gets caught in minutes

alerts get a much looser sla
a quiet day genuinely produces no new alerts so a tight check there would fire on
good news
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class Severity(StrEnum):
    """whether a breach fails the dag or only warns"""

    CRITICAL = "critical"
    WARNING = "warning"


@dataclass(frozen=True)
class FreshnessSLA:
    """a freshness contract for one table

    event_time_column has to be an event time column meaning when the mta published
    the fact and not a load time one
    checking silver_loaded_at would call a stalled pipeline fresh for as long as it
    kept rewriting stale rows

    min_rows_per_partition catches fresh but nearly empty which is what a producer
    reaching one feed out of eight looks like
    CRITICAL breaches fail the dag
    """

    table: str
    event_time_column: str
    max_lag_seconds: int
    min_rows_per_partition: int
    severity: Severity = Severity.CRITICAL
    description: str = ""

    def __post_init__(self) -> None:
        if self.max_lag_seconds <= 0:
            raise ValueError(f"{self.table}: max_lag_seconds must be positive")
        if self.min_rows_per_partition < 0:
            raise ValueError(f"{self.table}: min_rows_per_partition cannot be negative")


# The contracts enforced by the Airflow freshness task
FRESHNESS_SLAS: tuple[FreshnessSLA, ...] = (
    FreshnessSLA(
        table="bronze.trip_updates",
        event_time_column="feed_timestamp_ms",
        max_lag_seconds=300,
        min_rows_per_partition=10_000,
        description=(
            "Stop-level predictions from eight subway feeds. 5 minutes is about 3x "
            "the expected 90s end-to-end lag (30s feed, 30s trigger, merge)."
        ),
    ),
    FreshnessSLA(
        table="bronze.vehicle_positions",
        event_time_column="feed_timestamp_ms",
        max_lag_seconds=300,
        min_rows_per_partition=1_000,
        description="Vehicle positions, far lower volume than trip updates.",
    ),
    FreshnessSLA(
        table="silver.trip_stop_predictions",
        event_time_column="feed_timestamp",
        max_lag_seconds=420,
        min_rows_per_partition=10_000,
        description=(
            "Silver is merged in the same micro-batch as bronze, so its SLA is "
            "bronze's plus one trigger interval of slack."
        ),
    ),
    FreshnessSLA(
        table="silver.vehicle_positions",
        event_time_column="reported_at",
        max_lag_seconds=420,
        min_rows_per_partition=1_000,
        description="Silver vehicle positions.",
    ),
    FreshnessSLA(
        table="bronze.alerts",
        event_time_column="feed_timestamp_ms",
        max_lag_seconds=21_600,
        min_rows_per_partition=0,
        severity=Severity.WARNING,
        description=(
            "6 hours, warning only. A quiet day produces no new alerts, so a "
            "tight check here would fire on good news."
        ),
    ),
)

SLAS_BY_TABLE: dict[str, FreshnessSLA] = {s.table: s for s in FRESHNESS_SLAS}


@dataclass(frozen=True)
class FreshnessObservation:
    """what the lakehouse reports for one table and partition"""

    table: str
    partition_value: str | None
    row_count: int
    max_event_time_epoch_seconds: float | None
    observed_at_epoch_seconds: float

    @property
    def lag_seconds(self) -> float | None:
        """seconds between the newest event and the observation

        None for an empty table
        that is a different condition from lag is huge and evaluate_freshness keeps them
        apart
        """
        if self.max_event_time_epoch_seconds is None:
            return None
        return self.observed_at_epoch_seconds - self.max_event_time_epoch_seconds


@dataclass(frozen=True)
class SlaBreach:
    table: str
    kind: str  # stale | empty | thin | future_timestamp
    severity: Severity
    detail: str

    @property
    def fails_dag(self) -> bool:
        return self.severity is Severity.CRITICAL


def evaluate_freshness(
    observation: FreshnessObservation,
    sla: FreshnessSLA,
    *,
    clock_skew_tolerance_seconds: int = 120,
) -> list[SlaBreach]:
    """compare one observation against its sla

    four failure modes kept apart because they have different causes

    empty is no rows
    producer down or the wrong partition asked for
    thin is rows present but under min_rows_per_partition
    usually a subset of feeds failing while the dashboard still looks fine
    stale is a newest event older than max_lag_seconds
    future_timestamp is a newest event past the clock skew tolerance
    checked because a future timestamp makes every lag look healthy so a feed with a
    bad clock would otherwise hide a real outage
    reported and never corrected
    """
    breaches: list[SlaBreach] = []

    if observation.row_count == 0:
        breaches.append(
            SlaBreach(
                sla.table,
                "empty",
                sla.severity,
                f"partition {observation.partition_value!r} has no rows",
            )
        )
        return breaches

    if observation.row_count < sla.min_rows_per_partition:
        breaches.append(
            SlaBreach(
                sla.table,
                "thin",
                sla.severity,
                f"partition {observation.partition_value!r} has {observation.row_count} rows, "
                f"expected at least {sla.min_rows_per_partition}",
            )
        )

    lag = observation.lag_seconds
    if lag is None:
        breaches.append(
            SlaBreach(
                sla.table,
                "empty",
                sla.severity,
                f"{observation.row_count} rows but no non-null {sla.event_time_column}",
            )
        )
        return breaches

    if lag < -clock_skew_tolerance_seconds:
        breaches.append(
            SlaBreach(
                sla.table,
                "future_timestamp",
                sla.severity,
                f"newest {sla.event_time_column} is {-lag:.0f}s in the future "
                f"(tolerance {clock_skew_tolerance_seconds}s)",
            )
        )
    elif lag > sla.max_lag_seconds:
        breaches.append(
            SlaBreach(
                sla.table,
                "stale",
                sla.severity,
                f"newest {sla.event_time_column} is {lag:.0f}s old, "
                f"SLA is {sla.max_lag_seconds}s",
            )
        )

    return breaches


def evaluate_all(
    observations: list[FreshnessObservation],
    slas: tuple[FreshnessSLA, ...] = FRESHNESS_SLAS,
) -> list[SlaBreach]:
    """evaluate many observations

    a table with no sla is itself a breach
    skipping an unmatched table quietly is how a monitored table stops being
    monitored
    """
    by_table = {s.table: s for s in slas}
    breaches: list[SlaBreach] = []
    for obs in observations:
        sla = by_table.get(obs.table)
        if sla is None:
            breaches.append(
                SlaBreach(
                    obs.table,
                    "stale",
                    Severity.WARNING,
                    f"no freshness SLA defined for {obs.table}; table is unmonitored",
                )
            )
            continue
        breaches.extend(evaluate_freshness(obs, sla))

    observed = {o.table for o in observations}
    for sla in slas:
        if sla.table not in observed:
            breaches.append(
                SlaBreach(
                    sla.table,
                    "empty",
                    sla.severity,
                    f"{sla.table} has an SLA but was not observed; the probe failed or "
                    "the table does not exist",
                )
            )
    return breaches
