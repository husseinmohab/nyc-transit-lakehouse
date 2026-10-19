"""great expectations suites built off the freshness slas

the pure functions in freshness.py decide pass or fail
ge adds what hand rolled asserts do not

a uniform result per check with the observed value next to the threshold so on
call sees lag was 612s against an sla of 420s instead of freshness check failed
data docs which is a browsable history of which contract broke when
one vocabulary for the freshness gates and the row level gates that run after a
backfill

the slas get compiled into expectations instead of written as ge json so every
threshold change is a reviewable diff in freshness.py and the two cannot
disagree
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from nyc_transit.quality.freshness import (
    FRESHNESS_SLAS,
    FreshnessObservation,
    FreshnessSLA,
    Severity,
    SlaBreach,
    evaluate_all,
)

if TYPE_CHECKING:  # pragma: no cover
    import pandas as pd

log = logging.getLogger(__name__)

FRESHNESS_SUITE_NAME = "lakehouse_freshness"
PARTITION_SUITE_NAME = "silver_partition_integrity"


@dataclass
class ValidationOutcome:
    """flattened ge result plus the sla verdict the dag acts on"""

    suite_name: str
    success: bool
    checks: list[dict[str, Any]]
    breaches: list[SlaBreach]

    @property
    def critical_breaches(self) -> list[SlaBreach]:
        return [b for b in self.breaches if b.fails_dag]

    @property
    def should_fail_dag(self) -> bool:
        return bool(self.critical_breaches)

    def render(self) -> str:
        lines = [f"suite={self.suite_name} ge_success={self.success}"]
        for check in self.checks:
            flag = "PASS" if check["success"] else "FAIL"
            lines.append(
                f"  [{flag}] {check['expectation']} {check.get('column', '')} "
                f"observed={check.get('observed')!r}"
            )
        for breach in self.breaches:
            lines.append(f"  [{breach.severity.value.upper()}] {breach.kind}: {breach.detail}")
        return "\n".join(lines)


def observations_to_frame(observations: list[FreshnessObservation]) -> pd.DataFrame:
    """one row per observed table

    the frame ge validates
    lag_seconds is a real column instead of being worked out inside an expectation so
    the observed value shows up verbatim in the ge result
    """
    import pandas as pd

    return pd.DataFrame(
        [
            {
                "table": o.table,
                "partition_value": o.partition_value,
                "row_count": o.row_count,
                "max_event_time_epoch_seconds": o.max_event_time_epoch_seconds,
                "observed_at_epoch_seconds": o.observed_at_epoch_seconds,
                "lag_seconds": o.lag_seconds,
            }
            for o in observations
        ],
        columns=[
            "table",
            "partition_value",
            "row_count",
            "max_event_time_epoch_seconds",
            "observed_at_epoch_seconds",
            "lag_seconds",
        ],
    )


def build_freshness_expectations(slas: tuple[FreshnessSLA, ...] = FRESHNESS_SLAS) -> list[Any]:
    """compile the slas into ge expectation objects

    per table expectations use row_condition so one suite covers every table in a
    single frame
    condition_parser pandas is required for row_condition on the pandas engine
    """
    import great_expectations.expectations as gxe

    expectations: list[Any] = [
        # Structural checks These hold regardless of any SLA and catch a broken
        # probe which would otherwise look like a passing check
        gxe.ExpectColumnValuesToNotBeNull(column="table"),
        gxe.ExpectColumnValuesToNotBeNull(column="observed_at_epoch_seconds"),
        gxe.ExpectColumnValuesToBeUnique(column="table"),
        gxe.ExpectColumnValuesToBeInSet(
            column="table", value_set=[s.table for s in slas], mostly=1.0
        ),
    ]
    for sla in slas:
        condition = f'table=="{sla.table}"'
        expectations.append(
            gxe.ExpectColumnValuesToBeBetween(
                column="lag_seconds",
                min_value=-120,
                max_value=sla.max_lag_seconds,
                row_condition=condition,
                condition_parser="pandas",
                meta={
                    "sla_table": sla.table,
                    "severity": sla.severity.value,
                    "rationale": sla.description,
                },
            )
        )
        if sla.min_rows_per_partition > 0:
            expectations.append(
                gxe.ExpectColumnValuesToBeBetween(
                    column="row_count",
                    min_value=sla.min_rows_per_partition,
                    row_condition=condition,
                    condition_parser="pandas",
                    meta={"sla_table": sla.table, "severity": sla.severity.value},
                )
            )
    return expectations


def build_partition_expectations() -> list[Any]:
    """row level gates for a rebuilt silver partition

    the backfill dag runs these on the partition it just wrote
    they are the invariants the merge is meant to guarantee asserted on their own
    which is the only way the claim survives someone editing the sql
    """
    import great_expectations.expectations as gxe

    return [
        gxe.ExpectColumnValuesToNotBeNull(column="event_id"),
        gxe.ExpectColumnValuesToBeUnique(
            column="event_id",
            meta={"why": "the MERGE key; a duplicate here means dedup regressed"},
        ),
        gxe.ExpectColumnValuesToNotBeNull(column="service_date"),
        gxe.ExpectColumnValuesToNotBeNull(column="feed_timestamp_ms"),
        gxe.ExpectColumnValuesToNotBeNull(column="route_id", mostly=0.99),
        gxe.ExpectColumnValuesToBeBetween(
            column="stop_sequence",
            min_value=1,
            max_value=300,
            mostly=0.999,
            meta={"why": "GTFS stop_sequence is 1-based; 300 is beyond any MTA route"},
        ),
        gxe.ExpectColumnValuesToBeBetween(
            column="prediction_horizon_seconds",
            min_value=-3600,
            max_value=14400,
            mostly=0.995,
            meta={
                "why": "the MTA predicts a few hours ahead; negatives are real "
                "(already-passed stops) but a 4h horizon is not"
            },
        ),
    ]


def _run_suite(
    frame: pd.DataFrame,
    expectations: list[Any],
    suite_name: str,
) -> tuple[bool, list[dict[str, Any]]]:
    """run expectations against a pandas frame on an ephemeral ge context

    ephemeral keeps ge from dropping a gx dir into the repo on every run
    the airflow deployment points at a filesystem context so data docs persist
    """
    import great_expectations as gx

    context = gx.get_context(mode="ephemeral")
    source = context.data_sources.add_pandas(f"{suite_name}_source")
    asset = source.add_dataframe_asset(f"{suite_name}_asset")
    batch_definition = asset.add_batch_definition_whole_dataframe(f"{suite_name}_batch")

    suite = context.suites.add(gx.ExpectationSuite(name=suite_name))
    for expectation in expectations:
        suite.add_expectation(expectation)

    validation_definition = context.validation_definitions.add(
        gx.ValidationDefinition(data=batch_definition, suite=suite, name=f"{suite_name}_vd")
    )
    result = validation_definition.run(batch_parameters={"dataframe": frame})

    checks: list[dict[str, Any]] = []
    for item in result.results:
        config = item.expectation_config
        kwargs = dict(config.kwargs or {})
        checks.append(
            {
                "expectation": config.type,
                "column": kwargs.get("column"),
                "row_condition": kwargs.get("row_condition"),
                "success": bool(item.success),
                "observed": (item.result or {}).get("observed_value")
                or (item.result or {}).get("partial_unexpected_list"),
                "unexpected_count": (item.result or {}).get("unexpected_count"),
                "meta": config.meta or {},
            }
        )
    return bool(result.success), checks


def validate_freshness(
    observations: list[FreshnessObservation],
    *,
    slas: tuple[FreshnessSLA, ...] = FRESHNESS_SLAS,
) -> ValidationOutcome:
    """validate freshness observations and hand back what the dag acts on

    ges success and the sla verdict come back separately
    a WARNING breach makes ge unsuccessful but must not fail the dag and conflating
    them would let the loose alerts sla page someone at 3am
    """
    frame = observations_to_frame(observations)
    if frame.empty:
        # No observations at all is itself a breach and GE cannot express that
        # over an empty frame
        breaches = evaluate_all([], slas)
        return ValidationOutcome(FRESHNESS_SUITE_NAME, False, [], breaches)

    success, checks = _run_suite(frame, build_freshness_expectations(slas), FRESHNESS_SUITE_NAME)
    breaches = evaluate_all(observations, slas)
    return ValidationOutcome(FRESHNESS_SUITE_NAME, success, checks, breaches)


def validate_partition(frame: pd.DataFrame, *, partition_value: str) -> ValidationOutcome:
    """validate a rebuilt silver partitions row level integrity"""
    if frame.empty:
        return ValidationOutcome(
            PARTITION_SUITE_NAME,
            False,
            [],
            [
                SlaBreach(
                    "silver.trip_stop_predictions",
                    "empty",
                    Severity.CRITICAL,
                    f"backfill produced 0 rows for partition {partition_value}",
                )
            ],
        )
    success, checks = _run_suite(frame, build_partition_expectations(), PARTITION_SUITE_NAME)
    breaches = (
        []
        if success
        else [
            SlaBreach(
                "silver.trip_stop_predictions",
                "thin",
                Severity.CRITICAL,
                f"partition {partition_value} failed "
                f"{sum(1 for c in checks if not c['success'])} integrity expectation(s)",
            )
        ]
    )
    return ValidationOutcome(PARTITION_SUITE_NAME, success, checks, breaches)
