"""the dbt sql literals have to match the python sla definitions

dbt cannot import python so dq_pipeline_freshness.sql restates the thresholds as
literals
restating a number is fine and restating it quietly is not so this parses the sql
and compares
its mirror assert_freshness_thresholds_match_python.sql asserts the same thing
from inside dbt so a drift fails twice
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from nyc_transit.quality.freshness import SLAS_BY_TABLE, Severity

REPO_ROOT = Path(__file__).resolve().parents[2]
MODEL = (
    REPO_ROOT / "dbt" / "nyc_transit" / "models" / "marts" / "quality" / "dq_pipeline_freshness.sql"
)
SINGULAR_TEST = (
    REPO_ROOT
    / "dbt"
    / "nyc_transit"
    / "tests"
    / "singular"
    / "assert_freshness_thresholds_match_python.sql"
)

# The silver tables dbt models mapped to the Python SLA that governs them
# silver.alert_informed_entities has no SLA of its own it derives from
# bronze.alerts and inherits that contract
DBT_TO_PYTHON = {
    "silver.trip_stop_predictions": "silver.trip_stop_predictions",
    "silver.vehicle_positions": "silver.vehicle_positions",
    "silver.alert_informed_entities": "bronze.alerts",
}


def _parse_model() -> dict[str, tuple[int, str]]:
    """Extract table_name -> (max_lag_seconds_sla, severity) from the model"""
    text = MODEL.read_text()
    pattern = re.compile(
        r"'(?P<table>silver\.[a-z_]+)'\s+as\s+table_name.*?"
        r"(?P<lag>\d+)\s+as\s+max_lag_seconds_sla.*?"
        r"'(?P<severity>critical|warning)'\s+as\s+severity",
        re.DOTALL,
    )
    return {
        m.group("table"): (int(m.group("lag")), m.group("severity")) for m in pattern.finditer(text)
    }


class TestThresholdsAgree:
    def test_the_model_declares_every_expected_table(self):
        assert set(_parse_model()) == set(DBT_TO_PYTHON)

    @pytest.mark.parametrize("dbt_table", sorted(DBT_TO_PYTHON))
    def test_lag_budget_matches_python(self, dbt_table):
        parsed_lag, _ = _parse_model()[dbt_table]
        expected = SLAS_BY_TABLE[DBT_TO_PYTHON[dbt_table]].max_lag_seconds
        assert parsed_lag == expected, (
            f"{dbt_table}: dbt says {parsed_lag}s, "
            f"nyc_transit.quality.freshness says {expected}s"
        )

    @pytest.mark.parametrize("dbt_table", sorted(DBT_TO_PYTHON))
    def test_severity_matches_python(self, dbt_table):
        _, parsed_severity = _parse_model()[dbt_table]
        assert parsed_severity == SLAS_BY_TABLE[DBT_TO_PYTHON[dbt_table]].severity.value

    def test_the_singular_test_restates_the_same_values(self):
        """The dbt-side assertion and the Python side must not drift apart"""
        text = SINGULAR_TEST.read_text()
        for dbt_table, python_table in DBT_TO_PYTHON.items():
            sla = SLAS_BY_TABLE[python_table]
            assert f"('{dbt_table}'," in text
            assert re.search(
                rf"\('{re.escape(dbt_table)}',\s*{sla.max_lag_seconds},\s*'{sla.severity.value}'\)",
                text,
            ), f"{dbt_table} row in the singular test does not match Python"

    def test_warning_severity_is_only_used_where_python_says_so(self):
        warnings = {t for t, (_, sev) in _parse_model().items() if sev == Severity.WARNING.value}
        expected = {
            t for t, py in DBT_TO_PYTHON.items() if SLAS_BY_TABLE[py].severity is Severity.WARNING
        }
        assert warnings == expected
