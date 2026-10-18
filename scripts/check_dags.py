#!/usr/bin/env python
"""import every dag and check the expected dag_ids with no import errors

ci runs this and so does make airflow-check

a script and not a pytest test because it needs airflow installed against
airflows own constraints file which fights the resolution the main test venv uses
see requirements/airflow.txt
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DAGS_FOLDER = REPO_ROOT / "airflow" / "dags"

# Every dag_id this repository is expected to publish A DAG that disappears,
# renamed or silently failing to import is as much a bug as one that fails
EXPECTED_DAG_IDS = {
    "gtfs_streaming_supervisor",
    "gtfs_silver_backfill",
    "iceberg_maintenance",
}


def main() -> int:
    os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "false")
    os.environ.setdefault("AIRFLOW__CORE__DAGS_FOLDER", str(DAGS_FOLDER))
    sys.path.insert(0, str(REPO_ROOT / "airflow" / "plugins"))
    sys.path.insert(0, str(REPO_ROOT / "src"))

    from airflow.models import DagBag

    bag = DagBag(dag_folder=str(DAGS_FOLDER), include_examples=False)

    if bag.import_errors:
        print("DAG import errors:", file=sys.stderr)
        for filename, error in bag.import_errors.items():
            print(f"\n--- {filename} ---\n{error}", file=sys.stderr)
        return 1

    # bag.dags not bag.get_dag() get_dag() queries the metadata DB to check
    # whether a serialized DAG is stale which would make this need a migrated
    # Airflow database the in-memory dict answers the actual question
    dags = bag.dags
    found = set(dags)
    missing = EXPECTED_DAG_IDS - found
    unexpected = found - EXPECTED_DAG_IDS

    for dag_id in sorted(found):
        dag = dags[dag_id]
        print(
            f"ok  {dag_id}: {len(dag.tasks)} tasks, "
            f"schedule={dag.schedule_interval!r}, "
            f"catchup={dag.catchup}, max_active_runs={dag.max_active_runs}"
        )

    status = 0
    if missing:
        print(f"\nMISSING expected dag_ids: {sorted(missing)}", file=sys.stderr)
        status = 1
    if unexpected:
        print(
            f"\nUNEXPECTED dag_ids (add them to EXPECTED_DAG_IDS if intended): "
            f"{sorted(unexpected)}",
            file=sys.stderr,
        )
        status = 1

    # Structural checks a plain import would not catch
    for dag_id in sorted(found & EXPECTED_DAG_IDS):
        dag = dags[dag_id]
        if not dag.tasks:
            print(f"\n{dag_id} has no tasks", file=sys.stderr)
            status = 1
        if dag.catchup and dag.max_active_runs == 0:
            print(f"\n{dag_id} has catchup enabled with unbounded max_active_runs", file=sys.stderr)
            status = 1

    return status


if __name__ == "__main__":
    raise SystemExit(main())
