"""idempotent partition scoped daily backfill of silver

one dag run rebuilds one gtfs service date
re running a date by retry by airflow tasks clear or by hand gives the same
result because the unit of work is a whole partition replacement inside one
iceberg transaction

tasks are wait_for_partition which is a sensor so work never starts on a
partition bronze has not finished receiving then rebuild_silver_partition then
validate_partition which is great expectations and fails the dag then
freshness_gate then dbt_build_partition scoped to the same service date via
--vars

depends_on_past is off
because each run is partition scoped and idempotent day n does not need day n-1
so one bad day does not block the backlog
that is the payoff of building the backfill around partition replacement
"""

from __future__ import annotations

import sys
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.models.dag import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "src"))

from nyc_transit_airflow.callables import (  # noqa: E402
    resolve_service_date,
    run_freshness_gate,
    run_partition_backfill,
    validate_backfilled_partition,
)

DBT_PROJECT_DIR = "/opt/project/dbt/nyc_transit"

DEFAULT_ARGS = {
    "owner": "mohab",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "depends_on_past": False,
    "email_on_failure": False,
}


def bronze_partition_is_ready(**context) -> bool:
    """sensor

    does bronze hold a plausible number of rows for this date

    guards against the usual backfill failure where the job runs before the partition
    is complete rebuilds silver off a quarter of the data passes every integrity test
    since the rows present are all valid and leaves a permanently thin partition
    nobody notices

    the threshold is the same min_rows_per_partition the freshness sla uses so the
    sensor and the gate cannot disagree
    """
    from nyc_transit.config import load_settings
    from nyc_transit.quality.freshness import SLAS_BY_TABLE
    from nyc_transit.streaming.session import build_spark_session

    service_date = resolve_service_date(
        context["logical_date"], lookback_days=int(context["params"]["lookback_days"])
    )
    settings = load_settings()
    sla = SLAS_BY_TABLE["bronze.trip_updates"]
    table = settings.lakehouse.bronze("trip_updates")

    spark = build_spark_session(
        settings.lakehouse, app_name="nyc-transit-backfill-sensor", with_kafka=False
    )
    try:
        rows = spark.sql(
            f"SELECT count(*) AS n FROM {table} WHERE service_date = DATE '{service_date}'"
        ).collect()[0]["n"]
    except Exception:
        # A missing table is "not ready yet" not an error on a first deploy
        # the sensor should wait for the streaming job to create it
        return False
    finally:
        spark.stop()

    ready = int(rows) >= sla.min_rows_per_partition
    print(f"{table} {service_date}: {rows} rows (need {sla.min_rows_per_partition}) -> {ready}")
    return ready


with DAG(
    dag_id="gtfs_silver_backfill",
    description="Rebuild one service date of the Iceberg silver layer, then gate and dbt it.",
    default_args=DEFAULT_ARGS,
    schedule="30 3 * * *",  # 03:30 UTC, about 23:30 ET, after the service day closes
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=True,
    # Three concurrent runs during a catch-up enough to move a backlog few
    # enough that three Spark drivers do not exhaust the cluster
    max_active_runs=3,
    dagrun_timeout=timedelta(hours=3),
    tags=["backfill", "iceberg", "dbt", "great_expectations"],
    params={
        "lookback_days": Param(
            0,
            type="integer",
            minimum=0,
            maximum=90,
            description="Shift the target service date back N days.",
        ),
        "dry_run": Param(False, type="boolean", description="Print the SQL without executing it."),
    },
    doc_md=__doc__,
) as dag:
    wait_for_partition = PythonSensor(
        task_id="wait_for_bronze_partition",
        python_callable=bronze_partition_is_ready,
        poke_interval=timedelta(minutes=10),
        timeout=timedelta(hours=2),
        # reschedule not poke a 2-hour poke holds a worker slot for 2 hours
        mode="reschedule",
        soft_fail=False,
        doc_md=bronze_partition_is_ready.__doc__,
    )

    rebuild = PythonOperator(
        task_id="rebuild_silver_partition",
        python_callable=run_partition_backfill,
        execution_timeout=timedelta(hours=1),
        doc_md=run_partition_backfill.__doc__,
    )

    partition_gate = PythonOperator(
        task_id="validate_partition",
        python_callable=validate_backfilled_partition,
        doc_md=validate_backfilled_partition.__doc__,
    )

    freshness = PythonOperator(
        task_id="freshness_gate",
        python_callable=run_freshness_gate,
        doc_md=run_freshness_gate.__doc__,
    )

    # The service date comes from the validation task's XCom rather than being
    # recomputed so every task in the run operates on one agreed partition
    SERVICE_DATE = "{{ ti.xcom_pull(task_ids='validate_partition')['service_date'] }}"

    # --vars run_date makes the dbt incremental models process exactly this
    # partition which is what makes a dbt re-run as idempotent as the Spark
    # rebuild above it without it dbt would use its lookback window and a retry
    # would process a different set of rows than the first attempt
    dbt_build = BashOperator(
        task_id="dbt_build_partition",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            "dbt build --target ${DBT_TARGET:-spark} "
            f'--vars \'{{"run_date": "{SERVICE_DATE}"}}\''
        ),
        execution_timeout=timedelta(hours=1),
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt snapshot --target ${{DBT_TARGET:-spark}}",
        execution_timeout=timedelta(minutes=30),
        doc_md="SCD2 capture of per-route service status. Runs after the models "
        "because its source is route_alert_status.",
    )

    wait_for_partition >> rebuild >> partition_gate >> freshness >> dbt_build >> dbt_snapshot
