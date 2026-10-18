"""streaming supervision

is the kafka to iceberg pipeline alive
runs every 15 minutes

check_streaming_queries reads the iceberg commit ledger and fails if a query has
not committed inside STREAM_MAX_IDLE_MINUTES
freshness_gate runs the great expectations sla gate
drain_available_now does a short trigger availableNow run that drains whatever is
in kafka and exits

the long running streaming queries are their own process and not an airflow task
docker compose up spark-streaming locally or an emr step or a kubernetes
deployment in production
airflow is a scheduler and not a process supervisor and a spark-submit held open
for days blocks a worker slot and dies with every scheduler restart
the availableNow task is the safety net
if that process is down this still advances the same checkpoint every 15 minutes
so the lake falls behind by minutes instead of by the whole outage

max_active_runs 1 and catchup off because two runs would both try to advance the
same checkpoint which spark refuses with a confusing error
"""

from __future__ import annotations

import sys
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

# The project package is mounted at /opt/project/src in the Airflow image the
# plugin path is handled by Airflow itself
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from nyc_transit_airflow.callables import check_streaming_queries, run_freshness_gate  # noqa: E402

DEFAULT_ARGS = {
    "owner": "mohab",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    # Exponential backoff a transient Spark/Kafka failure clears in seconds,
    # but a genuine outage should not be retried every two minutes for an hour
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=20),
    "depends_on_past": False,
    "email_on_failure": False,
}

with DAG(
    dag_id="gtfs_streaming_supervisor",
    description="Supervise the Kafka -> Iceberg streaming queries and enforce freshness SLAs.",
    default_args=DEFAULT_ARGS,
    schedule=timedelta(minutes=15),
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    # A supervision run that takes longer than its interval means the cluster
    # is in trouble killing it keeps the scheduler from queueing a backlog of
    # identical probes
    dagrun_timeout=timedelta(minutes=14),
    tags=["streaming", "monitoring", "iceberg"],
    doc_md=__doc__,
) as dag:
    start = EmptyOperator(task_id="start")

    query_health = PythonOperator(
        task_id="check_streaming_queries",
        python_callable=check_streaming_queries,
        doc_md=check_streaming_queries.__doc__,
    )

    # availableNow drains the topic and exits sharing the long-running job's
    # checkpoint so this is a catch-up and never a competing consumer
    drain = BashOperator(
        task_id="drain_available_now",
        bash_command=(
            "python -m nyc_transit.streaming.jobs "
            "--once --starting-offsets latest --create-tables"
        ),
        execution_timeout=timedelta(minutes=10),
        doc_md="trigger(availableNow): advance the shared checkpoint if the "
        "long-running streaming process is down.",
    )

    freshness = PythonOperator(
        task_id="freshness_gate",
        python_callable=run_freshness_gate,
        doc_md=run_freshness_gate.__doc__,
    )

    done = EmptyOperator(task_id="done")

    # The drain runs even when the health check failed (that is when
    # it is most useful) but the freshness gate waits for the drain so it
    # measures the state after catch-up rather than before it
    start >> query_health
    start >> drain >> freshness >> done
    query_health >> done
