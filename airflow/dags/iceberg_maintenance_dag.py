"""weekly iceberg maintenance

streaming writes with merge on read deletes build up three kinds of debt and all
of it is required maintenance
without it the tables get slower every day until queries time out

rewrite_data_files compacts the small files a 30 second micro batch cadence
produces into files near write.target-file-size-bytes
rewrite_position_delete_files folds the delete files every merge writes back into
the data since read cost grows with their number
expire_snapshots drops snapshots past the 7 day retention the backfills rollback
path needs
remove_orphan_files cleans up files left by failed commits with a conservative
older_than of 3 days and not hours because deleting a live in flight file is the
one maintenance action that destroys data

sunday 05:00 utc is the lowest mta service volume so compaction competes least
with the streaming writers
"""

from __future__ import annotations

import sys
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

DEFAULT_ARGS = {
    "owner": "mohab",
    "retries": 1,
    "retry_delay": timedelta(minutes=15),
    "email_on_failure": False,
}


def run_maintenance(**context) -> dict:
    """call the iceberg stored procedures for every managed table"""
    from nyc_transit.config import load_settings
    from nyc_transit.streaming.ddl import ALL_SPECS
    from nyc_transit.streaming.session import build_spark_session

    settings = load_settings()
    catalog = settings.lakehouse.catalog_name
    results: dict[str, dict] = {}

    spark = build_spark_session(
        settings.lakehouse, app_name="nyc-transit-maintenance", with_kafka=False
    )
    try:
        for spec in ALL_SPECS:
            fqn = f"{catalog}.{spec.namespace}.{spec.name}"
            table_results: dict[str, str] = {}
            procedures = {
                "rewrite_data_files": (
                    f"CALL {catalog}.system.rewrite_data_files("
                    f"table => '{spec.namespace}.{spec.name}', "
                    # Only touch partitions with enough small files to be worth
                    # rewriting otherwise every run rewrites every partition
                    f"options => map('min-input-files', '5', 'target-file-size-bytes', "
                    f"'134217728'))"
                ),
                "rewrite_position_delete_files": (
                    f"CALL {catalog}.system.rewrite_position_delete_files("
                    f"table => '{spec.namespace}.{spec.name}')"
                ),
                "expire_snapshots": (
                    f"CALL {catalog}.system.expire_snapshots("
                    f"table => '{spec.namespace}.{spec.name}', "
                    f"older_than => TIMESTAMP '{{{{ macros.ds_add(ds, -7) }}}} 00:00:00', "
                    f"retain_last => 10)"
                ),
                "remove_orphan_files": (
                    f"CALL {catalog}.system.remove_orphan_files("
                    f"table => '{spec.namespace}.{spec.name}', "
                    f"older_than => TIMESTAMP '{{{{ macros.ds_add(ds, -3) }}}} 00:00:00')"
                ),
            }
            for name, sql in procedures.items():
                rendered = context["task"].render_template(sql, context)
                try:
                    spark.sql(rendered)
                    table_results[name] = "ok"
                except Exception as exc:  # noqa: BLE001
                    # One procedure failing on one table must not skip
                    # maintenance for the rest which is how a cluster ends up
                    # with one pathological table and no compaction anywhere
                    table_results[name] = f"failed: {exc}"
            results[fqn] = table_results
    finally:
        spark.stop()

    failures = {
        table: {k: v for k, v in res.items() if v != "ok"}
        for table, res in results.items()
        if any(v != "ok" for v in res.values())
    }
    print("maintenance results:", results)
    if failures:
        raise RuntimeError(f"Iceberg maintenance failures: {failures}")
    return results


with DAG(
    dag_id="iceberg_maintenance",
    description="Weekly compaction, delete-file rewrite, snapshot expiry and orphan cleanup.",
    default_args=DEFAULT_ARGS,
    schedule="0 5 * * 0",
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=6),
    tags=["maintenance", "iceberg"],
    doc_md=__doc__,
) as dag:
    PythonOperator(
        task_id="run_iceberg_maintenance",
        python_callable=run_maintenance,
        execution_timeout=timedelta(hours=5),
        doc_md=run_maintenance.__doc__,
    )
