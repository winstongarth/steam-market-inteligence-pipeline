"""steam_market_batch — the orchestration DAG.

    wait_for_bronze  ->  copy_into_snowflake  ->  run_quality_gates  ->  dbt_seed
         ->  dbt_run_staging  ->  dbt_test_staging  (BLOCKING)
         ->  dbt_snapshot (needs stg_item_attributes from staging — see task-chain comment)
         ->  dbt_run_marts    ->  dbt_test_marts    (BLOCKING)
         ->  detect_anomalies ->  publish_alerts

Runs inside the `airflow-scheduler`/`airflow-webserver` containers (docker-compose.yml),
with the whole project mounted at /opt/project — same reasoning as running Spark in
Docker (docs/DECISIONS.md): keeps this out of Windows-native-support gaps, and matches
how dbt/streaming already run.

## Deliberate simplifications, stated plainly (not hidden)

- **`copy_into_snowflake` is a documented no-op this phase.** Snowflake is deliberately
  deferred until the soak test and unattended-DAG gaps close (docs/DECISIONS.md) — the
  24h soak test hasn't completed. The task exists so the DAG's structure matches the
  intended pipeline shape, and logs clearly that it's skipped rather than silently
  pretending to do something.
- **`wait_for_bronze` checks Bronze has *any* recent-ish data, rather than blocking with
  a real Airflow Sensor's poke/timeout semantics against a continuously-running
  scheduler.** `ingest/scheduler.py` isn't running unattended yet (same 24h-soak-test
  gap) — a true sensor waiting on continuous fresh partitions would just time out in this
  environment. Revisit once the scheduler runs unattended.
- **`detect_anomalies` is a stub.** Anomaly detection hasn't been built yet — this task
  exists so the DAG's shape is complete, and logs explicitly that it's a placeholder
  rather than silently no-op-ing without saying so.

## Gates that actually block

`dbt_test_staging` and `dbt_test_marts` run `dbt test` via BashOperator — dbt's CLI exits
1 on any test failure at `severity: error` (the default), which fails the Airflow task and
halts `dbt_run_marts`/downstream by Airflow's default trigger rules. `run_quality_gates`
(the Great Expectations Bronze suite, quality/expectations/bronze_suite.py) raises
AirflowException on failure for the same reason. Proven with real injected bad data before
this DAG was written — see docs/evidence/quality-gate-failure-great-expectations.txt and
docs/evidence/quality-gate-failure-dbt.txt, and docs/DECISIONS.md for the full account.

`publish_alerts` runs with `trigger_rule=TriggerRule.ALL_DONE` — it must run and report
even when upstream tasks failed, or routing failures to a webhook wouldn't actually
happen on the failure path that matters most.
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

PROJECT_DIR = os.environ.get("STEAM_PROJECT_DIR", "/opt/project")
DBT_DIR = f"{PROJECT_DIR}/dbt"

if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

logger = logging.getLogger("airflow.task.steam_market_batch")

DBT_ENV = {
    **os.environ,
    "S3_ENDPOINT": os.environ.get("S3_ENDPOINT", "minio:9000"),
    "S3_ACCESS_KEY": os.environ.get("S3_ACCESS_KEY", "minioadmin"),
    "S3_SECRET_KEY": os.environ.get("S3_SECRET_KEY", "minioadmin"),
    # Absolute path — see profiles.yml for why a relative path breaks under BashOperator.
    "DUCKDB_PATH": os.environ.get("DUCKDB_PATH", f"{DBT_DIR}/steam_market.duckdb"),
}


def _dbt_cmd(subcommand: str, *args: str) -> str:
    """This dbt version takes --project-dir/--profiles-dir as options of the SUBCOMMAND,
    not global options before it (`dbt snapshot --project-dir X`, not `dbt --project-dir X
    snapshot`) — confirmed via `dbt snapshot --help` inside the container; the global-flag
    form fails with 'No such option --project-dir'. Found empirically, not assumed."""
    return f"dbt {subcommand} --project-dir {DBT_DIR} --profiles-dir {DBT_DIR} {' '.join(args)}".strip()


def _ensure_project_on_path() -> None:
    """Belt-and-braces re-insertion of PROJECT_DIR onto sys.path inside each task
    callable, not just at DAG-file module scope. Found necessary empirically: the
    module-level sys.path.insert (still kept, above) parses fine and imports work fine
    when tested directly (`python3 -c "sys.path.insert(...); import quality"` succeeds in
    the same container), but `airflow dags test` still hit `ModuleNotFoundError: No
    module named 'quality'` inside a PythonOperator callable — Airflow's task execution
    doesn't reliably preserve module-level sys.path mutations into the process/thread
    that actually calls the operator's python_callable. Cheap and idempotent to just
    re-assert it at call time rather than chase Airflow's exact execution model further.
    """
    if PROJECT_DIR not in sys.path:
        sys.path.insert(0, PROJECT_DIR)


def _wait_for_bronze(**context) -> None:
    """Confirms Bronze has data at all. See module docstring for why this isn't a real
    Airflow Sensor with poke/timeout semantics yet."""
    _ensure_project_on_path()
    import duckdb

    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(
        f"""
        SET s3_endpoint='{DBT_ENV["S3_ENDPOINT"]}';
        SET s3_access_key_id='{DBT_ENV["S3_ACCESS_KEY"]}';
        SET s3_secret_access_key='{DBT_ENV["S3_SECRET_KEY"]}';
        SET s3_use_ssl=false;
        SET s3_url_style='path';
        """
    )
    try:
        count = con.execute(
            "SELECT count(*) FROM read_parquet('s3://steam-lake/bronze/**/*.parquet', union_by_name=true)"
        ).fetchone()[0]
    except Exception as exc:
        raise AirflowException(f"Bronze is unreadable or empty: {exc}") from exc

    if count == 0:
        raise AirflowException("Bronze has zero rows — nothing to process")
    logger.info("wait_for_bronze: found %d rows in Bronze", count)
    context["ti"].xcom_push(key="bronze_row_count", value=count)


def _copy_into_snowflake(**context) -> None:
    logger.warning(
        "copy_into_snowflake: SKIPPED. Snowflake is deliberately deferred until the soak "
        "test and unattended-DAG gaps close. The 24h soak test hasn't completed. "
        "See docs/DECISIONS.md."
    )


def _run_quality_gates(**context) -> None:
    _ensure_project_on_path()
    import duckdb
    import pandas as pd

    from quality.expectations.bronze_suite import run_bronze_quality_gates

    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(
        f"""
        SET s3_endpoint='{DBT_ENV["S3_ENDPOINT"]}';
        SET s3_access_key_id='{DBT_ENV["S3_ACCESS_KEY"]}';
        SET s3_secret_access_key='{DBT_ENV["S3_SECRET_KEY"]}';
        SET s3_use_ssl=false;
        SET s3_url_style='path';
        """
    )
    df = con.execute(
        "SELECT * FROM read_parquet('s3://steam-lake/bronze/**/*.parquet', union_by_name=true)"
    ).fetchdf()
    df["observed_at"] = pd.to_datetime(df["observed_at"].astype(str).str.replace("Z", "+00:00"), utc=True)

    result = run_bronze_quality_gates(df)
    context["ti"].xcom_push(key="quality_gate_result", value={"success": result.success, "summary": result.summary})

    if not result.success:
        raise AirflowException(f"Bronze quality gate failed: {result.summary}. Failed: {result.failed_expectations}")
    logger.info("run_quality_gates: %s", result.summary)


def _detect_anomalies(**context) -> None:
    logger.warning(
        "detect_anomalies: STUB. Anomaly detection (z-score/EWMA/volume-spike/crossed-book) "
        "hasn't been built yet — this task exists so the DAG's shape is complete, not to "
        "claim anomaly detection is implemented."
    )


def _publish_alerts(**context) -> None:
    _ensure_project_on_path()
    from quality.alerting import Alert, send_alert

    ti = context["ti"]
    dag_run = context["dag_run"]

    failed_tasks = [ti_.task_id for ti_ in dag_run.get_task_instances() if ti_.state == "failed"]

    if failed_tasks:
        alert = Alert(
            severity="error",
            source="steam_market_batch",
            summary=f"DAG run had {len(failed_tasks)} failed task(s): {', '.join(failed_tasks)}",
            details={"failed_tasks": failed_tasks, "dag_run_id": dag_run.run_id},
        )
    else:
        alert = Alert(
            severity="info",
            source="steam_market_batch",
            summary="DAG run completed with no failed tasks",
            details={"dag_run_id": dag_run.run_id},
        )

    sent = send_alert(alert)
    logger.info("publish_alerts: alert sent=%s, severity=%s, summary=%s", sent, alert.severity, alert.summary)


default_args = {
    "owner": "steam-market-pipeline",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="steam_market_batch",
    description="Bronze -> quality gates -> dbt staging/marts -> anomaly detection -> alerts",
    default_args=default_args,
    schedule=None,  # triggered manually / via `airflow dags test` for now — see docs/METRICS.md
    start_date=datetime(2026, 8, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["steam-market", "phase4"],
) as dag:
    wait_for_bronze = PythonOperator(
        task_id="wait_for_bronze",
        python_callable=_wait_for_bronze,
    )

    copy_into_snowflake = PythonOperator(
        task_id="copy_into_snowflake",
        python_callable=_copy_into_snowflake,
    )

    run_quality_gates = PythonOperator(
        task_id="run_quality_gates",
        python_callable=_run_quality_gates,
    )

    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=_dbt_cmd("seed"),
        env=DBT_ENV,
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=_dbt_cmd("snapshot"),
        env=DBT_ENV,
    )

    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=_dbt_cmd("run", "--select", "path:models/staging", "path:models/intermediate"),
        env=DBT_ENV,
    )

    dbt_test_staging = BashOperator(
        task_id="dbt_test_staging",
        bash_command=_dbt_cmd("test", "--select", "path:models/staging", "path:models/intermediate"),
        env=DBT_ENV,
    )

    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=_dbt_cmd("run", "--select", "path:models/marts"),
        env=DBT_ENV,
    )

    dbt_test_marts = BashOperator(
        task_id="dbt_test_marts",
        bash_command=_dbt_cmd("test", "--select", "path:models/marts"),
        env=DBT_ENV,
    )

    detect_anomalies = PythonOperator(
        task_id="detect_anomalies",
        python_callable=_detect_anomalies,
    )

    publish_alerts = PythonOperator(
        task_id="publish_alerts",
        python_callable=_publish_alerts,
        trigger_rule=TriggerRule.ALL_DONE,  # must run even if upstream tasks failed
    )

    (
        wait_for_bronze
        >> copy_into_snowflake
        >> run_quality_gates
        >> dbt_seed
        >> dbt_run_staging
        >> dbt_test_staging
        # dbt_snapshot depends on stg_item_attributes (a staging model), so it must run
        # after dbt_run_staging, not before — dim_item (in dbt_run_marts) then depends on
        # the snapshot. Same ordering constraint hit manually earlier this session
        # (docs/DECISIONS.md's dbt-duckdb setup), fixed here in the DAG itself.
        >> dbt_snapshot
        >> dbt_run_marts
        >> dbt_test_marts
        >> detect_anomalies
        >> publish_alerts
    )
