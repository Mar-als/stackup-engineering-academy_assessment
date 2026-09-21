"""
StackUp Engineering Academy — Data Engineering Assessment
Pillar 3 — Big Data Processing
Author: maryamalshehhi

Task covered:
  Task 3.3 -> presight_etl_pipeline DAG (extract -> DQ gate -> transform ->
              load -> report)

NOT LIVE-VERIFIED IN THIS SESSION: Apache Airflow does not run natively on
Windows (it requires Docker or WSL — see docs/setup/DOCKER_SETUP.md), and
neither is available in this environment. This DAG has been written
carefully against the Airflow 2.x API and reuses the REAL, already-tested
pipeline functions from Pillar 1 and Pillar 2, but it has not been loaded
into a live Airflow scheduler or seen in the Airflow UI.

To actually run it once Docker is available:
  1. `docker compose up -d` (repo root) to start Airflow.
  2. Copy/symlink this file into the Airflow `dags/` folder Docker mounts
     (see docker-compose.yml's airflow volume mapping).
  3. Open http://localhost:8081 (admin/admin) and confirm `presight_etl_pipeline`
     appears with schedule "0 6 * * *" in the Asia/Dubai timezone.
  4. `airflow dags trigger presight_etl_pipeline` or trigger from the UI.

DQ gate note (Task 3.3d): the DQ gate reuses the REAL Task 4.3 framework
(solutions/submissions/maryamalshehhi/04_infrastructure/dq_framework.py —
DQ_CONFIG + run_data_quality_checks()) rather than a second, simplified
implementation, so the two pillars don't drift out of sync with each other.
"""

import os
import json
import logging
from datetime import timedelta

import pendulum
import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))
DATA_DIR = os.path.join(BASE_DIR, "datasets")
OUTPUT_DIR = os.path.join(BASE_DIR, "outputs")

# Reuse the REAL Pillar 1 / Pillar 2 / Pillar 4 pipeline functions instead of
# reimplementing cleaning/DQ logic a third time. Loaded explicitly by file
# path (not via sys.path + `import etl_pipeline`) because Pillar 1 and
# Pillar 2 both name their module "etl_pipeline" — two sys.path.insert(0, ..)
# calls would leave whichever was inserted LAST resolving first, silently
# shadowing the other module's `etl_pipeline` on every subsequent import.
import importlib.util as _ilu


def _load_module(name, path):
    spec = _ilu.spec_from_file_location(name, path)
    mod = _ilu.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


_SUBMISSIONS_DIR = os.path.join(BASE_DIR, "solutions", "submissions", "maryamalshehhi")

_pillar1 = _load_module("etl_pipeline_pillar1", os.path.join(_SUBMISSIONS_DIR, "01_foundations", "etl_pipeline.py"))
load_projects = _pillar1.load_projects
transform_projects = _pillar1.transform_projects
load_employees = _pillar1.load_employees
clean_employees = _pillar1.clean_employees

_pillar2 = _load_module("etl_pipeline_pillar2", os.path.join(_SUBMISSIONS_DIR, "02_sql_and_viz", "etl_pipeline.py"))
load_transactions = _pillar2.load_transactions
enrich_transactions = _pillar2.enrich_transactions

_pillar4 = _load_module("dq_framework_pillar4", os.path.join(_SUBMISSIONS_DIR, "04_infrastructure", "dq_framework.py"))
run_data_quality_checks = _pillar4.run_data_quality_checks
DQ_CONFIG = _pillar4.DQ_CONFIG


# ==============================================================================
# TASK 3.3a — Default args
# ==============================================================================

default_args = {
    "owner": "maryamalshehhi",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "depends_on_past": False,
}


# ==============================================================================
# TASK 3.3b — Task functions
# ==============================================================================

def task_extract_projects(**context):
    ti = context["ti"]
    df = load_projects(os.path.join(DATA_DIR, "projects.csv"))
    ti.xcom_push(key="projects_raw_count", value=len(df))
    logger.info("Extracted %d raw project rows", len(df))
    return f"Extracted {len(df)} project rows"


def task_extract_employees(**context):
    ti = context["ti"]
    df = load_employees(os.path.join(DATA_DIR, "employees.csv"))
    ti.xcom_push(key="employees_raw_count", value=len(df))
    logger.info("Extracted %d raw employee rows", len(df))
    return f"Extracted {len(df)} employee rows"


def task_extract_transactions(**context):
    ti = context["ti"]
    df = load_transactions(os.path.join(DATA_DIR, "transactions.json"))
    ti.xcom_push(key="transactions_raw_count", value=len(df))
    logger.info("Extracted %d raw transaction rows", len(df))
    return f"Extracted {len(df)} transaction rows"


def task_validate_data_quality(**context):
    """
    DQ GATE: reloads all three raw datasets fresh (extract tasks only pushed
    counts to XCom, not the DataFrames themselves — XCom is for small
    values, not multi-MB DataFrames), runs the full Task 4.3 framework on
    each, and fails the DAG run if the primary-key column's completeness
    drops below 80% on any dataset (Task 3.3d's explicit gate condition).
    Downstream tasks never run if this raises, because Airflow stops
    propagating through a failed upstream dependency by default.
    """
    ti = context["ti"]

    projects = load_projects(os.path.join(DATA_DIR, "projects.csv"))
    employees = load_employees(os.path.join(DATA_DIR, "employees.csv"))
    transactions = load_transactions(os.path.join(DATA_DIR, "transactions.json"))
    all_dfs = {"projects": projects, "employees": employees, "transactions": transactions}

    dq_reports = {
        name: run_data_quality_checks(df, name, DQ_CONFIG[name], all_dataframes=all_dfs)
        for name, df in all_dfs.items()
    }

    failures = []
    for name, report in dq_reports.items():
        pk_col = DQ_CONFIG[name]["pk_columns"][0]
        pk_completeness_pct = report["results"]["completeness"]["details"].get(pk_col, 0.0) * 100
        if pk_completeness_pct < 80.0:
            failures.append(f"{name}: {pk_col} completeness {pk_completeness_pct:.1f}% < 80%")
    if failures:
        raise ValueError("Data quality gate failed: " + "; ".join(failures))

    dq_summary = {
        name: {"checks_passed": r["checks_passed"], "checks_failed": r["checks_failed"]}
        for name, r in dq_reports.items()
    }
    ti.xcom_push(key="dq_results", value=dq_summary)
    logger.info("DQ gate passed: %s", dq_summary)
    return "DQ checks passed"


def task_transform_and_enrich(**context):
    ti = context["ti"]

    projects = load_projects(os.path.join(DATA_DIR, "projects.csv"))
    clean_projects = transform_projects(projects)

    employees = load_employees(os.path.join(DATA_DIR, "employees.csv"))
    clean_emp = clean_employees(employees)

    transactions = load_transactions(os.path.join(DATA_DIR, "transactions.json"))
    enriched_txn = enrich_transactions(transactions, clean_projects, clean_emp)

    ti.xcom_push(key="projects_clean_count", value=len(clean_projects))
    ti.xcom_push(key="employees_clean_count", value=len(clean_emp))
    ti.xcom_push(key="transactions_clean_count", value=len(enriched_txn))

    # Stash to a fixed scratch path so task_load_to_output (a separate
    # process in production Airflow) can pick them back up — XCom is for
    # small values like counts, not for passing multi-MB DataFrames between
    # tasks.
    scratch_dir = os.path.join(OUTPUT_DIR, "_dag_scratch")
    os.makedirs(scratch_dir, exist_ok=True)
    clean_projects.to_parquet(os.path.join(scratch_dir, "projects.parquet"))
    clean_emp.to_parquet(os.path.join(scratch_dir, "employees.parquet"))
    enriched_txn.to_parquet(os.path.join(scratch_dir, "transactions.parquet"))

    logger.info("Transformed: %d projects, %d employees, %d transactions",
                len(clean_projects), len(clean_emp), len(enriched_txn))
    return "Transform complete"


def task_load_to_output(**context):
    scratch_dir = os.path.join(OUTPUT_DIR, "_dag_scratch")
    clean_projects = pd.read_parquet(os.path.join(scratch_dir, "projects.parquet"))
    clean_emp = pd.read_parquet(os.path.join(scratch_dir, "employees.parquet"))
    enriched_txn = pd.read_parquet(os.path.join(scratch_dir, "transactions.parquet"))

    paths = {
        "projects": os.path.join(OUTPUT_DIR, "projects_clean.csv"),
        "employees": os.path.join(OUTPUT_DIR, "employees_clean.csv"),
        "transactions": os.path.join(OUTPUT_DIR, "transactions_clean.csv"),
    }
    clean_projects.to_csv(paths["projects"], index=False)
    clean_emp.to_csv(paths["employees"], index=False)
    enriched_txn.to_csv(paths["transactions"], index=False)

    for name, path in paths.items():
        logger.info("Wrote %s -> %s", name, path)
    return paths


def task_generate_pipeline_report(**context):
    ti = context["ti"]
    execution_date = context["execution_date"]

    projects_raw = ti.xcom_pull(task_ids="extract_projects", key="projects_raw_count")
    employees_raw = ti.xcom_pull(task_ids="extract_employees", key="employees_raw_count")
    transactions_raw = ti.xcom_pull(task_ids="extract_transactions", key="transactions_raw_count")
    dq_results = ti.xcom_pull(task_ids="validate_data_quality", key="dq_results")
    projects_clean = ti.xcom_pull(task_ids="transform_and_enrich", key="projects_clean_count")
    employees_clean = ti.xcom_pull(task_ids="transform_and_enrich", key="employees_clean_count")
    transactions_clean = ti.xcom_pull(task_ids="transform_and_enrich", key="transactions_clean_count")

    report_path = os.path.join(OUTPUT_DIR, f"pipeline_report_{execution_date.date()}.txt")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("Presight ETL Pipeline — DAG Run Report\n")
        f.write("=" * 50 + "\n")
        f.write(f"DAG run date: {execution_date.isoformat()}\n\n")

        f.write("Row counts (raw -> clean)\n")
        f.write("-" * 50 + "\n")
        f.write(f"projects:     {projects_raw:>7} -> {projects_clean:>7}\n")
        f.write(f"employees:    {employees_raw:>7} -> {employees_clean:>7}\n")
        f.write(f"transactions: {transactions_raw:>7} -> {transactions_clean:>7}\n\n")

        f.write("Data quality gate results\n")
        f.write("-" * 50 + "\n")
        f.write(json.dumps(dq_results, indent=2) + "\n\n")

        f.write("Files written\n")
        f.write("-" * 50 + "\n")
        for name in ("projects_clean.csv", "employees_clean.csv", "transactions_clean.csv"):
            f.write(f"  outputs/{name}\n")

    logger.info("Pipeline report written to %s", report_path)
    return report_path


# ==============================================================================
# TASK 3.3a — DAG definition
# ==============================================================================

with DAG(
    dag_id="presight_etl_pipeline",
    default_args=default_args,
    description="Daily ETL pipeline for Presight project management data",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Dubai"),
    schedule_interval="0 6 * * *",  # 06:00 in the DAG's Asia/Dubai timezone (UTC+4)
    catchup=False,
    max_active_runs=1,
    tags=["presight", "etl", "assessment"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    extract_projects = PythonOperator(task_id="extract_projects", python_callable=task_extract_projects)
    extract_employees = PythonOperator(task_id="extract_employees", python_callable=task_extract_employees)
    extract_transactions = PythonOperator(task_id="extract_transactions", python_callable=task_extract_transactions)

    validate_dq = PythonOperator(task_id="validate_data_quality", python_callable=task_validate_data_quality)

    transform_enrich = PythonOperator(task_id="transform_and_enrich", python_callable=task_transform_and_enrich)
    load_output = PythonOperator(task_id="load_to_output", python_callable=task_load_to_output)
    pipeline_report = PythonOperator(task_id="generate_pipeline_report", python_callable=task_generate_pipeline_report)

    start >> [extract_projects, extract_employees, extract_transactions] >> validate_dq
    validate_dq >> transform_enrich >> load_output >> pipeline_report >> end
