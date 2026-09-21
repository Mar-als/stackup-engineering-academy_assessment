"""
StackUp Engineering Academy — Data Engineering Assessment
Pillar 2 — SQL & Data Visualization
Author: maryamalshehhi

Task covered:
  Task 2.2 -> load_transactions() / enrich_transactions() / write_outputs()

This pipeline assumes Pillar 1 has already been run — it reads the cleaned
outputs/projects_clean.csv and outputs/employees_clean.csv rather than
re-deriving them, since Task 2.2 is scoped to the transactions ETL only.

Run:
  python solutions/submissions/maryamalshehhi/02_sql_and_viz/etl_pipeline.py

Output:
  outputs/transactions_clean.csv
  outputs/pipeline_summary.txt
"""

import os
import time
import logging
from datetime import datetime

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))
DATA_DIR = os.path.join(BASE_DIR, "datasets")
OUTPUT_DIR = os.path.join(BASE_DIR, "outputs")
os.makedirs(OUTPUT_DIR, exist_ok=True)


# ==============================================================================
# TASK 2.2 — Load transactions.json
# ==============================================================================

def load_transactions(filepath: str) -> pd.DataFrame:
    """
    Load transactions.json (already a flat list of records — pd.read_json
    handles the JSON->tabular flattening directly, no nested structures to
    unpack) and parse transaction_date as a proper date type.

    Null-handling decisions (documented, not silently applied):
      - amount: ~740 rows have a null amount. We do NOT drop these rows —
        a transaction with an unknown amount is still a real transaction
        (e.g. an invoice awaiting finance entry) and dropping it would
        understate transaction_count in downstream aggregates. We keep NaN
        here in load_transactions() and defer the "nulls -> 0.0" policy to
        enrich_transactions()'s amount_aed column, so the raw/clean
        distinction is preserved: `amount` = what the source actually said,
        `amount_aed` = the analytics-ready version.
      - approved_by: ~2,444 rows have no approver on file. This is left as
        NaN (not imputed to a placeholder employee) because "unknown
        approver" is itself meaningful information for the business
        (is_approved = False downstream) — inventing an approver would
        corrupt the audit trail.
    """
    logger.info("Loading transactions data from %s", filepath)
    df = pd.read_json(filepath)
    logger.info("Loaded %d transaction rows", len(df))

    df["transaction_date"] = pd.to_datetime(df["transaction_date"], errors="coerce")
    bad_dates = df["transaction_date"].isna().sum()
    if bad_dates:
        logger.warning("%d transaction_date values failed to parse", bad_dates)

    null_amount = df["amount"].isna().sum()
    null_approved_by = df["approved_by"].isna().sum()
    logger.info("Null amount: %d (%.1f%%) — kept as NaN, see amount_aed for the zero-filled version",
                null_amount, 100 * null_amount / len(df))
    logger.info("Null approved_by: %d (%.1f%%) — kept as NaN, reflected in is_approved",
                null_approved_by, 100 * null_approved_by / len(df))

    return df


# ==============================================================================
# TASK 2.2 — Enrich transactions with project & employee context
# ==============================================================================

def enrich_transactions(
    transactions: pd.DataFrame,
    projects: pd.DataFrame,
    employees: pd.DataFrame,
) -> pd.DataFrame:
    """
    Enrich transactions with project_name/department and approver full_name.

    Both merges are left joins keyed on the natural id columns, with explicit
    on=/how= per the task spec. projects and employees are each unique on
    their id column (verified below) so a left join cannot fan out rows —
    the enriched frame has exactly len(transactions) rows in and out.
    """
    logger.info("Enriching transactions...")
    before_rows = len(transactions)

    # Guard against silent row duplication: if either dimension had duplicate
    # keys, a left merge would multiply transaction rows.
    dup_projects = projects["project_id"].duplicated().sum()
    dup_employees = employees["employee_id"].duplicated().sum()
    if dup_projects or dup_employees:
        raise ValueError(
            f"Cannot safely merge: {dup_projects} duplicate project_id, "
            f"{dup_employees} duplicate employee_id would fan out transaction rows."
        )

    enriched = transactions.merge(
        projects[["project_id", "project_name", "department"]],
        on="project_id",
        how="left",
    )
    enriched = enriched.merge(
        employees[["employee_id", "full_name"]].rename(
            columns={"employee_id": "approved_by", "full_name": "approver_name"}
        ),
        on="approved_by",
        how="left",
    )

    assert len(enriched) == before_rows, "Row count changed during enrichment merge — join fanned out."

    enriched["is_approved"] = enriched["approved_by"].notna()
    enriched["amount_aed"] = enriched["amount"].astype(float).fillna(0.0)
    enriched["transaction_year_month"] = enriched["transaction_date"].dt.to_period("M").astype(str)

    unmatched_projects = enriched["project_name"].isna().sum()
    if unmatched_projects:
        logger.warning("%d transactions reference a project_id not found in projects_clean.csv", unmatched_projects)

    logger.info("Enrichment complete: %d rows, %d columns", *enriched.shape)
    return enriched


# ==============================================================================
# TASK 2.2 — Write outputs + pipeline summary
# ==============================================================================

def write_outputs(
    projects: pd.DataFrame,
    employees: pd.DataFrame,
    transactions: pd.DataFrame,
    row_counts: dict,
    elapsed_seconds: float,
):
    """
    Write the enriched transactions CSV and a pipeline_summary.txt covering
    run timestamp, row counts, data quality decisions, and execution time.

    projects_clean.csv / employees_clean.csv are Pillar 1 deliverables and
    are not rewritten here — only transactions_clean.csv is new in Task 2.2.
    """
    logger.info("Writing outputs...")

    transactions_path = os.path.join(OUTPUT_DIR, "transactions_clean.csv")
    transactions.to_csv(transactions_path, index=False)
    logger.info("Wrote %s (%d rows x %d cols)", transactions_path, *transactions.shape)

    summary_path = os.path.join(OUTPUT_DIR, "pipeline_summary.txt")
    with open(summary_path, "w", encoding="utf-8") as f:
        f.write("Presight ETL Pipeline — Run Summary\n")
        f.write("=" * 50 + "\n")
        f.write(f"Run timestamp: {datetime.now().isoformat(timespec='seconds')}\n")
        f.write(f"Pipeline execution time: {elapsed_seconds:.2f} seconds\n\n")

        f.write("Row counts\n")
        f.write("-" * 50 + "\n")
        for name, (before, after) in row_counts.items():
            f.write(f"{name:<20} before: {before:>7}   after: {after:>7}\n")

        f.write("\nData quality decisions\n")
        f.write("-" * 50 + "\n")
        f.write("- transactions.amount: ~740 nulls kept as NaN in the raw column; "
                "amount_aed zero-fills them so aggregates don't break, while the "
                "raw amount column still reflects 'unknown', not 'zero-cost'.\n")
        f.write("- transactions.approved_by: ~2,444 nulls kept as-is (no invented "
                "approver); is_approved=False flags these for finance review "
                "instead of silently attributing them to someone.\n")
        f.write("- project/employee merges are left joins on verified-unique keys, "
                "so the merge cannot duplicate transaction rows (asserted in code).\n")

    logger.info("Wrote %s", summary_path)


# ==============================================================================
# PIPELINE ENTRY POINT
# ==============================================================================

def run_pipeline():
    start = time.perf_counter()
    logger.info("=" * 60)
    logger.info("Starting Pillar 2 pipeline (Task 2.2 — transactions ETL)")
    logger.info("=" * 60)

    projects_path = os.path.join(OUTPUT_DIR, "projects_clean.csv")
    employees_path = os.path.join(OUTPUT_DIR, "employees_clean.csv")
    if not (os.path.exists(projects_path) and os.path.exists(employees_path)):
        raise FileNotFoundError(
            "outputs/projects_clean.csv and outputs/employees_clean.csv are required. "
            "Run Pillar 1's etl_pipeline.py first."
        )

    projects = pd.read_csv(projects_path)
    employees = pd.read_csv(employees_path)

    raw_transactions = load_transactions(os.path.join(DATA_DIR, "transactions.json"))
    enriched_txn = enrich_transactions(raw_transactions, projects, employees)

    elapsed = time.perf_counter() - start
    logger.info("Pipeline processed %d transactions in %.2f seconds", len(enriched_txn), elapsed)

    row_counts = {
        "transactions": (len(raw_transactions), len(enriched_txn)),
        "projects (reference)": (len(projects), len(projects)),
        "employees (reference)": (len(employees), len(employees)),
    }
    write_outputs(projects, employees, enriched_txn, row_counts, elapsed)

    logger.info("Pipeline complete.")


if __name__ == "__main__":
    run_pipeline()
