"""
StackUp Engineering Academy — Data Engineering Assessment
Pillar 1 — Foundations
Author: maryamalshehhi

Tasks covered:
  Task 1.1 -> load_projects() / transform_projects()
  Task 1.3 -> load_employees() / clean_employees()

Run:
  python solutions/submissions/maryamalshehhi/01_foundations/etl_pipeline.py

Output:
  outputs/projects_clean.csv
  outputs/employees_clean.csv
"""

import os
import logging
from datetime import datetime

import numpy as np
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)

# This file lives at solutions/submissions/<name>/01_foundations/etl_pipeline.py
# -> parents[4] is the repo root.
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))
DATA_DIR = os.path.join(BASE_DIR, "datasets")
OUTPUT_DIR = os.path.join(BASE_DIR, "outputs")
os.makedirs(OUTPUT_DIR, exist_ok=True)


# ==============================================================================
# TASK 1.1 — Load and transform projects.csv
# ==============================================================================

def load_projects(filepath: str) -> pd.DataFrame:
    """
    Load projects.csv, fix dtypes, and add the three derived columns that only
    need the raw source columns (budget_variance, is_over_budget, duration_days).
    """
    logger.info("Loading projects data from %s", filepath)
    df = pd.read_csv(filepath)
    logger.info("Loaded %d project rows", len(df))

    # Parse dates. Many "Not Started" projects legitimately have no start/end
    # date yet -> errors='coerce' turns those blanks into NaT instead of raising.
    df["start_date"] = pd.to_datetime(df["start_date"], errors="coerce")
    df["end_date"] = pd.to_datetime(df["end_date"], errors="coerce")

    # budget_variance: NaN when either side is missing (can't be computed yet —
    # we don't want to claim "0 variance" for a project with no budget on file).
    df["budget_variance"] = df["actual_cost"] - df["budget"]

    # is_over_budget: only True when both values are known and actual > budget.
    # Rows with a missing budget or actual_cost can't be evaluated, so they are
    # False rather than NaN — this is a boolean column, not a tri-state one.
    df["is_over_budget"] = (df["actual_cost"] > df["budget"]).fillna(False)

    # duration_days: only when both dates exist.
    df["duration_days"] = (df["end_date"] - df["start_date"]).dt.days

    return df


def transform_projects(df: pd.DataFrame) -> pd.DataFrame:
    """
    Business-logic transforms: status cleanup, status_category, budget_utilisation_pct,
    null handling on budget/actual_cost, and the combined risk_level.
    """
    logger.info("Transforming projects data...")

    # Standardise status text (defends against stray whitespace/casing even
    # though this dataset's values already look clean).
    df["status"] = df["status"].astype(str).str.strip().str.title()

    status_map = {
        "In Progress": "Active",
        "Completed": "Closed",
        "Not Started": "Pending",
        "On Hold": "Pending",
    }
    df["status_category"] = df["status"].map(status_map)
    unmapped = df["status_category"].isna().sum()
    if unmapped:
        logger.warning("%d rows have a status not in the known mapping: %s",
                        unmapped, df.loc[df["status_category"].isna(), "status"].unique())
        df["status_category"] = df["status_category"].fillna("Pending")

    # budget_utilisation_pct computed BEFORE nulls are zero-filled, so a
    # missing budget correctly yields NaN (undefined) rather than div-by-zero
    # or a misleading 0%/inf value once budget is later coerced to 0.
    df["budget_utilisation_pct"] = np.where(
        df["budget"] > 0,
        df["actual_cost"] / df["budget"] * 100,
        np.nan,
    )

    # Null budget/actual_cost -> 0, per spec. Done after the derived metrics
    # above so those metrics keep their "unknown" (NaN) semantics instead of
    # silently becoming "on budget".
    null_budget = df["budget"].isna().sum()
    null_actual = df["actual_cost"].isna().sum()
    logger.info("Filling %d null budget and %d null actual_cost values with 0", null_budget, null_actual)
    df["budget"] = df["budget"].fillna(0)
    df["actual_cost"] = df["actual_cost"].fillna(0)

    # risk_level: High if priority=Critical OR over budget; Medium if priority=High
    # OR utilisation > 90%; Low otherwise. Order matters — High is checked first.
    high_risk = (df["priority"] == "Critical") | (df["is_over_budget"])
    medium_risk = (df["priority"] == "High") | (df["budget_utilisation_pct"] > 90)
    df["risk_level"] = np.select(
        [high_risk, medium_risk],
        ["High", "Medium"],
        default="Low",
    )

    logger.info("risk_level distribution:\n%s", df["risk_level"].value_counts().to_string())
    return df


# ==============================================================================
# TASK 1.3 — Data quality issues in employees.csv
# ==============================================================================

def load_employees(filepath: str) -> pd.DataFrame:
    """Load employees.csv and log a null-count summary (no fixes here)."""
    logger.info("Loading employees data from %s", filepath)
    df = pd.read_csv(filepath)
    logger.info("Loaded %d employee rows", len(df))
    logger.info("Null counts per column:\n%s", df.isna().sum().to_string())
    return df


def clean_employees(df: pd.DataFrame) -> pd.DataFrame:
    """
    Detect and fix data quality issues in employees.csv. Every check is
    vectorised (boolean masks / aggregations) — no .iterrows().
    """
    logger.info("Cleaning employees data...")
    df = df.copy()
    quality_summary = {}

    # ------------------------------------------------------------------
    # Issue 1 — Missing values (email)
    # A handful of rows have a null/empty email. Real system exports don't
    # invent an email, so we build a deterministic placeholder from the
    # employee_id (keeps the column unique instead of piling every missing
    # row onto one shared "unknown@presight.ai" value, which would silently
    # break any future email-uniqueness check).
    # ------------------------------------------------------------------
    missing_email = df["email"].isna() | (df["email"].astype(str).str.strip() == "")
    quality_summary["missing_email"] = int(missing_email.sum())
    logger.info("Missing emails: %d", missing_email.sum())
    df.loc[missing_email, "email"] = (
        "missing." + df.loc[missing_email, "employee_id"].str.lower() + "@presight.ai"
    )

    # Generic completeness sweep across the remaining required columns, in
    # case other columns also carry nulls/blank strings.
    required_cols = ["full_name", "department", "role", "level", "manager_id", "region", "status"]
    for col in required_cols:
        missing = df[col].isna() | (df[col].astype(str).str.strip() == "")
        if missing.any():
            quality_summary[f"missing_{col}"] = int(missing.sum())
            logger.info("Missing %s: %d", col, missing.sum())
            df.loc[missing, col] = "Unknown"

    # ------------------------------------------------------------------
    # Issue 2 — Invalid date formats in hire_date
    # Some rows contain sentinel garbage like "-999" instead of a real date.
    # errors="coerce" turns anything unparsable into NaT so we can detect it
    # with a boolean mask rather than a per-row try/except.
    # ------------------------------------------------------------------
    original_hire_date = df["hire_date"].astype(str)
    parsed_hire_date = pd.to_datetime(df["hire_date"], errors="coerce")
    invalid_format = parsed_hire_date.isna() & original_hire_date.str.strip().ne("") & original_hire_date.str.lower().ne("nan")
    quality_summary["invalid_hire_date_format"] = int(invalid_format.sum())
    logger.info("Invalid hire_date formats (unparsable): %d", invalid_format.sum())

    # ------------------------------------------------------------------
    # Issue 3 — Implausible dates
    # A date can parse successfully (e.g. year 99999 does NOT fit in pandas'
    # Timestamp range and also comes back as NaT from to_datetime, so it is
    # already caught above) but we additionally guard against any hire_date
    # outside a sane business range: before the company could plausibly have
    # existed, or in the future.
    # ------------------------------------------------------------------
    today = pd.Timestamp(datetime.now().date())
    earliest_plausible = pd.Timestamp("1990-01-01")
    implausible = parsed_hire_date.notna() & ((parsed_hire_date < earliest_plausible) | (parsed_hire_date > today))
    quality_summary["implausible_hire_date"] = int(implausible.sum())
    logger.info("Implausible hire_date values (outside 1990-01-01..today): %d", implausible.sum())

    # Fix: rows with an invalid/implausible hire_date get NaT, then imputed
    # with the dataset median hire_date (documented decision — the original
    # value is unrecoverable, and median avoids skew from outliers).
    bad_hire_date = invalid_format | implausible
    parsed_hire_date[bad_hire_date] = pd.NaT
    median_hire_date = parsed_hire_date.median()
    df["hire_date"] = parsed_hire_date.fillna(median_hire_date)
    quality_summary["hire_date_imputed"] = int(bad_hire_date.sum())
    logger.info("hire_date rows imputed with dataset median (%s): %d", median_hire_date.date(), bad_hire_date.sum())

    # ------------------------------------------------------------------
    # Issue 4 — Numeric out-of-range values (years_experience)
    # Negative years of experience is not possible.
    # ------------------------------------------------------------------
    negative_experience = df["years_experience"] < 0
    quality_summary["negative_years_experience"] = int(negative_experience.sum())
    logger.info("Negative years_experience: %d", negative_experience.sum())
    df.loc[negative_experience, "years_experience"] = 0

    implausible_experience = df["years_experience"] > 50
    quality_summary["implausible_years_experience"] = int(implausible_experience.sum())
    if implausible_experience.any():
        logger.info("years_experience above 50 (implausible): %d", implausible_experience.sum())
        df.loc[implausible_experience, "years_experience"] = 50

    # ------------------------------------------------------------------
    # Issue 5 — Logical inconsistencies (salary doesn't match level)
    # Expected salary bands are derived from the data itself (median +/- 2x
    # IQR per level) rather than hardcoded, so the check adapts if the
    # dataset changes. Rows far outside their level's own distribution are
    # flagged and capped back to the nearest band edge.
    # ------------------------------------------------------------------
    level_stats = df.groupby("level")["salary"].agg(["median", lambda s: s.quantile(0.75) - s.quantile(0.25)])
    level_stats.columns = ["median", "iqr"]
    lower_bound = (level_stats["median"] - 2 * level_stats["iqr"]).clip(lower=0)
    upper_bound = level_stats["median"] + 2 * level_stats["iqr"]

    df_lower = df["level"].map(lower_bound)
    df_upper = df["level"].map(upper_bound)
    salary_mismatch = (df["salary"] < df_lower) | (df["salary"] > df_upper)
    quality_summary["salary_level_mismatch"] = int(salary_mismatch.sum())
    logger.info("Salary/level logical inconsistencies (outside median +/- 2*IQR for their level): %d",
                salary_mismatch.sum())
    if salary_mismatch.any():
        logger.info("Affected employees:\n%s",
                     df.loc[salary_mismatch, ["employee_id", "level", "salary"]].to_string(index=False))
    # Cap to the nearest band edge instead of dropping — preserves the row
    # while removing the implausible value.
    df.loc[salary_mismatch & (df["salary"] < df_lower), "salary"] = df_lower[salary_mismatch & (df["salary"] < df_lower)]
    df.loc[salary_mismatch & (df["salary"] > df_upper), "salary"] = df_upper[salary_mismatch & (df["salary"] > df_upper)]

    # ------------------------------------------------------------------
    # Issue 6 — Status conflicts (self-referencing manager)
    # A row where manager_id == employee_id is a logical impossibility —
    # an employee cannot manage themselves. EMP0000 as manager_id is treated
    # separately as a valid sentinel for "no manager / top of org chart"
    # since it consistently only appears on the most senior records.
    # ------------------------------------------------------------------
    self_managed = df["manager_id"] == df["employee_id"]
    quality_summary["self_referencing_manager"] = int(self_managed.sum())
    logger.info("Employees listed as their own manager: %d", self_managed.sum())
    df.loc[self_managed, "manager_id"] = None

    logger.info("Data quality summary (rows affected per fix): %s", quality_summary)
    return df


# ==============================================================================
# PIPELINE ENTRY POINT
# ==============================================================================

def run_pipeline():
    logger.info("=" * 60)
    logger.info("Starting Pillar 1 pipeline (Tasks 1.1 & 1.3)")
    logger.info("=" * 60)

    raw_projects = load_projects(os.path.join(DATA_DIR, "projects.csv"))
    clean_projects = transform_projects(raw_projects)

    raw_employees = load_employees(os.path.join(DATA_DIR, "employees.csv"))
    clean_emp = clean_employees(raw_employees)

    projects_path = os.path.join(OUTPUT_DIR, "projects_clean.csv")
    employees_path = os.path.join(OUTPUT_DIR, "employees_clean.csv")
    clean_projects.to_csv(projects_path, index=False)
    clean_emp.to_csv(employees_path, index=False)

    logger.info("Wrote %s (%d rows x %d cols)", projects_path, *clean_projects.shape)
    logger.info("Wrote %s (%d rows x %d cols)", employees_path, *clean_emp.shape)
    logger.info("Pipeline complete.")


if __name__ == "__main__":
    run_pipeline()
