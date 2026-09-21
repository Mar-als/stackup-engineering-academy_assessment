"""
StackUp Engineering Academy — Data Engineering Assessment
Pillar 4 — Infrastructure & Governance
Author: maryamalshehhi

Task covered:
  Task 4.3 -> configurable, reusable data quality framework (6 required
              checks + all 3 bonus checks)

Design principle (per the task spec): new rules are added by EDITING
DQ_CONFIG, never by touching run_data_quality_checks() itself. Every check
reads its thresholds/columns/ranges from config; none are hardcoded in the
check functions.

KNOWN FALSE-POSITIVE MODES (found by running this against the real data —
documented rather than silently tuned away, since the spec's checks are
implemented literally as described):
  - Distribution check: a flat 30% top-value threshold flags legitimate
    business skew on any low-cardinality column. E.g. employees.status is
    96% 'Active' (40/1000 employees are Inactive — that's normal, not a
    load error), and projects.region is 44% 'Abu Dhabi' (plausibly just
    where most offices are). A more robust version would compare against
    the column's own uniform-baseline share (e.g. flag only when
    top_share > max(0.30, 2 / n_distinct_values)) instead of one fixed
    number for every column regardless of cardinality.
  - Outlier check (z-score, 3 std dev): transaction amounts are strongly
    right-skewed (many small transactions, few large ones), so a mean/std
    based z-score flags ~1,600 "outliers" that are really just the normal
    long tail of the distribution. An IQR-based or log-transformed z-score
    would be more appropriate for skewed financial data than a raw z-score.
  - Freshness check: transactions.transaction_date's most recent value is
    ~43 days before "today" in this environment, which fails the 30-day
    threshold — expected for a static, point-in-time assessment dataset
    rather than a live feed; this check is meaningful once wired into an
    actually-refreshing pipeline, not on a frozen sample.

Run (prints a full report for projects/employees/transactions against the
RAW source data, and writes outputs/dq_report_<dataset>.md for each):
  python solutions/submissions/maryamalshehhi/04_infrastructure/dq_framework.py
"""

import os
import logging
from datetime import datetime, timedelta

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))
DATA_DIR = os.path.join(BASE_DIR, "datasets")
OUTPUT_DIR = os.path.join(BASE_DIR, "outputs")


# ==============================================================================
# CONFIG — every rule lives here, not in code
# ==============================================================================

DQ_CONFIG = {
    "projects": {
        "completeness_threshold": 0.90,
        "pk_columns": ["project_id"],
        "numeric_ranges": {
            "budget": {"min": 0, "max": 10_000_000},
            "actual_cost": {"min": 0, "max": 10_000_000},
        },
        "date_columns": ["start_date", "end_date"],
        "consistency_rules": [
            {"type": "before", "columns": ["start_date", "end_date"]},
            {"type": "non_negative", "column": "actual_cost"},
        ],
        "foreign_keys": {
            "project_manager_id": ("employees", "employee_id"),
        },
        "distribution_check_columns": ["status", "department", "priority", "region"],
    },
    "employees": {
        "completeness_threshold": 0.85,
        "pk_columns": ["employee_id"],
        "numeric_ranges": {
            "salary": {"min": 10_000, "max": 100_000},
            "years_experience": {"min": 0, "max": 50},
        },
        "date_columns": ["hire_date"],
        "consistency_rules": [
            {"type": "non_negative", "column": "years_experience"},
            {
                "type": "salary_level_match",
                "level_column": "level",
                "salary_column": "salary",
                # Bands are business rules, not derived stats — kept here so
                # they can be tuned without touching the check logic.
                "bands": {
                    "Junior": (10_000, 20_000),
                    "Mid": (15_000, 25_000),
                    "Senior": (23_000, 37_000),
                    "Lead": (36_000, 52_000),
                    "Director": (48_000, 72_000),
                },
            },
        ],
        "foreign_keys": {
            "manager_id": ("employees", "employee_id"),
        },
        # EMP0000 is a documented sentinel for "no manager" (top-of-org-chart),
        # not a real employee — excluding it from the FK check avoids flagging
        # every senior employee as a referential-integrity violation.
        "foreign_key_sentinels": {
            "manager_id": ["EMP0000"],
        },
        "distribution_check_columns": ["department", "role", "level", "region", "status"],
    },
    "transactions": {
        "completeness_threshold": 0.80,
        "pk_columns": ["transaction_id"],
        "numeric_ranges": {
            "amount": {"min": 0, "max": 5_000_000},
        },
        "date_columns": ["transaction_date"],
        "consistency_rules": [
            {"type": "non_negative", "column": "amount"},
        ],
        "foreign_keys": {
            "project_id": ("projects", "project_id"),
            "approved_by": ("employees", "employee_id"),
        },
        "distribution_check_columns": ["category", "currency", "payment_status"],
        "freshness_check": {"column": "transaction_date", "max_age_days": 30},
        "outlier_check_columns": ["amount"],
    },
}


# ==============================================================================
# Individual check functions — each returns {"status": PASS|FAIL, "details": ...}
# ==============================================================================

def _check_completeness(df: pd.DataFrame, threshold: float) -> dict:
    completeness = {}
    failed_columns = []
    for col in df.columns:
        non_null_frac = 1 - (df[col].isna() | (df[col].astype(str).str.strip() == "")).mean()
        completeness[col] = round(float(non_null_frac), 4)
        if non_null_frac < threshold:
            failed_columns.append(col)
    status = "FAIL" if failed_columns else "PASS"
    return {"status": status, "details": completeness, "failed_columns": failed_columns}


def _check_uniqueness(df: pd.DataFrame, pk_columns: list) -> dict:
    details = {}
    failed = []
    for col in pk_columns:
        if col not in df.columns:
            continue
        total = len(df)
        unique = df[col].nunique(dropna=True)
        details[col] = f"{unique} unique / {total} total"
        if unique < total:
            failed.append(col)
    status = "FAIL" if failed else "PASS"
    return {"status": status, "details": "; ".join(details.values()), "failed_columns": failed}


def _check_validity_numeric(df: pd.DataFrame, numeric_ranges: dict) -> dict:
    messages = []
    failed = []
    for col, bounds in numeric_ranges.items():
        if col not in df.columns:
            continue
        series = pd.to_numeric(df[col], errors="coerce")
        below = (series < bounds["min"]).sum()
        above = (series > bounds["max"]).sum()
        if below or above:
            failed.append(col)
            parts = []
            if below:
                parts.append(f"{below} value(s) below minimum ({bounds['min']})")
            if above:
                parts.append(f"{above} value(s) above maximum ({bounds['max']})")
            messages.append(f"{col}: {', '.join(parts)}")
    status = "FAIL" if failed else "PASS"
    return {"status": status, "details": "; ".join(messages) if messages else "all values within range", "failed_columns": failed}


def _check_validity_date(df: pd.DataFrame, date_columns: list, allow_future: bool = False) -> dict:
    messages = []
    failed = []
    today = pd.Timestamp(datetime.now().date())
    for col in date_columns:
        if col not in df.columns:
            continue
        parsed = pd.to_datetime(df[col], errors="coerce")
        unparsable = parsed.isna() & df[col].notna() & (df[col].astype(str).str.strip() != "")
        future = pd.Series(False, index=df.index)
        if not allow_future:
            future = parsed.notna() & (parsed > today)
        n_bad = int(unparsable.sum() + future.sum())
        if n_bad:
            failed.append(col)
            messages.append(f"{col}: {int(unparsable.sum())} unparsable, {int(future.sum())} in the future")
    status = "FAIL" if failed else "PASS"
    return {"status": status, "details": "; ".join(messages) if messages else "all dates valid", "failed_columns": failed}


def _check_consistency(df: pd.DataFrame, rules: list) -> dict:
    messages = []
    failed = []
    for rule in rules:
        rtype = rule["type"]

        if rtype == "before":
            c1, c2 = rule["columns"]
            if c1 not in df.columns or c2 not in df.columns:
                continue
            a, b = pd.to_datetime(df[c1], errors="coerce"), pd.to_datetime(df[c2], errors="coerce")
            violations = ((a.notna() & b.notna()) & (a > b)).sum()
            if violations:
                failed.append(f"{c1}<{c2}")
                messages.append(f"{c1} > {c2} in {int(violations)} row(s)")

        elif rtype == "non_negative":
            col = rule["column"]
            if col not in df.columns:
                continue
            series = pd.to_numeric(df[col], errors="coerce")
            violations = (series < 0).sum()
            if violations:
                failed.append(col)
                messages.append(f"{col}: {int(violations)} negative value(s)")

        elif rtype == "salary_level_match":
            level_col, salary_col, bands = rule["level_column"], rule["salary_column"], rule["bands"]
            if level_col not in df.columns or salary_col not in df.columns:
                continue
            lower = df[level_col].map(lambda lv: bands.get(lv, (None, None))[0])
            upper = df[level_col].map(lambda lv: bands.get(lv, (None, None))[1])
            salary = pd.to_numeric(df[salary_col], errors="coerce")
            mismatched = ((lower.notna()) & ((salary < lower) | (salary > upper))).sum()
            if mismatched:
                failed.append(f"{salary_col}~{level_col}")
                messages.append(f"{mismatched} employee(s) with salary outside their level's band")

    status = "FAIL" if failed else "PASS"
    return {"status": status, "details": "; ".join(messages) if messages else "all consistency rules satisfied", "failed_columns": failed}


def _check_referential_integrity(df: pd.DataFrame, foreign_keys: dict, all_dataframes: dict, sentinels: dict = None) -> dict:
    sentinels = sentinels or {}
    messages = []
    failed = []
    for fk_col, (ref_table, ref_col) in foreign_keys.items():
        if fk_col not in df.columns or ref_table not in all_dataframes:
            continue
        ref_values = set(all_dataframes[ref_table][ref_col].dropna().unique())
        allowed_sentinels = set(sentinels.get(fk_col, []))
        fk_values = df[fk_col].dropna()
        fk_values = fk_values[~fk_values.isin(allowed_sentinels)]
        orphans = ~fk_values.isin(ref_values)
        n_orphans = int(orphans.sum())
        if n_orphans:
            failed.append(fk_col)
            messages.append(f"{fk_col}: {n_orphans} value(s) not found in {ref_table}.{ref_col}")
    status = "FAIL" if failed else "PASS"
    return {"status": status, "details": "; ".join(messages) if messages else "all foreign keys resolve", "failed_columns": failed}


def _check_distribution(df: pd.DataFrame, columns: list, threshold: float = 0.30) -> dict:
    messages = []
    failed = []
    for col in columns:
        if col not in df.columns or df[col].dropna().empty:
            continue
        top_share = df[col].value_counts(normalize=True, dropna=True).iloc[0]
        if top_share > threshold:
            top_value = df[col].value_counts(dropna=True).index[0]
            failed.append(col)
            messages.append(f"{col}: '{top_value}' is {top_share:.0%} of non-null values (> {threshold:.0%})")
    status = "FAIL" if failed else "PASS"
    return {"status": status, "details": "; ".join(messages) if messages else "no column dominated by one value", "failed_columns": failed}


def _check_freshness(df: pd.DataFrame, column: str, max_age_days: int) -> dict:
    if column not in df.columns:
        return {"status": "PASS", "details": "column not present — skipped"}
    parsed = pd.to_datetime(df[column], errors="coerce")
    if parsed.isna().all():
        return {"status": "PASS", "details": "no valid dates to evaluate"}
    max_date = parsed.max()
    age_days = (pd.Timestamp(datetime.now().date()) - max_date).days
    status = "FAIL" if age_days > max_age_days else "PASS"
    return {"status": status, "details": f"most recent {column} is {age_days} day(s) old (threshold {max_age_days})"}


def _check_outliers(df: pd.DataFrame, columns: list, n_std: float = 3.0) -> dict:
    messages = []
    failed = []
    for col in columns:
        if col not in df.columns:
            continue
        series = pd.to_numeric(df[col], errors="coerce").dropna()
        if series.empty or series.std() == 0:
            continue
        z = (series - series.mean()).abs() / series.std()
        n_outliers = int((z > n_std).sum())
        if n_outliers:
            failed.append(col)
            messages.append(f"{col}: {n_outliers} value(s) beyond {n_std} std dev from the mean")
    status = "FAIL" if failed else "PASS"
    return {"status": status, "details": "; ".join(messages) if messages else "no extreme outliers", "failed_columns": failed}


# ==============================================================================
# Orchestrator
# ==============================================================================

def run_data_quality_checks(df: pd.DataFrame, dataset_name: str, config: dict, all_dataframes: dict = None) -> dict:
    """
    Runs every check named in `config` against `df` and returns the
    dataset_name/checks_run/checks_passed/checks_failed/results structure
    required by Task 4.3. Referential integrity needs the OTHER loaded
    datasets to check against, passed in `all_dataframes` (keyed by
    dataset name, e.g. {"projects": projects_df, "employees": employees_df}).
    """
    all_dataframes = all_dataframes or {}
    results = {}

    results["completeness"] = _check_completeness(df, config["completeness_threshold"])
    results["uniqueness"] = _check_uniqueness(df, config["pk_columns"])
    results["validity_numeric"] = _check_validity_numeric(df, config.get("numeric_ranges", {}))
    results["validity_date"] = _check_validity_date(df, config.get("date_columns", []))
    results["consistency"] = _check_consistency(df, config.get("consistency_rules", []))
    results["referential_integrity"] = _check_referential_integrity(
        df, config.get("foreign_keys", {}), all_dataframes, config.get("foreign_key_sentinels", {})
    )

    # Bonus checks — only run if the dataset's config opts in.
    if "distribution_check_columns" in config:
        results["distribution"] = _check_distribution(df, config["distribution_check_columns"])
    if "freshness_check" in config:
        fc = config["freshness_check"]
        results["freshness"] = _check_freshness(df, fc["column"], fc["max_age_days"])
    if "outlier_check_columns" in config:
        results["outliers"] = _check_outliers(df, config["outlier_check_columns"])

    checks_run = len(results)
    checks_failed = sum(1 for r in results.values() if r["status"] == "FAIL")
    checks_passed = checks_run - checks_failed

    for check_name, r in results.items():
        if r["status"] == "FAIL":
            logger.warning("[%s] %s FAILED: %s", dataset_name, check_name, r["details"])

    return {
        "dataset_name": dataset_name,
        "checks_run": checks_run,
        "checks_passed": checks_passed,
        "checks_failed": checks_failed,
        "results": results,
    }


def write_markdown_report(report: dict, output_dir: str) -> str:
    path = os.path.join(output_dir, f"dq_report_{report['dataset_name']}.md")
    lines = [
        f"# Data Quality Report — {report['dataset_name']}",
        "",
        f"Generated: {datetime.now().isoformat(timespec='seconds')}",
        "",
        f"**Checks run:** {report['checks_run']}  •  **Passed:** {report['checks_passed']}  •  **Failed:** {report['checks_failed']}",
        "",
        "| Check | Status | Details |",
        "|---|---|---|",
    ]
    for name, r in report["results"].items():
        status_icon = "✅ PASS" if r["status"] == "PASS" else "❌ FAIL"
        details = r["details"]
        if isinstance(details, dict):
            details = ", ".join(f"{k}={v}" for k, v in list(details.items())[:6]) + (", ..." if len(details) > 6 else "")
        lines.append(f"| {name} | {status_icon} | {details} |")

    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    return path


# ==============================================================================
# DEMO ENTRY POINT
# ==============================================================================

def run_pipeline():
    import json

    projects = pd.read_csv(os.path.join(DATA_DIR, "projects.csv"))
    employees = pd.read_csv(os.path.join(DATA_DIR, "employees.csv"))
    transactions = pd.read_json(os.path.join(DATA_DIR, "transactions.json"))

    all_dfs = {"projects": projects, "employees": employees, "transactions": transactions}

    for name, df in all_dfs.items():
        report = run_data_quality_checks(df, name, DQ_CONFIG[name], all_dataframes=all_dfs)
        logger.info("=" * 60)
        logger.info("%s: %d/%d checks passed", name, report["checks_passed"], report["checks_run"])
        logger.info(json.dumps({k: v["status"] for k, v in report["results"].items()}, indent=2))
        path = write_markdown_report(report, OUTPUT_DIR)
        logger.info("Wrote %s", path)


if __name__ == "__main__":
    run_pipeline()
