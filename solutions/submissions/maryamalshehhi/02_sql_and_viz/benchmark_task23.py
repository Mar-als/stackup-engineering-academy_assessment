"""
StackUp Engineering Academy — Data Engineering Assessment
Pillar 2, Task 2.3 — Query optimisation benchmark
Author: maryamalshehhi

Reproduces the EXPLAIN QUERY PLAN output and timings quoted as comments in
Section 4 of data_model.sql. Uses SQLite (Python's built-in sqlite3) rather
than DuckDB — see the "METHODOLOGY NOTE" at the top of Section 4 for why.

The 50,000-row transactions.json is too small for a full-scan vs.
index-seek difference to be observable, so this script replicates it up to
2,000,000 rows PURELY INSIDE A THROWAWAY BENCHMARK DATABASE — it never
touches outputs/transactions_clean.csv or any other pipeline output.

Run:
  python solutions/submissions/maryamalshehhi/02_sql_and_viz/benchmark_task23.py
"""

import os
import sqlite3
import time

import pandas as pd

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))
DB_PATH = os.path.join(BASE_DIR, "outputs", "results", "_benchmark_task23.db")

N_COPIES = 40  # 50,000 x 40 = 2,000,000 transaction rows

ORIGINAL_QUERY = """
SELECT
    e.full_name, e.department, e.role, p.project_name, p.status, p.budget, p.actual_cost,
    t.amount, t.category, t.payment_status, t.transaction_date
FROM employees e, projects p, transactions t
WHERE e.employee_id = p.project_manager_id
AND   p.project_id  = t.project_id
AND   p.status NOT IN ('Completed', 'On Hold')
AND   t.payment_status = 'Pending'
AND   t.amount > (SELECT AVG(amount) FROM transactions WHERE payment_status = 'Pending')
ORDER BY e.department, t.amount DESC;
"""

REWRITTEN_QUERY = """
WITH pending_avg AS (
    SELECT AVG(amount) AS avg_pending_amount
    FROM transactions
    WHERE payment_status = 'Pending'
),
open_projects AS (
    SELECT project_id, project_name, status, budget, actual_cost, project_manager_id
    FROM projects
    WHERE status NOT IN ('Completed', 'On Hold')
)
SELECT
    e.full_name, e.department, e.role,
    op.project_name, op.status, op.budget, op.actual_cost,
    t.amount, t.category, t.payment_status, t.transaction_date
FROM open_projects op
JOIN transactions t
  ON t.project_id = op.project_id
 AND t.payment_status = 'Pending'
JOIN employees e ON e.employee_id = op.project_manager_id
CROSS JOIN pending_avg
WHERE t.amount > pending_avg.avg_pending_amount
ORDER BY e.department, t.amount DESC;
"""

INDEXES = [
    "CREATE INDEX idx_transactions_project_status_amount ON transactions (project_id, payment_status, amount);",
    "CREATE INDEX idx_transactions_payment_status_amount ON transactions (payment_status, amount);",
    "CREATE INDEX idx_projects_status ON projects (status);",
    "CREATE INDEX idx_projects_project_manager_id ON projects (project_manager_id);",
    "CREATE INDEX idx_employees_employee_id ON employees (employee_id);",
]


def run_timed(con, label, sql, n=2):
    times = []
    rows = []
    for _ in range(n):
        t0 = time.perf_counter()
        rows = con.execute(sql).fetchall()
        times.append(time.perf_counter() - t0)
    best = min(times)
    print(f"{label}: best={best*1000:.1f} ms  rows={len(rows)}  all_runs_ms={[round(t*1000,1) for t in times]}")
    return best


def explain(con, label, sql):
    print(f"\nEXPLAIN QUERY PLAN — {label}")
    for row in con.execute("EXPLAIN QUERY PLAN " + sql).fetchall():
        print(" ", row)


def main():
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    con = sqlite3.connect(DB_PATH)

    employees = pd.read_csv(os.path.join(BASE_DIR, "outputs", "employees_clean.csv"))
    projects = pd.read_csv(os.path.join(BASE_DIR, "outputs", "projects_clean.csv"))
    base_txn = pd.read_json(os.path.join(BASE_DIR, "datasets", "transactions.json"))
    base_txn["transaction_date"] = base_txn["transaction_date"].astype(str)

    transactions = pd.concat(
        [base_txn.assign(transaction_id=base_txn["transaction_id"] + f"-R{i}") for i in range(N_COPIES)],
        ignore_index=True,
    )
    print(f"Benchmark scale: {len(transactions):,} transaction rows "
          f"({len(base_txn):,} real rows x {N_COPIES} — replicated for this benchmark only)")

    employees.to_sql("employees", con, index=False)
    projects.to_sql("projects", con, index=False)
    transactions.to_sql("transactions", con, index=False, chunksize=100_000)
    con.commit()

    print("\n" + "=" * 70)
    print("BEFORE — original query, no indexes")
    print("=" * 70)
    explain(con, "original, no indexes", ORIGINAL_QUERY)
    t_before = run_timed(con, "original, no indexes", ORIGINAL_QUERY)

    for stmt in INDEXES:
        con.execute(stmt)
    con.commit()
    con.execute("ANALYZE;")
    con.commit()
    print("\nCreated indexes + ran ANALYZE:")
    for stmt in INDEXES:
        print(" ", stmt)

    print("\n" + "=" * 70)
    print("AFTER — rewritten query, with indexes + ANALYZE")
    print("=" * 70)
    explain(con, "rewritten, with indexes", REWRITTEN_QUERY)
    t_after_rewrite = run_timed(con, "rewritten, with indexes", REWRITTEN_QUERY)

    print("\n" + "=" * 70)
    print("AFTER — original query SHAPE, same indexes (isolates rewrite's own effect)")
    print("=" * 70)
    explain(con, "original shape, with indexes", ORIGINAL_QUERY)
    t_after_original = run_timed(con, "original shape, with indexes", ORIGINAL_QUERY)

    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Original, no indexes:            {t_before*1000:8.1f} ms")
    print(f"Rewritten, with indexes+ANALYZE: {t_after_rewrite*1000:8.1f} ms  ({t_before/t_after_rewrite:.2f}x)")
    print(f"Original shape, same indexes:    {t_after_original*1000:8.1f} ms  ({t_before/t_after_original:.2f}x)")

    con.close()
    os.remove(DB_PATH)


if __name__ == "__main__":
    main()
