-- =============================================================================
-- StackUp Engineering Academy — Data Engineering Assessment
-- Pillar 2, Task 2.1 — Six Business Questions
-- Author: maryamalshehhi
-- Dialect: DuckDB
--
-- PREREQUISITE: run, in order,
--   1. solutions/submissions/maryamalshehhi/01_foundations/etl_pipeline.py
--   2. solutions/submissions/maryamalshehhi/02_sql_and_viz/etl_pipeline.py
-- so outputs/projects_clean.csv, outputs/employees_clean.csv and
-- outputs/transactions_clean.csv all exist.
--
-- Section 1 (DDL) and Section 2 (load) are carried over unchanged from
-- Task 1.2 — see 01_foundations/data_model.sql for the full design
-- rationale on each table. They are repeated here so this file is
-- runnable standalone for Pillar 2 grading.
--
-- HOW TO RUN (from the repo root):
--   duckdb warehouse.db < solutions/submissions/maryamalshehhi/02_sql_and_viz/data_model.sql
-- =============================================================================


-- =============================================================================
-- SECTION 1 — DDL (carried over from Task 1.2)
-- =============================================================================

DROP TABLE IF EXISTS fact_transactions;
DROP TABLE IF EXISTS bridge_employee_project;
DROP TABLE IF EXISTS dim_vendor;
DROP TABLE IF EXISTS dim_employee;
DROP TABLE IF EXISTS dim_project;
DROP TABLE IF EXISTS dim_date;

CREATE TABLE dim_date (
    date_key    INTEGER PRIMARY KEY,
    full_date   DATE NOT NULL UNIQUE,
    year        INTEGER NOT NULL,
    quarter     INTEGER NOT NULL,
    month       INTEGER NOT NULL,
    month_name  VARCHAR NOT NULL,
    week        INTEGER NOT NULL,
    day         INTEGER NOT NULL,
    day_of_week VARCHAR NOT NULL,
    is_weekend  BOOLEAN NOT NULL
);

CREATE TABLE dim_project (
    project_key         INTEGER PRIMARY KEY,
    project_id          VARCHAR NOT NULL UNIQUE,
    project_name        VARCHAR,
    department          VARCHAR,
    status              VARCHAR,
    status_category     VARCHAR,
    priority            VARCHAR,
    region              VARCHAR,
    project_manager_id  VARCHAR,
    start_date          DATE,
    end_date            DATE,
    budget              DECIMAL(14, 2),
    actual_cost         DECIMAL(14, 2),
    risk_level          VARCHAR
);

CREATE TABLE dim_employee (
    employee_key    INTEGER PRIMARY KEY,
    employee_id     VARCHAR NOT NULL,
    full_name       VARCHAR,
    email           VARCHAR,
    department      VARCHAR,
    role            VARCHAR,
    level           VARCHAR,
    salary          DECIMAL(12, 2),
    manager_id      VARCHAR,
    region          VARCHAR,
    status          VARCHAR,
    change_reason   VARCHAR,
    valid_from      DATE NOT NULL,
    valid_to        DATE NOT NULL,
    is_current      BOOLEAN NOT NULL
);

CREATE TABLE dim_vendor (
    vendor_key   INTEGER PRIMARY KEY,
    vendor_id    VARCHAR NOT NULL UNIQUE,
    vendor_name  VARCHAR
);

CREATE TABLE bridge_employee_project (
    employee_key  INTEGER REFERENCES dim_employee(employee_key),
    project_key   INTEGER REFERENCES dim_project(project_key),
    project_role  VARCHAR NOT NULL,
    PRIMARY KEY (employee_key, project_key, project_role)
);

CREATE TABLE fact_transactions (
    transaction_key  INTEGER PRIMARY KEY,
    transaction_id   VARCHAR NOT NULL UNIQUE,
    project_key      INTEGER REFERENCES dim_project(project_key),
    employee_key     INTEGER REFERENCES dim_employee(employee_key),
    vendor_key       INTEGER REFERENCES dim_vendor(vendor_key),
    date_key         INTEGER REFERENCES dim_date(date_key),
    amount           DECIMAL(14, 2),
    currency         VARCHAR,
    category         VARCHAR,
    payment_status   VARCHAR,
    invoice_ref      VARCHAR
);


-- =============================================================================
-- SECTION 2 — Load staging data & populate the warehouse (carried over)
-- =============================================================================

INSERT INTO dim_date
SELECT
    CAST(strftime(d, '%Y%m%d') AS INTEGER)  AS date_key,
    CAST(d AS DATE)                         AS full_date,
    EXTRACT(YEAR FROM d)                    AS year,
    EXTRACT(QUARTER FROM d)                 AS quarter,
    EXTRACT(MONTH FROM d)                   AS month,
    strftime(d, '%B')                       AS month_name,
    EXTRACT(WEEK FROM d)                    AS week,
    EXTRACT(DAY FROM d)                     AS day,
    strftime(d, '%A')                       AS day_of_week,
    EXTRACT(ISODOW FROM d) IN (6, 7)        AS is_weekend
FROM generate_series(DATE '2000-01-01', DATE '2027-12-31', INTERVAL 1 DAY) AS t(d);

INSERT INTO dim_project
SELECT
    ROW_NUMBER() OVER (ORDER BY project_id)  AS project_key,
    project_id, project_name, department, status, status_category,
    priority, region, project_manager_id, start_date, end_date,
    budget, actual_cost, risk_level
FROM read_csv_auto('outputs/projects_clean.csv');

INSERT INTO dim_vendor
SELECT
    ROW_NUMBER() OVER (ORDER BY vendor_id)  AS vendor_key,
    vendor_id, vendor_name
FROM (SELECT DISTINCT vendor_id, vendor_name FROM read_json_auto('datasets/transactions.json'));

CREATE OR REPLACE TEMP TABLE stg_history_versions AS
SELECT
    employee_id,
    new_salary                                                              AS salary,
    new_role                                                                AS role,
    new_level                                                               AS level,
    change_reason,
    effective_date                                                          AS valid_from,
    COALESCE(
        LEAD(effective_date) OVER (PARTITION BY employee_id ORDER BY effective_date),
        DATE '9999-12-31'
    )                                                                        AS valid_to,
    LEAD(effective_date) OVER (PARTITION BY employee_id ORDER BY effective_date) IS NULL AS is_current
FROM read_csv_auto('datasets/employees_salary_history.csv');

CREATE OR REPLACE TEMP TABLE stg_no_history_versions AS
SELECT
    e.employee_id,
    e.salary,
    e.role,
    e.level,
    'Initial appointment'  AS change_reason,
    e.hire_date             AS valid_from,
    DATE '9999-12-31'       AS valid_to,
    TRUE                    AS is_current
FROM read_csv_auto('outputs/employees_clean.csv') e
WHERE NOT EXISTS (
    SELECT 1 FROM read_csv_auto('datasets/employees_salary_history.csv') h
    WHERE h.employee_id = e.employee_id
);

INSERT INTO dim_employee
SELECT
    ROW_NUMBER() OVER (ORDER BY v.employee_id, v.valid_from)  AS employee_key,
    v.employee_id,
    e.full_name, e.email, e.department,
    v.role, v.level, v.salary,
    e.manager_id, e.region, e.status,
    v.change_reason, v.valid_from, v.valid_to, v.is_current
FROM (
    SELECT * FROM stg_history_versions
    UNION ALL
    SELECT * FROM stg_no_history_versions
) v
JOIN read_csv_auto('outputs/employees_clean.csv') e ON e.employee_id = v.employee_id
ORDER BY v.employee_id, v.valid_from;

INSERT INTO bridge_employee_project
SELECT de.employee_key, dp.project_key, 'Manager'
FROM dim_project dp
JOIN dim_employee de
  ON de.employee_id = dp.project_manager_id AND de.is_current = TRUE;

INSERT INTO bridge_employee_project
SELECT DISTINCT de.employee_key, dp.project_key, 'Approver'
FROM read_json_auto('datasets/transactions.json') t
JOIN dim_project dp ON dp.project_id = t.project_id
JOIN dim_employee de ON de.employee_id = t.approved_by AND de.is_current = TRUE
WHERE t.approved_by IS NOT NULL;

INSERT INTO fact_transactions
SELECT
    ROW_NUMBER() OVER (ORDER BY t.transaction_id)  AS transaction_key,
    t.transaction_id,
    dp.project_key,
    de.employee_key,
    dv.vendor_key,
    CAST(strftime(CAST(t.transaction_date AS DATE), '%Y%m%d') AS INTEGER)  AS date_key,
    t.amount, t.currency, t.category, t.payment_status, t.invoice_ref
FROM read_json_auto('datasets/transactions.json') t
LEFT JOIN dim_project dp ON dp.project_id = t.project_id
LEFT JOIN dim_vendor dv ON dv.vendor_id = t.vendor_id
LEFT JOIN dim_employee de
       ON de.employee_id = t.approved_by
      AND CAST(t.transaction_date AS DATE) >= de.valid_from
      AND CAST(t.transaction_date AS DATE) <  de.valid_to;


-- =============================================================================
-- SECTION 3 — TASK 2.1: Six business questions
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Q1 — Department budget performance
-- Approach: aggregate budget/actual_cost per department straight off
-- dim_project (one row per project, no fan-out risk), compute spend_percentage
-- as a ratio guarded against a department with zero total budget, and filter
-- for >90% utilisation OR over budget (a department could be over budget while
-- under 90% of ITS OWN budget is impossible arithmetically here since
-- actual > budget implies >100% > 90%, but the OR is kept literal to match
-- the spec: "over 90%, including over-budget departments").
-- -----------------------------------------------------------------------------
SELECT
    department,
    SUM(budget)                                                  AS total_budget,
    SUM(actual_cost)                                             AS total_actual_cost,
    ROUND(100.0 * SUM(actual_cost) / NULLIF(SUM(budget), 0), 2)  AS spend_percentage,
    SUM(actual_cost) > SUM(budget)                               AS over_budget
FROM dim_project
GROUP BY department
HAVING SUM(actual_cost) / NULLIF(SUM(budget), 0) > 0.90
ORDER BY spend_percentage DESC;


-- -----------------------------------------------------------------------------
-- Q2 — Project manager workload (current employee data)
-- Approach: join dim_project to dim_employee ON project_manager_id, filtering
-- to is_current = TRUE per the task note (a manager's name/email should
-- reflect who they are TODAY, not whichever SCD2 version happens to match).
-- Only 'In Progress' / 'On Hold' projects count as "active" (status_category
-- = 'Active' covers "In Progress"; "On Hold" is modelled as Pending in
-- Task 1.1, so this uses the raw status column directly to mean
-- "not yet finished and not cancelled", matching what a delivery-risk read
-- of "active projects" should include).
-- Grouping is by (full_name, email) rather than full_name alone because two
-- different employees in this dataset happen to share the name
-- "Hassan Hamdan" (EMP0192, EMP0737) — grouping on name only would silently
-- merge two different people's workloads.
-- VERIFIED: on the real dataset this correctly returns ZERO rows — 500
-- projects are spread across 300 distinct managers, and the busiest manager
-- (by employee_id) oversees 3 active projects, not more. That is the
-- genuinely correct answer for this data, not a broken query.
-- -----------------------------------------------------------------------------
SELECT
    de.full_name,
    de.email,
    COUNT(*)                    AS active_project_count,
    SUM(dp.budget)               AS combined_budget_responsibility,
    SUM(dp.actual_cost)          AS combined_actual_spend
FROM dim_project dp
JOIN dim_employee de
  ON de.employee_id = dp.project_manager_id
 AND de.is_current = TRUE
WHERE dp.status IN ('In Progress', 'On Hold')
GROUP BY de.full_name, de.email
HAVING COUNT(*) > 3
ORDER BY active_project_count DESC;


-- -----------------------------------------------------------------------------
-- Q3 — Vendor concentration risk
-- Approach: a CTE computes each vendor's spend share of the grand total in
-- one pass (window function SUM() OVER () avoids a self-join against the
-- whole fact table), then the outer query filters to >5% and classifies risk.
-- Using 'Paid' + 'Pending' + 'Disputed' amounts alike here — concentration
-- risk is about how much business flows through a vendor, not just what's
-- been settled.
-- VERIFIED: on the real dataset this correctly returns ZERO rows — 25
-- vendors split ~50,000 transactions fairly evenly, and the top vendor
-- (Integra Tech) accounts for 4.36% of total spend, under the 5% threshold.
-- That is the genuinely correct answer for this data, not a broken query.
-- -----------------------------------------------------------------------------
WITH vendor_spend AS (
    SELECT
        dv.vendor_name,
        SUM(ft.amount)                                    AS total_spend,
        COUNT(*)                                          AS transaction_count,
        SUM(ft.amount) * 100.0 / SUM(SUM(ft.amount)) OVER ()  AS percentage_of_total_spend
    FROM fact_transactions ft
    JOIN dim_vendor dv ON dv.vendor_key = ft.vendor_key
    GROUP BY dv.vendor_name
)
SELECT
    vendor_name,
    total_spend,
    transaction_count,
    ROUND(percentage_of_total_spend, 2)  AS percentage_of_total_spend,
    CASE
        WHEN percentage_of_total_spend > 10 THEN 'HIGH'
        WHEN percentage_of_total_spend >= 5 THEN 'MEDIUM'
        ELSE 'NORMAL'
    END                                    AS risk_flag
FROM vendor_spend
WHERE percentage_of_total_spend > 5
ORDER BY percentage_of_total_spend DESC;


-- -----------------------------------------------------------------------------
-- Q4 — Projects with open financial issues
-- Approach: filter fact_transactions to Pending/Disputed (the two "open"
-- statuses), aggregate per project, then keep only projects whose open
-- total exceeds 50,000 AED. Filtering payment_status BEFORE the join/group
-- (predicate pushdown) keeps the aggregation working over the smallest
-- possible row set.
-- -----------------------------------------------------------------------------
SELECT
    dp.project_id,
    dp.project_name,
    dp.department,
    dp.status                    AS project_status,
    COUNT(*)                     AS open_transaction_count,
    SUM(ft.amount)                AS open_transaction_value
FROM fact_transactions ft
JOIN dim_project dp ON dp.project_key = ft.project_key
WHERE ft.payment_status IN ('Pending', 'Disputed')
GROUP BY dp.project_id, dp.project_name, dp.department, dp.status
HAVING SUM(ft.amount) > 50000
ORDER BY open_transaction_value DESC;


-- -----------------------------------------------------------------------------
-- Q5 — Monthly spend trend with running total
-- Approach: aggregate spend per (category, year_month) once, then use
-- SUM() OVER (PARTITION BY category ORDER BY year_month) for the running
-- total and LAG() for the prior month's spend to compute % change — both
-- window functions read the same pre-aggregated rows, no repeated scans.
-- -----------------------------------------------------------------------------
WITH monthly AS (
    SELECT
        strftime(dd.full_date, '%Y-%m')  AS year_month,
        ft.category,
        SUM(ft.amount)                    AS monthly_spend
    FROM fact_transactions ft
    JOIN dim_date dd ON dd.date_key = ft.date_key
    GROUP BY strftime(dd.full_date, '%Y-%m'), ft.category
)
SELECT
    year_month,
    category,
    monthly_spend,
    SUM(monthly_spend) OVER (
        PARTITION BY category ORDER BY year_month
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )                                                              AS running_total,
    ROUND(
        100.0 * (monthly_spend - LAG(monthly_spend) OVER (PARTITION BY category ORDER BY year_month))
        / NULLIF(LAG(monthly_spend) OVER (PARTITION BY category ORDER BY year_month), 0),
        2
    )                                                              AS month_over_month_pct_change
FROM monthly
ORDER BY category, year_month;


-- -----------------------------------------------------------------------------
-- Q6 — Employee compensation history analysis
-- Approach: self-join dim_employee to itself, matching one version's
-- valid_to to the next version's valid_from for the same employee_id — this
-- is exactly consecutive SCD2 versions (no gap, no overlap, per the Task 1.2
-- validation queries), so it directly identifies "previous salary -> new
-- salary" pairs without needing a LAG() window (which would require an
-- ordered frame per employee anyway — the self-join is equivalent and
-- reads more directly as "adjacent version to adjacent version").
-- -----------------------------------------------------------------------------
SELECT
    curr.employee_id,
    curr.full_name,
    curr.valid_from      AS change_date,
    prev.salary           AS previous_salary,
    curr.salary           AS new_salary,
    curr.salary - prev.salary                                       AS increase_amount,
    ROUND(100.0 * (curr.salary - prev.salary) / NULLIF(prev.salary, 0), 2)  AS increase_pct
FROM dim_employee curr
JOIN dim_employee prev
  ON prev.employee_id = curr.employee_id
 AND prev.valid_to = curr.valid_from
WHERE curr.salary > prev.salary
ORDER BY increase_amount DESC
LIMIT 20;


-- =============================================================================
-- SECTION 4 — TASK 2.3: Query optimisation
-- =============================================================================
--
-- METHODOLOGY NOTE: this section's benchmarks were run against SQLite
-- (Python's built-in sqlite3), not DuckDB. DuckDB's cost-based optimiser
-- automatically decorrelates scalar subqueries and treats implicit
-- comma-joins identically to explicit INNER JOINs before execution — it
-- would silently neutralise the exact anti-patterns this exercise asks us
-- to diagnose, making a before/after comparison meaningless. SQLite's
-- planner is closer to what a naive query actually costs in production
-- engines, and its EXPLAIN QUERY PLAN output is easy to read column-by-column.
-- All numbers below are REAL measurements, not estimates — see
-- solutions/submissions/maryamalshehhi/02_sql_and_viz/benchmark_task23.py
-- for the exact script used to produce them.
--
-- The provided datasets/transactions.json (50,000 rows) is too small for a
-- full-scan vs. index-seek difference to be observable at all — everything
-- fits in a couple of SQLite pages worth of cache. Per this section's own
-- instructions ("load sufficient data to make the performance difference
-- meaningful"), the benchmark replicates transactions.json up to 2,000,000
-- rows purely inside the benchmark database. outputs/transactions_clean.csv
-- (the real Task 2.2 deliverable) is completely untouched by this.

-- ORIGINAL QUERY (unchanged from the starter file — reproduced here for
-- reference; benchmarked as-is, not modified):
-- ---------------------------------------------------------------------------
-- SELECT
--     e.full_name, e.department, e.role, p.project_name, p.status,
--     p.budget, p.actual_cost, t.amount, t.category, t.payment_status,
--     t.transaction_date
-- FROM employees e, projects p, transactions t
-- WHERE e.employee_id = p.project_manager_id
-- AND   p.project_id  = t.project_id
-- AND   p.status NOT IN ('Completed', 'On Hold')
-- AND   t.payment_status = 'Pending'
-- AND   t.amount > (SELECT AVG(amount) FROM transactions WHERE payment_status = 'Pending')
-- ORDER BY e.department, t.amount DESC;

-- ---------------------------------------------------------------------------
-- 4a) EXPLAIN QUERY PLAN — original query, no indexes, 2,000,000 transaction rows
-- ---------------------------------------------------------------------------
-- (id, parent, notused, detail) as returned by SQLite:
--   (5,   0, 216, 'SCAN t')
--   (13,  0,   0, 'SCALAR SUBQUERY 1')
--   (18, 13, 216, 'SCAN transactions')
--   (53,  0,  53, 'SEARCH p USING AUTOMATIC PARTIAL COVERING INDEX (project_id=?)')
--   (76,  0,  53, 'SEARCH e USING AUTOMATIC COVERING INDEX (employee_id=?)')
--   (100, 0,   0, 'USE TEMP B-TREE FOR ORDER BY')
--
-- Measured: 746.8 ms best-of-2 (rows returned: 36,920). Re-running
-- benchmark_task23.py will show slightly different absolute numbers run to
-- run (system noise) — the ~2x order of magnitude and the plan shapes below
-- are what's reproducible, not the exact millisecond figures.
--
-- Bottleneck analysis:
--   - 'SCAN t' + 'SCAN transactions': the transactions table is scanned
--     TWICE in full — once for the main FROM-list row source, once again
--     inside the scalar subquery to compute AVG(amount). At 2M rows this is
--     the dominant cost. Neither scan can use an index because there is none.
--   - IMPORTANT CORRECTNESS NOTE on "is the correlated subquery re-executed
--     per row?": as written, `t.amount > (SELECT AVG(amount) FROM
--     transactions WHERE payment_status = 'Pending')` does NOT reference
--     any column from the outer query (e, p, or t) — it is a constant,
--     NON-correlated scalar subquery. SQLite's planner correctly recognises
--     this (it appears exactly once in the plan as 'SCALAR SUBQUERY 1',
--     not once per outer row) and computes it a single time. A truly
--     correlated version (e.g. an average scoped to each t.project_id)
--     WOULD be re-evaluated per outer row and would be far more expensive —
--     but that is not what this query does. Calling it out explicitly here
--     because assuming it without checking the plan would be the kind of
--     mistake this exercise is designed to catch.
--   - SQLite is already choosing reasonable automatic indexes for the
--     project_id/employee_id lookups (temporary structures built just for
--     this query) — the implicit `FROM A, B, C` comma-join syntax is NOT
--     itself a performance problem here: SQLite (like Postgres and MySQL 8)
--     parses comma-joins into the identical join tree as explicit INNER
--     JOINs and reorders them the same way. Converting the syntax alone
--     (4b's first bullet) is a readability/maintainability win, not a
--     measured speed win — verified below.
--   - The real, measurable bottleneck is the absence of any index that lets
--     SQLite avoid scanning all 2,000,000 transaction rows to find the
--     ~400,000 'Pending' ones and the ~185,000 that also clear the
--     average-amount threshold.

-- ---------------------------------------------------------------------------
-- 4b) REWRITTEN QUERY
-- ---------------------------------------------------------------------------
-- Optimisations applied (4 of the suggested 5):
--   1. Explicit JOIN ... ON instead of implicit FROM A, B, C (readability;
--      confirmed above this does not change the plan SQLite picks).
--   2. The non-correlated AVG(amount) subquery is pulled into its own CTE
--      (pending_avg) computed once, joined with CROSS JOIN — this makes the
--      "compute once" behaviour explicit and self-documenting instead of
--      relying on the optimiser to infer non-correlation, and protects the
--      query if someone later touches the WHERE clause.
--   3. Predicates are pushed down into CTEs (open_projects filters status
--      early; the payment_status='Pending' filter is applied inside the
--      transactions CTE before any join) instead of filtering after an
--      implicit cross-join of all three tables.
--   4. Only the columns actually needed are selected in each CTE (no
--      SELECT *) so intermediate result sets carry less data into the joins.
--
-- The join is additionally restructured to drive FROM the small side first:
-- open_projects (~300 rows after the status filter) probes into transactions
-- via an index on (project_id, payment_status, amount), instead of starting
-- from a full scan of the (much larger) transactions table.

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
    e.full_name,
    e.department,
    e.role,
    op.project_name,
    op.status,
    op.budget,
    op.actual_cost,
    t.amount,
    t.category,
    t.payment_status,
    t.transaction_date
FROM open_projects op
JOIN transactions t
  ON t.project_id = op.project_id
 AND t.payment_status = 'Pending'
JOIN employees e ON e.employee_id = op.project_manager_id
CROSS JOIN pending_avg
WHERE t.amount > pending_avg.avg_pending_amount
ORDER BY e.department, t.amount DESC;


-- ---------------------------------------------------------------------------
-- 4c) Indexes
-- ---------------------------------------------------------------------------
-- FIRST ATTEMPT (documented because the result is instructive, not because
-- it worked): adding an index on transactions(payment_status, amount) alone,
-- WITHOUT running ANALYZE afterwards, made the query SLOWER, not faster —
-- 853.7 ms (no index) -> 1,373.9 ms, and running ANALYZE with that same
-- index made it WORSE again (3,085.5 ms). SQLite's planner, without fresh
-- statistics on the new index, chose to drive the join from `transactions`
-- via the new index and then fell back to a full unindexed scan of
-- `projects` per candidate row (500 rows x ~185,000 candidates), which is
-- far more expensive than the original plan's automatic temp-index nested
-- loop. Lesson: an index is not automatically an improvement — you must
-- re-run EXPLAIN (and usually ANALYZE) and check the resulting plan, not
-- just assume CREATE INDEX helps.
--
-- WORKING INDEX SET (verified by EXPLAIN + timing below):

CREATE INDEX idx_transactions_project_status_amount
    ON transactions (project_id, payment_status, amount);
-- Accelerates: the open_projects -> transactions join in 4b, which probes
-- "give me this project's Pending transactions and their amounts" once per
-- open project. project_id leads because that is the join key coming from
-- the small (driving) side; payment_status second narrows within a project;
-- amount is included last purely as a covering column so SQLite can read it
-- straight from the index without a lookup into the table. Write cost: one
-- extra B-tree entry per transaction insert — acceptable for a table that is
-- append-mostly (financial transactions are rarely updated after posting).

CREATE INDEX idx_transactions_payment_status_amount
    ON transactions (payment_status, amount);
-- Accelerates: the independent pending_avg CTE (AVG(amount) WHERE
-- payment_status = 'Pending'), which has no project_id in its predicate at
-- all, so it needs payment_status leading on its own. Covering on amount
-- again avoids a table lookup for the aggregate. This and the index above
-- overlap in purpose but serve two different access paths in the plan (see
-- 4d) — collapsing them into one composite index would force one of the two
-- queries to use a less selective prefix.

CREATE INDEX idx_projects_status ON projects (status);
-- Accelerates: the open_projects CTE's status filter. At only 500 rows this
-- table is nearly free to scan either way (its impact is negligible in this
-- benchmark) — included because at production scale (thousands of concurrent
-- projects) a project-status filter run on every dashboard refresh
-- benefits from not scanning the whole table.

CREATE INDEX idx_projects_project_manager_id ON projects (project_manager_id);
-- Accelerates: the open_projects -> employees join.

CREATE INDEX idx_employees_employee_id ON employees (employee_id);
-- Accelerates: the final employees lookup by employee_id. Same "low impact
-- at this table size, matters at production scale" caveat as idx_projects_status.

ANALYZE;
-- Not an index, but required alongside these indexes: SQLite's planner uses
-- sampled statistics (sqlite_stat1) to decide whether a new index is worth
-- using and in what join order. Without ANALYZE after creating indexes on a
-- freshly-loaded table, the planner is working from stale/absent statistics
-- and can pick a worse plan than having no index at all (see the "first
-- attempt" note above). In production this means: re-run ANALYZE (or your
-- engine's equivalent, e.g. Postgres's autovacuum/ANALYZE) after any bulk
-- load or schema change that adds indexes, not just once at setup.

-- ---------------------------------------------------------------------------
-- 4d) Benchmark after rewrite + indexes + ANALYZE (2,000,000 transaction rows)
-- ---------------------------------------------------------------------------
-- EXPLAIN QUERY PLAN — rewritten query, with the indexes above + ANALYZE:
--   (3,   0,   0, 'MATERIALIZE pending_avg')
--   (7,   3, 199, 'SEARCH transactions USING COVERING INDEX idx_transactions_payment_status_amount (payment_status=?)')
--   (27,  0, 105, 'SCAN projects')
--   (36,  0,  39, 'SEARCH e USING INDEX idx_employees_employee_id (employee_id=?)')
--   (42,  0, 126, 'SEARCH t USING INDEX idx_transactions_project_status_amount (project_id=? AND payment_status=?)')
--   (51,  0,  16, 'SCAN pending_avg')
--   (77,  0,   0, 'USE TEMP B-TREE FOR ORDER BY')
--
-- 'SCAN t' has become 'SEARCH t USING INDEX ... (project_id=? AND
-- payment_status=?)' — confirmation the indexes are actually being used,
-- not just present.
--
-- Timings (best-of-2, 2,000,000 transaction rows):
--   Original query, no indexes:            746.8 ms
--   Rewritten query, with indexes+ANALYZE: 356.4 ms   -> 2.10x faster
--   Original query SHAPE, same indexes:    343.4 ms   -> 2.17x faster
--
-- Honest conclusion: the query-shape rewrite (4b) contributes almost
-- nothing to raw speed on its own — SQLite's planner already handles the
-- implicit joins and the (non-correlated) subquery well. The real,
-- measurable, justified win is the composite index + ANALYZE, worth ~2.2x
-- at this scale. That is a genuine, reproducible number, not the "10x+"
-- this section's brief anticipates — and the reason is diagnosable, not
-- hand-waved: the query's filters (payment_status = 'Pending', status NOT
-- IN (...)) are only moderately selective (~20-30% of rows survive each),
-- so an index seek still has to touch and return a large fraction of the
-- table. Re-testing at 10,000,000 rows makes this explicit: the speedup
-- actually SHRINKS to ~1.5-1.6x, because the ~185,000-row (at 2M scale) to
-- 1,850,000-row (at 10M scale) result set itself, plus the final sort, comes
-- to dominate total cost regardless of how efficiently the qualifying rows
-- were located. Indexes pay off most on highly selective predicates (e.g.
-- an exact transaction_id or invoice_ref lookup); a moderately-selective
-- multi-predicate dashboard filter like this one will always have a lower
-- ceiling on achievable speedup, no matter how well-indexed.
