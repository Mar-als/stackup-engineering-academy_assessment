-- =============================================================================
-- StackUp Engineering Academy — Data Engineering Assessment
-- Pillar 1, Task 1.2 — Star Schema with SCD Type 2 dim_employee
-- Author: maryamalshehhi
-- Dialect: DuckDB (also runs, with minor tweaks noted inline, on Postgres)
--
-- PREREQUISITE: run solutions/submissions/maryamalshehhi/01_foundations/etl_pipeline.py
-- first so outputs/projects_clean.csv and outputs/employees_clean.csv exist.
--
-- HOW TO RUN (from the repo root):
--   duckdb warehouse.db < solutions/submissions/maryamalshehhi/01_foundations/data_model.sql
-- =============================================================================


-- =============================================================================
-- SECTION 1 — DDL
-- =============================================================================

-- dim_date: a full calendar date dimension rather than a bare DATE column so
-- reporting queries can group by quarter/month-name/weekend without repeating
-- EXTRACT() logic everywhere. date_key is YYYYMMDD as an INTEGER: it sorts
-- correctly, joins as a plain integer (fast), and is human-readable in ad-hoc
-- queries — a common warehouse convention.
-- Surrogate keys below are assigned with ROW_NUMBER() at load time rather
-- than sequences, since every dimension is fully reloaded in one INSERT.
-- DROPs make this script safe to re-run from scratch.
DROP TABLE IF EXISTS fact_transactions;
DROP TABLE IF EXISTS bridge_employee_project;
DROP TABLE IF EXISTS dim_vendor;
DROP TABLE IF EXISTS dim_employee;
DROP TABLE IF EXISTS dim_project;
DROP TABLE IF EXISTS dim_date;

CREATE TABLE dim_date (
    date_key    INTEGER PRIMARY KEY,      -- YYYYMMDD
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

-- dim_project: one row per project. Surrogate project_key insulates the
-- warehouse from the source project_id ever being reused/renumbered;
-- project_id is kept as the natural key for traceability back to the source
-- system. Sourced from the already-cleaned Task 1.1 output so budget/actual
-- nulls, status_category and risk_level are already resolved.
CREATE TABLE dim_project (
    project_key         INTEGER PRIMARY KEY,
    project_id          VARCHAR NOT NULL UNIQUE,
    project_name        VARCHAR,
    department          VARCHAR,
    status              VARCHAR,
    status_category     VARCHAR,
    priority            VARCHAR,
    region              VARCHAR,
    project_manager_id  VARCHAR,          -- natural key, resolved to dim_employee via bridge/point-in-time join
    start_date          DATE,
    end_date            DATE,
    budget              DECIMAL(14, 2),
    actual_cost         DECIMAL(14, 2),
    risk_level          VARCHAR
);

-- dim_employee (SCD TYPE 2): salary, role and level change over an employee's
-- tenure (see employees_salary_history.csv), so a single current-state row
-- would silently misattribute historical transactions/facts to today's
-- salary. Each row is one version of an employee; employee_key is the
-- surrogate identifying that *version*, employee_id is the natural key
-- identifying the *person* across all their versions.
-- department/manager_id/region/status are NOT tracked as SCD2 attributes
-- because the source history file only records role/level/salary changes —
-- documented assumption, not an oversight.
CREATE TABLE dim_employee (
    employee_key    INTEGER PRIMARY KEY,
    employee_id     VARCHAR NOT NULL,     -- natural key, repeats across versions
    full_name       VARCHAR,
    email           VARCHAR,
    department      VARCHAR,
    role            VARCHAR,
    level           VARCHAR,
    salary          DECIMAL(12, 2),
    manager_id      VARCHAR,
    region          VARCHAR,
    status          VARCHAR,
    change_reason   VARCHAR,              -- bonus: carried over from the history file
    valid_from      DATE NOT NULL,
    valid_to        DATE NOT NULL,        -- 9999-12-31 sentinel for the current version
    is_current      BOOLEAN NOT NULL
);

-- dim_vendor: vendors only appear embedded in transactions.json (no separate
-- vendor master file), so this dimension is derived by de-duplicating
-- vendor_id/vendor_name pairs out of the fact source.
CREATE TABLE dim_vendor (
    vendor_key   INTEGER PRIMARY KEY,
    vendor_id    VARCHAR NOT NULL UNIQUE,
    vendor_name  VARCHAR
);

-- bridge_employee_project: resolves the employee <-> project many-to-many
-- relationship. The source data has no explicit project-team roster, so this
-- is derived from two observable relationships and tagged with project_role
-- so the two are never confused downstream:
--   'Manager'  — dim_project.project_manager_id
--   'Approver' — any employee who appears as approved_by on a transaction
--                for that project (a proxy for "touched this project")
-- This is a documented modelling assumption, not source-verified team data.
CREATE TABLE bridge_employee_project (
    employee_key  INTEGER REFERENCES dim_employee(employee_key),
    project_key   INTEGER REFERENCES dim_project(project_key),
    project_role  VARCHAR NOT NULL,
    PRIMARY KEY (employee_key, project_key, project_role)
);

-- fact_transactions: one row per financial transaction. employee_key is
-- resolved to whichever dim_employee VERSION was current on the
-- transaction_date (point-in-time SCD2 join) — not just "today's" version —
-- so a transaction approved in 2022 reports against that employee's 2022
-- role/level/salary, matching how SCD2 is meant to be used.
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
-- SECTION 2 — Load staging data & populate the warehouse
-- =============================================================================

-- ---- dim_date: one row per day across the range the source data spans ----
INSERT INTO dim_date
SELECT
    CAST(strftime(d, '%Y%m%d') AS INTEGER)  AS date_key,
    d                                       AS full_date,
    EXTRACT(YEAR FROM d)                    AS year,
    EXTRACT(QUARTER FROM d)                 AS quarter,
    EXTRACT(MONTH FROM d)                   AS month,
    strftime(d, '%B')                       AS month_name,
    EXTRACT(WEEK FROM d)                    AS week,
    EXTRACT(DAY FROM d)                     AS day,
    strftime(d, '%A')                       AS day_of_week,
    EXTRACT(ISODOW FROM d) IN (6, 7)        AS is_weekend
FROM generate_series(DATE '2000-01-01', DATE '2027-12-31', INTERVAL 1 DAY) AS t(d);

-- ---- dim_project: straight load from the Task 1.1 cleaned output ----
INSERT INTO dim_project
SELECT
    ROW_NUMBER() OVER (ORDER BY project_id)  AS project_key,
    project_id, project_name, department, status, status_category,
    priority, region, project_manager_id, start_date, end_date,
    budget, actual_cost, risk_level
FROM read_csv_auto('outputs/projects_clean.csv');

-- ---- dim_vendor: distinct vendors seen in the transactions feed ----
INSERT INTO dim_vendor
SELECT
    ROW_NUMBER() OVER (ORDER BY vendor_id)  AS vendor_key,
    vendor_id, vendor_name
FROM (SELECT DISTINCT vendor_id, vendor_name FROM read_json_auto('datasets/transactions.json'));

-- ---- dim_employee: SCD2 build from employees_clean.csv + salary history ----

-- Versions coming from the history file: each history row's new_* values are
-- the state that became effective on effective_date and lasted until the
-- *next* recorded change for that employee (LEAD), or forever (9999-12-31)
-- if it's their most recent change.
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

-- Employees who never appear in the history file get a single version
-- spanning their entire tenure, sourced from the cleaned current-state file.
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

-- ---- bridge_employee_project ----
INSERT INTO bridge_employee_project
SELECT de.employee_key, dp.project_key, 'Manager'
FROM dim_project dp
JOIN dim_employee de
  ON de.employee_id = dp.project_manager_id AND de.is_current = TRUE;

-- DISTINCT + project_role='Approver' keeps this insert's rows unique on their
-- own; the 'Manager' insert above uses a different project_role, so no
-- primary-key clash is possible between the two statements.
INSERT INTO bridge_employee_project
SELECT DISTINCT de.employee_key, dp.project_key, 'Approver'
FROM read_json_auto('datasets/transactions.json') t
JOIN dim_project dp ON dp.project_id = t.project_id
JOIN dim_employee de ON de.employee_id = t.approved_by AND de.is_current = TRUE
WHERE t.approved_by IS NOT NULL;

-- ---- fact_transactions ----
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
-- SECTION 3 — SCD2 validation queries (required by Task 1.2)
-- =============================================================================

-- Q1: No employee has more than one current record. Must return zero rows.
SELECT employee_id, COUNT(*) AS current_count
FROM dim_employee
WHERE is_current = TRUE
GROUP BY employee_id
HAVING COUNT(*) > 1;

-- Q2: Employees with the most version history (expect 2-5 versions each,
-- matching employees_salary_history.csv's ~60% coverage of the workforce).
SELECT employee_id, COUNT(*) AS version_count
FROM dim_employee
GROUP BY employee_id
ORDER BY version_count DESC
LIMIT 10;

-- Q3: Self-join to detect overlapping validity periods per employee.
-- Two DIFFERENT versions of the same employee overlap if one starts before
-- the other ends and vice versa. employee_key < employee_key avoids matching
-- a row against itself and de-duplicates each pair (a,b) vs (b,a).
-- Must return zero rows if the SCD2 build is correct.
SELECT
    a.employee_id,
    a.employee_key AS version_a, a.valid_from AS a_valid_from, a.valid_to AS a_valid_to,
    b.employee_key AS version_b, b.valid_from AS b_valid_from, b.valid_to AS b_valid_to
FROM dim_employee a
JOIN dim_employee b
  ON a.employee_id = b.employee_id
 AND a.employee_key < b.employee_key
 AND a.valid_from < b.valid_to
 AND b.valid_from < a.valid_to;
