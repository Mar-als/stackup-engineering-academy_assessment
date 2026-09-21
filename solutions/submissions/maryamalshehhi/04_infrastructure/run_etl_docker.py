"""
StackUp Engineering Academy — Data Engineering Assessment
Pillar 4 — Infrastructure & Governance
Author: maryamalshehhi

Task covered:
  Task 4.1 -> container entry point for the full ETL (Task 2.2's pipeline,
              which is Pillar 1's projects/employees cleaning plus Pillar 2's
              transactions enrichment run back-to-back)

This is the script the Dockerfile's ENTRYPOINT runs. It does not hardcode
paths the way the individual Pillar 1/2 etl_pipeline.py run_pipeline()
functions do (those derive paths from the repo's on-disk layout, which
doesn't exist the same way inside a container) — instead it reads DATA_DIR
and OUTPUT_DIR from the environment, defaulting to the container's expected
mount points, and passes explicit paths into each pipeline's individual
functions (which already take a filepath parameter — only their
module-level run_pipeline() entry points assume the repo layout).

Run locally (outside Docker) to sanity-check before building the image:
  python solutions/submissions/maryamalshehhi/04_infrastructure/run_etl_docker.py
"""

import os
import sys
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))

DATA_DIR = os.environ.get("DATA_DIR", os.path.join(REPO_ROOT, "datasets"))
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", os.path.join(REPO_ROOT, "outputs"))

sys.path.insert(0, os.path.join(REPO_ROOT, "solutions", "submissions", "maryamalshehhi", "01_foundations"))
import etl_pipeline as pillar1  # noqa: E402

import importlib.util as _ilu


def _load_module(name, path):
    spec = _ilu.spec_from_file_location(name, path)
    mod = _ilu.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# Pillar 1 and Pillar 2 both define a module literally named "etl_pipeline" —
# load Pillar 2's by explicit file path so it doesn't shadow (or get shadowed
# by) the Pillar 1 import above.
pillar2 = _load_module(
    "etl_pipeline_pillar2",
    os.path.join(REPO_ROOT, "solutions", "submissions", "maryamalshehhi", "02_sql_and_viz", "etl_pipeline.py"),
)


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    logger.info("DATA_DIR=%s", DATA_DIR)
    logger.info("OUTPUT_DIR=%s", OUTPUT_DIR)

    raw_projects = pillar1.load_projects(os.path.join(DATA_DIR, "projects.csv"))
    clean_projects = pillar1.transform_projects(raw_projects)

    raw_employees = pillar1.load_employees(os.path.join(DATA_DIR, "employees.csv"))
    clean_employees = pillar1.clean_employees(raw_employees)

    clean_projects.to_csv(os.path.join(OUTPUT_DIR, "projects_clean.csv"), index=False)
    clean_employees.to_csv(os.path.join(OUTPUT_DIR, "employees_clean.csv"), index=False)
    logger.info("Wrote projects_clean.csv (%d rows) and employees_clean.csv (%d rows)",
                len(clean_projects), len(clean_employees))

    raw_transactions = pillar2.load_transactions(os.path.join(DATA_DIR, "transactions.json"))
    enriched_txn = pillar2.enrich_transactions(raw_transactions, clean_projects, clean_employees)
    enriched_txn.to_csv(os.path.join(OUTPUT_DIR, "transactions_clean.csv"), index=False)
    logger.info("Wrote transactions_clean.csv (%d rows)", len(enriched_txn))

    logger.info("Container ETL run complete.")


if __name__ == "__main__":
    main()
