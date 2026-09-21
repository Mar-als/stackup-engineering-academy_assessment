# =============================================================================
# StackUp Engineering Academy — Data Engineering Assessment
# Pillar 4, Task 4.1 — Containerised ETL pipeline
# Author: maryamalshehhi
#
# Multi-stage build: the "builder" stage compiles/installs Python packages
# into an isolated prefix; the final stage copies only the installed
# packages plus the source code, without pip's build caches, wheel files
# or compiler toolchains — the runtime image never contains anything the
# builder stage downloaded to build packages, only what's needed to run.
#
# BUILD:  docker build -t presight-etl .
# RUN:    docker run -v "$(pwd)/outputs:/app/outputs" presight-etl
# TIME:   time docker run -v "$(pwd)/outputs:/app/outputs" presight-etl
#
# NOT LIVE-VERIFIED IN THIS SESSION — no Docker on this machine (see
# solutions/submissions/maryamalshehhi/03_big_data/ for the same
# environment limitation on Kafka/Airflow). Build and run this yourself
# once Docker Desktop is installed; this file is written to work but has
# not been build-tested.
# =============================================================================

# ---- Stage 1: builder ------------------------------------------------------
FROM python:3.11-slim AS builder

WORKDIR /build

# Only requirements.txt is needed to resolve/install dependencies — copying
# it alone (before the rest of the source) means Docker's layer cache is
# reused on every rebuild that doesn't touch requirements.txt, even if the
# pipeline code changes.
COPY requirements.txt .

# NOTE ON SCOPE: requirements.txt also lists apache-airflow, pyspark and
# great-expectations, none of which run_etl_docker.py (this container's
# entrypoint) actually imports — they're needed for Pillars 3/4's other
# tasks, not this one. Installing the full file per Task 4.1's literal
# instruction ("install dependencies from requirements.txt") makes this a
# one-time slow, large build; it does not affect the timed criterion, which
# is the ETL's own run time inside the already-built container. A
# production image for just this pipeline would instead pin a scoped
# requirements-etl.txt (pandas, numpy only) to keep both the build and the
# image small.
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# ---- Stage 2: runtime -------------------------------------------------------
FROM python:3.11-slim

WORKDIR /app

# Copy only the installed packages from the builder stage — none of pip's
# download cache, no compilers, no intermediate wheel files.
COPY --from=builder /install /usr/local

# Pipeline code and the data it needs. datasets/ is copied in (read-only
# input data baked into the image); outputs/ is NOT copied — it is written
# at runtime to whatever volume is mounted at /app/outputs.
COPY datasets/ /app/datasets/
COPY solutions/ /app/solutions/

# Defaults match the volume-mount points documented in the run command
# above; both are overridable at `docker run -e DATA_DIR=... -e OUTPUT_DIR=...`.
ENV DATA_DIR=/app/datasets
ENV OUTPUT_DIR=/app/outputs
ENV PYTHONUNBUFFERED=1

RUN mkdir -p /app/outputs

ENTRYPOINT ["python", "solutions/submissions/maryamalshehhi/04_infrastructure/run_etl_docker.py"]
