"""
StackUp Engineering Academy — Data Engineering Assessment
Pillar 3 — Big Data Processing
Author: maryamalshehhi

Task covered:
  Task 3.1 -> load_events() / validate_events() / five aggregation functions /
              write_parquet() / performance timing

Run:
  python solutions/submissions/maryamalshehhi/03_big_data/spark_pipeline.py

Output:
  outputs/spark/project_activity_summary/
  outputs/spark/user_activity_summary/
  outputs/spark/escalation_log/            (partitioned by severity)
  outputs/spark/daily_event_volume/        (partitioned by event_date)
  outputs/spark/peak_usage_analysis/
"""

import os
import time
import logging

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, MapType

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))
EVENTS_DIR = os.path.join(BASE_DIR, "datasets", "events_stream")
OUTPUT_DIR = os.path.join(BASE_DIR, "outputs", "spark")


# ==============================================================================
# STEP 1 — Spark session
# ==============================================================================

def get_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("PresightEventsProcessing")
        .master("local[*]")
        # spark.sql.shuffle.partitions defaults to 200 — tuned for cluster-scale
        # data, not ~100K rows on a laptop. Left at the default, every shuffle
        # (every groupBy/join/window in this pipeline) fans out into 200 tiny
        # tasks whose JVM scheduling/serialisation overhead dwarfs the actual
        # per-task work, which measured ~15 minutes for this dataset on this
        # machine. Set to roughly 2x local core count instead, which measured
        # well under a minute for the same pipeline (see the performance
        # baseline this script prints at the end for the actual, current run).
        .config("spark.sql.shuffle.partitions", os.cpu_count() or 8)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ==============================================================================
# TASK 3.1a — Load events
# ==============================================================================

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("project_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("timestamp", StringType(), True),   # parsed to TimestampType explicitly below
    StructField("payload", MapType(StringType(), StringType()), True),
])


def load_events(spark: SparkSession, events_dir: str):
    """
    Load all 12 monthly JSONL files via a wildcard path with an explicit
    schema (no inferSchema — that would force a full extra read pass over
    100K rows just to guess types). timestamp is read as STRING then cast
    to TimestampType — reading it directly as TimestampType in the schema
    would make Spark's JSON parser reject/null out any value that doesn't
    match its default timestamp format, which is riskier than an explicit
    to_timestamp() cast we control. payload values in the source JSON are
    a mix of strings, numbers and nested objects (e.g. comment_length: 100)
    — MapType(StringType, StringType) coerces every value to its string
    form, which is what the task spec asks for and keeps the schema stable
    even as new payload keys appear across event types.
    """
    logger.info("Loading events from %s", events_dir)
    wildcard_path = os.path.join(events_dir, "events_*.jsonl")
    # Resolved in Python rather than handed to Spark as a literal glob string:
    # on Windows, Spark's wildcard resolution goes through Hadoop's
    # FileSystem.globStatus -> RawLocalFileSystem.listStatus -> FileUtil.canRead,
    # which calls a native Windows JNI shim (NativeIO$Windows.access0) that
    # reliably UnsatisfiedLinkErrors on a plain winutils.exe/hadoop.dll install
    # without the matching VC++ runtime present (no admin rights available to
    # install that here). Resolving the same wildcard with Python's own glob
    # and handing Spark an explicit file list is functionally identical —
    # still "all 12 monthly files matched by events_*.jsonl" — and avoids that
    # code path entirely.
    import glob as _glob
    file_paths = sorted(_glob.glob(wildcard_path))
    if not file_paths:
        raise FileNotFoundError(f"No files matched {wildcard_path}")
    logger.info("Resolved %d files via wildcard %s", len(file_paths), wildcard_path)

    df = spark.read.schema(EVENT_SCHEMA).json(file_paths)
    df = df.withColumn("timestamp", F.to_timestamp("timestamp"))

    count = df.count()
    logger.info("Loaded %d event rows (expected ~100,000)", count)
    return df


# ==============================================================================
# TASK 3.1b — Validate and clean
# ==============================================================================

def validate_events(df):
    before = df.count()

    missing_ids = df.filter(F.col("event_id").isNull() | F.col("user_id").isNull()).count()
    df = df.filter(F.col("event_id").isNotNull() & F.col("user_id").isNotNull())
    after_null_drop = df.count()
    logger.info("Dropped %d rows with null event_id/user_id (%d -> %d)",
                missing_ids, before, after_null_drop)

    # Keep first occurrence by timestamp per event_id: rank rows within each
    # event_id by timestamp, keep rank 1. row_number() (not just dropDuplicates,
    # which keeps an arbitrary row) is what makes "first occurrence" well-defined.
    w_dedup = Window.partitionBy("event_id").orderBy(F.col("timestamp").asc())
    df = (
        df.withColumn("_rn", F.row_number().over(w_dedup))
          .filter(F.col("_rn") == 1)
          .drop("_rn")
    )
    after_dedup = df.count()
    logger.info("Dropped %d duplicate event_id rows (%d -> %d)",
                after_null_drop - after_dedup, after_null_drop, after_dedup)

    df = (
        df.withColumn("event_date", F.to_date("timestamp"))
          .withColumn("event_hour", F.hour("timestamp"))
          .withColumn("event_month", F.date_format("timestamp", "yyyy-MM"))
    )
    return df


# ==============================================================================
# TASK 3.1c — Five aggregated tables
# ==============================================================================

def project_activity_summary(df):
    return (
        df.filter(F.col("project_id").isNotNull())
          .groupBy("project_id")
          .agg(
              F.count("*").alias("total_events"),
              F.sum(F.when(F.col("event_type") == "escalation_raised", 1).otherwise(0)).alias("escalation_count"),
              F.sum(F.when(F.col("event_type") == "task_completed", 1).otherwise(0)).alias("task_completions"),
              F.sum(F.when(F.col("event_type") == "document_uploaded", 1).otherwise(0)).alias("document_uploads"),
              F.max("timestamp").alias("last_event_timestamp"),
              F.countDistinct("user_id").alias("unique_users"),
              F.countDistinct("event_type").alias("unique_event_types"),
          )
          .orderBy(F.col("total_events").desc())
    )


def user_activity_summary(df):
    return (
        df.groupBy("user_id")
          .agg(
              F.sum(F.when(F.col("event_type") == "login", 1).otherwise(0)).alias("login_count"),
              F.sum(F.when(F.col("event_type") == "logout", 1).otherwise(0)).alias("logout_count"),
              F.sum(F.when(~F.col("event_type").isin("login", "logout"), 1).otherwise(0)).alias("actions_taken"),
              F.countDistinct(F.when(F.col("project_id").isNotNull(), F.col("project_id"))).alias("projects_touched"),
              F.min("timestamp").alias("first_active"),
              F.max("timestamp").alias("last_active"),
              F.countDistinct("event_date").alias("active_days"),
          )
          .orderBy(F.col("actions_taken").desc())
    )


def escalation_log(df):
    """
    Pairs each escalation_raised with its escalation_resolved using
    chronological sequence per project rather than a plain join on
    project_id: 461 of ~500 projects have MORE THAN ONE raised escalation
    (up to 10 for a single project), so project_id alone would fan out
    every raise against every resolve for that project. Instead, each side
    gets a per-project sequence number ordered by timestamp (1st raise,
    2nd raise, ...), and the join matches raise #N to resolve #N for the
    same project — a FIFO assumption (escalations are worked and resolved
    in the order they were raised), which is the only ordering the source
    data actually supports. A left join keeps still-open escalations with
    resolved = False.
    """
    raised = (
        df.filter(F.col("event_type") == "escalation_raised")
          .select(
              F.col("event_id"),
              F.col("project_id"),
              F.col("user_id").alias("raised_by"),
              F.col("timestamp").alias("raised_at"),
              F.col("payload").getItem("severity").alias("severity"),
          )
    )
    raised = raised.withColumn(
        "_seq", F.row_number().over(Window.partitionBy("project_id").orderBy("raised_at"))
    )

    resolved = (
        df.filter(F.col("event_type") == "escalation_resolved")
          .select(
              F.col("project_id"),
              F.col("timestamp").alias("resolved_at"),
              F.col("payload").getItem("resolved_by").alias("resolved_by"),
          )
    )
    resolved = resolved.withColumn(
        "_seq", F.row_number().over(Window.partitionBy("project_id").orderBy("resolved_at"))
    )

    joined = raised.join(resolved, on=["project_id", "_seq"], how="left")

    return (
        joined
        .withColumn("resolved", F.col("resolved_at").isNotNull())
        .withColumn(
            "resolution_time_hours",
            F.when(
                F.col("resolved_at").isNotNull(),
                (F.col("resolved_at").cast("long") - F.col("raised_at").cast("long")) / 3600.0,
            ),
        )
        .select(
            "event_id", "project_id", "raised_by", "raised_at", "severity",
            "resolved", "resolved_by", "resolved_at", "resolution_time_hours",
        )
    )


def daily_event_volume(df):
    daily = (
        df.groupBy("event_date", "event_type")
          .agg(F.count("*").alias("event_count"))
    )
    w_cum = Window.partitionBy("event_type").orderBy("event_date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    daily = daily.withColumn("cumulative_count", F.sum("event_count").over(w_cum))
    return daily.orderBy(F.col("event_date").asc(), F.col("event_count").desc())


def peak_usage_analysis(df):
    return (
        df.groupBy("event_date", "event_hour")
          .agg(
              F.count("*").alias("total_events"),
              F.countDistinct("user_id").alias("unique_users"),
              F.countDistinct("event_type").alias("event_types_per_hour"),
          )
          .orderBy(F.col("total_events").desc())
          .limit(20)
    )


# ==============================================================================
# TASK 3.1d — Write outputs
# ==============================================================================

def write_parquet(df, name: str, output_dir: str, partition_by=None, small=False):
    path = os.path.join(output_dir, name)
    writer = df.coalesce(1) if small else df
    writer = writer.write.mode("overwrite")
    if partition_by:
        writer = writer.partitionBy(partition_by)
    writer.parquet(path)
    n = df.count()
    logger.info("Wrote %s (%d rows)%s", path, n, f" partitioned by {partition_by}" if partition_by else "")
    return n


# ==============================================================================
# PIPELINE ENTRY POINT — TASK 3.1e performance baseline
# ==============================================================================

def run_pipeline():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    pipeline_start = time.time()
    timings = {}

    spark = get_spark_session()

    t0 = time.time()
    raw = load_events(spark, EVENTS_DIR)
    clean = validate_events(raw)
    clean.cache()
    total_rows = clean.count()
    timings["load_and_validate"] = time.time() - t0

    t0 = time.time()
    proj_summary = project_activity_summary(clean)
    n_proj = write_parquet(proj_summary, "project_activity_summary", OUTPUT_DIR, small=True)
    timings["project_activity_summary"] = time.time() - t0

    t0 = time.time()
    user_summary = user_activity_summary(clean)
    n_user = write_parquet(user_summary, "user_activity_summary", OUTPUT_DIR, small=True)
    timings["user_activity_summary"] = time.time() - t0

    t0 = time.time()
    esc_log = escalation_log(clean)
    n_esc = write_parquet(esc_log, "escalation_log", OUTPUT_DIR, partition_by="severity")
    timings["escalation_log"] = time.time() - t0

    t0 = time.time()
    daily_vol = daily_event_volume(clean)
    n_daily = write_parquet(daily_vol, "daily_event_volume", OUTPUT_DIR, partition_by="event_date")
    timings["daily_event_volume"] = time.time() - t0

    t0 = time.time()
    peak_usage = peak_usage_analysis(clean)
    n_peak = write_parquet(peak_usage, "peak_usage_analysis", OUTPUT_DIR, small=True)
    timings["peak_usage_analysis"] = time.time() - t0

    total_time = time.time() - pipeline_start

    logger.info("=" * 60)
    logger.info("PERFORMANCE BASELINE")
    logger.info("=" * 60)
    for stage, secs in timings.items():
        logger.info("  %-28s %6.2f s", stage, secs)
    logger.info("  %-28s %6.2f s", "TOTAL", total_time)
    logger.info("Total rows processed: %d", total_rows)
    logger.info("Throughput: %.0f events/second", total_rows / total_time)
    logger.info("Output row counts: project=%d user=%d escalation=%d daily=%d peak=%d",
                n_proj, n_user, n_esc, n_daily, n_peak)

    spark.stop()
    logger.info("Spark pipeline complete.")


if __name__ == "__main__":
    run_pipeline()
