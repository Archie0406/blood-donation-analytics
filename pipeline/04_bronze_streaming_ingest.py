"""
Enterprise Rare Blood Type Availability Analytics
Phase 5 — Bronze Layer: Streaming Data Ingestion
=====================================================

ARCHITECTURE CONTEXT
----------------------
    Phase 4                          Phase 5 (this script)
    Python Live Donation Generator
              |
              v
    JSON Landing Zone                Spark Structured Streaming
    data/streaming/landing/  ----->  reads new files every trigger interval
              |                                |
              |                                v
              |                      Bronze Streaming Parquet
              |                      data/bronze/streaming_donations/
              |                                |
              |                                v
              |                      Checkpoint
              |                      data/checkpoints/bronze_streaming_donations/

Phase 4 and Phase 5 run as two INDEPENDENT processes. The generator writes
files; this script watches the same directory and picks up new files on its
own schedule (the `--trigger` interval). Neither process needs the other to
be running -- Phase 5 will simply pick up whatever new files exist since the
last processed checkpoint.

BRONZE PRINCIPLE (same as Phase 3, applied to streaming data)
-----------------------------------------------------------------
    BRONZE STREAMING = RAW JSON EVENT + STREAMING INGESTION METADATA

This script performs ONLY technical streaming ingestion:
    - Watch data/streaming/landing/ for new JSON files.
    - Parse them with an explicit schema (no inference).
    - Add _ingested_at / _ingestion_date / _source_file metadata.
    - Append the result to Bronze Streaming Parquet.
    - Maintain a Spark checkpoint so files are never reprocessed.

It does NOT deduplicate, filter rare blood groups, join with other
datasets, calculate inventory or shortage status, or compute any KPIs.
Those responsibilities belong to the Silver and Gold layers in later phases.

KEY STREAMING CONCEPTS (for reference while reading the code)
-------------------------------------------------------------------
  Structured Streaming : Spark's engine for treating a stream of arriving
                          files (or other sources) as an unbounded table,
                          processed incrementally in small chunks.
  Micro-batch           : Each time the trigger fires, Spark gathers whatever
                          new files have arrived since the last check and
                          processes them together as one "batch".
  Checkpoint             : A directory where Spark records which files/offsets
                          it has already processed. On restart, Spark reads
                          the checkpoint and resumes from where it left off,
                          so files are never re-ingested and no progress is
                          lost.
  Append mode            : Only new rows are ever written to the sink. This
                          suits Bronze because every donation event is an
                          immutable fact -- nothing is updated in place.
  processingTime trigger : Tells Spark how often to check for new files
                          (e.g. every 5 seconds), rather than reacting to
                          every single file the instant it lands.
  event_time vs _ingested_at:
      event_time    = when the donation was simulated to have happened
                       (set by the Phase 4 generator).
      _ingested_at  = when THIS Spark job actually processed the record.
      These will usually be close together in a demo, but in a real system
      they can differ significantly (e.g. a backlog of unprocessed files),
      which is exactly why both are kept separately.
  Explicit schema vs inference:
      inferSchema requires Spark to read data up front to guess types, which
      is not possible for an open-ended stream (new files keep arriving) and
      would make the schema unstable across batches. An explicit schema is
      required for a streaming JSON source and keeps the Bronze contract
      fixed and predictable.
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_date, current_timestamp, input_file_name
from pyspark.sql.streaming import StreamingQuery, StreamingQueryListener
from pyspark.sql.types import (
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ============================================================================
# CONFIGURATION / PATHS
# ============================================================================

APP_NAME = "Enterprise Rare Blood Analytics - Bronze Streaming"
QUERY_NAME = "bronze_blood_donation_stream"

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent

DEFAULT_INPUT_PATH = PROJECT_ROOT / "data" / "streaming" / "landing"
DEFAULT_OUTPUT_PATH = PROJECT_ROOT / "data" / "bronze" / "streaming_donations"
DEFAULT_CHECKPOINT_PATH = PROJECT_ROOT / "data" / "checkpoints" / "bronze_streaming_donations"

DEFAULT_MINUTES = 3.0
DEFAULT_TRIGGER_SECONDS = 5

# Bronze ingestion metadata column names (matches Phase 3's convention)
COL_INGESTED_AT = "_ingested_at"
COL_SOURCE_FILE = "_source_file"
COL_INGESTION_DATE = "_ingestion_date"
METADATA_COLUMNS = [COL_INGESTED_AT, COL_SOURCE_FILE, COL_INGESTION_DATE]

RARE_BLOOD_GROUPS = ["O-", "AB-", "B-", "A-"]  # used only for validation/demo printout

# ============================================================================
# STREAMING SCHEMA (must match the flat JSON produced by Phase 4)
# ============================================================================

STREAMING_DONATION_SCHEMA = StructType([
    StructField("donation_id", StringType(), True),
    StructField("donor_id", StringType(), True),
    StructField("blood_bank_id", StringType(), True),
    StructField("camp_id", StringType(), True),
    StructField("donation_date", DateType(), True),
    StructField("blood_group", StringType(), True),
    StructField("units_donated", IntegerType(), True),
    StructField("donation_status", StringType(), True),
    StructField("event_time", TimestampType(), True),
    StructField("source", StringType(), True),
    StructField("event_type", StringType(), True),
])


# ============================================================================
# GRACEFUL CTRL+C HANDLING
# ============================================================================
#
# NOTE: We deliberately do NOT install a custom signal.signal(SIGINT, ...)
# handler here. query.awaitTermination() is a blocking call into the JVM via
# py4j, and the JVM installs its own signal handling underneath -- a custom
# Python-level SIGINT handler is often not reliably delivered while that
# blocking call is in progress. Instead, we poll awaitTermination() in short
# (1 second) slices in a plain Python loop. Between each poll, control
# returns fully to the Python interpreter, so Python's DEFAULT SIGINT
# behavior (raising KeyboardInterrupt on the main thread) fires reliably and
# is simply caught with a normal try/except.
#
# ============================================================================
# STREAMING QUERY PROGRESS LISTENER (console monitoring, no business logic)
# ============================================================================

class BronzeStreamingListener(StreamingQueryListener):
    """Prints a short, readable line per micro-batch. Never prints full event
    contents -- only batch id and record counts, per the Bronze principle of
    staying lightweight and non-business-facing."""

    def onQueryStarted(self, event) -> None:  # noqa: N802
        print(f"Streaming query started: {event.name} (id={event.id})\n")

    def onQueryProgress(self, event) -> None:  # noqa: N802
        progress = event.progress
        num_rows = progress.numInputRows
        print("-" * 47)
        print("Streaming Batch")
        print(f"Batch ID: {progress.batchId}")
        print(f"Records processed: {num_rows}")
        print("-" * 47 + "\n")

    def onQueryTerminated(self, event) -> None:  # noqa: N802
        print("Streaming query terminated.\n")

    def onQueryIdle(self, event) -> None:  # noqa: N802
        pass


# ============================================================================
# SPARK SESSION
# ============================================================================

def create_spark_session() -> SparkSession:
    """Create a local-mode SparkSession sized appropriately for a laptop."""
    try:
        spark = (
            SparkSession.builder.appName(APP_NAME)
            .master("local[*]")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.streaming.schemaInference", "false")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")
        return spark
    except Exception as exc:  # noqa: BLE001
        print(f"ERROR: Failed to initialize SparkSession: {exc}")
        raise


# ============================================================================
# INPUT / OUTPUT PREPARATION
# ============================================================================

def ensure_directories(input_path: Path, output_path: Path, checkpoint_path: Path) -> None:
    """Make sure the landing directory exists (auto-created if missing, since
    a fresh project may not have run Phase 4 yet) and that the output and
    checkpoint parents exist. Spark will create the output/checkpoint
    directories themselves when the query starts -- we just prepare parents.
    """
    if not input_path.exists():
        print(f"Input directory not found. Creating it now: {input_path}")
        input_path.mkdir(parents=True, exist_ok=True)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)


# ============================================================================
# STREAM DEFINITION
# ============================================================================

def read_donation_stream(spark: SparkSession, input_path: Path) -> DataFrame:
    """Read new-arriving JSON donation events using Spark Structured Streaming
    with an explicit schema (schema inference is not usable for an
    open-ended, continuously-arriving stream)."""
    # multiLine=true is required because Phase 4 writes each event as a
    # pretty-printed (indented, multi-line) JSON object rather than a single
    # compact line. Spark's JSON source defaults to one-JSON-object-per-line
    # (JSON Lines); without multiLine, each line of a pretty-printed file
    # would be parsed as a separate, mostly-corrupt record.
    return (
        spark.readStream.schema(STREAMING_DONATION_SCHEMA)
        .option("multiLine", "true")
        .option("maxFilesPerTrigger", 1000)
        .json(str(input_path))
    )


def add_streaming_metadata(stream_df: DataFrame) -> DataFrame:
    """Attach Bronze streaming ingestion metadata without altering any of the
    original event fields.

    _ingested_at    : timestamp this record was processed by this streaming job
    _source_file    : the landing-zone JSON file this record came from
    _ingestion_date : date this record was processed
    """
    return (
        stream_df.withColumn(COL_INGESTED_AT, current_timestamp())
        .withColumn(COL_SOURCE_FILE, input_file_name())
        .withColumn(COL_INGESTION_DATE, current_date())
    )


def start_streaming_query(
    bronze_stream_df: DataFrame,
    output_path: Path,
    checkpoint_path: Path,
    trigger_seconds: int,
) -> StreamingQuery:
    """Start the writeStream query: append-only Parquet, with a checkpoint.

    Append mode is used because every donation event is an immutable fact --
    Bronze Streaming only ever adds new rows, never updates or replaces them.
    coalesce(1) is intentionally NOT used: Bronze should behave like a normal
    streaming data lake, producing standard Spark part files per micro-batch.
    """
    return (
        bronze_stream_df.writeStream.format("parquet")
        .outputMode("append")
        .option("path", str(output_path))
        .option("checkpointLocation", str(checkpoint_path))
        .trigger(processingTime=f"{trigger_seconds} seconds")
        .queryName(QUERY_NAME)
        .start()
    )


# ============================================================================
# VALIDATION (post-run, technical + light demo-only distribution printout)
# ============================================================================

def validate_bronze_streaming_output(spark: SparkSession, output_path: Path, checkpoint_path: Path) -> bool:
    """Run basic technical validation of the Bronze Streaming output.

    The rare-blood distribution printed here is for DEMO/VALIDATION purposes
    only -- it is not a Gold table, and no filtering, aggregation for
    business use, or inventory logic is stored anywhere.
    """
    print("\nValidating Bronze streaming output...\n")

    if not output_path.exists() or not any(output_path.glob("*.parquet")):
        print("Bronze streaming validation: FAIL — no Parquet output found "
              "(no events may have arrived during this run).")
        return False

    if not checkpoint_path.exists():
        print("Bronze streaming validation: FAIL — checkpoint directory not found.")
        return False

    df = spark.read.parquet(str(output_path))
    total_records = df.count()

    print(f"Total records ingested: {total_records}")

    if total_records == 0:
        print("Bronze streaming validation: FAIL — zero records ingested.")
        return False

    expected_business_columns = {field.name for field in STREAMING_DONATION_SCHEMA.fields}
    actual_columns = set(df.columns)

    missing_business = expected_business_columns - actual_columns
    missing_metadata = set(METADATA_COLUMNS) - actual_columns
    if missing_business or missing_metadata:
        print(f"Bronze streaming validation: FAIL — missing columns: "
              f"{missing_business | missing_metadata}")
        return False

    null_ids = df.filter(col("donation_id").isNull()).count()
    null_event_times = df.filter(col("event_time").isNull()).count()
    if null_ids > 0 or null_event_times > 0:
        print(f"Bronze streaming validation: FAIL — {null_ids} null donation_id, "
              f"{null_event_times} null event_time records found.")
        return False

    print("\nSchema:")
    df.printSchema()

    print("Sample records (first 10):")
    df.orderBy(col(COL_INGESTED_AT)).show(10, truncate=False)

    print("Blood group distribution (validation/demo only, NOT a Gold table):")
    df.groupBy("blood_group").count().orderBy("blood_group").show(truncate=False)

    rare_count = df.filter(col("blood_group").isin(RARE_BLOOD_GROUPS)).count()
    print(f"Rare blood records (O-, AB-, B-, A- combined): {rare_count}")

    print("Bronze streaming validation: PASS")
    return True


# ============================================================================
# CONSOLE OUTPUT
# ============================================================================

def print_header(input_path: Path, output_path: Path, checkpoint_path: Path,
                  trigger_seconds: int, minutes: float) -> None:
    print("=" * 45)
    print("BRONZE STREAMING INGESTION")
    print("Enterprise Rare Blood Type Availability Analytics")
    print("=" * 45)
    print(f"\nInput:\n{input_path}")
    print(f"\nOutput:\n{output_path}")
    print(f"\nCheckpoint:\n{checkpoint_path}")
    print(f"\nTrigger:\n{trigger_seconds} seconds")
    print(f"\nDuration:\n{minutes} minutes\n")


# ============================================================================
# CLI
# ============================================================================

def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Bronze Streaming Ingestion for live blood donation events (Phase 5)."
    )
    parser.add_argument("--minutes", type=float, default=DEFAULT_MINUTES,
                         help=f"How long the streaming query should run, in minutes (default: {DEFAULT_MINUTES})")
    parser.add_argument("--trigger", type=int, default=DEFAULT_TRIGGER_SECONDS,
                         help=f"processingTime trigger interval in seconds (default: {DEFAULT_TRIGGER_SECONDS})")
    parser.add_argument("--input", type=str, default=str(DEFAULT_INPUT_PATH),
                         help="JSON landing directory to watch (default: data/streaming/landing/)")
    parser.add_argument("--output", type=str, default=str(DEFAULT_OUTPUT_PATH),
                         help="Bronze streaming Parquet output directory")
    parser.add_argument("--checkpoint", type=str, default=str(DEFAULT_CHECKPOINT_PATH),
                         help="Spark checkpoint directory")

    args = parser.parse_args()

    if args.minutes <= 0:
        parser.error("--minutes must be a positive number")
    if args.trigger <= 0:
        parser.error("--trigger must be a positive integer")

    return args


# ============================================================================
# MAIN
# ============================================================================

def main() -> None:
    args = parse_arguments()

    input_path = Path(args.input)
    output_path = Path(args.output)
    checkpoint_path = Path(args.checkpoint)
    duration_seconds = args.minutes * 60

    print_header(input_path, output_path, checkpoint_path, args.trigger, args.minutes)

    ensure_directories(input_path, output_path, checkpoint_path)

    print("Starting Spark...\n")
    spark = create_spark_session()
    spark.streams.addListener(BronzeStreamingListener())

    query = None
    try:
        raw_stream_df = read_donation_stream(spark, input_path)
        bronze_stream_df = add_streaming_metadata(raw_stream_df)

        print("Streaming schema:")
        bronze_stream_df.printSchema()

        print("Starting streaming query...\n")
        query = start_streaming_query(bronze_stream_df, output_path, checkpoint_path, args.trigger)

        print("Waiting for incoming donation events...\n")
        print(f"(Run for up to {args.minutes} minute(s), or press Ctrl+C to stop early)\n")

        # Poll in short slices so Ctrl+C (KeyboardInterrupt) is reliably
        # delivered between polls -- see note above.
        start_time = time.time()
        poll_seconds = 1.0
        while query.isActive:
            elapsed = time.time() - start_time
            remaining = duration_seconds - elapsed
            if remaining <= 0:
                break
            query.awaitTermination(timeout=min(poll_seconds, remaining))

        elapsed = time.time() - start_time
        if query.isActive:
            print(f"\nStreaming duration completed ({elapsed:.0f}s elapsed).")
            query.stop()
        else:
            print("\nStreaming query stopped unexpectedly (check exception below, if any).")
            if query.exception():
                print(f"Query exception: {query.exception()}")

    except KeyboardInterrupt:
        print("\nStreaming interrupted by user.")
        print("Stopping query...")
        if query is not None and query.isActive:
            query.stop()
        print("Query stopped successfully.")

    except Exception as exc:  # noqa: BLE001
        print(f"\nERROR: Bronze streaming ingestion failed: {exc}")
        if query is not None and query.isActive:
            query.stop()
        sys.exit(1)

    finally:
        try:
            passed = validate_bronze_streaming_output(spark, output_path, checkpoint_path)
        except Exception as exc:  # noqa: BLE001
            print(f"Validation could not be completed: {exc}")
            passed = False

        print("\n" + "=" * 45)
        print("BRONZE STREAMING INGESTION COMPLETE")
        print("=" * 45)

        spark.stop()

        if not passed:
            sys.exit(1)


if __name__ == "__main__":
    main()
