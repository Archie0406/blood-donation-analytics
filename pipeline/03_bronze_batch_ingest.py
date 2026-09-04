"""
Enterprise Rare Blood Type Availability Analytics
Phase 3 — Bronze Layer: Batch Data Ingestion
=====================================================

MEDALLION ARCHITECTURE CONTEXT
--------------------------------
    RAW  ->  BRONZE  ->  SILVER  ->  GOLD  ->  DASHBOARD

    RAW      : Original CSV files generated in Phase 2 (data/raw/).
    BRONZE   : This phase. Raw historical data stored as Parquet, with
               ingestion metadata added. No business logic is applied.
    SILVER   : (Later phase) Will clean, deduplicate, validate and enrich
               the Bronze data.
    GOLD     : (Later phase) Will contain business-ready analytical tables,
               including rare blood availability summaries.

BRONZE PRINCIPLE
------------------
    BRONZE = RAW DATA + INGESTION METADATA

This script performs ONLY technical ingestion:
    - Read each raw CSV with an explicit schema (no inferSchema).
    - Add three ingestion metadata columns.
    - Write each dataset to its own Bronze Parquet location.
    - Run basic technical validation (row counts, metadata presence).

It intentionally does NOT deduplicate, clean, validate business rules,
filter rare blood groups, calculate inventory status, join datasets, or
compute any KPIs. Those responsibilities belong to the Silver and Gold
layers in later phases.
"""

from __future__ import annotations

import logging
import sys
from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_date, current_timestamp, lit
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ============================================================================
# CONFIGURATION / PATHS
# ============================================================================

APP_NAME = "Enterprise Blood Donation Analytics - Bronze Batch Ingestion"

# Resolve paths relative to the project root, regardless of the OS or the
# current working directory the script is launched from.
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
RAW_DATA_PATH = PROJECT_ROOT / "data" / "raw"
BRONZE_DATA_PATH = PROJECT_ROOT / "data" / "bronze"

# Bronze ingestion metadata column names
COL_INGESTED_AT = "_ingested_at"
COL_SOURCE_FILE = "_source_file"
COL_INGESTION_DATE = "_ingestion_date"
METADATA_COLUMNS = [COL_INGESTED_AT, COL_SOURCE_FILE, COL_INGESTION_DATE]

# ============================================================================
# LOGGING
# ============================================================================

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("bronze_ingestion")


# ============================================================================
# DATASET REGISTRY (name -> source CSV, Bronze output dir, explicit schema)
# ============================================================================
#
# Explicit schemas are used instead of inferSchema=True. inferSchema requires
# an extra full pass over each file to guess types, is not always reliable
# (e.g. it can misread an ID like "007" as a number), and produces a
# non-deterministic contract for downstream layers. Declaring the schema up
# front is cheap here (five small files) and keeps Bronze's contract explicit
# and stable -- an important property for a data lake layer.

DONORS_SCHEMA = StructType([
    StructField("donor_id", StringType(), True),
    StructField("full_name", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("blood_group", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("registration_date", DateType(), True),
    StructField("last_donation_date", DateType(), True),
    StructField("eligibility_status", StringType(), True),
])

BLOOD_BANKS_SCHEMA = StructType([
    StructField("blood_bank_id", StringType(), True),
    StructField("blood_bank_name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("address", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("storage_capacity", IntegerType(), True),
    StructField("operating_status", StringType(), True),
])

BLOOD_INVENTORY_SCHEMA = StructType([
    StructField("inventory_id", StringType(), True),
    StructField("blood_bank_id", StringType(), True),
    StructField("blood_group", StringType(), True),
    StructField("available_units", IntegerType(), True),
    StructField("reserved_units", IntegerType(), True),
    StructField("last_updated", TimestampType(), True),
    StructField("inventory_status", StringType(), True),
])

DONATION_CAMPS_SCHEMA = StructType([
    StructField("camp_id", StringType(), True),
    StructField("camp_name", StringType(), True),
    StructField("organizer", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("camp_date", DateType(), True),
    StructField("location", StringType(), True),
    StructField("expected_donors", IntegerType(), True),
])

DONATIONS_SCHEMA = StructType([
    StructField("donation_id", StringType(), True),
    StructField("donor_id", StringType(), True),
    StructField("blood_bank_id", StringType(), True),
    StructField("camp_id", StringType(), True),
    StructField("donation_date", DateType(), True),
    StructField("blood_group", StringType(), True),
    StructField("units_donated", IntegerType(), True),
    StructField("donation_status", StringType(), True),
])


@dataclass(frozen=True)
class DatasetSpec:
    """Describes one Bronze dataset: its source file, schema, and output dir."""

    name: str
    source_filename: str
    schema: StructType
    bronze_subdir: str


DATASET_SPECS: list[DatasetSpec] = [
    DatasetSpec("donors", "donors.csv", DONORS_SCHEMA, "donors"),
    DatasetSpec("blood_banks", "blood_banks.csv", BLOOD_BANKS_SCHEMA, "blood_banks"),
    DatasetSpec("blood_inventory", "blood_inventory.csv", BLOOD_INVENTORY_SCHEMA, "blood_inventory"),
    DatasetSpec("donation_camps", "donation_camps.csv", DONATION_CAMPS_SCHEMA, "donation_camps"),
    DatasetSpec("donations", "donations.csv", DONATIONS_SCHEMA, "donations"),
]


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
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")
        return spark
    except Exception as exc:  # noqa: BLE001 - surface a clear, top-level failure
        logger.error("Failed to initialize SparkSession: %s", exc)
        raise


# ============================================================================
# VALIDATION: INPUT FILES
# ============================================================================

def validate_input_files() -> None:
    """Confirm every required raw CSV file exists before Spark does any work.
    Stops execution gracefully (with a clear message) if anything is missing.
    """
    logger.info("Validating input files...")
    missing_files = []

    for spec in DATASET_SPECS:
        file_path = RAW_DATA_PATH / spec.source_filename
        if not file_path.exists():
            missing_files.append(str(file_path))

    if missing_files:
        logger.error("ERROR: The following required input file(s) are missing:")
        for path in missing_files:
            logger.error("  - %s", path)
        logger.error("Run Phase 2 (data_generation/01_generate_seed_data.py) first.")
        sys.exit(1)

    logger.info("All input files found.\n")


# ============================================================================
# READING RAW CSV
# ============================================================================

def read_raw_dataset(spark: SparkSession, spec: DatasetSpec) -> DataFrame:
    """Read one raw CSV file using its explicit schema, and show basic stats."""
    file_path = RAW_DATA_PATH / spec.source_filename
    logger.info("Reading %s", spec.source_filename)

    # multiLine=true is required because some source fields (e.g. blood bank
    # addresses) contain embedded newlines inside quoted CSV values. Without
    # it, Spark's line-based CSV reader would incorrectly split one logical
    # record into multiple rows. This is a technical CSV-parsing setting, not
    # a business transformation.
    df = (
        spark.read.option("header", "true")
        .option("multiLine", "true")
        .schema(spec.schema)
        .csv(str(file_path))
    )

    row_count = df.count()
    logger.info("Rows: %d", row_count)
    df.printSchema()
    df.show(5, truncate=False)

    return df


# ============================================================================
# INGESTION METADATA
# ============================================================================

def add_ingestion_metadata(df: DataFrame, source_filename: str) -> DataFrame:
    """Attach Bronze ingestion metadata without altering any business column.

    _ingested_at    : timestamp the record was ingested by this Spark job
    _source_file    : name of the original raw CSV file
    _ingestion_date : date the record was ingested
    """
    return (
        df.withColumn(COL_INGESTED_AT, current_timestamp())
        .withColumn(COL_SOURCE_FILE, lit(source_filename))
        .withColumn(COL_INGESTION_DATE, current_date())
    )


# ============================================================================
# WRITE BRONZE (PARQUET)
# ============================================================================

def write_bronze(df: DataFrame, spec: DatasetSpec) -> Path:
    """Write a Bronze dataset to its own Parquet directory.

    Uses overwrite mode (appropriate for this project's initial/full batch
    run) and intentionally does NOT coalesce(1) -- Bronze should behave like
    a real data lake layer with normal Spark part files.
    """
    output_path = BRONZE_DATA_PATH / spec.bronze_subdir
    logger.info("%s -> %s", spec.name, output_path)

    try:
        df.write.mode("overwrite").parquet(str(output_path))
    except Exception as exc:  # noqa: BLE001
        logger.error("Failed to write Bronze dataset '%s' to %s: %s", spec.name, output_path, exc)
        raise

    return output_path


def read_and_write_bronze(spark: SparkSession, spec: DatasetSpec) -> tuple[int, int]:
    """Read one raw dataset, tag it with metadata, write it to Bronze, and
    return (raw_row_count, bronze_row_count) for later validation."""
    raw_df = read_raw_dataset(spark, spec)
    raw_row_count = raw_df.count()

    bronze_df = add_ingestion_metadata(raw_df, spec.source_filename)
    write_bronze(bronze_df, spec)

    bronze_row_count = spark.read.parquet(str(BRONZE_DATA_PATH / spec.bronze_subdir)).count()
    return raw_row_count, bronze_row_count


# ============================================================================
# VALIDATION: BRONZE OUTPUT
# ============================================================================

def validate_bronze_output(spark: SparkSession, spec: DatasetSpec, raw_row_count: int) -> dict:
    """Perform basic TECHNICAL validation of one Bronze dataset:
      1. Output directory / Parquet files exist.
      2. Row count > 0.
      3. Expected business columns exist.
      4. Metadata columns exist and are not null.
      5. Raw row count matches Bronze row count.

    No business validation (e.g. rare blood stock levels) happens here.
    """
    output_path = BRONZE_DATA_PATH / spec.bronze_subdir
    result = {"name": spec.name, "raw_rows": raw_row_count, "bronze_rows": 0, "status": "FAIL", "reason": ""}

    if not output_path.exists() or not any(output_path.glob("*.parquet")):
        result["reason"] = "Bronze output directory or Parquet files not found"
        return result

    bronze_df = spark.read.parquet(str(output_path))
    bronze_row_count = bronze_df.count()
    result["bronze_rows"] = bronze_row_count

    if bronze_row_count == 0:
        result["reason"] = "Bronze dataset has zero rows"
        return result

    expected_columns = set(field.name for field in spec.schema.fields)
    actual_columns = set(bronze_df.columns)
    missing_business_columns = expected_columns - actual_columns
    if missing_business_columns:
        result["reason"] = f"Missing business columns: {missing_business_columns}"
        return result

    missing_metadata_columns = set(METADATA_COLUMNS) - actual_columns
    if missing_metadata_columns:
        result["reason"] = f"Missing metadata columns: {missing_metadata_columns}"
        return result

    null_metadata_count = bronze_df.filter(
        col(COL_INGESTED_AT).isNull() | col(COL_SOURCE_FILE).isNull() | col(COL_INGESTION_DATE).isNull()
    ).count()
    if null_metadata_count > 0:
        result["reason"] = f"{null_metadata_count} rows have null ingestion metadata"
        return result

    if raw_row_count != bronze_row_count:
        result["reason"] = f"Row count mismatch (raw={raw_row_count}, bronze={bronze_row_count})"
        return result

    result["status"] = "PASS"
    return result


def print_validation_report(results: list[dict]) -> bool:
    """Print the row-count validation table and return overall pass/fail."""
    print("\n" + "=" * 45)
    print("BRONZE VALIDATION")
    print("=" * 45)
    print(f"{'Dataset':<20}{'Raw':<10}{'Bronze':<12}{'Status'}")
    print("-" * 50)

    all_passed = True
    for r in results:
        print(f"{r['name']:<20}{r['raw_rows']:<10}{r['bronze_rows']:<12}{r['status']}")
        if r["status"] != "PASS":
            all_passed = False
            print(f"    -> {r['reason']}")

    metadata_status = "PASS" if all_passed else "FAIL"
    print(f"\nMetadata validation: {metadata_status}")
    return all_passed


# ============================================================================
# MAIN
# ============================================================================

def main() -> None:
    print("=" * 45)
    print("BRONZE BATCH INGESTION")
    print("Enterprise Rare Blood Type Analytics")
    print("=" * 45 + "\n")

    validate_input_files()

    spark = create_spark_session()
    validation_results: list[dict] = []

    try:
        logger.info("Adding ingestion metadata and writing Bronze datasets...\n")

        for spec in DATASET_SPECS:
            raw_row_count, _ = read_and_write_bronze(spark, spec)
            result = validate_bronze_output(spark, spec, raw_row_count)
            validation_results.append(result)

        all_passed = print_validation_report(validation_results)

        print("\n" + "=" * 45)
        if all_passed:
            print("Bronze ingestion completed successfully.")
        else:
            print("Bronze ingestion completed WITH VALIDATION FAILURES. See details above.")
        print("=" * 45)

        if not all_passed:
            sys.exit(1)

    except Exception as exc:  # noqa: BLE001
        logger.error("Bronze ingestion failed: %s", exc)
        sys.exit(1)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
