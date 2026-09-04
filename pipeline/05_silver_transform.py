"""
Enterprise Rare Blood Type Availability Analytics
Phase 6 — Silver Layer: Data Cleaning & Enrichment
=====================================================

ARCHITECTURE CONTEXT
----------------------
                     BRONZE
                        |
            ------------+------------
            |                       |
     Historical Donations    Streaming Donations
            |                       |
            ------------+------------
                        v
                   UNION DATA
                        |
                        v
                  DEDUPLICATION
                  (by donation_id)
                        |
                        v
                  DATA CLEANING
        (blood group / dates / quantities / status)
                        |
                        v
                  DATA QUALITY FLAGS
                        |
                        v
                   ENRICHMENT
        (donors + blood banks + camps + inventory)
                        |
                        v
                  SILVER LAYER
                        |
                        v
                     PHASE 7
             Rare Blood Availability

BRONZE -> SILVER PRINCIPLE
-----------------------------
This phase is strictly: CLEAN + VALIDATE + DEDUPLICATE + ENRICH.

It does NOT calculate final rare-blood availability, shortage status, or any
Gold-layer KPI. It only prepares clean, trustworthy, enriched data so Phase 7
can answer questions like "which blood banks currently have O-?" without
having to worry about duplicate donations, inconsistent blood-group text, or
missing joins.

Bad records are never silently deleted (the only exception is an EXACT
duplicate donation_id, which by definition represents the same real-world
event recorded twice). Everything else is cleaned where possible and flagged
with data-quality columns for review, preserving the original information.
"""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
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

APP_NAME = "Enterprise Rare Blood Analytics - Silver Transformation"

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent

BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"
SILVER_DIR = PROJECT_ROOT / "data" / "silver"

BRONZE_DONORS = BRONZE_DIR / "donors"
BRONZE_BLOOD_BANKS = BRONZE_DIR / "blood_banks"
BRONZE_INVENTORY = BRONZE_DIR / "blood_inventory"
BRONZE_CAMPS = BRONZE_DIR / "donation_camps"
BRONZE_DONATIONS = BRONZE_DIR / "donations"
BRONZE_STREAMING_DONATIONS = BRONZE_DIR / "streaming_donations"

SILVER_DONORS = SILVER_DIR / "donors"
SILVER_BLOOD_BANKS = SILVER_DIR / "blood_banks"
SILVER_INVENTORY = SILVER_DIR / "blood_inventory"
SILVER_CAMPS = SILVER_DIR / "donation_camps"
SILVER_DONATIONS = SILVER_DIR / "donations"
SILVER_ENRICHED_DONATIONS = SILVER_DIR / "enriched_donations"
SILVER_DATA_QUALITY = SILVER_DIR / "data_quality"

# Project-defined "rare" blood groups (demo/analytics classification only --
# NOT a medical guideline). Used consistently since Phase 1.
RARE_BLOOD_GROUPS = ["O-", "AB-", "B-", "A-"]
VALID_BLOOD_GROUPS = ["A+", "A-", "B+", "B-", "AB+", "AB-", "O+", "O-"]

# Standardized donation-status vocabulary. Completed/Cancelled/Pending were
# specified by the project; "Rejected" is included as well because it is a
# real status value produced by Phase 2 and Phase 4 and deserves its own
# category rather than being folded into "Cancelled".
VALID_DONATION_STATUSES = ["Completed", "Cancelled", "Pending", "Rejected"]

# Reasonable synthetic-data validation ranges (documented, not hard-coded
# throughout the script)
DONOR_AGE_MIN, DONOR_AGE_MAX = 18, 65
DONATION_UNITS_MIN, DONATION_UNITS_MAX = 1, 5


# ============================================================================
# SPARK SESSION
# ============================================================================

def create_spark_session() -> SparkSession:
    """Create a local-mode SparkSession sized appropriately for a laptop."""
    spark = (
        SparkSession.builder.appName(APP_NAME)
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


# ============================================================================
# READING BRONZE DATA
# ============================================================================

def read_bronze_dataset(spark: SparkSession, path: Path, name: str) -> DataFrame:
    """Read one Bronze Parquet dataset, failing clearly if it is missing."""
    if not path.exists():
        raise FileNotFoundError(
            f"Required Bronze dataset '{name}' not found at {path}. "
            f"Run Phase 3 (pipeline/03_bronze_batch_ingest.py) first."
        )
    return spark.read.parquet(str(path))


def read_streaming_bronze(spark: SparkSession, path: Path) -> DataFrame | None:
    """Read the Bronze streaming donations dataset if it exists.

    The streaming demo (Phase 5) is optional -- the Silver pipeline must
    still run successfully with historical data only. Returns None if the
    streaming Bronze dataset has never been created.
    """
    if not path.exists():
        print("WARNING:")
        print("Bronze streaming donations not found.")
        print("Continuing with historical donation data only.\n")
        return None
    return spark.read.parquet(str(path))


# ============================================================================
# BLOOD GROUP STANDARDIZATION (reusable, Spark-native, no Python UDF)
# ============================================================================

def standardize_blood_group(df: DataFrame, input_col: str, output_col: str = "blood_group") -> DataFrame:
    """Normalize free-text blood-group values into the canonical 8-value set.

    Handles case-insensitive variants such as:
        "O negative", "O NEG", "o-"        -> "O-"
        "AB positive", "AB POS", "ab+"     -> "AB+"
    Longer words are replaced before their abbreviations so "POSITIVE" is
    handled before a stray "POS" substring is considered.

    Any value that still doesn't match one of the 8 valid blood groups after
    normalization is left as-is (never invented) and will be caught by
    is_valid_blood_group downstream.
    """
    normalized = F.upper(F.trim(F.col(input_col)))
    normalized = F.regexp_replace(normalized, r"POSITIVE", "+")
    normalized = F.regexp_replace(normalized, r"\bPOS\b", "+")
    normalized = F.regexp_replace(normalized, r"NEGATIVE", "-")
    normalized = F.regexp_replace(normalized, r"\bNEG\b", "-")
    normalized = F.regexp_replace(normalized, r"\s+", "")
    return df.withColumn(output_col, normalized)


def add_blood_group_validity(df: DataFrame, blood_group_col: str = "blood_group") -> DataFrame:
    """Flag whether a (already-standardized) blood group is one of the 8
    accepted values. Invalid values are flagged, never guessed or deleted."""
    return df.withColumn(
        "is_valid_blood_group", F.col(blood_group_col).isin(VALID_BLOOD_GROUPS)
    )


def add_rare_blood_flag(df: DataFrame, blood_group_col: str = "blood_group") -> DataFrame:
    """Classify rows using the project-defined rare blood groups.

    NOTE: "rare" here is a project/demo analytical classification
    (O-, AB-, B-, A-), not a universal medical rarity guideline.
    """
    return df.withColumn("is_rare_blood", F.col(blood_group_col).isin(RARE_BLOOD_GROUPS))


# ============================================================================
# DONATION STATUS STANDARDIZATION
# ============================================================================

def standardize_donation_status(df: DataFrame, input_col: str, output_col: str = "donation_status") -> DataFrame:
    """Normalize donation status text into a fixed vocabulary.

    Mapping:
        Completed / completed / COMPLETE / Successful / success -> Completed
        Cancelled / cancelled / Canceled                         -> Cancelled
        Pending / pending                                        -> Pending
        Rejected / rejected                                      -> Rejected  (project extension)
    Anything else is upper-trimmed and kept as-is, then flagged invalid by
    add_donation_status_validity() rather than silently coerced.
    """
    normalized = F.upper(F.trim(F.col(input_col)))
    standardized = (
        F.when(normalized.isin("COMPLETED", "COMPLETE", "SUCCESSFUL", "SUCCESS"), F.lit("Completed"))
        .when(normalized.isin("CANCELLED", "CANCELED"), F.lit("Cancelled"))
        .when(normalized.isin("PENDING"), F.lit("Pending"))
        .when(normalized.isin("REJECTED"), F.lit("Rejected"))
        .otherwise(F.initcap(normalized))
    )
    return df.withColumn(output_col, standardized)


def add_donation_status_validity(df: DataFrame, status_col: str = "donation_status") -> DataFrame:
    return df.withColumn(
        "is_valid_donation_status", F.col(status_col).isin(VALID_DONATION_STATUSES)
    )


# ============================================================================
# CLEAN: DONORS
# ============================================================================

def clean_donors(df: DataFrame) -> DataFrame:
    """Clean and validate the donor dataset.

    - Deduplicate by donor_id (keep one record per donor).
    - Standardize blood_group; flag validity (never invented).
    - Flag age validity using DONOR_AGE_MIN/MAX; the value itself is
      preserved so analysts can see exactly what was in the source data.
    - Fill missing gender/city/eligibility_status with "Unknown" rather than
      dropping the donor -- a donor with an incomplete profile can still
      have valid, useful donations.
    - Cast registration_date / last_donation_date to DateType.
    """
    df = df.dropDuplicates(["donor_id"])
    df = standardize_blood_group(df, "blood_group")
    df = add_blood_group_validity(df)

    df = df.withColumn("age", F.col("age").cast(IntegerType()))
    df = df.withColumn(
        "is_valid_age",
        F.col("age").isNotNull() & (F.col("age") >= DONOR_AGE_MIN) & (F.col("age") <= DONOR_AGE_MAX),
    )

    df = df.withColumn(
        "gender",
        F.when(F.col("gender").isNull() | (F.trim(F.col("gender")) == ""), F.lit("Unknown"))
        .otherwise(F.trim(F.col("gender"))),
    )
    df = df.withColumn(
        "city",
        F.when(F.col("city").isNull() | (F.trim(F.col("city")) == ""), F.lit("Unknown"))
        .otherwise(F.trim(F.col("city"))),
    )
    df = df.withColumn(
        "eligibility_status",
        F.when(F.col("eligibility_status").isNull() | (F.trim(F.col("eligibility_status")) == ""), F.lit("Unknown"))
        .otherwise(F.trim(F.col("eligibility_status"))),
    )

    df = df.withColumn("registration_date", F.col("registration_date").cast(DateType()))
    df = df.withColumn("last_donation_date", F.col("last_donation_date").cast(DateType()))

    df = df.withColumn(
        "overall_data_quality_status",
        F.when(F.col("is_valid_blood_group") & F.col("is_valid_age"), F.lit("VALID")).otherwise(F.lit("REVIEW")),
    )
    return df


# ============================================================================
# CLEAN: BLOOD BANKS
# ============================================================================

def clean_blood_banks(df: DataFrame) -> DataFrame:
    """Clean and validate the blood bank dataset. Records are never removed
    for failing validation -- only flagged, since a blood bank with a data
    issue may still hold real, important inventory."""
    df = df.dropDuplicates(["blood_bank_id"])

    df = df.withColumn("is_valid_id", F.col("blood_bank_id").isNotNull())
    df = df.withColumn("storage_capacity", F.col("storage_capacity").cast(IntegerType()))
    df = df.withColumn("is_valid_capacity", F.col("storage_capacity").isNotNull() & (F.col("storage_capacity") > 0))

    df = df.withColumn("latitude", F.col("latitude").cast(DoubleType()))
    df = df.withColumn("longitude", F.col("longitude").cast(DoubleType()))
    df = df.withColumn(
        "is_valid_coordinates",
        F.col("latitude").between(-90, 90) & F.col("longitude").between(-180, 180),
    )

    df = df.withColumn(
        "overall_data_quality_status",
        F.when(
            F.col("is_valid_id") & F.col("is_valid_capacity") & F.col("is_valid_coordinates"), F.lit("VALID")
        ).otherwise(F.lit("REVIEW")),
    )
    return df


# ============================================================================
# CLEAN: BLOOD INVENTORY
# ============================================================================

def clean_inventory(df: DataFrame, valid_blood_bank_ids: DataFrame) -> DataFrame:
    """Clean and validate the inventory dataset.

    IMPORTANT: this only cleans and flags the inventory snapshot. It does
    NOT calculate future/derived inventory balances -- that is Phase 7's job.
    """
    df = df.dropDuplicates(["inventory_id"])
    df = standardize_blood_group(df, "blood_group")
    df = add_blood_group_validity(df)
    df = add_rare_blood_flag(df)

    df = df.withColumn("available_units", F.col("available_units").cast(IntegerType()))
    df = df.withColumn("reserved_units", F.col("reserved_units").cast(IntegerType()))
    df = df.withColumn(
        "is_valid_units",
        F.col("available_units").isNotNull() & (F.col("available_units") >= 0),
    )

    df = df.withColumn("last_updated", F.col("last_updated").cast(TimestampType()))

    # Referential check against cleaned blood banks (flag only, never drop)
    df = df.join(
        valid_blood_bank_ids.select(F.col("blood_bank_id").alias("_valid_bb_id")),
        df["blood_bank_id"] == F.col("_valid_bb_id"),
        how="left",
    )
    df = df.withColumn("blood_bank_exists", F.col("_valid_bb_id").isNotNull()).drop("_valid_bb_id")

    df = df.withColumn(
        "overall_data_quality_status",
        F.when(
            F.col("is_valid_blood_group") & F.col("is_valid_units") & F.col("blood_bank_exists"), F.lit("VALID")
        ).otherwise(F.lit("REVIEW")),
    )
    return df


# ============================================================================
# CLEAN: DONATION CAMPS
# ============================================================================

def clean_donation_camps(df: DataFrame) -> DataFrame:
    """Clean and validate the donation camp dataset."""
    df = df.dropDuplicates(["camp_id"])
    df = df.withColumn("camp_date", F.col("camp_date").cast(DateType()))

    df = df.withColumn(
        "organizer",
        F.when(F.col("organizer").isNull() | (F.trim(F.col("organizer")) == ""), F.lit("Unknown"))
        .otherwise(F.trim(F.col("organizer"))),
    )
    df = df.withColumn(
        "city",
        F.when(F.col("city").isNull() | (F.trim(F.col("city")) == ""), F.lit("Unknown"))
        .otherwise(F.trim(F.col("city"))),
    )

    df = df.withColumn(
        "overall_data_quality_status",
        F.when(F.col("camp_id").isNotNull() & F.col("camp_date").isNotNull(), F.lit("VALID")).otherwise(F.lit("REVIEW")),
    )
    return df


# ============================================================================
# CLEAN: DONATIONS (historical + streaming, prepared for union)
# ============================================================================

# The final, unified Silver donation column set. Historical Bronze donations
# in THIS project already use "donation_status" (not "status"), but the
# rename-if-present logic below is kept general so the pipeline still works
# against a source where historical data uses "status" instead.
FINAL_DONATION_COLUMNS = [
    "donation_id", "donor_id", "blood_bank_id", "camp_id", "donation_date",
    "blood_group", "units_donated", "donation_status", "event_time",
    "source", "event_type", "_ingested_at", "_ingestion_date", "_source_file",
]


def _standardize_status_column_name(df: DataFrame) -> DataFrame:
    """Ensure the donation status column is always named donation_status,
    regardless of whether the source used 'status' or 'donation_status'."""
    if "status" in df.columns and "donation_status" not in df.columns:
        df = df.withColumnRenamed("status", "donation_status")
    return df


def clean_historical_donations(df: DataFrame) -> DataFrame:
    """Clean the historical (batch) donation dataset and align its schema
    with the streaming donation dataset so the two can be safely unioned."""
    df = _standardize_status_column_name(df)

    df = standardize_blood_group(df, "blood_group")
    df = standardize_donation_status(df, "donation_status")
    df = df.withColumn("units_donated", F.col("units_donated").cast(IntegerType()))
    df = df.withColumn("donation_date", F.col("donation_date").cast(DateType()))

    # Historical donations have no live event_time -- add missing streaming
    # fields as nulls / descriptive literals so the union has a full,
    # consistent schema. This is metadata continuity, not business logic.
    if "event_time" not in df.columns:
        df = df.withColumn("event_time", F.lit(None).cast(TimestampType()))
    if "source" not in df.columns:
        df = df.withColumn("source", F.lit("historical_batch"))
    if "event_type" not in df.columns:
        df = df.withColumn("event_type", F.lit("blood_donation"))

    return df.select(*FINAL_DONATION_COLUMNS)


def clean_streaming_donations(df: DataFrame) -> DataFrame:
    """Clean the streaming (live) donation dataset and align its schema with
    the historical donation dataset so the two can be safely unioned."""
    df = _standardize_status_column_name(df)

    df = standardize_blood_group(df, "blood_group")
    df = standardize_donation_status(df, "donation_status")
    df = df.withColumn("units_donated", F.col("units_donated").cast(IntegerType()))
    df = df.withColumn("donation_date", F.col("donation_date").cast(DateType()))
    df = df.withColumn("event_time", F.col("event_time").cast(TimestampType()))

    return df.select(*FINAL_DONATION_COLUMNS)


def combine_donations(historical_df: DataFrame, streaming_df: DataFrame | None) -> DataFrame:
    """Union historical and (optional) streaming donations into one dataset.

    unionByName(allowMissingColumns=True) is used defensively even though
    both sides are already select()-ed to FINAL_DONATION_COLUMNS, in case a
    future source schema drifts slightly.
    """
    if streaming_df is None:
        return historical_df
    return historical_df.unionByName(streaming_df, allowMissingColumns=True)


def deduplicate_donations(df: DataFrame) -> tuple[DataFrame, int]:
    """Deduplicate the combined donation dataset by donation_id.

    donation_id is the natural key for a donation event -- if the exact same
    ID appears more than once (e.g. an accidental duplicate ingested from
    Bronze), only one Silver record should remain. This is the ONE case
    where Silver is allowed to remove rows outright, because a duplicate ID
    represents the same real-world event, not new information.
    """
    before_count = df.count()
    deduped = df.dropDuplicates(["donation_id"])
    after_count = deduped.count()
    return deduped, before_count - after_count


def add_donation_quality_flags(df: DataFrame) -> DataFrame:
    """Add all Silver donation-level data-quality flags (see docstring at
    top of file for the "flag, don't delete" principle)."""
    df = add_blood_group_validity(df)
    df = add_rare_blood_flag(df)
    df = add_donation_status_validity(df)

    df = df.withColumn(
        "is_valid_donation_quantity",
        F.col("units_donated").isNotNull()
        & (F.col("units_donated") >= DONATION_UNITS_MIN)
        & (F.col("units_donated") <= DONATION_UNITS_MAX),
    )

    df = df.withColumn(
        "is_valid_donation_date",
        F.col("donation_date").isNotNull() & (F.col("donation_date") <= F.current_date()),
    )

    df = df.withColumn(
        "overall_data_quality_status",
        F.when(
            F.col("is_valid_blood_group")
            & F.col("is_valid_donation_quantity")
            & F.col("is_valid_donation_date")
            & F.col("is_valid_donation_status"),
            F.lit("VALID"),
        ).otherwise(F.lit("REVIEW")),
    )
    return df


# ============================================================================
# ENRICHMENT
# ============================================================================

def enrich_donations(
    silver_donations: DataFrame,
    silver_donors: DataFrame,
    silver_blood_banks: DataFrame,
    silver_camps: DataFrame,
    silver_inventory: DataFrame,
) -> DataFrame:
    """Left-join cleaned donations with donor, blood bank, camp, and
    inventory information. LEFT JOIN is used throughout so a donation is
    never dropped just because a related record is missing -- that is
    itself useful data-quality information (captured in the *_exists flags).

    Inventory is deliberately NOT filled with 0 when missing: a missing
    inventory match means "we don't have a matching inventory snapshot for
    this blood bank + blood group", which is different from "zero units are
    available". Silently defaulting to 0 would fabricate a shortage signal
    that Phase 7 could act on incorrectly.
    """
    donors_alias = silver_donors.select(
        F.col("donor_id"),
        F.col("full_name").alias("donor_name"),
        F.col("age").alias("donor_age"),
        F.col("gender").alias("donor_gender"),
        F.col("city").alias("donor_city"),
        F.col("blood_group").alias("donor_blood_group"),
        F.col("eligibility_status").alias("donor_eligibility_status"),
    )

    banks_alias = silver_blood_banks.select(
        F.col("blood_bank_id"),
        F.col("blood_bank_name"),
        F.col("city").alias("blood_bank_city"),
        F.col("state").alias("blood_bank_state"),
        F.col("latitude"),
        F.col("longitude"),
        F.col("storage_capacity"),
    )

    camps_alias = silver_camps.select(
        F.col("camp_id"),
        F.col("camp_name"),
        F.col("city").alias("camp_city"),
        F.col("organizer"),
        F.col("camp_date"),
    )

    inventory_alias = silver_inventory.select(
        F.col("blood_bank_id").alias("_inv_blood_bank_id"),
        F.col("blood_group").alias("_inv_blood_group"),
        F.col("available_units").alias("current_available_units"),
        F.col("last_updated").alias("inventory_last_updated"),
    )

    enriched = (
        silver_donations
        .join(donors_alias, on="donor_id", how="left")
        .join(banks_alias, on="blood_bank_id", how="left")
        .join(camps_alias, on="camp_id", how="left")
        .join(
            inventory_alias,
            (F.col("blood_bank_id") == F.col("_inv_blood_bank_id"))
            & (F.col("blood_group") == F.col("_inv_blood_group")),
            how="left",
        )
        .drop("_inv_blood_bank_id", "_inv_blood_group")
    )

    enriched = enriched.withColumn("donor_exists", F.col("donor_name").isNotNull())
    enriched = enriched.withColumn("blood_bank_exists", F.col("blood_bank_name").isNotNull())
    enriched = enriched.withColumn("inventory_record_exists", F.col("current_available_units").isNotNull())

    # camp_exists is only meaningful when a camp was actually referenced;
    # a donation made directly at a blood bank (camp_id is null) is not a
    # data-quality problem, so we leave this null rather than false.
    enriched = enriched.withColumn(
        "camp_exists",
        F.when(F.col("camp_id").isNotNull(), F.col("camp_name").isNotNull()),
    )

    # blood_group_match compares the donation's blood group with the donor's
    # recorded blood group. Left null (unknown) when the donor didn't match
    # at all, rather than defaulting to false, since "no donor found" and
    # "donor found but blood group differs" are different situations.
    enriched = enriched.withColumn(
        "blood_group_match",
        F.when(F.col("donor_blood_group").isNotNull(), F.col("blood_group") == F.col("donor_blood_group")),
    )

    return enriched


# ============================================================================
# WRITE SILVER OUTPUTS
# ============================================================================

def write_parquet(df: DataFrame, path: Path, label: str) -> None:
    """Write a Silver dataset as Parquet, overwriting any previous run.
    coalesce(1) is intentionally NOT used -- Silver is an intermediate
    analytics layer, not a final export."""
    df.write.mode("overwrite").parquet(str(path))
    print(f"  {label} -> {path}")


def write_silver_outputs(
    silver_donors: DataFrame,
    silver_blood_banks: DataFrame,
    silver_inventory: DataFrame,
    silver_camps: DataFrame,
    silver_donations: DataFrame,
    enriched_donations: DataFrame,
    dq_summary: DataFrame,
) -> None:
    print("\nWriting Silver datasets...")
    write_parquet(silver_donors, SILVER_DONORS, "donors")
    write_parquet(silver_blood_banks, SILVER_BLOOD_BANKS, "blood_banks")
    write_parquet(silver_inventory, SILVER_INVENTORY, "blood_inventory")
    write_parquet(silver_camps, SILVER_CAMPS, "donation_camps")
    write_parquet(silver_donations, SILVER_DONATIONS, "donations")
    write_parquet(enriched_donations, SILVER_ENRICHED_DONATIONS, "enriched_donations")
    write_parquet(dq_summary, SILVER_DATA_QUALITY, "data_quality (summary)")


# ============================================================================
# DATA QUALITY REPORT
# ============================================================================

def build_data_quality_report(
    spark: SparkSession,
    historical_count: int,
    streaming_count: int,
    combined_count: int,
    duplicates_removed: int,
    silver_donations: DataFrame,
    enriched_donations: DataFrame,
) -> DataFrame:
    """Compute and print the Phase 6 data-quality report, and return it as a
    one-row Spark DataFrame for optional persistence."""
    valid_count = silver_donations.filter(F.col("overall_data_quality_status") == "VALID").count()
    review_count = silver_donations.filter(F.col("overall_data_quality_status") == "REVIEW").count()
    rare_blood_count = silver_donations.filter(F.col("is_rare_blood")).count()

    missing_donor_matches = enriched_donations.filter(~F.col("donor_exists")).count()
    missing_bank_matches = enriched_donations.filter(~F.col("blood_bank_exists")).count()
    missing_inventory_matches = enriched_donations.filter(~F.col("inventory_record_exists")).count()

    print("\n" + "=" * 45)
    print("SILVER TRANSFORMATION DATA QUALITY REPORT")
    print("=" * 45)
    print(f"\nHistorical donations: {historical_count}")
    print(f"Streaming donations: {streaming_count}")
    print(f"Combined donations: {combined_count}")
    print(f"Duplicates removed: {duplicates_removed}")
    print(f"\nValid donations: {valid_count}")
    print(f"Records requiring review: {review_count}")
    print(f"\nRare blood donations: {rare_blood_count}")
    print(f"\nMissing donor matches: {missing_donor_matches}")
    print(f"Missing blood bank matches: {missing_bank_matches}")
    print(f"Missing inventory matches: {missing_inventory_matches}")
    print("=" * 45)

    report_schema = StructType([
        StructField("historical_donations", IntegerType(), False),
        StructField("streaming_donations", IntegerType(), False),
        StructField("combined_donations", IntegerType(), False),
        StructField("duplicates_removed", IntegerType(), False),
        StructField("valid_donations", IntegerType(), False),
        StructField("records_requiring_review", IntegerType(), False),
        StructField("rare_blood_donations", IntegerType(), False),
        StructField("missing_donor_matches", IntegerType(), False),
        StructField("missing_blood_bank_matches", IntegerType(), False),
        StructField("missing_inventory_matches", IntegerType(), False),
        StructField("report_generated_at", TimestampType(), True),
    ])
    report_row = [(
        historical_count, streaming_count, combined_count, duplicates_removed,
        valid_count, review_count, rare_blood_count,
        missing_donor_matches, missing_bank_matches, missing_inventory_matches,
        None,
    )]
    report_df = spark.createDataFrame(report_row, schema=report_schema)
    report_df = report_df.withColumn("report_generated_at", F.current_timestamp())
    return report_df


# ============================================================================
# VALIDATION AFTER WRITING
# ============================================================================

def validate_silver_outputs(spark: SparkSession) -> None:
    """Read the written Silver Parquet outputs back and print a validation
    summary, including a demo-only rare-blood preview. This does NOT
    calculate shortage status, rank blood banks, or build any Gold table --
    it is purely a check that Phase 6 produced usable output for Phase 7."""
    print("\nValidating Silver outputs...\n")

    donations_df = spark.read.parquet(str(SILVER_DONATIONS))
    enriched_df = spark.read.parquet(str(SILVER_ENRICHED_DONATIONS))

    print("--- data/silver/donations/ ---")
    print(f"Record count: {donations_df.count()}")
    donations_df.printSchema()
    donations_df.show(10, truncate=False)

    print("Blood Group Distribution:")
    donations_df.groupBy("blood_group").count().orderBy("blood_group").show(truncate=False)

    rare_count = donations_df.filter(F.col("is_rare_blood")).count()
    print(f"Rare blood donation records: {rare_count}")

    print("\n--- data/silver/enriched_donations/ ---")
    print(f"Record count: {enriched_df.count()}")
    enriched_df.printSchema()

    print("Rare blood preview (blood_group, blood_bank_name, blood_bank_city, current_available_units):")
    (
        enriched_df.filter(F.col("is_rare_blood"))
        .select("blood_group", "blood_bank_name", "blood_bank_city", "current_available_units")
        .orderBy("blood_group")
        .show(10, truncate=False)
    )


# ============================================================================
# MAIN
# ============================================================================

def main() -> None:
    print("=" * 45)
    print("SILVER TRANSFORMATION")
    print("Enterprise Rare Blood Type Availability Analytics")
    print("=" * 45)

    spark = create_spark_session()

    try:
        print("\nReading Bronze datasets...\n")
        donors_bronze = read_bronze_dataset(spark, BRONZE_DONORS, "donors")
        blood_banks_bronze = read_bronze_dataset(spark, BRONZE_BLOOD_BANKS, "blood_banks")
        inventory_bronze = read_bronze_dataset(spark, BRONZE_INVENTORY, "blood_inventory")
        camps_bronze = read_bronze_dataset(spark, BRONZE_CAMPS, "donation_camps")
        historical_donations_bronze = read_bronze_dataset(spark, BRONZE_DONATIONS, "donations")
        streaming_donations_bronze = read_streaming_bronze(spark, BRONZE_STREAMING_DONATIONS)

        print(f"Donors loaded: {donors_bronze.count()}")
        print(f"Blood banks loaded: {blood_banks_bronze.count()}")
        print(f"Inventory records loaded: {inventory_bronze.count()}")
        print(f"Donation camps loaded: {camps_bronze.count()}")

        historical_count = historical_donations_bronze.count()
        streaming_count = streaming_donations_bronze.count() if streaming_donations_bronze is not None else 0
        print(f"\nHistorical donations: {historical_count}")
        print(f"Streaming donations: {streaming_count}")

        # ---- Clean reference datasets ----
        silver_donors = clean_donors(donors_bronze)
        silver_blood_banks = clean_blood_banks(blood_banks_bronze)
        silver_inventory = clean_inventory(inventory_bronze, silver_blood_banks)
        silver_camps = clean_donation_camps(camps_bronze)

        # ---- Clean + combine donations ----
        cleaned_historical = clean_historical_donations(historical_donations_bronze)
        cleaned_streaming = (
            clean_streaming_donations(streaming_donations_bronze)
            if streaming_donations_bronze is not None
            else None
        )

        print("\nCombining donation datasets...")
        combined_donations = combine_donations(cleaned_historical, cleaned_streaming)
        combined_count = combined_donations.count()

        combined_donations, duplicates_removed = deduplicate_donations(combined_donations)
        print(f"\nCombined donations: {combined_count}")
        print(f"Duplicates removed: {duplicates_removed}")

        print("\nCleaning blood groups...")
        print("Cleaning donation dates...")
        print("Validating donation quantities...")
        print("Standardizing donation status...")
        silver_donations = add_donation_quality_flags(combined_donations)

        # ---- Enrichment ----
        print("\nEnriching donations with donor information...")
        print("Enriching donations with blood bank information...")
        print("Enriching donations with camp information...")
        print("Enriching donations with inventory information...")
        enriched_donations = enrich_donations(
            silver_donations, silver_donors, silver_blood_banks, silver_camps, silver_inventory
        )
        print("\nRare blood classification complete.")

        # ---- Data quality report ----
        dq_summary = build_data_quality_report(
            spark, historical_count, streaming_count, combined_count,
            duplicates_removed, silver_donations, enriched_donations,
        )

        # ---- Write outputs ----
        write_silver_outputs(
            silver_donors, silver_blood_banks, silver_inventory, silver_camps,
            silver_donations, enriched_donations, dq_summary,
        )

        print("\nSilver transformation complete.")

        # ---- Post-write validation ----
        validate_silver_outputs(spark)

        print("\n" + "=" * 45)
        print("SILVER LAYER COMPLETE")
        print("=" * 45)

    except FileNotFoundError as exc:
        print(f"\nERROR: {exc}")
        raise SystemExit(1)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
