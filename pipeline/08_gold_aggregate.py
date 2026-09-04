"""
Enterprise Rare Blood Type Availability Analytics
Phase 8 — Gold Layer: Business Aggregations & Analytics
=====================================================

NAMING NOTE
-------------
The Phase 8 brief suggested the filename pipeline/06_gold_aggregate.py.
This project already uses 05_silver_transform.py for Phase 6 (Silver) and
07_rare_blood_availability.py for the "Rare Blood Availability Processing"
stage shown between Silver and Gold in the project's own architecture
diagram. To keep the pipeline/ folder in the correct run order, this script
is named 08_gold_aggregate.py instead -- the brief explicitly allows
"exact paths can be adjusted to match the existing project structure."

RELATIONSHIP TO PHASE 7
--------------------------
Phase 7 (07_rare_blood_availability.py) already produced a first set of
rare-blood-focused Gold-ready tables (prefixed gold_*) directly answering
"which centers have rare blood". Phase 8 is a broader, INDEPENDENT Gold
layer built directly from Silver (per this brief's own architecture
diagram: SILVER -> Gold Aggregation), covering rare blood analytics again
under this brief's own schema/thresholds/naming PLUS the wider set of
donation, blood-bank, camp, and donor analytics Phase 7 did not produce.
The two phases intentionally use different threshold vocabularies
(Phase 7: CRITICAL/LOW/MODERATE/GOOD, Phase 8: OUT_OF_STOCK/CRITICAL/LOW/
AVAILABLE) and different output folder names, so both can coexist under
data/gold/ without collision.

BUSINESS PRIORITY (per brief section 23)
--------------------------------------------
    1. Rare Blood Availability
    2. Blood Bank Locations with Rare Blood
    3. Blood Shortage Detection
    4. City-wise Rare Blood Availability
    5. Rare Blood Donor Analysis
    6. Overall Donation Analytics
    7. Blood Bank & Camp Performance
The script builds datasets in roughly this priority order.

COMPLETED-ONLY DONATION COUNTING
------------------------------------
Every donation-based aggregate in this script (daily/monthly summaries,
blood group distribution, blood bank performance, camp performance, donor
summaries) counts only donations with donation_status == "Completed"
(configurable via COUNT_ONLY_COMPLETED_DONATIONS). A Cancelled or Rejected
donation never actually added usable blood, so counting it as a real
donation would overstate supply and donor activity.
"""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

# ============================================================================
# CONFIGURATION (single source of truth for thresholds, groups, and paths)
# ============================================================================

APP_NAME = "Enterprise Rare Blood Analytics - Phase 8 Gold Aggregation"

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent

SILVER_DIR = PROJECT_ROOT / "data" / "silver"
GOLD_DIR = PROJECT_ROOT / "data" / "gold"

SILVER_DONORS = SILVER_DIR / "donors"
SILVER_BLOOD_BANKS = SILVER_DIR / "blood_banks"
SILVER_INVENTORY = SILVER_DIR / "blood_inventory"
SILVER_DONATION_CAMPS = SILVER_DIR / "donation_camps"
SILVER_DONATIONS = SILVER_DIR / "donations"

GOLD_DAILY_DONATION_SUMMARY = GOLD_DIR / "daily_donation_summary"
GOLD_MONTHLY_DONATION_SUMMARY = GOLD_DIR / "monthly_donation_summary"
GOLD_BLOOD_GROUP_DISTRIBUTION = GOLD_DIR / "blood_group_distribution"
GOLD_RARE_BLOOD_AVAILABILITY = GOLD_DIR / "rare_blood_availability"
GOLD_RARE_BLOOD_SUMMARY = GOLD_DIR / "rare_blood_summary"
GOLD_BLOOD_SHORTAGE_REPORT = GOLD_DIR / "blood_shortage_report"
GOLD_BLOOD_BANK_RARE_STOCK = GOLD_DIR / "blood_bank_rare_stock"
GOLD_BLOOD_BANK_RARE_STOCK_AVAILABLE = GOLD_DIR / "blood_bank_rare_stock_available"  # optional filtered variant
GOLD_CITY_BLOOD_AVAILABILITY = GOLD_DIR / "city_blood_availability"
GOLD_BLOOD_BANK_PERFORMANCE = GOLD_DIR / "blood_bank_performance"
GOLD_DONATION_CAMP_PERFORMANCE = GOLD_DIR / "donation_camp_performance"
GOLD_DONOR_SUMMARY = GOLD_DIR / "donor_summary"
GOLD_RARE_DONOR_SUMMARY = GOLD_DIR / "rare_donor_summary"

# All valid blood groups
VALID_BLOOD_GROUPS = ["A+", "A-", "B+", "B-", "AB+", "AB-", "O+", "O-"]

# Project-defined rare/priority blood groups. Change this list to redefine
# "rare" for the whole Gold layer -- nothing else hard-codes it.
RARE_BLOOD_GROUPS = ["O-", "AB-", "B-", "A-"]

# Inventory status thresholds. These are DEMO/SYNTHETIC thresholds for this
# project only, not real medical blood-bank inventory guidelines.
#   available_units == 0            -> OUT_OF_STOCK
#   available_units <= CRITICAL_THRESHOLD (and > 0) -> CRITICAL
#   available_units <= LOW_THRESHOLD (and > CRITICAL_THRESHOLD) -> LOW
#   available_units > LOW_THRESHOLD -> AVAILABLE
CRITICAL_THRESHOLD = 5
LOW_THRESHOLD = 10

# Severity score used to sort the shortage report, worst first
SEVERITY_SCORE = {
    "OUT_OF_STOCK": 3,
    "CRITICAL": 2,
    "LOW": 1,
    "AVAILABLE": 0,
}

# Only donations in this status are counted as real, completed contributions
COUNT_ONLY_COMPLETED_DONATIONS = True
COMPLETED_STATUS = "Completed"


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
# READ SILVER DATA
# ============================================================================

def read_silver_dataset(spark: SparkSession, path: Path, name: str, required_columns: list[str]) -> DataFrame:
    """Read one Silver Parquet dataset, failing clearly if the dataset or a
    required column is missing."""
    if not path.exists():
        raise FileNotFoundError(
            f"Required Silver dataset '{name}' not found at {path}. "
            f"Run Phase 6 (pipeline/05_silver_transform.py) first."
        )

    df = spark.read.parquet(str(path))
    missing_columns = [c for c in required_columns if c not in df.columns]
    if missing_columns:
        raise ValueError(
            f"Silver dataset '{name}' is missing required column(s): {missing_columns}. "
            f"Found columns: {df.columns}"
        )
    return df


# ============================================================================
# STATUS / SCORING HELPERS
# ============================================================================

def availability_status_expr(units_col: str) -> F.Column:
    """Classify a unit count into OUT_OF_STOCK / CRITICAL / LOW / AVAILABLE
    using the configured thresholds. Reused at every grain (per-record,
    per-city, per-blood-group aggregate)."""
    units = F.col(units_col)
    return (
        F.when(units.isNull(), F.lit(None))
        .when(units == 0, F.lit("OUT_OF_STOCK"))
        .when(units <= CRITICAL_THRESHOLD, F.lit("CRITICAL"))
        .when(units <= LOW_THRESHOLD, F.lit("LOW"))
        .otherwise(F.lit("AVAILABLE"))
    )


def severity_score_expr(status_col: str) -> F.Column:
    """Map a shortage status to its configured severity score."""
    mapping = F.create_map([F.lit(x) for pair in SEVERITY_SCORE.items() for x in pair])
    return mapping[F.col(status_col)]


def completed_donations_only(donations_df: DataFrame) -> DataFrame:
    """Filter to Completed donations only, if COUNT_ONLY_COMPLETED_DONATIONS
    is enabled (the project-wide default -- see module docstring)."""
    if COUNT_ONLY_COMPLETED_DONATIONS:
        return donations_df.filter(F.col("donation_status") == COMPLETED_STATUS)
    return donations_df


# ============================================================================
# DATA QUALITY VALIDATION (section 18)
# ============================================================================

def run_quality_checks(
    donors_df: DataFrame, blood_banks_df: DataFrame, inventory_df: DataFrame,
    camps_df: DataFrame, donations_df: DataFrame,
) -> None:
    """Re-verify core data-quality rules against the exact Silver data this
    Gold layer is about to aggregate, and print a report. Problems are
    reported, never silently hidden -- but Phase 8 does not abort on them
    since Silver already flags records for review; this is a second,
    independent check specific to what Gold actually needs."""
    print("\n" + "=" * 45)
    print("PHASE 8 DATA QUALITY VALIDATION")
    print("=" * 45)

    null_bank_id = inventory_df.filter(F.col("blood_bank_id").isNull()).count()
    null_blood_group_inv = inventory_df.filter(F.col("blood_group").isNull()).count()
    null_available_units = inventory_df.filter(F.col("available_units").isNull()).count()
    negative_inventory = inventory_df.filter(F.col("available_units") < 0).count()
    invalid_inventory_groups = inventory_df.filter(~F.col("blood_group").isin(VALID_BLOOD_GROUPS)).count()

    null_donation_date = donations_df.filter(F.col("donation_date").isNull()).count()
    non_positive_units_donated = donations_df.filter(
        F.col("units_donated").isNull() | (F.col("units_donated") <= 0)
    ).count()
    invalid_donation_groups = donations_df.filter(~F.col("blood_group").isin(VALID_BLOOD_GROUPS)).count()

    duplicate_donation_ids = (
        donations_df.groupBy("donation_id").count().filter(F.col("count") > 1).count()
    )
    duplicate_donor_ids = (
        donors_df.groupBy("donor_id").count().filter(F.col("count") > 1).count()
    )
    duplicate_bank_ids = (
        blood_banks_df.groupBy("blood_bank_id").count().filter(F.col("count") > 1).count()
    )
    duplicate_inventory_ids = (
        inventory_df.groupBy("inventory_id").count().filter(F.col("count") > 1).count()
    )

    print(f"Null blood_bank_id (inventory): {null_bank_id}")
    print(f"Null blood_group (inventory): {null_blood_group_inv}")
    print(f"Null available_units: {null_available_units}")
    print(f"Negative available_units: {negative_inventory}")
    print(f"Invalid blood groups (inventory): {invalid_inventory_groups}")
    print(f"Null donation_date: {null_donation_date}")
    print(f"Non-positive units_donated: {non_positive_units_donated}")
    print(f"Invalid blood groups (donations): {invalid_donation_groups}")
    print(f"Duplicate donation_id: {duplicate_donation_ids}")
    print(f"Duplicate donor_id: {duplicate_donor_ids}")
    print(f"Duplicate blood_bank_id: {duplicate_bank_ids}")
    print(f"Duplicate inventory_id: {duplicate_inventory_ids}")
    print("=" * 45)


# ============================================================================
# GOLD DATASET 1 — DAILY DONATION SUMMARY
# ============================================================================

def build_daily_donation_summary(donations_df: DataFrame) -> DataFrame:
    completed = completed_donations_only(donations_df)
    summary = completed.groupBy("donation_date").agg(
        F.count("donation_id").alias("total_donations"),
        F.sum("units_donated").alias("total_units_donated"),
        F.countDistinct("donor_id").alias("unique_donors"),
        F.countDistinct("blood_bank_id").alias("active_blood_banks"),
    )
    return summary.select(
        "donation_date", "total_donations", "total_units_donated", "unique_donors", "active_blood_banks"
    ).orderBy("donation_date")


# ============================================================================
# GOLD DATASET 2 — MONTHLY DONATION SUMMARY
# ============================================================================

def build_monthly_donation_summary(donations_df: DataFrame) -> DataFrame:
    completed = completed_donations_only(donations_df)
    with_month = completed.withColumn("year", F.year("donation_date")).withColumn(
        "month", F.month("donation_date")
    ).withColumn("month_name", F.date_format("donation_date", "MMMM"))

    summary = with_month.groupBy("year", "month", "month_name").agg(
        F.count("donation_id").alias("total_donations"),
        F.sum("units_donated").alias("total_units_donated"),
        F.countDistinct("donor_id").alias("unique_donors"),
    )
    return summary.select(
        "year", "month", "month_name", "total_donations", "total_units_donated", "unique_donors"
    ).orderBy("year", "month")


# ============================================================================
# GOLD DATASET 3 — BLOOD GROUP DISTRIBUTION
# ============================================================================

def build_blood_group_distribution(donations_df: DataFrame) -> DataFrame:
    completed = completed_donations_only(donations_df)
    by_group = completed.groupBy("blood_group").agg(
        F.count("donation_id").alias("total_donations"),
        F.sum("units_donated").alias("total_units_donated"),
    )

    grand_total_row = by_group.agg(F.sum("total_units_donated").alias("grand_total")).collect()[0]
    grand_total = grand_total_row["grand_total"] or 0

    distribution = by_group.withColumn(
        "percentage_of_total",
        F.when(F.lit(grand_total) > 0, F.round(F.col("total_units_donated") / F.lit(grand_total) * 100, 2)),
    )
    return distribution.select(
        "blood_group", "total_donations", "total_units_donated", "percentage_of_total"
    ).orderBy(F.desc("total_units_donated"))


# ============================================================================
# SHARED HELPER — RARE INVENTORY JOINED WITH BLOOD BANKS
# ============================================================================

def build_rare_inventory_with_banks(inventory_df: DataFrame, blood_banks_df: DataFrame) -> DataFrame:
    """Rare-blood-group inventory rows joined with their blood bank details.
    Shared by Gold Datasets 4, 7, and the ranking-style analytics, so the
    join logic exists in exactly one place."""
    rare_inventory = inventory_df.filter(F.col("blood_group").isin(RARE_BLOOD_GROUPS))
    banks = blood_banks_df.select(
        "blood_bank_id", "blood_bank_name", "city", "state", "latitude", "longitude", "storage_capacity",
    )
    return rare_inventory.join(banks, on="blood_bank_id", how="left")


# ============================================================================
# GOLD DATASET 4 — RARE BLOOD AVAILABILITY (record-level, most important table)
# ============================================================================

def build_rare_blood_availability(rare_with_banks: DataFrame) -> DataFrame:
    df = rare_with_banks.withColumn("availability_status", availability_status_expr("available_units"))
    return df.select(
        "blood_bank_id", "blood_bank_name", "city", "state", "blood_group",
        "available_units", "storage_capacity", "availability_status",
        "last_updated", "latitude", "longitude",
    ).orderBy("blood_group", F.desc("available_units"))


# ============================================================================
# GOLD DATASET 5 — RARE BLOOD SUMMARY (blood-group grain)
# ============================================================================

def build_rare_blood_summary(inventory_df: DataFrame) -> DataFrame:
    rare_inventory = inventory_df.filter(F.col("blood_group").isin(RARE_BLOOD_GROUPS))

    totals = rare_inventory.groupBy("blood_group").agg(
        F.sum("available_units").alias("total_available_units"),
        F.countDistinct("blood_bank_id").alias("number_of_blood_banks"),
    )
    with_stock = (
        rare_inventory.filter(F.col("available_units") > 0)
        .groupBy("blood_group")
        .agg(F.countDistinct("blood_bank_id").alias("number_of_banks_with_stock"))
    )
    out_of_stock = (
        rare_inventory.filter(F.col("available_units") == 0)
        .groupBy("blood_group")
        .agg(F.countDistinct("blood_bank_id").alias("number_of_banks_out_of_stock"))
    )

    summary = (
        totals.join(with_stock, on="blood_group", how="left")
        .join(out_of_stock, on="blood_group", how="left")
        .withColumn("number_of_banks_with_stock", F.coalesce(F.col("number_of_banks_with_stock"), F.lit(0)))
        .withColumn("number_of_banks_out_of_stock", F.coalesce(F.col("number_of_banks_out_of_stock"), F.lit(0)))
    )
    summary = summary.withColumn(
        "average_units_per_bank",
        F.when(F.col("number_of_blood_banks") > 0, F.round(F.col("total_available_units") / F.col("number_of_blood_banks"), 2)),
    )
    summary = summary.withColumn("availability_status", availability_status_expr("total_available_units"))

    return summary.select(
        "blood_group", "total_available_units", "number_of_blood_banks",
        "number_of_banks_with_stock", "number_of_banks_out_of_stock",
        "average_units_per_bank", "availability_status",
    ).orderBy("blood_group")


# ============================================================================
# GOLD DATASET 6 — BLOOD SHORTAGE REPORT
# ============================================================================

def build_blood_shortage_report(rare_blood_summary: DataFrame) -> DataFrame:
    report = rare_blood_summary.withColumnRenamed(
        "number_of_banks_with_stock", "banks_with_stock"
    ).withColumnRenamed("availability_status", "shortage_status")
    report = report.withColumn("severity_score", severity_score_expr("shortage_status"))

    return report.select(
        "blood_group", "total_available_units", "number_of_blood_banks",
        "banks_with_stock", "shortage_status", "severity_score",
    ).orderBy(F.desc("severity_score"), "total_available_units")


# ============================================================================
# GOLD DATASET 7 — BLOOD BANK RARE STOCK
# ============================================================================

def build_blood_bank_rare_stock(rare_with_banks: DataFrame) -> DataFrame:
    """Answers: 'Where can a patient or hospital find a specific rare blood
    type?' A second, filtered (available_units > 0 only) variant is written
    separately as an optional convenience dataset for the dashboard."""
    df = rare_with_banks.withColumn("availability_status", availability_status_expr("available_units"))
    return df.select(
        "blood_bank_id", "blood_bank_name", "city", "state", "blood_group",
        "available_units", "availability_status", "latitude", "longitude", "last_updated",
    ).orderBy("blood_group", F.desc("available_units"))


# ============================================================================
# GOLD DATASET 8 — CITY-WISE BLOOD AVAILABILITY (rare blood, by city)
# ============================================================================

def build_city_blood_availability(rare_with_banks: DataFrame) -> DataFrame:
    totals = rare_with_banks.groupBy("city", "state", "blood_group").agg(
        F.sum("available_units").alias("total_available_units"),
        F.countDistinct("blood_bank_id").alias("number_of_blood_banks"),
    )
    with_stock = (
        rare_with_banks.filter(F.col("available_units") > 0)
        .groupBy("city", "state", "blood_group")
        .agg(F.countDistinct("blood_bank_id").alias("banks_with_stock"))
    )

    city_summary = totals.join(with_stock, on=["city", "state", "blood_group"], how="left")
    city_summary = city_summary.withColumn("banks_with_stock", F.coalesce(F.col("banks_with_stock"), F.lit(0)))
    city_summary = city_summary.withColumn("availability_status", availability_status_expr("total_available_units"))

    return city_summary.select(
        "city", "state", "blood_group", "total_available_units",
        "number_of_blood_banks", "banks_with_stock", "availability_status",
    ).orderBy("blood_group", F.desc("total_available_units"))


# ============================================================================
# GOLD DATASET 9 — BLOOD BANK PERFORMANCE
# ============================================================================

def build_blood_bank_performance(
    donations_df: DataFrame, blood_banks_df: DataFrame, inventory_df: DataFrame
) -> DataFrame:
    """Combines donation activity (from Silver donations) with CURRENT rare
    blood stock (from Silver inventory) for an overall activity + capability
    view of each blood bank."""
    completed = completed_donations_only(donations_df)
    donation_activity = completed.groupBy("blood_bank_id").agg(
        F.count("donation_id").alias("total_donations_received"),
        F.sum("units_donated").alias("total_units_received"),
        F.countDistinct("donor_id").alias("unique_donors"),
    )

    rare_inventory = inventory_df.filter(F.col("blood_group").isin(RARE_BLOOD_GROUPS))
    rare_stock = rare_inventory.groupBy("blood_bank_id").agg(
        F.sum("available_units").alias("rare_blood_units"),
        F.countDistinct(F.when(F.col("available_units") > 0, F.col("blood_group"))).alias("rare_blood_types_available"),
    )

    banks = blood_banks_df.select("blood_bank_id", "blood_bank_name", "city", "state")

    performance = (
        banks.join(donation_activity, on="blood_bank_id", how="left")
        .join(rare_stock, on="blood_bank_id", how="left")
        .fillna(0, subset=["total_donations_received", "total_units_received", "unique_donors",
                            "rare_blood_units", "rare_blood_types_available"])
    )

    return performance.select(
        "blood_bank_id", "blood_bank_name", "city", "state",
        "total_donations_received", "total_units_received", "unique_donors",
        "rare_blood_units", "rare_blood_types_available",
    ).orderBy(F.desc("total_donations_received"))


# ============================================================================
# GOLD DATASET 10 — DONATION CAMP PERFORMANCE
# ============================================================================

def build_donation_camp_performance(donations_df: DataFrame, camps_df: DataFrame) -> DataFrame:
    completed = completed_donations_only(donations_df).filter(F.col("camp_id").isNotNull())

    camp_activity = completed.groupBy("camp_id").agg(
        F.count("donation_id").alias("total_donations"),
        F.sum("units_donated").alias("total_units_collected"),
        F.countDistinct("donor_id").alias("unique_donors"),
    )

    camps = camps_df.select(F.col("camp_id"), F.col("camp_name"), F.col("city"))
    performance = camps.join(camp_activity, on="camp_id", how="left").fillna(
        0, subset=["total_donations", "total_units_collected", "unique_donors"]
    )

    return performance.select(
        "camp_id", "camp_name", "city", "total_donations", "total_units_collected", "unique_donors",
    ).orderBy(F.desc("total_donations"))


# ============================================================================
# GOLD DATASET 11 — DONOR SUMMARY
# ============================================================================

def build_donor_summary(donations_df: DataFrame, donors_df: DataFrame) -> DataFrame:
    """Every donor appears, even those with zero completed donations
    (total_donations/total_units_donated default to 0, dates stay null) so
    this is a true donor roster, not just a list of active contributors."""
    completed = completed_donations_only(donations_df)
    donor_activity = completed.groupBy("donor_id").agg(
        F.count("donation_id").alias("total_donations"),
        F.sum("units_donated").alias("total_units_donated"),
        F.min("donation_date").alias("first_donation_date"),
        F.max("donation_date").alias("last_donation_date"),
    )

    donors = donors_df.select(
        F.col("donor_id"),
        F.col("full_name").alias("name"),
        F.col("blood_group"),
        F.col("city"),
    )

    summary = donors.join(donor_activity, on="donor_id", how="left")
    summary = summary.fillna(0, subset=["total_donations", "total_units_donated"])
    summary = summary.withColumn("is_rare_blood_donor", F.col("blood_group").isin(RARE_BLOOD_GROUPS))

    return summary.select(
        "donor_id", "name", "blood_group", "city",
        "total_donations", "total_units_donated",
        "first_donation_date", "last_donation_date", "is_rare_blood_donor",
    ).orderBy(F.desc("total_donations"))


# ============================================================================
# GOLD DATASET 12 — RARE DONOR SUMMARY
# ============================================================================

def build_rare_donor_summary(donor_summary: DataFrame) -> DataFrame:
    """Aggregates the donor_summary table down to one row per rare blood
    group, answering 'how many potential rare-blood donors are available?'.
    Classification uses the DONOR's own recorded blood_group (not the
    blood_group on any individual donation)."""
    rare_donors = donor_summary.filter(F.col("is_rare_blood_donor"))

    summary = rare_donors.groupBy("blood_group").agg(
        F.countDistinct("donor_id").alias("total_donors"),
        F.countDistinct(F.when(F.col("total_donations") > 0, F.col("donor_id"))).alias("active_donors"),
        F.sum("total_donations").alias("total_donations"),
        F.sum("total_units_donated").alias("total_units_donated"),
    )

    return summary.select(
        "blood_group", "total_donors", "active_donors", "total_donations", "total_units_donated"
    ).orderBy("blood_group")


# ============================================================================
# WRITE GOLD OUTPUTS
# ============================================================================

def write_gold_dataset(df: DataFrame, path: Path, label: str) -> None:
    """Write a Gold dataset as a single Parquet file (coalesce(1)) -- these
    tables are small by design and meant to be easy to inspect and consume
    directly from the Streamlit dashboard."""
    df.coalesce(1).write.mode("overwrite").parquet(str(path))
    print(f"  {label} -> {path}")


# ============================================================================
# FINAL SUMMARY LOGGING
# ============================================================================

def print_final_summary(
    donations_df: DataFrame,
    rare_blood_summary: DataFrame,
    blood_bank_rare_stock: DataFrame,
) -> None:
    completed = completed_donations_only(donations_df)
    total_donations = completed.count()
    total_units = completed.agg(F.sum("units_donated").alias("total")).collect()[0]["total"] or 0

    print("\n" + "=" * 40)
    print("GOLD LAYER COMPLETED")
    print("=" * 40)
    print(f"\nTotal donations: {total_donations}")
    print(f"Total donated units: {total_units}")

    print("\nRare blood availability:")
    rare_rows = rare_blood_summary.select("blood_group", "total_available_units").collect()
    for row in rare_rows:
        print(f"{row['blood_group']:<4} : {row['total_available_units']} units")

    critical_groups = [
        r["blood_group"] for r in rare_blood_summary.filter(F.col("availability_status") == "CRITICAL").collect()
    ]
    out_of_stock_groups = [
        r["blood_group"] for r in rare_blood_summary.filter(F.col("availability_status") == "OUT_OF_STOCK").collect()
    ]
    print(f"\nCritical blood groups:\n{', '.join(critical_groups) if critical_groups else 'None'}")
    print(f"\nOut of stock:\n{', '.join(out_of_stock_groups) if out_of_stock_groups else 'None'}")

    for group in RARE_BLOOD_GROUPS:
        bank_count = (
            blood_bank_rare_stock.filter((F.col("blood_group") == group) & (F.col("available_units") > 0))
            .select("blood_bank_id")
            .distinct()
            .count()
        )
        print(f"\nBlood banks with {group}:\n{bank_count}")

    print(f"\nGold datasets written successfully.\nOutput directory: {GOLD_DIR}")
    print("=" * 40)


# ============================================================================
# MAIN
# ============================================================================

def main() -> None:
    print("=" * 40)
    print("PHASE 8: GOLD LAYER AGGREGATION")
    print("Enterprise Rare Blood Type Availability Analytics")
    print("=" * 40)

    spark = create_spark_session()

    try:
        print("\nReading Silver datasets...")
        donors_df = read_silver_dataset(
            spark, SILVER_DONORS, "donors",
            ["donor_id", "full_name", "blood_group", "city"],
        )
        blood_banks_df = read_silver_dataset(
            spark, SILVER_BLOOD_BANKS, "blood_banks",
            ["blood_bank_id", "blood_bank_name", "city", "state", "latitude", "longitude", "storage_capacity"],
        )
        inventory_df = read_silver_dataset(
            spark, SILVER_INVENTORY, "blood_inventory",
            ["inventory_id", "blood_bank_id", "blood_group", "available_units", "last_updated"],
        )
        camps_df = read_silver_dataset(
            spark, SILVER_DONATION_CAMPS, "donation_camps",
            ["camp_id", "camp_name", "city"],
        )
        donations_df = read_silver_dataset(
            spark, SILVER_DONATIONS, "donations",
            ["donation_id", "donor_id", "blood_bank_id", "camp_id", "donation_date",
             "blood_group", "units_donated", "donation_status"],
        )

        print(f"Donors loaded: {donors_df.count()}")
        print(f"Blood banks loaded: {blood_banks_df.count()}")
        print(f"Inventory records loaded: {inventory_df.count()}")
        print(f"Donation camps loaded: {camps_df.count()}")
        print(f"Donations loaded: {donations_df.count()}")

        run_quality_checks(donors_df, blood_banks_df, inventory_df, camps_df, donations_df)

        # ---- Priority 6: overall donation analytics ----
        print("\nBuilding daily donation summary...")
        daily_summary = build_daily_donation_summary(donations_df)

        print("Building monthly donation summary...")
        monthly_summary = build_monthly_donation_summary(donations_df)

        print("Building blood group distribution...")
        blood_group_distribution = build_blood_group_distribution(donations_df)

        # ---- Priority 1-4: rare blood availability, locations, shortages, city ----
        rare_with_banks = build_rare_inventory_with_banks(inventory_df, blood_banks_df)

        print("Building rare blood availability (Priority 1)...")
        rare_blood_availability = build_rare_blood_availability(rare_with_banks)

        print("Building rare blood summary...")
        rare_blood_summary = build_rare_blood_summary(inventory_df)

        print("Building blood shortage report (Priority 3)...")
        blood_shortage_report = build_blood_shortage_report(rare_blood_summary)

        print("Building blood bank rare stock (Priority 2)...")
        blood_bank_rare_stock = build_blood_bank_rare_stock(rare_with_banks)
        blood_bank_rare_stock_available = blood_bank_rare_stock.filter(F.col("available_units") > 0)

        print("Building city-wise blood availability (Priority 4)...")
        city_blood_availability = build_city_blood_availability(rare_with_banks)

        # ---- Priority 7: blood bank & camp performance ----
        print("Building blood bank performance...")
        blood_bank_performance = build_blood_bank_performance(donations_df, blood_banks_df, inventory_df)

        print("Building donation camp performance...")
        donation_camp_performance = build_donation_camp_performance(donations_df, camps_df)

        # ---- Priority 5: donor analytics ----
        print("Building donor summary...")
        donor_summary = build_donor_summary(donations_df, donors_df)

        print("Building rare donor summary (Priority 5)...")
        rare_donor_summary = build_rare_donor_summary(donor_summary)

        # ---- Write everything to Gold ----
        print("\nWriting Gold datasets...")
        write_gold_dataset(daily_summary, GOLD_DAILY_DONATION_SUMMARY, "daily_donation_summary")
        write_gold_dataset(monthly_summary, GOLD_MONTHLY_DONATION_SUMMARY, "monthly_donation_summary")
        write_gold_dataset(blood_group_distribution, GOLD_BLOOD_GROUP_DISTRIBUTION, "blood_group_distribution")
        write_gold_dataset(rare_blood_availability, GOLD_RARE_BLOOD_AVAILABILITY, "rare_blood_availability")
        write_gold_dataset(rare_blood_summary, GOLD_RARE_BLOOD_SUMMARY, "rare_blood_summary")
        write_gold_dataset(blood_shortage_report, GOLD_BLOOD_SHORTAGE_REPORT, "blood_shortage_report")
        write_gold_dataset(blood_bank_rare_stock, GOLD_BLOOD_BANK_RARE_STOCK, "blood_bank_rare_stock")
        write_gold_dataset(
            blood_bank_rare_stock_available, GOLD_BLOOD_BANK_RARE_STOCK_AVAILABLE,
            "blood_bank_rare_stock_available (optional, available_units > 0 only)",
        )
        write_gold_dataset(city_blood_availability, GOLD_CITY_BLOOD_AVAILABILITY, "city_blood_availability")
        write_gold_dataset(blood_bank_performance, GOLD_BLOOD_BANK_PERFORMANCE, "blood_bank_performance")
        write_gold_dataset(donation_camp_performance, GOLD_DONATION_CAMP_PERFORMANCE, "donation_camp_performance")
        write_gold_dataset(donor_summary, GOLD_DONOR_SUMMARY, "donor_summary")
        write_gold_dataset(rare_donor_summary, GOLD_RARE_DONOR_SUMMARY, "rare_donor_summary")

        print_final_summary(donations_df, rare_blood_summary, blood_bank_rare_stock)

    except (FileNotFoundError, ValueError) as exc:
        print(f"\nERROR: {exc}")
        raise SystemExit(1)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
