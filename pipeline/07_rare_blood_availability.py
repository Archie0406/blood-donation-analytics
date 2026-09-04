"""
Enterprise Rare Blood Type Availability Analytics
Phase 7 — Inventory & Rare Blood Availability Processing
=====================================================

OBJECTIVE
-----------
This is the phase where the project's central business question finally gets
answered directly: "WHERE IS THE RARE BLOOD AVAILABLE?"

Phase 7 reads the clean Silver datasets produced by Phase 6 and produces a
set of small, Gold-ready analytical tables focused on:

    - current inventory availability (all blood groups, then rare groups)
    - which specific blood banks currently hold rare blood
    - which rare blood groups are in shortage
    - which blood banks have zero stock of a rare blood group
    - city-wise rare blood availability
    - a ranking of blood banks by rare blood availability
    - how donations are contributing to rare blood supply
    - a simple, transparent, rule-based priority score per rare blood group

This phase does NOT modify Bronze or Silver data -- it only reads Silver and
writes new Gold-ready outputs to data/gold/. It does not implement the final
Phase 8 Gold aggregation layer or the Phase 9 Streamlit dashboard.

RARE BLOOD DISCLAIMER
------------------------
The blood groups A-, B-, AB-, O- are treated as "rare/priority" groups for
THIS PROJECT's analytics only (see RARE_BLOOD_GROUPS below). This is a
project-defined classification for demonstration purposes, not a claim that
these groups are universally rare in every population.
"""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

# ============================================================================
# CONFIGURATION (single source of truth for thresholds, groups, and paths)
# ============================================================================

APP_NAME = "Enterprise Rare Blood Analytics - Phase 7 Rare Blood Availability"

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent

SILVER_DIR = PROJECT_ROOT / "data" / "silver"
GOLD_DIR = PROJECT_ROOT / "data" / "gold"

SILVER_DONORS = SILVER_DIR / "donors"
SILVER_BLOOD_BANKS = SILVER_DIR / "blood_banks"
SILVER_INVENTORY = SILVER_DIR / "blood_inventory"
SILVER_DONATION_CAMPS = SILVER_DIR / "donation_camps"
SILVER_DONATIONS = SILVER_DIR / "donations"

GOLD_RARE_BLOOD_AVAILABILITY = GOLD_DIR / "gold_rare_blood_availability"
GOLD_RARE_BLOOD_CENTERS = GOLD_DIR / "gold_rare_blood_centers"
GOLD_BLOOD_SHORTAGE_REPORT = GOLD_DIR / "gold_blood_shortage_report"
GOLD_ZERO_RARE_INVENTORY = GOLD_DIR / "gold_zero_rare_inventory"
GOLD_CITY_RARE_BLOOD_AVAILABILITY = GOLD_DIR / "gold_city_rare_blood_availability"
GOLD_RARE_BLOOD_CENTER_RANKING = GOLD_DIR / "gold_rare_blood_center_ranking"
GOLD_RARE_BLOOD_DONATION_SUMMARY = GOLD_DIR / "gold_rare_blood_donation_summary"
GOLD_RARE_BLOOD_DAILY_TREND = GOLD_DIR / "gold_rare_blood_daily_trend"

# All valid blood groups
VALID_BLOOD_GROUPS = ["A+", "A-", "B+", "B-", "AB+", "AB-", "O+", "O-"]

# Project-defined rare/priority blood groups. Change this list to redefine
# "rare" for the whole project -- nothing else in this script hard-codes it.
RARE_BLOOD_GROUPS = ["A-", "B-", "AB-", "O-"]

# Availability-status thresholds, applied to a unit count (whatever grain
# that count is at: a single center's stock, a city total, or a blood-group
# total). Configurable in ONE place.
CRITICAL_MAX_UNITS = 2    # 0-2 units    -> CRITICAL
LOW_MAX_UNITS = 5         # 3-5 units    -> LOW
MODERATE_MAX_UNITS = 10   # 6-10 units   -> MODERATE
                          # >10 units     -> GOOD

# Rule-based recommended actions per availability/shortage status
RECOMMENDED_ACTIONS = {
    "CRITICAL": "Urgent donor mobilization required",
    "LOW": "Targeted donation campaign recommended",
    "MODERATE": "Continue monitoring",
    "GOOD": "No immediate action required",
}

# Rule-based base priority score per status (higher = more urgent). This is
# a transparent, explainable formula -- NOT machine learning. See
# calculate_priority_score() for the full rule.
STATUS_BASE_PRIORITY_SCORE = {
    "CRITICAL": 100,
    "LOW": 75,
    "MODERATE": 40,
    "GOOD": 10,
}

# Only donations in this status are treated as having actually contributed
# stock (cancelled/rejected donations did not add usable blood).
CONTRIBUTING_DONATION_STATUS = "Completed"


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
# READ + VALIDATE SILVER DATA
# ============================================================================

def read_silver_dataset(spark: SparkSession, path: Path, name: str, required_columns: list[str]) -> DataFrame:
    """Read one Silver Parquet dataset and validate that every required
    column is present. Fails fast with a clear message if the dataset or a
    required column is missing -- Phase 7 should never silently work with
    an incomplete schema."""
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
# CLEANING / DEFENSIVE STANDARDIZATION
# ============================================================================

def defensive_standardize_blood_group(df: DataFrame, col_name: str = "blood_group") -> DataFrame:
    """Silver already standardizes blood_group text, but Phase 7 applies a
    light defensive pass (trim + uppercase) in case it is ever run against a
    Silver dataset produced outside this project's own Phase 6 script."""
    return df.withColumn(col_name, F.upper(F.trim(F.col(col_name))))


# ============================================================================
# STATUS / SCORING HELPERS (reused across every task)
# ============================================================================

def availability_status_expr(units_col: str) -> F.Column:
    """Return a Spark Column expression classifying a unit count into
    CRITICAL / LOW / MODERATE / GOOD using the configured thresholds.
    Reused everywhere a unit count needs a status label, whatever the grain
    (a single center, a city total, or a blood-group total)."""
    units = F.col(units_col)
    return (
        F.when(units.isNull(), F.lit(None))
        .when(units <= CRITICAL_MAX_UNITS, F.lit("CRITICAL"))
        .when(units <= LOW_MAX_UNITS, F.lit("LOW"))
        .when(units <= MODERATE_MAX_UNITS, F.lit("MODERATE"))
        .otherwise(F.lit("GOOD"))
    )


def recommended_action_expr(status_col: str) -> F.Column:
    """Map an availability/shortage status to a rule-based recommended
    action using the RECOMMENDED_ACTIONS configuration."""
    mapping = F.create_map([F.lit(x) for pair in RECOMMENDED_ACTIONS.items() for x in pair])
    return mapping[F.col(status_col)]


# ============================================================================
# TASK 2 — CURRENT INVENTORY SUMMARY (ALL BLOOD GROUPS)
# ============================================================================

def build_inventory_summary(inventory_df: DataFrame) -> DataFrame:
    """Summarize current inventory by blood group.

    DESIGN NOTE: number_of_centers counts blood banks that currently hold
    STOCK (available_units > 0) of that blood group -- a bank with a
    tracked-but-empty inventory row does not count as "holding" the group
    for this business question. minimum_units/maximum_units are computed
    across ALL matching inventory rows (including zero-stock ones) so the
    true range, including any zero, is visible.
    """
    total_by_group = (
        inventory_df.groupBy("blood_group")
        .agg(
            F.sum("available_units").alias("total_available_units"),
            F.min("available_units").alias("minimum_units"),
            F.max("available_units").alias("maximum_units"),
        )
    )

    centers_with_stock = (
        inventory_df.filter(F.col("available_units") > 0)
        .groupBy("blood_group")
        .agg(F.countDistinct("blood_bank_id").alias("number_of_centers"))
    )

    summary = total_by_group.join(centers_with_stock, on="blood_group", how="left")
    summary = summary.withColumn("number_of_centers", F.coalesce(F.col("number_of_centers"), F.lit(0)))
    summary = summary.withColumn(
        "average_units_per_center",
        F.when(F.col("number_of_centers") > 0, F.col("total_available_units") / F.col("number_of_centers")),
    )

    return summary.select(
        "blood_group", "total_available_units", "number_of_centers",
        "average_units_per_center", "minimum_units", "maximum_units",
    )


# ============================================================================
# TASK 3 — RARE BLOOD AVAILABILITY (+ TASK 11 PRIORITY SCORE)
# ============================================================================

def calculate_priority_score(status_col: str, centers_col: str) -> F.Column:
    """Transparent, rule-based priority score (NOT machine learning).

    Rule:
        base_score  = STATUS_BASE_PRIORITY_SCORE[status]   (urgency tier)
        adjustment  = up to -10 points as the number of centers holding
                      stock increases, capped at 10 centers, reflecting
                      that a shortage spread across more centers is
                      marginally less urgent than the same shortage
                      concentrated in very few centers.
        final_score = base_score - adjustment   (never below 0)

    This keeps the score fully explainable in a presentation: "a CRITICAL
    blood group starts at 100 points, minus a small discount for how many
    centers already carry it."
    """
    base_score = F.create_map(
        [F.lit(x) for pair in STATUS_BASE_PRIORITY_SCORE.items() for x in pair]
    )[F.col(status_col)]

    centers_capped = F.least(F.col(centers_col), F.lit(10))
    adjustment = centers_capped * F.lit(1)  # 1 point discount per center holding stock, capped at 10

    return F.greatest(base_score - adjustment, F.lit(0)).cast("int")


def build_rare_blood_availability(inventory_summary_all: DataFrame) -> DataFrame:
    """Task 3: filter the all-blood-group summary down to the configured
    rare/priority groups, and add availability_status + priority_score."""
    rare_summary = inventory_summary_all.filter(F.col("blood_group").isin(RARE_BLOOD_GROUPS))

    rare_summary = rare_summary.withColumn(
        "availability_status", availability_status_expr("total_available_units")
    )
    rare_summary = rare_summary.withColumn(
        "priority_score", calculate_priority_score("availability_status", "number_of_centers")
    )

    return rare_summary.select(
        "blood_group", "total_available_units", "number_of_centers",
        "average_units_per_center", "minimum_units", "maximum_units",
        "availability_status", "priority_score",
    )


# ============================================================================
# TASK 4 — BLOOD BANKS WITH RARE BLOOD (CENTER-LEVEL)
# ============================================================================

def build_rare_blood_centers(inventory_df: DataFrame, blood_banks_df: DataFrame) -> DataFrame:
    """Task 4: the most important table in this phase -- directly answers
    "which centers currently have this rare blood type?" at the
    blood_group + blood_bank grain, with availability_status computed
    per-center (not the aggregate blood-group status)."""
    rare_inventory = inventory_df.filter(F.col("blood_group").isin(RARE_BLOOD_GROUPS))

    banks = blood_banks_df.select(
        F.col("blood_bank_id"),
        F.col("blood_bank_name"),
        F.col("city"),
        F.col("state"),
    )

    centers = rare_inventory.join(banks, on="blood_bank_id", how="left")
    centers = centers.withColumn("availability_status", availability_status_expr("available_units"))

    return (
        centers.select(
            "blood_bank_id", "blood_bank_name", "city", "state",
            "blood_group", "available_units", "last_updated", "availability_status",
        )
        .orderBy("blood_group", F.desc("available_units"))
    )


# ============================================================================
# TASK 5 — CRITICAL SHORTAGE REPORT
# ============================================================================

def build_shortage_report(rare_blood_availability: DataFrame) -> DataFrame:
    """Task 5: reuse the Task 3 blood-group-level summary, rename status to
    shortage_level, and add a rule-based recommended_action."""
    report = rare_blood_availability.withColumnRenamed("availability_status", "shortage_level")
    report = report.withColumn("recommended_action", recommended_action_expr("shortage_level"))

    return report.select(
        "blood_group", "total_available_units", "number_of_centers",
        "shortage_level", "recommended_action",
    ).orderBy(F.asc("total_available_units"))


# ============================================================================
# TASK 6 — CENTERS WITH ZERO RARE BLOOD STOCK
# ============================================================================

def build_zero_rare_inventory(inventory_df: DataFrame, blood_banks_df: DataFrame) -> DataFrame:
    """Task 6: rare blood groups with exactly zero available units at a
    specific blood bank. Important because the bank still "exists" in the
    system for that blood group -- it's just currently empty."""
    rare_inventory = inventory_df.filter(
        F.col("blood_group").isin(RARE_BLOOD_GROUPS) & (F.col("available_units") == 0)
    )

    banks = blood_banks_df.select(
        F.col("blood_bank_id"), F.col("blood_bank_name"), F.col("city"), F.col("state"),
    )

    zero_stock = rare_inventory.join(banks, on="blood_bank_id", how="left")
    zero_stock = zero_stock.withColumn("status", F.lit("NO STOCK"))

    return zero_stock.select(
        "blood_bank_id", "blood_bank_name", "city", "state",
        "blood_group", "available_units", "status",
    ).orderBy("blood_group", "city")


# ============================================================================
# TASK 7 — CITY-WISE RARE BLOOD AVAILABILITY
# ============================================================================

def build_city_rare_blood_availability(inventory_df: DataFrame, blood_banks_df: DataFrame) -> DataFrame:
    """Task 7: rare blood availability aggregated by city + state."""
    rare_inventory = inventory_df.filter(F.col("blood_group").isin(RARE_BLOOD_GROUPS))

    banks = blood_banks_df.select(
        F.col("blood_bank_id"), F.col("city"), F.col("state"),
    )
    rare_with_location = rare_inventory.join(banks, on="blood_bank_id", how="left")

    totals = (
        rare_with_location.groupBy("city", "state", "blood_group")
        .agg(F.sum("available_units").alias("total_available_units"))
    )
    centers_with_stock = (
        rare_with_location.filter(F.col("available_units") > 0)
        .groupBy("city", "state", "blood_group")
        .agg(F.countDistinct("blood_bank_id").alias("number_of_centers"))
    )

    city_summary = totals.join(centers_with_stock, on=["city", "state", "blood_group"], how="left")
    city_summary = city_summary.withColumn("number_of_centers", F.coalesce(F.col("number_of_centers"), F.lit(0)))
    city_summary = city_summary.withColumn("status", availability_status_expr("total_available_units"))

    return city_summary.select(
        "city", "state", "blood_group", "total_available_units", "number_of_centers", "status",
    ).orderBy("blood_group", F.desc("total_available_units"))


# ============================================================================
# TASK 8 — BLOOD BANK RANKING (Window function)
# ============================================================================

def build_rare_blood_center_ranking(inventory_df: DataFrame, blood_banks_df: DataFrame) -> DataFrame:
    """Task 8: rank every blood bank by its total rare-blood stock, using a
    Spark Window function for the ranking itself."""
    rare_inventory = inventory_df.filter(F.col("blood_group").isin(RARE_BLOOD_GROUPS))

    per_bank = rare_inventory.groupBy("blood_bank_id").agg(
        F.sum("available_units").alias("rare_blood_units"),
        F.countDistinct(F.when(F.col("available_units") > 0, F.col("blood_group"))).alias("rare_blood_types_available"),
    )

    banks = blood_banks_df.select(
        F.col("blood_bank_id"), F.col("blood_bank_name"), F.col("city"), F.col("state"),
    )
    ranked_base = per_bank.join(banks, on="blood_bank_id", how="left")

    ranking_window = Window.orderBy(F.desc("rare_blood_units"))
    ranked = ranked_base.withColumn("center_rank", F.rank().over(ranking_window))

    return ranked.select(
        "blood_bank_id", "blood_bank_name", "city", "state",
        "rare_blood_units", "rare_blood_types_available", "center_rank",
    ).orderBy("center_rank")


# ============================================================================
# TASK 9 — RARE BLOOD DONATION CONTRIBUTION
# ============================================================================

def build_rare_blood_donation_summary(donations_df: DataFrame) -> DataFrame:
    """Task 9: how donations are contributing to rare blood supply.

    Only donations with status == "Completed" are counted as having
    actually contributed usable stock -- a Cancelled or Rejected donation
    never became available inventory.
    """
    contributing = donations_df.filter(
        F.col("is_rare_blood") & (F.col("donation_status") == CONTRIBUTING_DONATION_STATUS)
    )

    summary = contributing.groupBy("blood_group").agg(
        F.count("donation_id").alias("total_donations"),
        F.sum("units_donated").alias("total_units_donated"),
        F.countDistinct("donor_id").alias("active_donors"),
        F.countDistinct("blood_bank_id").alias("blood_banks_receiving_donations"),
    )

    return summary.select(
        "blood_group", "total_donations", "total_units_donated",
        "active_donors", "blood_banks_receiving_donations",
    ).orderBy("blood_group")


# ============================================================================
# TASK 10 — DAILY DONATION TREND (NOT current inventory)
# ============================================================================

def build_rare_blood_daily_trend(donations_df: DataFrame) -> DataFrame:
    """Task 10: a DONATION TREND over time, built entirely from donation
    history. This is explicitly NOT a historical inventory snapshot -- the
    Silver inventory dataset only contains a current point-in-time snapshot,
    and this script never invents historical inventory values."""
    contributing = donations_df.filter(
        F.col("is_rare_blood") & (F.col("donation_status") == CONTRIBUTING_DONATION_STATUS)
    )

    trend = contributing.groupBy(F.col("donation_date").alias("date"), "blood_group").agg(
        F.sum("units_donated").alias("units_donated"),
        F.count("donation_id").alias("donation_count"),
    )

    return trend.select("date", "blood_group", "units_donated", "donation_count").orderBy("date", "blood_group")


# ============================================================================
# TASK 12 — QUALITY CHECKS
# ============================================================================

def run_quality_checks(inventory_df: DataFrame, blood_banks_df: DataFrame) -> dict:
    """Run the Phase 7 inventory quality checks and print a concise summary.
    These checks are informational (Silver already flags most of these) --
    Phase 7 re-verifies them independently against the exact data it is
    about to use for rare blood availability analysis."""
    total_records = inventory_df.count()

    invalid_blood_groups = inventory_df.filter(~F.col("blood_group").isin(VALID_BLOOD_GROUPS)).count()
    negative_units = inventory_df.filter(F.col("available_units") < 0).count()
    null_blood_bank_id = inventory_df.filter(F.col("blood_bank_id").isNull()).count()
    null_blood_group = inventory_df.filter(F.col("blood_group").isNull()).count()
    null_available_units = inventory_df.filter(F.col("available_units").isNull()).count()
    null_last_updated = inventory_df.filter(F.col("last_updated").isNull()).count()
    non_numeric_units = inventory_df.filter(
        F.col("available_units").isNotNull() & ~F.col("available_units").cast("int").isNotNull()
    ).count()

    # Referential integrity: blood_bank_id must exist in blood_banks
    valid_bank_ids = blood_banks_df.select(F.col("blood_bank_id").alias("_valid_id"))
    missing_bank_matches = (
        inventory_df.join(valid_bank_ids, inventory_df["blood_bank_id"] == F.col("_valid_id"), "left")
        .filter(F.col("_valid_id").isNull())
        .count()
    )

    # Duplicate logical inventory keys: more than one row for the same
    # blood_bank_id + blood_group combination
    duplicate_keys = (
        inventory_df.groupBy("blood_bank_id", "blood_group")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    # Rare blood groups configured must all be within the valid set (a
    # sanity check on the configuration itself, not the data)
    invalid_rare_config = [g for g in RARE_BLOOD_GROUPS if g not in VALID_BLOOD_GROUPS]

    results = {
        "total_records": total_records,
        "invalid_blood_groups": invalid_blood_groups,
        "negative_units": negative_units,
        "null_blood_bank_id": null_blood_bank_id,
        "null_blood_group": null_blood_group,
        "null_available_units": null_available_units,
        "null_last_updated": null_last_updated,
        "non_numeric_units": non_numeric_units,
        "missing_bank_matches": missing_bank_matches,
        "duplicate_logical_keys": duplicate_keys,
        "invalid_rare_config": invalid_rare_config,
    }

    print("\n" + "=" * 45)
    print("PHASE 7 DATA QUALITY CHECKS")
    print("=" * 45)
    print(f"Inventory records: {results['total_records']}")
    print(f"Invalid blood groups: {results['invalid_blood_groups']}")
    print(f"Negative inventory records: {results['negative_units']}")
    print(f"Missing blood bank IDs (null): {results['null_blood_bank_id']}")
    print(f"Blood bank IDs not found in blood_banks: {results['missing_bank_matches']}")
    print(f"Null blood_group values: {results['null_blood_group']}")
    print(f"Null available_units values: {results['null_available_units']}")
    print(f"Null last_updated values: {results['null_last_updated']}")
    print(f"Duplicate (blood_bank_id, blood_group) records: {results['duplicate_logical_keys']}")
    if results["invalid_rare_config"]:
        print(f"WARNING: RARE_BLOOD_GROUPS contains invalid group(s): {results['invalid_rare_config']}")
    print("=" * 45)

    return results


# ============================================================================
# WRITE GOLD OUTPUTS
# ============================================================================

def write_gold_dataset(df: DataFrame, path: Path, label: str) -> None:
    """Write a small Gold-ready dataset as a single Parquet file (these
    tables are small by design and meant to be easy to inspect and consume
    directly from the Streamlit dashboard in a later phase)."""
    df.coalesce(1).write.mode("overwrite").parquet(str(path))
    print(f"  {label} -> {path}")


# ============================================================================
# TERMINAL SUMMARY
# ============================================================================

def print_final_summary(
    rare_availability: DataFrame,
    rare_centers: DataFrame,
    shortage_report: DataFrame,
) -> None:
    print("\n" + "=" * 40)
    print("RARE BLOOD AVAILABILITY ANALYSIS")
    print("=" * 40)

    print(f"\nRare Blood Groups:\n{', '.join(RARE_BLOOD_GROUPS)}")

    total_units_row = rare_availability.agg(F.sum("total_available_units").alias("total")).collect()[0]
    total_units = total_units_row["total"] or 0

    centers_with_rare_stock = rare_centers.filter(F.col("available_units") > 0).select("blood_bank_id").distinct().count()
    critical_count = rare_availability.filter(F.col("availability_status") == "CRITICAL").count()
    low_count = rare_availability.filter(F.col("availability_status") == "LOW").count()

    print(f"\nTotal Rare Blood Units: {total_units}")
    print(f"Centers With Rare Blood: {centers_with_rare_stock}")
    print(f"Critical Blood Groups: {critical_count}")
    print(f"Low Availability Blood Groups: {low_count}")

    print("\nTop Centers With Rare Blood:")
    (
        rare_centers.filter(F.col("available_units") > 0)
        .orderBy(F.desc("available_units"))
        .select("blood_group", "blood_bank_name", "city", "available_units", "availability_status")
        .show(10, truncate=False)
    )

    print("Critical Shortages:")
    shortage_report.filter(F.col("shortage_level") == "CRITICAL").show(truncate=False)

    print(f"Output datasets written to:\n{GOLD_DIR}")
    print("=" * 40)


# ============================================================================
# MAIN
# ============================================================================

def main() -> None:
    print("=" * 40)
    print("PHASE 7: RARE BLOOD AVAILABILITY PROCESSING")
    print("Enterprise Rare Blood Type Availability Analytics")
    print("=" * 40)

    spark = create_spark_session()

    try:
        print("\nReading Silver datasets...")
        inventory_df = read_silver_dataset(
            spark, SILVER_INVENTORY, "blood_inventory",
            ["inventory_id", "blood_bank_id", "blood_group", "available_units", "last_updated"],
        )
        blood_banks_df = read_silver_dataset(
            spark, SILVER_BLOOD_BANKS, "blood_banks",
            ["blood_bank_id", "blood_bank_name", "city", "state"],
        )
        donations_df = read_silver_dataset(
            spark, SILVER_DONATIONS, "donations",
            ["donation_id", "donor_id", "blood_bank_id", "donation_date",
             "blood_group", "units_donated", "donation_status", "is_rare_blood"],
        )

        print(f"Inventory records loaded: {inventory_df.count()}")
        print(f"Blood banks loaded: {blood_banks_df.count()}")
        print(f"Donation records loaded: {donations_df.count()}")

        inventory_df = defensive_standardize_blood_group(inventory_df)

        # ---- Task 12: quality checks (run before analysis) ----
        run_quality_checks(inventory_df, blood_banks_df)

        # ---- Task 2: current inventory summary (all blood groups) ----
        print("\nAnalyzing current inventory (all blood groups)...")
        inventory_summary_all = build_inventory_summary(inventory_df)

        # ---- Task 3 + 11: rare blood availability + priority score ----
        print("Calculating rare blood availability...")
        rare_availability = build_rare_blood_availability(inventory_summary_all)

        # ---- Task 4: centers with rare blood ----
        print("Identifying blood banks with rare blood...")
        rare_centers = build_rare_blood_centers(inventory_df, blood_banks_df)

        # ---- Task 5: shortage report ----
        print("Identifying critical shortages...")
        shortage_report = build_shortage_report(rare_availability)

        # ---- Task 6: zero-stock centers ----
        print("Identifying centers with zero rare blood stock...")
        zero_rare_inventory = build_zero_rare_inventory(inventory_df, blood_banks_df)

        # ---- Task 7: city-wise availability ----
        print("Calculating city-wise rare blood availability...")
        city_rare_availability = build_city_rare_blood_availability(inventory_df, blood_banks_df)

        # ---- Task 8: blood bank ranking ----
        print("Ranking blood banks by rare blood availability...")
        rare_center_ranking = build_rare_blood_center_ranking(inventory_df, blood_banks_df)

        # ---- Task 9: donation contribution ----
        print("Analyzing rare blood donation contribution...")
        rare_donation_summary = build_rare_blood_donation_summary(donations_df)

        # ---- Task 10: daily donation trend ----
        print("Building rare blood daily donation trend...")
        rare_daily_trend = build_rare_blood_daily_trend(donations_df)

        # ---- Task 13: write Gold outputs ----
        print("\nWriting Gold-ready datasets...")
        write_gold_dataset(rare_availability, GOLD_RARE_BLOOD_AVAILABILITY, "gold_rare_blood_availability")
        write_gold_dataset(rare_centers, GOLD_RARE_BLOOD_CENTERS, "gold_rare_blood_centers")
        write_gold_dataset(shortage_report, GOLD_BLOOD_SHORTAGE_REPORT, "gold_blood_shortage_report")
        write_gold_dataset(zero_rare_inventory, GOLD_ZERO_RARE_INVENTORY, "gold_zero_rare_inventory")
        write_gold_dataset(city_rare_availability, GOLD_CITY_RARE_BLOOD_AVAILABILITY, "gold_city_rare_blood_availability")
        write_gold_dataset(rare_center_ranking, GOLD_RARE_BLOOD_CENTER_RANKING, "gold_rare_blood_center_ranking")
        write_gold_dataset(rare_donation_summary, GOLD_RARE_BLOOD_DONATION_SUMMARY, "gold_rare_blood_donation_summary")
        write_gold_dataset(rare_daily_trend, GOLD_RARE_BLOOD_DAILY_TREND, "gold_rare_blood_daily_trend")

        # ---- Final terminal summary ----
        print_final_summary(rare_availability, rare_centers, shortage_report)

    except (FileNotFoundError, ValueError) as exc:
        print(f"\nERROR: {exc}")
        raise SystemExit(1)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
