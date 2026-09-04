"""
Enterprise Rare Blood Type Availability Analytics
Phase 10 — Shared Validation Engine
=====================================================

This module contains the actual validation LOGIC, returning plain,
structured dictionaries ({"status": "PASS"/"FAIL", ...details}). It has no
dependency on pytest or the report formatter, so the exact same functions
are reused by:

  - tests/*.py           (pytest wraps each result with a simple assert)
  - validation/validation_report.py  (formats results into a human report
                                       + validation_results.json)

PROJECT PATH / NAMING NOTE
------------------------------
The Phase 10 brief describes an idealized folder layout (e.g. bronze/inventory/,
gold/monthly_donation_trend/, gold/city_wise_blood_availability/,
gold/blood_bank_inventory_summary/). This project's actual pipeline (Phases
3-8) already established real names, which this module validates against:

    brief's name                    -> this project's actual name
    ------------------------------------------------------------
    bronze/inventory/                -> bronze/blood_inventory/
    donations.status                 -> donations.donation_status
    gold/monthly_donation_trend/     -> gold/monthly_donation_summary/
    gold/city_wise_blood_availability/ -> gold/city_blood_availability/
    gold/blood_bank_inventory_summary/ -> gold/rare_blood_summary/
                                          (closest existing analog -- a
                                          blood-bank-level inventory summary
                                          table was not created as a
                                          separate Gold dataset; rare_blood_
                                          summary and blood_bank_performance
                                          together cover this)

TESTING PRINCIPLES (see brief section 19)
----------------------------------------------
  - Validators are READ-ONLY. Nothing in this module writes to Bronze,
    Silver, or Gold.
  - Invalid records are never silently deleted -- validators count and
    report them.
  - Thresholds live in ONE config section (see below), not scattered
    through the module.
  - PySpark DataFrame operations are used for anything at real dataset
    scale; only small, already-aggregated results are collected to Python.
"""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# ============================================================================
# CONFIGURATION (single source of truth for paths, groups, thresholds)
# ============================================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent

BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"
SILVER_DIR = PROJECT_ROOT / "data" / "silver"
GOLD_DIR = PROJECT_ROOT / "data" / "gold"
STREAMING_LANDING_DIR = PROJECT_ROOT / "data" / "streaming" / "landing"
STREAMING_CHECKPOINT_DIR = PROJECT_ROOT / "data" / "checkpoints" / "bronze_streaming_donations"

BRONZE_PATHS = {
    "donors": BRONZE_DIR / "donors",
    "blood_banks": BRONZE_DIR / "blood_banks",
    "blood_inventory": BRONZE_DIR / "blood_inventory",
    "donation_camps": BRONZE_DIR / "donation_camps",
    "donations": BRONZE_DIR / "donations",
    "streaming_donations": BRONZE_DIR / "streaming_donations",
}

SILVER_PATHS = {
    "donors": SILVER_DIR / "donors",
    "blood_banks": SILVER_DIR / "blood_banks",
    "blood_inventory": SILVER_DIR / "blood_inventory",
    "donation_camps": SILVER_DIR / "donation_camps",
    "donations": SILVER_DIR / "donations",
    "enriched_donations": SILVER_DIR / "enriched_donations",
}

GOLD_PATHS = {
    "daily_donation_summary": GOLD_DIR / "daily_donation_summary",
    "monthly_donation_summary": GOLD_DIR / "monthly_donation_summary",
    "blood_group_distribution": GOLD_DIR / "blood_group_distribution",
    "rare_blood_availability": GOLD_DIR / "rare_blood_availability",
    "rare_blood_summary": GOLD_DIR / "rare_blood_summary",
    "blood_shortage_report": GOLD_DIR / "blood_shortage_report",
    "blood_bank_rare_stock": GOLD_DIR / "blood_bank_rare_stock",
    "city_blood_availability": GOLD_DIR / "city_blood_availability",
    "blood_bank_performance": GOLD_DIR / "blood_bank_performance",
    "donation_camp_performance": GOLD_DIR / "donation_camp_performance",
    "donor_summary": GOLD_DIR / "donor_summary",
    "rare_donor_summary": GOLD_DIR / "rare_donor_summary",
}

VALID_BLOOD_GROUPS = ["A+", "A-", "B+", "B-", "AB+", "AB-", "O+", "O-"]

# Project-defined rare/focus blood groups -- configurable in ONE place.
RARE_BLOOD_GROUPS = ["O-", "AB-", "B-", "A-"]

# Shortage classification thresholds -- configurable in ONE place, used by
# every validator that checks availability_status.
SHORTAGE_THRESHOLD = 10   # units < this  -> LOW (and units < CRITICAL -> CRITICAL)
CRITICAL_THRESHOLD = 5    # units < this  -> CRITICAL (0 units -> OUT_OF_STOCK)

DONOR_AGE_MIN, DONOR_AGE_MAX = 18, 65
DONATION_UNITS_MIN, DONATION_UNITS_MAX = 1, 5


# ============================================================================
# SPARK SESSION HELPER
# ============================================================================

def get_or_create_spark() -> SparkSession:
    spark = (
        SparkSession.builder.appName("Enterprise Rare Blood Analytics - Phase 10 Validation")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def read_parquet_safe(spark: SparkSession, path: Path) -> DataFrame | None:
    """Read a Parquet dataset, returning None (never raising) if missing or
    unreadable, so validators can report FAIL instead of crashing."""
    if not path.exists():
        return None
    try:
        return spark.read.parquet(str(path))
    except Exception:
        return None


def classify_status(units: int) -> str:
    """The exact shortage/availability rule under test (brief section 13)."""
    if units == 0:
        return "OUT_OF_STOCK"
    elif units < CRITICAL_THRESHOLD:
        return "CRITICAL"
    elif units < SHORTAGE_THRESHOLD:
        return "LOW"
    else:
        return "AVAILABLE"


# ============================================================================
# RESULT HELPERS
# ============================================================================

def _result(status: str, **details) -> dict:
    return {"status": status, **details}


def _pass_if(condition: bool, **details) -> dict:
    return _result("PASS" if condition else "FAIL", **details)


# ============================================================================
# SECTION 2 — BRONZE LAYER VALIDATION
# ============================================================================

BRONZE_REQUIRED_COLUMNS = {
    "donors": ["donor_id", "full_name", "blood_group", "city", "registration_date", "_ingested_at"],
    "blood_banks": ["blood_bank_id", "blood_bank_name", "city", "state", "_ingested_at"],
    "blood_inventory": ["inventory_id", "blood_bank_id", "blood_group", "available_units", "last_updated", "_ingested_at"],
    "donation_camps": ["camp_id", "camp_name", "city", "_ingested_at"],
    "donations": ["donation_id", "donor_id", "blood_bank_id", "blood_group", "donation_date", "units_donated", "_ingested_at"],
}


def validate_bronze_dataset(spark: SparkSession, name: str) -> dict:
    """Validate one Bronze dataset: exists, readable, required columns
    present, row count > 0, _ingested_at present."""
    path = BRONZE_PATHS[name]
    df = read_parquet_safe(spark, path)

    if df is None:
        return _result("FAIL", reason=f"Dataset not found or unreadable at {path}")

    required = BRONZE_REQUIRED_COLUMNS[name]
    missing_columns = [c for c in required if c not in df.columns]
    if missing_columns:
        return _result("FAIL", reason=f"Missing required column(s): {missing_columns}")

    row_count = df.count()
    if row_count == 0:
        return _result("FAIL", reason="Row count is zero", row_count=0)

    null_ingested_at = df.filter(F.col("_ingested_at").isNull()).count()

    return _pass_if(
        null_ingested_at == 0,
        row_count=row_count,
        null_ingested_at=null_ingested_at,
        reason=None if null_ingested_at == 0 else f"{null_ingested_at} rows have null _ingested_at",
    )


def validate_all_bronze_datasets(spark: SparkSession) -> dict:
    """Run validate_bronze_dataset for every expected Bronze dataset."""
    results = {name: validate_bronze_dataset(spark, name) for name in BRONZE_REQUIRED_COLUMNS}
    overall = "PASS" if all(r["status"] == "PASS" for r in results.values()) else "FAIL"
    return {"status": overall, "datasets": results}


# ============================================================================
# SECTION 3 — DUPLICATE DETECTION
# ============================================================================

def check_duplicates(df: DataFrame, key_columns: list[str], label: str) -> dict:
    """Detect duplicate records by key_columns (e.g. donation_id)."""
    duplicate_count = (
        df.groupBy(*key_columns).count().filter(F.col("count") > 1).count()
    )
    return _pass_if(duplicate_count == 0, label=label, key_columns=key_columns, duplicate_count=duplicate_count)


def validate_all_duplicates(
    donors_df: DataFrame, blood_banks_df: DataFrame, inventory_df: DataFrame,
    camps_df: DataFrame, donations_df: DataFrame,
) -> dict:
    results = {
        "donor_id": check_duplicates(donors_df, ["donor_id"], "Donor Duplicate Check"),
        "blood_bank_id": check_duplicates(blood_banks_df, ["blood_bank_id"], "Blood Bank Duplicate Check"),
        "inventory_id": check_duplicates(inventory_df, ["inventory_id"], "Inventory Duplicate Check"),
        "camp_id": check_duplicates(camps_df, ["camp_id"], "Camp Duplicate Check"),
        "donation_id": check_duplicates(donations_df, ["donation_id"], "Donation Duplicate Check"),
    }
    overall = "PASS" if all(r["status"] == "PASS" for r in results.values()) else "FAIL"
    return {"status": overall, "checks": results}


# ============================================================================
# SECTION 4 — MISSING VALUE VALIDATION
# ============================================================================

MISSING_VALUE_FIELDS = {
    "donors": ["donor_id", "blood_group", "city", "registration_date"],
    "blood_banks": ["blood_bank_id", "blood_bank_name", "city", "state"],
    "blood_inventory": ["inventory_id", "blood_bank_id", "blood_group", "available_units", "last_updated"],
    "donations": ["donation_id", "donor_id", "blood_bank_id", "blood_group", "donation_date", "units_donated", "donation_status"],
}


def check_missing_values(df: DataFrame, columns: list[str], label: str) -> dict:
    """Count nulls per column and report PASS/FAIL per column and overall."""
    column_results = {}
    total_row_count = df.count()

    for column in columns:
        if column not in df.columns:
            column_results[column] = _result("FAIL", null_count=None, reason="Column not found")
            continue
        null_count = df.filter(F.col(column).isNull()).count()
        column_results[column] = _pass_if(null_count == 0, null_count=null_count)

    overall = "PASS" if all(r["status"] == "PASS" for r in column_results.values()) else "FAIL"
    return {"status": overall, "label": label, "row_count": total_row_count, "columns": column_results}


def validate_all_missing_values(
    donors_df: DataFrame, blood_banks_df: DataFrame, inventory_df: DataFrame, donations_df: DataFrame,
) -> dict:
    # NOTE: this project's donations schema names the status column
    # "donation_status" (see module docstring path-mapping note); the
    # brief's field name "status" is checked via the mapping in the caller.
    results = {
        "donors": check_missing_values(donors_df, MISSING_VALUE_FIELDS["donors"], "Donors"),
        "blood_banks": check_missing_values(blood_banks_df, MISSING_VALUE_FIELDS["blood_banks"], "Blood Banks"),
        "blood_inventory": check_missing_values(inventory_df, MISSING_VALUE_FIELDS["blood_inventory"], "Blood Inventory"),
        "donations": check_missing_values(donations_df, MISSING_VALUE_FIELDS["donations"], "Donations"),
    }
    overall = "PASS" if all(r["status"] == "PASS" for r in results.values()) else "FAIL"
    return {"status": overall, "datasets": results}


# ============================================================================
# SECTION 5 — BLOOD GROUP VALIDATION
# ============================================================================

def validate_blood_groups(df: DataFrame, column: str, label: str) -> dict:
    """Detect blood group values outside the accepted 8-value set (e.g.
    'Unknown', 'A', 'O', 'XYZ', null)."""
    if column not in df.columns:
        return _result("FAIL", label=label, reason=f"Column '{column}' not found")

    invalid_rows = df.filter(~F.col(column).isin(VALID_BLOOD_GROUPS) | F.col(column).isNull())
    invalid_count = invalid_rows.count()
    sample_invalid_values = (
        [r[column] for r in invalid_rows.select(column).distinct().limit(10).collect()]
        if invalid_count > 0 else []
    )

    return _pass_if(invalid_count == 0, label=label, invalid_count=invalid_count, sample_invalid_values=sample_invalid_values)


# ============================================================================
# SECTION 6 — DONATION QUANTITY VALIDATION
# ============================================================================

def validate_donation_quantities(donations_df: DataFrame) -> dict:
    """Validate units_donated: not null, > 0, numeric, and flag (not
    delete) unrealistically large values for review."""
    null_units = donations_df.filter(F.col("units_donated").isNull()).count()
    zero_units = donations_df.filter(F.col("units_donated") == 0).count()
    negative_units = donations_df.filter(F.col("units_donated") < 0).count()
    unrealistic_units = donations_df.filter(F.col("units_donated") > DONATION_UNITS_MAX).count()

    passed = null_units == 0 and zero_units == 0 and negative_units == 0

    return _pass_if(
        passed,
        null_units=null_units,
        zero_units=zero_units,
        negative_units=negative_units,
        flagged_for_review_unrealistic_units=unrealistic_units,
    )


# ============================================================================
# SECTION 7 — DONOR ELIGIBILITY / DATA-QUALITY VALIDATION
# ============================================================================

def validate_donor_eligibility(donors_df: DataFrame) -> dict:
    """Pure data-quality checks on donor fields -- NOT a clinical
    eligibility system. Checks age range, eligibility_status vocabulary,
    blood group validity, and last_donation_date >= registration_date."""
    valid_eligibility_values = ["Eligible", "Temporarily Ineligible", "Inactive", "Unknown"]

    out_of_range_age = donors_df.filter(
        F.col("age").isNull() | (F.col("age") < DONOR_AGE_MIN) | (F.col("age") > DONOR_AGE_MAX)
    ).count()

    invalid_eligibility = donors_df.filter(~F.col("eligibility_status").isin(valid_eligibility_values)).count()

    blood_group_check = validate_blood_groups(donors_df, "blood_group", "Donor Blood Group")

    date_order_violations = donors_df.filter(
        F.col("last_donation_date").isNotNull()
        & F.col("registration_date").isNotNull()
        & (F.col("last_donation_date") < F.col("registration_date"))
    ).count()

    passed = (
        out_of_range_age == 0 and invalid_eligibility == 0
        and blood_group_check["status"] == "PASS" and date_order_violations == 0
    )

    return _pass_if(
        passed,
        out_of_range_age=out_of_range_age,
        invalid_eligibility_status=invalid_eligibility,
        blood_group_check=blood_group_check,
        last_donation_before_registration=date_order_violations,
    )


# ============================================================================
# SECTION 8 — FOREIGN KEY VALIDATION
# ============================================================================

def _count_orphans(child_df: DataFrame, child_key: str, parent_df: DataFrame, parent_key: str, allow_null: bool = False) -> int:
    """Count rows in child_df whose child_key value does not exist in
    parent_df's parent_key column. Null child_key values are excluded when
    allow_null=True (e.g. donations.camp_id, which is legitimately nullable)."""
    child = child_df.filter(F.col(child_key).isNotNull()) if allow_null else child_df
    parent_ids = parent_df.select(F.col(parent_key).alias("_parent_key")).distinct()
    orphans = child.join(parent_ids, child[child_key] == F.col("_parent_key"), "left").filter(
        F.col("_parent_key").isNull()
    )
    return orphans.count()


def validate_foreign_keys(
    donations_df: DataFrame, donors_df: DataFrame, blood_banks_df: DataFrame,
    camps_df: DataFrame, inventory_df: DataFrame,
) -> dict:
    invalid_donor_refs = _count_orphans(donations_df, "donor_id", donors_df, "donor_id")
    invalid_bank_refs = _count_orphans(donations_df, "blood_bank_id", blood_banks_df, "blood_bank_id")
    invalid_camp_refs = _count_orphans(donations_df, "camp_id", camps_df, "camp_id", allow_null=True)
    invalid_inventory_bank_refs = _count_orphans(inventory_df, "blood_bank_id", blood_banks_df, "blood_bank_id")

    passed = all(v == 0 for v in [invalid_donor_refs, invalid_bank_refs, invalid_camp_refs, invalid_inventory_bank_refs])

    return _pass_if(
        passed,
        invalid_donor_references=invalid_donor_refs,
        invalid_blood_bank_references=invalid_bank_refs,
        invalid_camp_references=invalid_camp_refs,
        invalid_inventory_blood_bank_references=invalid_inventory_bank_refs,
    )


# ============================================================================
# SECTION 9 — SILVER LAYER VALIDATION
# ============================================================================

def validate_silver_layer(spark: SparkSession) -> dict:
    """Validate the Silver layer: required datasets exist and are readable,
    duplicates are gone, and joins used to build enriched_donations did not
    unexpectedly multiply rows (a classic one-to-many join bug)."""
    donations = read_parquet_safe(spark, SILVER_PATHS["donations"])
    enriched = read_parquet_safe(spark, SILVER_PATHS["enriched_donations"])

    if donations is None or enriched is None:
        return _result("FAIL", reason="Silver donations or enriched_donations dataset not found")

    donations_count = donations.count()
    enriched_count = enriched.count()

    duplicate_check = check_duplicates(donations, ["donation_id"], "Silver Donations Duplicate Check")

    join_multiplication_ok = enriched_count == donations_count
    join_detail = {
        "before_join_donations": donations_count,
        "after_enrichment_join": enriched_count,
        "reason": None if join_multiplication_ok else (
            f"Row count changed after enrichment joins ({donations_count} -> {enriched_count}); "
            f"this indicates a possible one-to-many join problem (e.g. duplicate blood bank or "
            f"inventory keys)."
        ),
    }

    required_columns = ["blood_group", "donation_date", "units_donated", "donation_status", "is_rare_blood"]
    missing_columns = [c for c in required_columns if c not in donations.columns]

    passed = (
        duplicate_check["status"] == "PASS" and join_multiplication_ok and not missing_columns
    )

    return _pass_if(
        passed,
        duplicate_check=duplicate_check,
        join_check=join_detail,
        missing_required_columns=missing_columns,
    )


# ============================================================================
# SECTION 10 — INVENTORY CONSISTENCY TESTING
# ============================================================================

def validate_inventory_consistency(inventory_df: DataFrame, blood_banks_df: DataFrame) -> dict:
    """The most important technical test in the project: available_units
    must never be negative, and every inventory record must reference a
    real blood bank."""
    negative_units = inventory_df.filter(F.col("available_units") < 0).count()
    orphan_bank_refs = _count_orphans(inventory_df, "blood_bank_id", blood_banks_df, "blood_bank_id")

    by_group = (
        inventory_df.groupBy("blood_group")
        .agg(F.sum("available_units").alias("total_available_units"))
        .orderBy("blood_group")
        .collect()
    )
    by_bank = (
        inventory_df.groupBy("blood_bank_id")
        .agg(F.sum("available_units").alias("total_available_units"))
        .orderBy(F.desc("total_available_units"))
        .collect()
    )

    total_units = inventory_df.agg(F.sum("available_units").alias("total")).collect()[0]["total"] or 0

    passed = negative_units == 0 and orphan_bank_refs == 0

    return _pass_if(
        passed,
        negative_units=negative_units,
        orphan_blood_bank_references=orphan_bank_refs,
        total_available_units=int(total_units),
        available_units_by_blood_group={r["blood_group"]: int(r["total_available_units"]) for r in by_group},
        available_units_by_blood_bank_top5={r["blood_bank_id"]: int(r["total_available_units"]) for r in by_bank[:5]},
    )


# ============================================================================
# SECTION 11 — RARE BLOOD AVAILABILITY VALIDATION
# ============================================================================

def validate_rare_blood_availability(inventory_df: DataFrame, blood_banks_df: DataFrame) -> dict:
    """For each configured rare blood group: total units, centers with
    stock, cities with stock, and shortage classification."""
    rare_inventory = inventory_df.filter(F.col("blood_group").isin(RARE_BLOOD_GROUPS))
    with_city = rare_inventory.join(
        blood_banks_df.select(F.col("blood_bank_id"), F.col("city")), on="blood_bank_id", how="left"
    )

    rows = {}
    for group in RARE_BLOOD_GROUPS:
        group_df = with_city.filter(F.col("blood_group") == group)
        total_units_row = group_df.agg(F.sum("available_units").alias("total")).collect()[0]
        total_units = int(total_units_row["total"] or 0)
        centers_available = group_df.filter(F.col("available_units") > 0).select("blood_bank_id").distinct().count()
        cities_available = group_df.filter(F.col("available_units") > 0).select("city").distinct().count()
        status = classify_status(total_units)

        rows[group] = {
            "total_units": total_units,
            "centers_available": centers_available,
            "cities_available": cities_available,
            "status": status,
        }

    # This validation always PASSes structurally (it reports the current
    # state of availability); it only FAILs if the configured rare groups
    # are not even valid blood groups (a configuration error).
    invalid_config = [g for g in RARE_BLOOD_GROUPS if g not in VALID_BLOOD_GROUPS]

    return _pass_if(not invalid_config, rare_blood_groups=rows, invalid_configured_groups=invalid_config)


# ============================================================================
# SECTION 12 — BLOOD BANK AVAILABILITY VALIDATION
# ============================================================================

def validate_blood_bank_availability_display(rare_center_df: DataFrame) -> dict:
    """Verify that every row in a 'blood banks with rare blood' style table
    (Gold blood_bank_rare_stock filtered to available_units > 0) genuinely
    has available_units > 0, and that blood_bank_id/blood_group values are
    non-null (i.e. correctly joined, not orphaned placeholders)."""
    displayed = rare_center_df.filter(F.col("blood_group").isin(RARE_BLOOD_GROUPS) & (F.col("available_units") > 0))

    zero_or_negative = displayed.filter(F.col("available_units") <= 0).count()
    null_bank_id = displayed.filter(F.col("blood_bank_id").isNull()).count()
    null_blood_group = displayed.filter(F.col("blood_group").isNull()).count()

    passed = zero_or_negative == 0 and null_bank_id == 0 and null_blood_group == 0

    return _pass_if(
        passed,
        displayed_center_rows=displayed.count(),
        rows_with_zero_or_negative_units=zero_or_negative,
        rows_with_null_blood_bank_id=null_bank_id,
        rows_with_null_blood_group=null_blood_group,
    )


# ============================================================================
# SECTION 13 — SHORTAGE DETECTION BOUNDARY VALIDATION
# ============================================================================

def validate_shortage_boundaries() -> dict:
    """Test classify_status() at every boundary value around
    CRITICAL_THRESHOLD and SHORTAGE_THRESHOLD."""
    test_cases = [
        (0, "OUT_OF_STOCK"),
        (CRITICAL_THRESHOLD - 1, "CRITICAL"),   # 4
        (CRITICAL_THRESHOLD, "LOW"),            # 5  (>= CRITICAL_THRESHOLD, < SHORTAGE_THRESHOLD)
        (SHORTAGE_THRESHOLD - 1, "LOW"),        # 9
        (SHORTAGE_THRESHOLD, "AVAILABLE"),      # 10 (>= SHORTAGE_THRESHOLD)
        (SHORTAGE_THRESHOLD + 1, "AVAILABLE"),  # 11
    ]

    results = []
    all_passed = True
    for units, expected in test_cases:
        actual = classify_status(units)
        ok = actual == expected
        all_passed = all_passed and ok
        results.append({"units": units, "expected": expected, "actual": actual, "status": "PASS" if ok else "FAIL"})

    return _pass_if(all_passed, boundary_cases=results)


# ============================================================================
# SECTION 14 — GOLD LAYER VALIDATION
# ============================================================================

GOLD_REQUIRED_COLUMNS = {
    "daily_donation_summary": ["donation_date", "total_donations", "total_units_donated"],
    "monthly_donation_summary": ["year", "month", "total_donations", "total_units_donated"],
    "blood_group_distribution": ["blood_group", "total_donations", "total_units_donated", "percentage_of_total"],
    "rare_blood_availability": ["blood_bank_id", "blood_group", "available_units", "availability_status"],
    "rare_blood_summary": ["blood_group", "total_available_units", "availability_status"],
    "blood_shortage_report": ["blood_group", "total_available_units", "shortage_status", "severity_score"],
    "blood_bank_rare_stock": ["blood_bank_id", "blood_group", "available_units", "availability_status"],
    "city_blood_availability": ["city", "state", "blood_group", "total_available_units", "availability_status"],
    "blood_bank_performance": ["blood_bank_id", "total_donations_received", "total_units_received"],
    "donation_camp_performance": ["camp_id", "total_donations", "total_units_collected"],
    "donor_summary": ["donor_id", "blood_group", "is_rare_blood_donor"],
    "rare_donor_summary": ["blood_group", "total_donors", "total_units_donated"],
}

# Gold tables that are legitimately allowed to be empty in edge-case runs
# (e.g. no donations recorded on a given day); everything else must have
# row_count > 0.
GOLD_ALLOW_EMPTY = set()


def validate_gold_dataset(spark: SparkSession, name: str) -> dict:
    path = GOLD_PATHS[name]
    df = read_parquet_safe(spark, path)

    if df is None:
        return _result("FAIL", reason=f"Dataset not found or unreadable at {path}")

    required = GOLD_REQUIRED_COLUMNS[name]
    missing_columns = [c for c in required if c not in df.columns]
    if missing_columns:
        return _result("FAIL", reason=f"Missing required column(s): {missing_columns}")

    row_count = df.count()
    if row_count == 0 and name not in GOLD_ALLOW_EMPTY:
        return _result("FAIL", reason="Row count is zero", row_count=0)

    return _pass_if(True, row_count=row_count)


def validate_all_gold_datasets(spark: SparkSession) -> dict:
    results = {name: validate_gold_dataset(spark, name) for name in GOLD_REQUIRED_COLUMNS}
    overall = "PASS" if all(r["status"] == "PASS" for r in results.values()) else "FAIL"
    return {"status": overall, "datasets": results}


# ============================================================================
# SECTION 15 — GOLD AGGREGATION RECONCILIATION
# ============================================================================

def validate_gold_reconciliation(spark: SparkSession) -> dict:
    """Compare Gold aggregates against the Silver data they were built
    from, to catch aggregation bugs."""
    silver_donations = read_parquet_safe(spark, SILVER_PATHS["donations"])
    silver_inventory = read_parquet_safe(spark, SILVER_PATHS["blood_inventory"])
    gold_blood_group_dist = read_parquet_safe(spark, GOLD_PATHS["blood_group_distribution"])
    gold_rare_summary = read_parquet_safe(spark, GOLD_PATHS["rare_blood_summary"])

    if any(x is None for x in [silver_donations, silver_inventory, gold_blood_group_dist, gold_rare_summary]):
        return _result("FAIL", reason="One or more required Silver/Gold datasets not found")

    # Reconcile completed-donation units (Gold only counts Completed donations)
    silver_completed_units = (
        silver_donations.filter(F.col("donation_status") == "Completed")
        .agg(F.sum("units_donated").alias("total")).collect()[0]["total"] or 0
    )
    gold_units = gold_blood_group_dist.agg(F.sum("total_units_donated").alias("total")).collect()[0]["total"] or 0
    donation_units_match = int(silver_completed_units) == int(gold_units)

    # Reconcile rare blood inventory totals, per group
    rare_mismatches = {}
    for group in RARE_BLOOD_GROUPS:
        silver_total = (
            silver_inventory.filter(F.col("blood_group") == group)
            .agg(F.sum("available_units").alias("total")).collect()[0]["total"] or 0
        )
        gold_row = gold_rare_summary.filter(F.col("blood_group") == group).collect()
        gold_total = int(gold_row[0]["total_available_units"]) if gold_row else None
        if int(silver_total) != gold_total:
            rare_mismatches[group] = {"silver_total": int(silver_total), "gold_total": gold_total}

    passed = donation_units_match and not rare_mismatches

    return _pass_if(
        passed,
        silver_completed_donation_units=int(silver_completed_units),
        gold_blood_group_distribution_units=int(gold_units),
        donation_units_match=donation_units_match,
        rare_blood_inventory_mismatches=rare_mismatches,
    )


# ============================================================================
# SECTION 16 — STREAMING VALIDATION
# ============================================================================

def validate_streaming(spark: SparkSession) -> dict:
    """Validate the Structured Streaming pipeline artifacts: landing zone,
    Bronze streaming output, checkpoint directory, and basic streaming
    record quality. Does NOT require Kafka/Docker/a cluster -- everything
    here is just files on disk plus a batch read of the already-materialized
    Bronze streaming Parquet output."""
    landing_exists = STREAMING_LANDING_DIR.exists()
    landing_files = list(STREAMING_LANDING_DIR.glob("*.json")) if landing_exists else []

    checkpoint_exists = STREAMING_CHECKPOINT_DIR.exists()

    streaming_df = read_parquet_safe(spark, BRONZE_PATHS["streaming_donations"])
    if streaming_df is None:
        return _result(
            "FAIL",
            reason="Bronze streaming donations dataset not found -- run Phase 4 + Phase 5 first",
            landing_directory_exists=landing_exists,
            landing_json_file_count=len(landing_files),
            checkpoint_directory_exists=checkpoint_exists,
        )

    required_columns = ["donation_id", "donor_id", "blood_bank_id", "blood_group",
                         "units_donated", "donation_status", "event_time", "_ingested_at"]
    missing_columns = [c for c in required_columns if c not in streaming_df.columns]

    record_count = streaming_df.count()
    duplicate_check = check_duplicates(streaming_df, ["donation_id"], "Streaming Donation Duplicate Check")

    passed = (
        landing_exists and checkpoint_exists and not missing_columns
        and record_count > 0 and duplicate_check["status"] == "PASS"
    )

    return _pass_if(
        passed,
        landing_directory_exists=landing_exists,
        landing_json_file_count=len(landing_files),
        checkpoint_directory_exists=checkpoint_exists,
        bronze_streaming_record_count=record_count,
        missing_required_columns=missing_columns,
        duplicate_check=duplicate_check,
    )
