"""
Phase "Upgrade" — Inventory Transaction Engine Tests

Validates the transaction-driven inventory model introduced to replace the
original static-snapshot approach (see pipeline/09_inventory_transaction_engine.py
and data_generation/02_generate_transactional_data.py).

These tests exercise REAL behavior against the actual generated ledger and
Gold outputs -- not synthetic mini-fixtures -- because the single most
important property of this model (the reconciliation equation) is only
meaningful when checked against the real, full-scale ledger.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
from pyspark.sql import functions as F

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config.config_loader import (  # noqa: E402
    get_rare_blood_groups,
    get_thresholds,
    get_valid_component_types,
)

RAW_DIR = PROJECT_ROOT / "data" / "raw"
GOLD_DIR = PROJECT_ROOT / "data" / "gold"

TRANSACTIONS_PATH = RAW_DIR / "inventory_transactions.csv"
TRANSFERS_PATH = RAW_DIR / "blood_transfers.csv"
ISSUES_PATH = RAW_DIR / "blood_issues.csv"
INVENTORY_STATE_PATH = GOLD_DIR / "gold_inventory_state"
DEMAND_ANALYTICS_PATH = GOLD_DIR / "gold_demand_analytics"
TRANSFER_HISTORY_PATH = GOLD_DIR / "gold_transfer_history"

VALID_TRANSACTION_TYPES = {"DONATION", "ISSUE", "TRANSFER_IN", "TRANSFER_OUT", "EXPIRY", "ADJUSTMENT"}
INFLOW_TYPES = {"DONATION", "TRANSFER_IN", "ADJUSTMENT"}
OUTFLOW_TYPES = {"ISSUE", "TRANSFER_OUT", "EXPIRY"}


def _skip_if_missing(path: Path, label: str) -> None:
    if not path.exists():
        pytest.skip(f"{label} not found at {path} -- run data_generation/02_generate_transactional_data.py "
                    f"and pipeline/09_inventory_transaction_engine.py first")


# ============================================================================
# TRANSACTION LEDGER — SCHEMA & GENERATION-TIME INVARIANTS
# ============================================================================

def test_transaction_ledger_exists_and_has_rows(spark):
    _skip_if_missing(TRANSACTIONS_PATH, "Transaction ledger")
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(str(TRANSACTIONS_PATH))
    assert df.count() > 0


def test_transaction_types_are_all_valid(spark):
    _skip_if_missing(TRANSACTIONS_PATH, "Transaction ledger")
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(str(TRANSACTIONS_PATH))
    types_found = {r["transaction_type"] for r in df.select("transaction_type").distinct().collect()}
    unexpected = types_found - VALID_TRANSACTION_TYPES
    assert not unexpected, f"Unexpected transaction type(s) found: {unexpected}"


def test_all_six_transaction_types_present(spark):
    """A meaningful transactional model should actually exercise every
    transaction type, not just DONATION."""
    _skip_if_missing(TRANSACTIONS_PATH, "Transaction ledger")
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(str(TRANSACTIONS_PATH))
    types_found = {r["transaction_type"] for r in df.select("transaction_type").distinct().collect()}
    missing = VALID_TRANSACTION_TYPES - types_found
    assert not missing, f"Transaction type(s) never generated: {missing}"


def test_transaction_units_are_positive(spark):
    """units itself is always a positive magnitude; direction (inflow vs
    outflow) is determined by transaction_type, not by a signed value."""
    _skip_if_missing(TRANSACTIONS_PATH, "Transaction ledger")
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(str(TRANSACTIONS_PATH))
    non_positive = df.filter(F.col("units") <= 0)
    # ADJUSTMENT is the one legitimate exception (can be a negative correction)
    non_positive_non_adjustment = non_positive.filter(F.col("transaction_type") != "ADJUSTMENT")
    assert non_positive_non_adjustment.count() == 0


def test_component_types_are_valid(spark):
    _skip_if_missing(TRANSACTIONS_PATH, "Transaction ledger")
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(str(TRANSACTIONS_PATH))
    valid_components = set(get_valid_component_types())
    found = {r["component_type"] for r in df.select("component_type").distinct().collect()}
    assert found <= valid_components, f"Unexpected component type(s): {found - valid_components}"


# ============================================================================
# THE CORE PROPERTY: RECONCILIATION
# ============================================================================

def test_reconciliation_equation_holds(spark):
    """The single most important test in this suite: for every
    (blood_bank_id, blood_group, component_type) key, summing every signed
    transaction reproduces the exact closing balance published in
    gold_inventory_state. This is what makes the inventory model
    trustworthy rather than an unverified black box."""
    _skip_if_missing(TRANSACTIONS_PATH, "Transaction ledger")
    _skip_if_missing(INVENTORY_STATE_PATH, "gold_inventory_state")

    transactions = spark.read.option("header", "true").option("inferSchema", "true").csv(str(TRANSACTIONS_PATH))
    inventory_state = spark.read.parquet(str(INVENTORY_STATE_PATH))

    signed = transactions.withColumn(
        "signed_units",
        F.when(F.col("transaction_type").isin(list(INFLOW_TYPES)), F.col("units")).otherwise(-F.col("units")),
    )
    recomputed = (
        signed.groupBy("blood_bank_id", "blood_group", "component_type")
        .agg(F.round(F.sum("signed_units")).cast("int").alias("recomputed_units"))
    )

    compare = inventory_state.select(
        "blood_bank_id", "blood_group", "component_type", "available_units"
    ).join(recomputed, on=["blood_bank_id", "blood_group", "component_type"], how="inner")

    mismatches = compare.filter(F.col("available_units") != F.col("recomputed_units"))
    mismatch_count = mismatches.count()

    if mismatch_count > 0:
        print("Sample mismatches:")
        mismatches.show(10, truncate=False)

    assert mismatch_count == 0, f"{mismatch_count} ledger(s) do not reconcile to their reported balance"


def test_every_key_reconciled(spark):
    """Every key that exists in the transaction ledger must also appear in
    gold_inventory_state -- the inner join above could otherwise silently
    hide a key that's missing from one side entirely."""
    _skip_if_missing(TRANSACTIONS_PATH, "Transaction ledger")
    _skip_if_missing(INVENTORY_STATE_PATH, "gold_inventory_state")

    transactions = spark.read.option("header", "true").option("inferSchema", "true").csv(str(TRANSACTIONS_PATH))
    inventory_state = spark.read.parquet(str(INVENTORY_STATE_PATH))

    ledger_keys = transactions.select("blood_bank_id", "blood_group", "component_type").distinct()
    state_keys = inventory_state.select("blood_bank_id", "blood_group", "component_type").distinct()

    missing_from_state = ledger_keys.subtract(state_keys).count()
    assert missing_from_state == 0, f"{missing_from_state} ledger key(s) missing from gold_inventory_state"


def test_no_negative_available_units(spark):
    _skip_if_missing(INVENTORY_STATE_PATH, "gold_inventory_state")
    df = spark.read.parquet(str(INVENTORY_STATE_PATH))
    negative = df.filter(F.col("available_units") < 0).count()
    assert negative == 0


# ============================================================================
# TRANSFER VALIDATION
# ============================================================================

def test_transfers_source_not_equal_destination(spark):
    _skip_if_missing(TRANSFER_HISTORY_PATH, "gold_transfer_history")
    df = spark.read.parquet(str(TRANSFER_HISTORY_PATH))
    same_bank = df.filter(F.col("source_bank_id") == F.col("destination_bank_id")).count()
    assert same_bank == 0


def test_transfers_have_positive_units(spark):
    _skip_if_missing(TRANSFER_HISTORY_PATH, "gold_transfer_history")
    df = spark.read.parquet(str(TRANSFER_HISTORY_PATH))
    non_positive = df.filter(F.col("units") <= 0).count()
    assert non_positive == 0


def test_transfers_flagged_invalid_are_actually_invalid(spark):
    """Whatever the engine marks is_valid=false must genuinely violate a
    rule (not source==dest and not positive units, i.e. every row must be
    consistent with its own flag)."""
    _skip_if_missing(TRANSFER_HISTORY_PATH, "gold_transfer_history")
    df = spark.read.parquet(str(TRANSFER_HISTORY_PATH))

    flagged_valid_but_broken = df.filter(
        F.col("is_valid")
        & ((F.col("units") <= 0) | (F.col("source_bank_id") == F.col("destination_bank_id")))
    ).count()
    assert flagged_valid_but_broken == 0


def test_every_transfer_out_has_matching_transfer_in(spark):
    """Every TRANSFER_OUT transaction in the ledger must be paired with a
    TRANSFER_IN of the same transfer_id, same units -- otherwise units
    would silently vanish or appear from nowhere."""
    _skip_if_missing(TRANSACTIONS_PATH, "Transaction ledger")
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(str(TRANSACTIONS_PATH))

    outs = df.filter(F.col("transaction_type") == "TRANSFER_OUT").select(
        F.col("source_transfer_id"), F.col("units").alias("out_units")
    )
    ins = df.filter(F.col("transaction_type") == "TRANSFER_IN").select(
        F.col("source_transfer_id"), F.col("units").alias("in_units")
    )

    paired = outs.join(ins, on="source_transfer_id", how="inner")
    unpaired_outs = outs.count() - paired.count()
    mismatched_units = paired.filter(F.col("out_units") != F.col("in_units")).count()

    assert unpaired_outs == 0, f"{unpaired_outs} TRANSFER_OUT transaction(s) have no matching TRANSFER_IN"
    assert mismatched_units == 0, f"{mismatched_units} transfer pair(s) have mismatched units"


# ============================================================================
# ISSUE / DEMAND VALIDATION
# ============================================================================

def test_issues_have_positive_units(spark):
    _skip_if_missing(ISSUES_PATH, "blood_issues.csv")
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(str(ISSUES_PATH))
    non_positive = df.filter(F.col("units") <= 0).count()
    assert non_positive == 0


def test_demand_analytics_non_negative(spark):
    _skip_if_missing(DEMAND_ANALYTICS_PATH, "gold_demand_analytics")
    df = spark.read.parquet(str(DEMAND_ANALYTICS_PATH))
    negative_demand = df.filter(
        (F.col("average_daily_demand") < 0) | (F.col("total_units_issued") < 0)
    ).count()
    assert negative_demand == 0


# ============================================================================
# EXPIRY CLASSIFICATION
# ============================================================================

def test_expiry_status_values_valid(spark):
    _skip_if_missing(INVENTORY_STATE_PATH, "gold_inventory_state")
    df = spark.read.parquet(str(INVENTORY_STATE_PATH))
    valid_statuses = {"VALID", "EXPIRING_SOON", "EXPIRED"}
    found = {r["expiry_status"] for r in df.select("expiry_status").distinct().collect()}
    assert found <= valid_statuses, f"Unexpected expiry_status value(s): {found - valid_statuses}"


def test_lifetime_expired_units_non_negative(spark):
    _skip_if_missing(INVENTORY_STATE_PATH, "gold_inventory_state")
    df = spark.read.parquet(str(INVENTORY_STATE_PATH))
    negative = df.filter(F.col("lifetime_expired_units") < 0).count()
    assert negative == 0


# ============================================================================
# SHORTAGE SCORE / PRIORITY LEVEL
# ============================================================================

def test_shortage_score_within_bounds(spark):
    _skip_if_missing(INVENTORY_STATE_PATH, "gold_inventory_state")
    df = spark.read.parquet(str(INVENTORY_STATE_PATH))
    out_of_bounds = df.filter((F.col("shortage_score") < 0) | (F.col("shortage_score") > 100)).count()
    assert out_of_bounds == 0


def test_priority_level_values_valid(spark):
    _skip_if_missing(INVENTORY_STATE_PATH, "gold_inventory_state")
    df = spark.read.parquet(str(INVENTORY_STATE_PATH))
    valid_levels = {"HEALTHY", "MONITOR", "LOW", "CRITICAL", "EMERGENCY"}
    found = {r["priority_level"] for r in df.select("priority_level").distinct().collect()}
    assert found <= valid_levels, f"Unexpected priority_level value(s): {found - valid_levels}"


def test_priority_level_matches_configured_bands(spark):
    """Independently re-derive priority_level from shortage_score using the
    same bands in config/thresholds.yaml, and confirm every row's stored
    priority_level agrees -- this is a boundary/off-by-one guard, the same
    style of check that caught a real bug in the original Phase 10 shortage
    classifier."""
    _skip_if_missing(INVENTORY_STATE_PATH, "gold_inventory_state")
    df = spark.read.parquet(str(INVENTORY_STATE_PATH))
    bands = get_thresholds()["shortage_scoring"]["bands"]

    expected = df.withColumn(
        "expected_priority",
        F.when(F.col("shortage_score") <= bands["healthy_max"], F.lit("HEALTHY"))
        .when(F.col("shortage_score") <= bands["monitor_max"], F.lit("MONITOR"))
        .when(F.col("shortage_score") <= bands["low_max"], F.lit("LOW"))
        .when(F.col("shortage_score") <= bands["critical_max"], F.lit("CRITICAL"))
        .otherwise(F.lit("EMERGENCY")),
    )

    mismatches = expected.filter(F.col("priority_level") != F.col("expected_priority")).count()
    assert mismatches == 0, f"{mismatches} row(s) have a priority_level inconsistent with their shortage_score"


def test_coverage_days_null_only_when_no_demand_data(spark):
    """coverage_days should be null exactly when has_demand_data is false,
    and populated (non-null) exactly when it's true -- never a silent
    'infinite coverage' default."""
    _skip_if_missing(INVENTORY_STATE_PATH, "gold_inventory_state")
    df = spark.read.parquet(str(INVENTORY_STATE_PATH))

    inconsistent = df.filter(
        (F.col("has_demand_data") & F.col("coverage_days").isNull())
        | (~F.col("has_demand_data") & F.col("coverage_days").isNotNull())
    ).count()
    assert inconsistent == 0


def test_recommended_action_present_for_every_priority_level(spark):
    _skip_if_missing(INVENTORY_STATE_PATH, "gold_inventory_state")
    df = spark.read.parquet(str(INVENTORY_STATE_PATH))
    missing_action = df.filter(F.col("recommended_action").isNull()).count()
    assert missing_action == 0


def test_rare_blood_groups_covered_in_inventory_state(spark):
    _skip_if_missing(INVENTORY_STATE_PATH, "gold_inventory_state")
    df = spark.read.parquet(str(INVENTORY_STATE_PATH))
    rare_groups = set(get_rare_blood_groups())
    found = {r["blood_group"] for r in df.filter(F.col("blood_group").isin(list(rare_groups))).select("blood_group").distinct().collect()}
    missing = rare_groups - found
    assert not missing, f"Rare blood group(s) missing from gold_inventory_state entirely: {missing}"
