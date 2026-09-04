"""
Phase 10 — Rare Blood Availability & Shortage Detection Tests

The central business validation of the whole project: for each
project-defined rare/focus blood group, confirm total units, centers with
stock, and cities with stock are calculated correctly, and that the
shortage/availability classification rule is correct at every boundary.
"""

from __future__ import annotations

from validation import validators as v


def test_rare_blood_groups_are_valid_blood_groups():
    invalid = [g for g in v.RARE_BLOOD_GROUPS if g not in v.VALID_BLOOD_GROUPS]
    assert not invalid, f"RARE_BLOOD_GROUPS contains invalid blood group(s): {invalid}"


def test_rare_blood_availability_calculation(silver_inventory, silver_blood_banks):
    result = v.validate_rare_blood_availability(silver_inventory, silver_blood_banks)
    assert result["status"] == "PASS", result
    for group in v.RARE_BLOOD_GROUPS:
        assert group in result["rare_blood_groups"], f"Missing rare blood group in results: {group}"
        row = result["rare_blood_groups"][group]
        assert row["total_units"] >= 0
        assert row["centers_available"] >= 0
        assert row["status"] in ("OUT_OF_STOCK", "CRITICAL", "LOW", "AVAILABLE")


def test_blood_bank_availability_display(spark):
    """Every row shown in the 'blood banks with rare blood' Gold table must
    genuinely have available_units > 0."""
    rare_stock = v.read_parquet_safe(spark, v.GOLD_PATHS["blood_bank_rare_stock"])
    if rare_stock is None:
        import pytest
        pytest.skip("Gold blood_bank_rare_stock not found -- run Phase 8 first")

    result = v.validate_blood_bank_availability_display(rare_stock)
    assert result["status"] == "PASS", result


def test_shortage_boundary_classification():
    """Test classify_status() at boundary values: 0, 4, 5, 9, 10, 11."""
    result = v.validate_shortage_boundaries()
    for case in result["boundary_cases"]:
        assert case["status"] == "PASS", (
            f"units={case['units']}: expected {case['expected']}, got {case['actual']}"
        )
    assert result["status"] == "PASS"


def test_shortage_thresholds_are_configured_sensibly():
    assert v.CRITICAL_THRESHOLD < v.SHORTAGE_THRESHOLD, (
        "CRITICAL_THRESHOLD must be strictly less than SHORTAGE_THRESHOLD"
    )
    assert v.CRITICAL_THRESHOLD > 0
