"""
Phase 10 — Gold Layer Tests

Validates every Gold table exists, is readable, has required columns and a
positive row count, and reconciles key Gold aggregates back to Silver.
"""

from __future__ import annotations

from validation import validators as v


def test_all_gold_datasets_exist_and_valid(spark):
    result = v.validate_all_gold_datasets(spark)
    for name, dataset_result in result["datasets"].items():
        assert dataset_result["status"] == "PASS", f"Gold dataset '{name}' failed: {dataset_result}"
    assert result["status"] == "PASS"


def test_gold_reconciliation_against_silver(spark):
    result = v.validate_gold_reconciliation(spark)
    assert result["status"] == "PASS", result


def test_rare_blood_summary_covers_all_configured_groups(spark):
    df = v.read_parquet_safe(spark, v.GOLD_PATHS["rare_blood_summary"])
    if df is None:
        import pytest
        pytest.skip("Gold rare_blood_summary not found -- run Phase 8 first")

    present_groups = {r["blood_group"] for r in df.select("blood_group").distinct().collect()}
    missing = set(v.RARE_BLOOD_GROUPS) - present_groups
    assert not missing, f"rare_blood_summary is missing configured rare group(s): {missing}"


def test_blood_group_distribution_percentages_sum_near_100(spark):
    df = v.read_parquet_safe(spark, v.GOLD_PATHS["blood_group_distribution"])
    if df is None:
        import pytest
        pytest.skip("Gold blood_group_distribution not found -- run Phase 8 first")

    from pyspark.sql import functions as F
    total_pct = df.agg(F.sum("percentage_of_total").alias("total")).collect()[0]["total"] or 0
    assert 99.0 <= total_pct <= 101.0, f"Blood group percentages sum to {total_pct}, expected ~100"
