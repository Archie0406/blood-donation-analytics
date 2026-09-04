"""
Phase 10 — Silver Layer Tests

Validates that the Silver layer is properly deduplicated, has the expected
enrichment columns, and that the donor/blood-bank/camp/inventory joins used
to build enriched_donations did not unexpectedly multiply rows.
"""

from __future__ import annotations

from validation import validators as v


def test_silver_layer_overall(spark):
    result = v.validate_silver_layer(spark)
    assert result["status"] == "PASS", result


def test_silver_donations_deduplicated(silver_donations):
    result = v.check_duplicates(silver_donations, ["donation_id"], "Silver Donations Duplicate Check")
    assert result["status"] == "PASS", result


def test_silver_donations_have_quality_flags(silver_donations):
    required = ["is_valid_blood_group", "is_rare_blood", "is_valid_donation_quantity",
                "is_valid_donation_date", "overall_data_quality_status"]
    missing = [c for c in required if c not in silver_donations.columns]
    assert not missing, f"Silver donations missing data-quality columns: {missing}"


def test_silver_blood_groups_standardized(silver_donations):
    result = v.validate_blood_groups(silver_donations, "blood_group", "Silver Donation Blood Group")
    assert result["status"] == "PASS", result


def test_enrichment_join_does_not_multiply_rows(spark):
    """The core join-safety check: enriched_donations must have exactly the
    same row count as donations, since every join is a LEFT JOIN on a
    unique key. If this fails, a duplicate key on the donor/bank/camp/
    inventory side is fanning out donation rows."""
    donations = v.read_parquet_safe(spark, v.SILVER_PATHS["donations"])
    enriched = v.read_parquet_safe(spark, v.SILVER_PATHS["enriched_donations"])
    assert donations is not None and enriched is not None, "Silver donations/enriched_donations not found"

    before = donations.count()
    after = enriched.count()
    assert after == before, (
        f"Row count changed after enrichment joins ({before} -> {after}) -- "
        f"possible one-to-many join problem."
    )
