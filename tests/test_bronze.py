"""
Phase 10 — Bronze Layer Tests

Validates that every expected Bronze dataset exists, is readable, has the
required columns, has row_count > 0, and carries non-null _ingested_at
metadata. See validation/validators.py for the actual check logic.
"""

from __future__ import annotations

from validation import validators as v


def test_all_bronze_datasets_exist_and_valid(spark):
    result = v.validate_all_bronze_datasets(spark)
    for name, dataset_result in result["datasets"].items():
        assert dataset_result["status"] == "PASS", f"Bronze dataset '{name}' failed: {dataset_result}"
    assert result["status"] == "PASS"


def test_bronze_donors_has_donor_ids(bronze_donors):
    assert bronze_donors.filter(bronze_donors["donor_id"].isNull()).count() == 0


def test_bronze_blood_banks_has_bank_ids(bronze_blood_banks):
    assert bronze_blood_banks.filter(bronze_blood_banks["blood_bank_id"].isNull()).count() == 0


def test_bronze_donations_has_donation_ids(bronze_donations):
    assert bronze_donations.filter(bronze_donations["donation_id"].isNull()).count() == 0


def test_bronze_donations_has_blood_groups(bronze_donations):
    assert bronze_donations.filter(bronze_donations["blood_group"].isNull()).count() == 0


def test_bronze_row_counts_positive(bronze_donors, bronze_blood_banks, bronze_inventory, bronze_camps, bronze_donations):
    assert bronze_donors.count() > 0
    assert bronze_blood_banks.count() > 0
    assert bronze_inventory.count() > 0
    assert bronze_camps.count() > 0
    assert bronze_donations.count() > 0
