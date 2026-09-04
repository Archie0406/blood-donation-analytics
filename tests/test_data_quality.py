"""
Phase 10 — Data Quality Tests

Covers duplicate detection, missing value validation, blood group
validation, donation quantity validation, donor eligibility (data-quality
only, NOT a clinical system), and foreign key / referential integrity.
"""

from __future__ import annotations

from validation import validators as v


# ---- Duplicate detection ----

def test_no_duplicate_donor_ids(bronze_donors):
    result = v.check_duplicates(bronze_donors, ["donor_id"], "Donor Duplicate Check")
    assert result["status"] == "PASS", result


def test_no_duplicate_blood_bank_ids(bronze_blood_banks):
    result = v.check_duplicates(bronze_blood_banks, ["blood_bank_id"], "Blood Bank Duplicate Check")
    assert result["status"] == "PASS", result


def test_no_duplicate_inventory_ids(bronze_inventory):
    result = v.check_duplicates(bronze_inventory, ["inventory_id"], "Inventory Duplicate Check")
    assert result["status"] == "PASS", result


def test_no_duplicate_camp_ids(bronze_camps):
    result = v.check_duplicates(bronze_camps, ["camp_id"], "Camp Duplicate Check")
    assert result["status"] == "PASS", result


def test_bronze_donations_may_contain_duplicates_but_silver_must_not(bronze_donations, silver_donations):
    """Phase 2 intentionally injects a few duplicate donation_id rows into
    the raw/Bronze data to give Silver something to clean -- so Bronze is
    NOT asserted duplicate-free here. Silver, which runs dropDuplicates,
    must be."""
    bronze_check = v.check_duplicates(bronze_donations, ["donation_id"], "Bronze Donation Duplicate Check")
    silver_check = v.check_duplicates(silver_donations, ["donation_id"], "Silver Donation Duplicate Check")
    print(f"Bronze donation duplicates (expected, informational): {bronze_check['duplicate_count']}")
    assert silver_check["status"] == "PASS", silver_check


# ---- Missing value validation ----

def test_donor_missing_values(bronze_donors):
    result = v.check_missing_values(bronze_donors, v.MISSING_VALUE_FIELDS["donors"], "Donors")
    assert result["status"] == "PASS", result


def test_blood_bank_missing_values(bronze_blood_banks):
    result = v.check_missing_values(bronze_blood_banks, v.MISSING_VALUE_FIELDS["blood_banks"], "Blood Banks")
    assert result["status"] == "PASS", result


def test_inventory_missing_values(bronze_inventory):
    result = v.check_missing_values(bronze_inventory, v.MISSING_VALUE_FIELDS["blood_inventory"], "Blood Inventory")
    assert result["status"] == "PASS", result


def test_donation_missing_values(bronze_donations):
    result = v.check_missing_values(bronze_donations, v.MISSING_VALUE_FIELDS["donations"], "Donations")
    assert result["status"] == "PASS", result


# ---- Blood group validation ----

def test_donor_blood_groups_valid(bronze_donors):
    result = v.validate_blood_groups(bronze_donors, "blood_group", "Donor Blood Group")
    assert result["status"] == "PASS", result


def test_inventory_blood_groups_valid(bronze_inventory):
    result = v.validate_blood_groups(bronze_inventory, "blood_group", "Inventory Blood Group")
    assert result["status"] == "PASS", result


def test_donation_blood_groups_valid(bronze_donations):
    result = v.validate_blood_groups(bronze_donations, "blood_group", "Donation Blood Group")
    assert result["status"] == "PASS", result


# ---- Donation quantity validation ----

def test_donation_quantities_valid(bronze_donations):
    result = v.validate_donation_quantities(bronze_donations)
    assert result["status"] == "PASS", result


# ---- Donor eligibility (data-quality only) ----

def test_donor_eligibility_data_quality(bronze_donors):
    """NOTE: this is a data-quality check on synthetic fields, not a
    clinical eligibility determination."""
    result = v.validate_donor_eligibility(bronze_donors)
    assert result["status"] == "PASS", result


# ---- Foreign key validation ----

def test_foreign_keys(bronze_donations, bronze_donors, bronze_blood_banks, bronze_camps, bronze_inventory):
    result = v.validate_foreign_keys(bronze_donations, bronze_donors, bronze_blood_banks, bronze_camps, bronze_inventory)
    assert result["status"] == "PASS", result
