"""
Phase 10 — Inventory Consistency Tests

One of the most important test files in the project: available_units must
never be negative, and every inventory record must belong to a real,
existing blood bank.
"""

from __future__ import annotations

from validation import validators as v


def test_no_negative_available_units(silver_inventory):
    negative_count = silver_inventory.filter(silver_inventory["available_units"] < 0).count()
    assert negative_count == 0, f"Found {negative_count} inventory records with negative available_units"


def test_inventory_blood_bank_references_valid(silver_inventory, silver_blood_banks):
    orphan_count = v._count_orphans(silver_inventory, "blood_bank_id", silver_blood_banks, "blood_bank_id")
    assert orphan_count == 0, f"Found {orphan_count} inventory records referencing a non-existent blood bank"


def test_inventory_consistency_overall(silver_inventory, silver_blood_banks):
    result = v.validate_inventory_consistency(silver_inventory, silver_blood_banks)
    assert result["status"] == "PASS", result
    print("Available units by blood group:", result["available_units_by_blood_group"])


def test_inventory_units_are_numeric(silver_inventory):
    dtype = dict(silver_inventory.dtypes)["available_units"]
    assert dtype in ("int", "bigint", "smallint"), f"available_units has non-integer type: {dtype}"
