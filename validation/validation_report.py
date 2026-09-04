"""
Enterprise Rare Blood Type Availability Analytics
Phase 10 — End-to-End Validation Report
=====================================================

Run with:
    python validation/validation_report.py

Executes every major validation from Bronze through Gold and Streaming,
using the shared logic in validation/validators.py, and writes:

    validation/validation_results/validation_report.txt   (human-readable)
    validation/validation_results/validation_results.json (machine-readable)

This script is READ-ONLY: it never modifies Bronze, Silver, or Gold data.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from validation import validators as v  # noqa: E402

RESULTS_DIR = Path(__file__).resolve().parent / "validation_results"
REPORT_TXT_PATH = RESULTS_DIR / "validation_report.txt"
REPORT_JSON_PATH = RESULTS_DIR / "validation_results.json"


def _status_or_skip(result: dict | None, skip_reason: str = "") -> tuple[str, dict]:
    if result is None:
        return "SKIPPED", {"status": "SKIPPED", "reason": skip_reason}
    return result["status"], result


def run_all_validations() -> dict:
    """Run every Phase 10 validation section and return a structured
    results dict keyed by section number/name."""
    spark = v.get_or_create_spark()
    sections: dict[str, dict] = {}

    try:
        # ---- 1. Bronze Layer ----
        sections["1_bronze_layer"] = v.validate_all_bronze_datasets(spark)

        bronze_donors = v.read_parquet_safe(spark, v.BRONZE_PATHS["donors"])
        bronze_blood_banks = v.read_parquet_safe(spark, v.BRONZE_PATHS["blood_banks"])
        bronze_inventory = v.read_parquet_safe(spark, v.BRONZE_PATHS["blood_inventory"])
        bronze_camps = v.read_parquet_safe(spark, v.BRONZE_PATHS["donation_camps"])
        bronze_donations = v.read_parquet_safe(spark, v.BRONZE_PATHS["donations"])

        bronze_ready = all(x is not None for x in [bronze_donors, bronze_blood_banks, bronze_inventory, bronze_camps, bronze_donations])

        # ---- 2. Data Quality (missing values + blood groups + quantities + eligibility) ----
        if bronze_ready:
            missing_values = v.validate_all_missing_values(bronze_donors, bronze_blood_banks, bronze_inventory, bronze_donations)
            blood_group_checks = {
                "donors": v.validate_blood_groups(bronze_donors, "blood_group", "Donor Blood Group"),
                "inventory": v.validate_blood_groups(bronze_inventory, "blood_group", "Inventory Blood Group"),
                "donations": v.validate_blood_groups(bronze_donations, "blood_group", "Donation Blood Group"),
            }
            quantity_check = v.validate_donation_quantities(bronze_donations)
            eligibility_check = v.validate_donor_eligibility(bronze_donors)

            data_quality_pass = (
                missing_values["status"] == "PASS"
                and all(r["status"] == "PASS" for r in blood_group_checks.values())
                and quantity_check["status"] == "PASS"
                and eligibility_check["status"] == "PASS"
            )
            sections["2_data_quality"] = {
                "status": "PASS" if data_quality_pass else "FAIL",
                "missing_values": missing_values,
                "blood_group_checks": blood_group_checks,
                "donation_quantity_check": quantity_check,
                "donor_eligibility_check": eligibility_check,
            }
        else:
            sections["2_data_quality"] = {"status": "SKIPPED", "reason": "Bronze datasets not ready"}

        # ---- 3. Duplicate Checks ----
        # NOTE: Bronze intentionally preserves a few duplicate donation_id
        # rows injected by Phase 2 as a deliberate data-quality exercise for
        # Silver to clean up (see docs/Phase 6). Checking duplicates against
        # Bronze would therefore always FAIL by design and be misleading in
        # an end-to-end health report. This section instead validates the
        # SILVER layer, where every ID is expected to be genuinely unique
        # after Phase 6's dropDuplicates() step. (tests/test_data_quality.py
        # separately documents and checks the Bronze duplicate-preservation
        # behavior itself.)
        silver_donors = v.read_parquet_safe(spark, v.SILVER_PATHS["donors"])
        silver_blood_banks_dup = v.read_parquet_safe(spark, v.SILVER_PATHS["blood_banks"])
        silver_inventory_dup = v.read_parquet_safe(spark, v.SILVER_PATHS["blood_inventory"])
        silver_camps_dup = v.read_parquet_safe(spark, v.SILVER_PATHS["donation_camps"])
        silver_donations_dup = v.read_parquet_safe(spark, v.SILVER_PATHS["donations"])

        silver_ready = all(x is not None for x in [
            silver_donors, silver_blood_banks_dup, silver_inventory_dup, silver_camps_dup, silver_donations_dup
        ])

        if silver_ready:
            sections["3_duplicate_checks"] = v.validate_all_duplicates(
                silver_donors, silver_blood_banks_dup, silver_inventory_dup, silver_camps_dup, silver_donations_dup
            )
        else:
            sections["3_duplicate_checks"] = {"status": "SKIPPED", "reason": "Silver datasets not ready"}

        # ---- 4. Foreign Key Checks ----
        if bronze_ready:
            sections["4_foreign_key_checks"] = v.validate_foreign_keys(
                bronze_donations, bronze_donors, bronze_blood_banks, bronze_camps, bronze_inventory
            )
        else:
            sections["4_foreign_key_checks"] = {"status": "SKIPPED", "reason": "Bronze datasets not ready"}

        # ---- 5. Silver Layer ----
        sections["5_silver_layer"] = v.validate_silver_layer(spark)

        # ---- 6. Inventory Validation ----
        silver_inventory = v.read_parquet_safe(spark, v.SILVER_PATHS["blood_inventory"])
        silver_blood_banks = v.read_parquet_safe(spark, v.SILVER_PATHS["blood_banks"])
        if silver_inventory is not None and silver_blood_banks is not None:
            sections["6_inventory_validation"] = v.validate_inventory_consistency(silver_inventory, silver_blood_banks)
        else:
            sections["6_inventory_validation"] = {"status": "SKIPPED", "reason": "Silver inventory/blood_banks not found"}

        # ---- 7. Rare Blood Availability ----
        if silver_inventory is not None and silver_blood_banks is not None:
            sections["7_rare_blood_availability"] = v.validate_rare_blood_availability(silver_inventory, silver_blood_banks)
        else:
            sections["7_rare_blood_availability"] = {"status": "SKIPPED", "reason": "Silver inventory/blood_banks not found"}

        # ---- 8. Shortage Detection ----
        sections["8_shortage_detection"] = v.validate_shortage_boundaries()

        # ---- 9. Gold Layer ----
        gold_result = v.validate_all_gold_datasets(spark)
        reconciliation_result = v.validate_gold_reconciliation(spark)
        gold_overall_pass = gold_result["status"] == "PASS" and reconciliation_result["status"] == "PASS"
        sections["9_gold_layer"] = {
            "status": "PASS" if gold_overall_pass else "FAIL",
            "gold_datasets": gold_result,
            "reconciliation": reconciliation_result,
        }

        # ---- 10. Streaming Validation ----
        sections["10_streaming_validation"] = v.validate_streaming(spark)

    finally:
        spark.stop()

    return sections


# ============================================================================
# REPORT FORMATTING
# ============================================================================

SECTION_TITLES = {
    "1_bronze_layer": "Bronze Layer",
    "2_data_quality": "Data Quality",
    "3_duplicate_checks": "Duplicate Checks",
    "4_foreign_key_checks": "Foreign Key Checks",
    "5_silver_layer": "Silver Layer",
    "6_inventory_validation": "Inventory Validation",
    "7_rare_blood_availability": "Rare Blood Availability",
    "8_shortage_detection": "Shortage Detection",
    "9_gold_layer": "Gold Layer",
    "10_streaming_validation": "Streaming Validation",
}


def _collect_failures(sections: dict) -> list[str]:
    """Walk the nested results structure and collect human-readable failure
    descriptions for every FAIL found anywhere in the tree."""
    failures = []

    def walk(node, path):
        if isinstance(node, dict):
            if node.get("status") == "FAIL":
                reason = node.get("reason", "See details in validation_results.json")
                failures.append(f"{' > '.join(path)}: {reason}")
            for key, value in node.items():
                if key in ("status", "reason"):
                    continue
                walk(value, path + [str(key)])

    walk(sections, [])
    return failures


def format_text_report(sections: dict) -> str:
    lines = []
    lines.append("=" * 40)
    lines.append("RARE BLOOD ANALYTICS VALIDATION REPORT")
    lines.append("=" * 40)
    lines.append(f"\nGenerated: {datetime.now().isoformat(timespec='seconds')}\n")

    for i, (key, title) in enumerate(SECTION_TITLES.items(), start=1):
        result = sections.get(key, {"status": "SKIPPED"})
        lines.append(f"{i}. {title}")
        lines.append(f"   {result['status']}")
        if result["status"] == "SKIPPED" and "reason" in result:
            lines.append(f"   Reason: {result['reason']}")
        lines.append("")

    overall_statuses = [sections.get(k, {}).get("status", "SKIPPED") for k in SECTION_TITLES]
    if "FAIL" in overall_statuses:
        overall = "FAIL"
    elif all(s == "SKIPPED" for s in overall_statuses):
        overall = "SKIPPED"
    else:
        overall = "PASS"

    lines.append("=" * 40)
    lines.append(f"OVERALL RESULT: {overall}")
    lines.append("=" * 40)

    failures = _collect_failures(sections)
    if failures:
        lines.append("\nFAILURE DETAILS:")
        for f in failures:
            lines.append(f"  - {f}")

    return "\n".join(lines), overall


def write_reports(sections: dict) -> str:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    text_report, overall = format_text_report(sections)
    REPORT_TXT_PATH.write_text(text_report, encoding="utf-8")

    json_payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "overall_result": overall,
        "sections": sections,
    }
    REPORT_JSON_PATH.write_text(json.dumps(json_payload, indent=2, default=str), encoding="utf-8")

    return text_report


def main() -> None:
    sections = run_all_validations()
    text_report = write_reports(sections)
    print(text_report)
    print(f"\nReports written to:\n  {REPORT_TXT_PATH}\n  {REPORT_JSON_PATH}")


if __name__ == "__main__":
    main()
