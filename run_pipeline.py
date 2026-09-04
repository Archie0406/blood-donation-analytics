"""
Enterprise Rare Blood Type Availability Analytics
Master Pipeline Runner
=====================================================

Runs the full pipeline in the correct order with one command, instead of
requiring each script to be invoked manually. This wraps the existing,
already-working scripts via subprocess -- it does not reimplement any of
their logic -- so a failure in any stage stops the run with a clear message
naming exactly which script failed.

Usage:
    python run_pipeline.py                 # full pipeline (batch only)
    python run_pipeline.py --skip-generate  # reuse existing data/raw/*.csv
    python run_pipeline.py --validate       # run tests + validation report only
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
PYTHON = sys.executable

# Ordered list of (label, script) for the full batch pipeline.
BATCH_STAGES = [
    ("Generate synthetic seed data", "data_generation/01_generate_seed_data.py"),
    ("Generate transactional inventory data", "data_generation/02_generate_transactional_data.py"),
    ("Bronze batch ingestion", "pipeline/03_bronze_batch_ingest.py"),
    ("Silver cleaning & enrichment", "pipeline/05_silver_transform.py"),
    ("Rare blood availability processing", "pipeline/07_rare_blood_availability.py"),
    ("Gold aggregation", "pipeline/08_gold_aggregate.py"),
    ("Inventory transaction engine", "pipeline/09_inventory_transaction_engine.py"),
]

VALIDATE_STAGES = [
    ("Automated test suite", None),  # special-cased: runs pytest, not a script
    ("End-to-end validation report", "validation/validation_report.py"),
]


def run_script(label: str, script_path: str) -> float:
    print(f"\n{'=' * 60}\n{label}\n({script_path})\n{'=' * 60}")
    start = time.time()
    result = subprocess.run([PYTHON, str(PROJECT_ROOT / script_path)])
    elapsed = time.time() - start

    if result.returncode != 0:
        print(f"\nERROR: '{script_path}' failed (exit code {result.returncode}). Stopping pipeline.")
        sys.exit(result.returncode)

    print(f"\n[OK] {label} completed in {elapsed:.1f}s")
    return elapsed


def run_pytest() -> float:
    print(f"\n{'=' * 60}\nAutomated test suite (pytest tests/)\n{'=' * 60}")
    start = time.time()
    result = subprocess.run([PYTHON, "-m", "pytest", "tests/", "-q"])
    elapsed = time.time() - start
    if result.returncode != 0:
        print(f"\nERROR: test suite failed. Stopping pipeline.")
        sys.exit(result.returncode)
    print(f"\n[OK] Test suite completed in {elapsed:.1f}s")
    return elapsed


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the full Rare Blood Analytics pipeline.")
    parser.add_argument("--skip-generate", action="store_true",
                         help="Skip synthetic data generation steps; reuse existing data/raw/*.csv")
    parser.add_argument("--validate", action="store_true",
                         help="Only run the test suite and end-to-end validation report")
    args = parser.parse_args()

    print("=" * 60)
    print("ENTERPRISE RARE BLOOD TYPE AVAILABILITY ANALYTICS")
    print("Master Pipeline Runner")
    print("=" * 60)

    total_start = time.time()
    timings: dict[str, float] = {}

    if args.validate:
        timings["Automated test suite"] = run_pytest()
        timings["End-to-end validation report"] = run_script(
            "End-to-end validation report", "validation/validation_report.py"
        )
    else:
        stages = BATCH_STAGES
        if args.skip_generate:
            stages = [s for s in stages if not s[1].startswith("data_generation/")]
            print("\n--skip-generate: reusing existing data/raw/*.csv")

        for label, script in stages:
            timings[label] = run_script(label, script)

    total_elapsed = time.time() - total_start

    print("\n" + "=" * 60)
    print("PIPELINE SUMMARY")
    print("=" * 60)
    for label, elapsed in timings.items():
        print(f"  {label:<45} {elapsed:6.1f}s")
    print(f"\n  {'TOTAL':<45} {total_elapsed:6.1f}s")
    print("=" * 60)
    print("PIPELINE COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()
