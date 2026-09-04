"""
Phase 10 — Streaming Validation Tests

Validates the Structured Streaming pipeline artifacts on disk: the JSON
landing zone, the materialized Bronze streaming Parquet output, and the
checkpoint directory. No Kafka, Docker, or cluster required -- these tests
work directly against the files Phase 4 and Phase 5 already produced.
"""

from __future__ import annotations

import json

import pytest

from validation import validators as v


def test_streaming_landing_directory_exists():
    assert v.STREAMING_LANDING_DIR.exists(), (
        f"Streaming landing directory not found at {v.STREAMING_LANDING_DIR} -- "
        f"run streaming/02_stream_donation_generator.py first"
    )


def test_streaming_landing_json_files_readable():
    json_files = list(v.STREAMING_LANDING_DIR.glob("*.json")) if v.STREAMING_LANDING_DIR.exists() else []
    if not json_files:
        pytest.skip("No JSON files in the streaming landing directory yet")

    # Simulate a small stream check (10-20 records) without needing a live
    # Structured Streaming query -- just confirm the files are valid,
    # well-formed, single JSON objects with the expected fields.
    sample = json_files[:20]
    required_fields = {"donation_id", "donor_id", "blood_bank_id", "blood_group",
                        "units_donated", "donation_status", "event_time"}

    for file_path in sample:
        with open(file_path, "r", encoding="utf-8") as f:
            event = json.load(f)
        missing = required_fields - event.keys()
        assert not missing, f"{file_path.name} is missing required field(s): {missing}"


def test_checkpoint_directory_exists():
    assert v.STREAMING_CHECKPOINT_DIR.exists(), (
        f"Checkpoint directory not found at {v.STREAMING_CHECKPOINT_DIR} -- "
        f"run pipeline/04_bronze_streaming_ingest.py first"
    )


def test_bronze_streaming_dataset_readable(spark):
    df = v.read_parquet_safe(spark, v.BRONZE_PATHS["streaming_donations"])
    if df is None:
        pytest.skip("Bronze streaming_donations not found -- run Phase 5 first")
    assert df.count() > 0


def test_bronze_streaming_has_required_columns(spark):
    df = v.read_parquet_safe(spark, v.BRONZE_PATHS["streaming_donations"])
    if df is None:
        pytest.skip("Bronze streaming_donations not found -- run Phase 5 first")

    required = ["donation_id", "donor_id", "blood_bank_id", "blood_group",
                "units_donated", "donation_status", "event_time", "_ingested_at"]
    missing = [c for c in required if c not in df.columns]
    assert not missing, f"Bronze streaming_donations missing column(s): {missing}"


def test_bronze_streaming_no_duplicate_donation_ids(spark):
    df = v.read_parquet_safe(spark, v.BRONZE_PATHS["streaming_donations"])
    if df is None:
        pytest.skip("Bronze streaming_donations not found -- run Phase 5 first")

    result = v.check_duplicates(df, ["donation_id"], "Streaming Donation Duplicate Check")
    assert result["status"] == "PASS", result


def test_streaming_overall_validation(spark):
    result = v.validate_streaming(spark)
    assert result["status"] == "PASS", result
