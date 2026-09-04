"""
Shared pytest fixtures for Phase 10 tests.

A single, session-scoped SparkSession is reused across every test module to
keep the suite fast on a laptop. Datasets are loaded lazily inside each test
via fixtures below, and tests skip (rather than fail) when a prerequisite
dataset from an earlier phase hasn't been generated yet -- this keeps the
suite usable even if someone runs `pytest` before running the full
pipeline.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from validation import validators as v  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    session = v.get_or_create_spark()
    yield session
    session.stop()


def _load_or_skip(spark_session, path: Path, label: str):
    df = v.read_parquet_safe(spark_session, path)
    if df is None:
        pytest.skip(f"{label} not found at {path} -- run the corresponding pipeline phase first")
    return df


@pytest.fixture(scope="session")
def bronze_donors(spark):
    return _load_or_skip(spark, v.BRONZE_PATHS["donors"], "Bronze donors")


@pytest.fixture(scope="session")
def bronze_blood_banks(spark):
    return _load_or_skip(spark, v.BRONZE_PATHS["blood_banks"], "Bronze blood_banks")


@pytest.fixture(scope="session")
def bronze_inventory(spark):
    return _load_or_skip(spark, v.BRONZE_PATHS["blood_inventory"], "Bronze blood_inventory")


@pytest.fixture(scope="session")
def bronze_camps(spark):
    return _load_or_skip(spark, v.BRONZE_PATHS["donation_camps"], "Bronze donation_camps")


@pytest.fixture(scope="session")
def bronze_donations(spark):
    return _load_or_skip(spark, v.BRONZE_PATHS["donations"], "Bronze donations")


@pytest.fixture(scope="session")
def silver_donors(spark):
    return _load_or_skip(spark, v.SILVER_PATHS["donors"], "Silver donors")


@pytest.fixture(scope="session")
def silver_blood_banks(spark):
    return _load_or_skip(spark, v.SILVER_PATHS["blood_banks"], "Silver blood_banks")


@pytest.fixture(scope="session")
def silver_inventory(spark):
    return _load_or_skip(spark, v.SILVER_PATHS["blood_inventory"], "Silver blood_inventory")


@pytest.fixture(scope="session")
def silver_camps(spark):
    return _load_or_skip(spark, v.SILVER_PATHS["donation_camps"], "Silver donation_camps")


@pytest.fixture(scope="session")
def silver_donations(spark):
    return _load_or_skip(spark, v.SILVER_PATHS["donations"], "Silver donations")
