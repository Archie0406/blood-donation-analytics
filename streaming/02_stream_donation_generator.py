"""
Enterprise Rare Blood Type Availability Analytics
Phase 4 — Live Donation Stream Simulation
=====================================================

ARCHITECTURE CONTEXT
----------------------
    Historical Source Data (Phase 2)
              +
    Python Live Event Generator (this script)
              |
              v
      JSON Landing Zone (data/streaming/landing/)
              |
              v
    Phase 5: Spark Structured Streaming  (NOT implemented here)
              |
              v
        Bronze Streaming Parquet

This script simulates a real-time blood donation feed WITHOUT any message
broker (no Kafka, no cloud queues) so the whole project can run on a laptop.
It generates one donation event at a time, waits a configurable interval,
and writes each event as its own flat JSON file into a landing directory
that Phase 5's Spark Structured Streaming job will later watch.

WHAT THIS SCRIPT DOES NOT DO
-------------------------------
    - No PySpark.
    - No cleaning, deduplication, or business validation beyond a basic
      pre-write sanity check.
    - No inventory updates.
    - No rare-blood-availability or shortage calculations.
    - No joins, aggregations, or Gold-table logic.
    - No modification of any file under data/raw/ or data/bronze/.

It ONLY reads donors.csv, blood_banks.csv, and donation_camps.csv (for valid
IDs and blood groups) and WRITES new JSON files under data/streaming/landing/.
"""

from __future__ import annotations

import argparse
import csv
import json
import random
import re
import signal
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

# ============================================================================
# CONFIGURATION (single source of truth for all tunable defaults)
# ============================================================================

SEED = 42

RARE_BLOOD_GROUPS = ["O-", "AB-", "B-", "A-"]
RARE_BLOOD_PROBABILITY = 0.25   # ~25% of events prioritize a rare-blood donor
CAMP_EVENT_PROBABILITY = 0.40   # ~40% of events are linked to a donation camp

DONATION_STATUS_PROBABILITIES = {"Completed": 90, "Cancelled": 5, "Rejected": 5}
VALID_DONATION_STATUSES = set(DONATION_STATUS_PROBABILITIES.keys())

UNITS_MIN, UNITS_MAX = 1, 2
ACTIVE_BANK_WEIGHT = 5   # relative selection weight for Active blood banks
INACTIVE_BANK_WEIGHT = 1  # relative selection weight for non-Active blood banks

DEFAULT_INTERVAL = 2.0
DEFAULT_COUNT = 100

FILE_PREFIX = "donation_"
DONATION_ID_PREFIX = "LIVE-DONATION-"

# Paths (relative to project root, OS-independent)
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
RAW_DATA_PATH = PROJECT_ROOT / "data" / "raw"
DEFAULT_LANDING_PATH = PROJECT_ROOT / "data" / "streaming" / "landing"

DONORS_FILE = RAW_DATA_PATH / "donors.csv"
BLOOD_BANKS_FILE = RAW_DATA_PATH / "blood_banks.csv"
DONATION_CAMPS_FILE = RAW_DATA_PATH / "donation_camps.csv"


# ============================================================================
# CONFIG OBJECT
# ============================================================================

@dataclass
class GeneratorConfig:
    """Holds all runtime configuration for a single generator run."""

    interval: float = DEFAULT_INTERVAL
    count: int = DEFAULT_COUNT
    rare_probability: float = RARE_BLOOD_PROBABILITY
    camp_probability: float = CAMP_EVENT_PROBABILITY
    output_dir: Path = field(default_factory=lambda: DEFAULT_LANDING_PATH)
    seed: int = SEED
    clear: bool = False


# ============================================================================
# GRACEFUL CTRL+C HANDLING
# ============================================================================

class StreamInterrupted(Exception):
    """Raised when the user presses Ctrl+C during stream generation."""


def _handle_sigint(signum, frame) -> None:  # noqa: ARG001
    raise StreamInterrupted()


# ============================================================================
# LOADING SOURCE DATA (READ-ONLY)
# ============================================================================

def _read_csv(path: Path) -> list[dict]:
    """Read a CSV file into a list of dicts using the standard library only."""
    if not path.exists():
        print(f"ERROR: Required source file not found: {path}")
        print("Run Phase 2 (data_generation/01_generate_seed_data.py) first.")
        sys.exit(1)

    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        return list(reader)


def load_donors() -> list[dict]:
    """Load donors.csv (read-only)."""
    return _read_csv(DONORS_FILE)


def load_blood_banks() -> list[dict]:
    """Load blood_banks.csv (read-only)."""
    return _read_csv(BLOOD_BANKS_FILE)


def load_donation_camps() -> list[dict]:
    """Load donation_camps.csv (read-only)."""
    return _read_csv(DONATION_CAMPS_FILE)


# ============================================================================
# SELECTION LOGIC
# ============================================================================

def select_donor(donors: list[dict], rare_donor_pool: list[dict], rng: random.Random, rare_probability: float) -> dict:
    """Select a donor for the event.

    Most of the time a donor is picked uniformly at random. With probability
    `rare_probability`, a donor already carrying a rare blood group is
    preferred instead -- this keeps the live stream demo meaningfully
    populated with rare-blood events without making every event rare.
    """
    if rare_donor_pool and rng.random() < rare_probability:
        return rng.choice(rare_donor_pool)
    return rng.choice(donors)


def select_blood_bank(blood_banks: list[dict], rng: random.Random) -> dict:
    """Select a blood bank, favoring Active banks when operating_status exists."""
    weights = [
        ACTIVE_BANK_WEIGHT if bank.get("operating_status") == "Active" else INACTIVE_BANK_WEIGHT
        for bank in blood_banks
    ]
    return rng.choices(blood_banks, weights=weights, k=1)[0]


def select_camp(camps: list[dict], rng: random.Random, camp_probability: float) -> Optional[dict]:
    """Optionally associate the event with a donation camp."""
    if camps and rng.random() < camp_probability:
        return rng.choice(camps)
    return None


def select_donation_status(rng: random.Random) -> str:
    """Pick a donation status using the configured weighted probabilities."""
    statuses = list(DONATION_STATUS_PROBABILITIES.keys())
    weights = list(DONATION_STATUS_PROBABILITIES.values())
    return rng.choices(statuses, weights=weights, k=1)[0]


# ============================================================================
# EVENT GENERATION
# ============================================================================

def generate_donation_event(
    sequence: int,
    donors: list[dict],
    rare_donor_pool: list[dict],
    blood_banks: list[dict],
    camps: list[dict],
    rng: random.Random,
    config: GeneratorConfig,
) -> dict:
    """Build one flat, Spark-friendly donation event dict.

    The event's blood_group ALWAYS comes from the selected donor's actual
    blood_group -- it is never generated independently.
    """
    donor = select_donor(donors, rare_donor_pool, rng, config.rare_probability)
    bank = select_blood_bank(blood_banks, rng)
    camp = select_camp(camps, rng, config.camp_probability)
    now = datetime.now()

    return {
        "donation_id": f"{DONATION_ID_PREFIX}{sequence:06d}",
        "donor_id": donor["donor_id"],
        "blood_bank_id": bank["blood_bank_id"],
        "camp_id": camp["camp_id"] if camp else None,
        "donation_date": now.strftime("%Y-%m-%d"),
        "blood_group": donor["blood_group"],
        "units_donated": rng.randint(UNITS_MIN, UNITS_MAX),
        "donation_status": select_donation_status(rng),
        "event_time": now.strftime("%Y-%m-%dT%H:%M:%S"),
        "source": "live_donation_simulator",
        "event_type": "blood_donation",
    }


# ============================================================================
# VALIDATION (basic technical safety net, applied before every write)
# ============================================================================

def validate_event(
    event: dict,
    donor_ids: set,
    bank_ids: set,
    camp_ids: set,
    donor_blood_group_lookup: dict,
    seen_donation_ids: set,
) -> tuple[bool, str]:
    """Run basic technical checks before writing an event to disk.

    Returns (is_valid, reason). reason is empty when is_valid is True.
    """
    if event["donation_id"] in seen_donation_ids:
        return False, f"Duplicate donation_id: {event['donation_id']}"

    if event["donor_id"] not in donor_ids:
        return False, f"Unknown donor_id: {event['donor_id']}"

    if event["blood_bank_id"] not in bank_ids:
        return False, f"Unknown blood_bank_id: {event['blood_bank_id']}"

    if event["camp_id"] is not None and event["camp_id"] not in camp_ids:
        return False, f"Unknown camp_id: {event['camp_id']}"

    expected_blood_group = donor_blood_group_lookup.get(event["donor_id"])
    if event["blood_group"] != expected_blood_group:
        return False, (
            f"blood_group mismatch for {event['donor_id']}: "
            f"event={event['blood_group']} donor={expected_blood_group}"
        )

    if not isinstance(event["units_donated"], int) or event["units_donated"] <= 0:
        return False, f"Invalid units_donated: {event['units_donated']}"

    if event["donation_status"] not in VALID_DONATION_STATUSES:
        return False, f"Invalid donation_status: {event['donation_status']}"

    if not event.get("event_time"):
        return False, "Missing event_time"

    if not event.get("donation_date"):
        return False, "Missing donation_date"

    return True, ""


# ============================================================================
# WRITING EVENTS
# ============================================================================

def get_starting_sequence(output_dir: Path) -> int:
    """Determine the next sequence number to use, based on any donation_*.json
    files already present in the output directory. This guarantees new files
    never overwrite files from a previous run (unless --clear was used)."""
    pattern = re.compile(rf"^{re.escape(FILE_PREFIX)}(\d+)\.json$")
    max_seq = 0
    if output_dir.exists():
        for file in output_dir.glob(f"{FILE_PREFIX}*.json"):
            match = pattern.match(file.name)
            if match:
                max_seq = max(max_seq, int(match.group(1)))
    return max_seq + 1


def write_event(event: dict, output_dir: Path, sequence: int) -> Path:
    """Write a single donation event as its own flat JSON file.

    One JSON object per file (never a combined/appended file) so Spark
    Structured Streaming can detect each new file as a discrete event.
    """
    filename = f"{FILE_PREFIX}{sequence:06d}.json"
    file_path = output_dir / filename

    if file_path.exists():
        raise FileExistsError(f"Refusing to overwrite existing event file: {file_path}")

    with file_path.open("w", encoding="utf-8") as f:
        json.dump(event, f, indent=2)

    return file_path


def clear_landing_directory(output_dir: Path) -> int:
    """Remove existing donation_*.json files from the landing directory.
    Only called when the user explicitly passes --clear."""
    removed = 0
    if output_dir.exists():
        for file in output_dir.glob(f"{FILE_PREFIX}*.json"):
            file.unlink()
            removed += 1
    return removed


# ============================================================================
# STREAM GENERATION LOOP
# ============================================================================

def generate_stream(config: GeneratorConfig) -> dict:
    """Run the full stream-generation loop and return summary statistics."""
    rng = random.Random(config.seed)

    donors = load_donors()
    blood_banks = load_blood_banks()
    camps = load_donation_camps()

    if not donors or not blood_banks:
        print("ERROR: donors.csv or blood_banks.csv contains no records. Cannot generate events.")
        sys.exit(1)

    donor_ids = {d["donor_id"] for d in donors}
    bank_ids = {b["blood_bank_id"] for b in blood_banks}
    camp_ids = {c["camp_id"] for c in camps}
    donor_blood_group_lookup = {d["donor_id"]: d["blood_group"] for d in donors}
    rare_donor_pool = [d for d in donors if d["blood_group"] in RARE_BLOOD_GROUPS]

    config.output_dir.mkdir(parents=True, exist_ok=True)

    if config.clear:
        removed = clear_landing_directory(config.output_dir)
        print(f"--clear specified: removed {removed} existing event file(s) from {config.output_dir}\n")

    starting_sequence = get_starting_sequence(config.output_dir)
    seen_donation_ids: set = set()

    stats = {
        "generated": 0,
        "skipped_invalid": 0,
        "status_counts": {status: 0 for status in VALID_DONATION_STATUSES},
        "rare_blood_counts": {group: 0 for group in RARE_BLOOD_GROUPS},
    }

    signal.signal(signal.SIGINT, _handle_sigint)

    print("Starting stream...\n")
    try:
        for i in range(config.count):
            sequence = starting_sequence + i
            event = generate_donation_event(
                sequence, donors, rare_donor_pool, blood_banks, camps, rng, config
            )

            is_valid, reason = validate_event(
                event, donor_ids, bank_ids, camp_ids, donor_blood_group_lookup, seen_donation_ids
            )

            if not is_valid:
                print(f"[SKIPPED] Invalid event at sequence {sequence}: {reason}")
                stats["skipped_invalid"] += 1
                continue

            seen_donation_ids.add(event["donation_id"])
            file_path = write_event(event, config.output_dir, sequence)

            stats["generated"] += 1
            stats["status_counts"][event["donation_status"]] += 1
            if event["blood_group"] in RARE_BLOOD_GROUPS:
                stats["rare_blood_counts"][event["blood_group"]] += 1

            print(f"[{i + 1}/{config.count}]")
            print(f"Donation ID: {event['donation_id']}")
            print(f"Donor: {event['donor_id']}")
            print(f"Blood Group: {event['blood_group']}")
            print(f"Blood Bank: {event['blood_bank_id']}")
            print(f"Status: {event['donation_status']}")
            print(f"File: {file_path.name}\n")

            if i < config.count - 1:
                time.sleep(config.interval)

    except StreamInterrupted:
        print("\nStream generation interrupted by user.")

    return stats


# ============================================================================
# CONSOLE OUTPUT
# ============================================================================

def print_header(config: GeneratorConfig) -> None:
    print("=" * 45)
    print("LIVE DONATION STREAM GENERATOR")
    print("Rare Blood Availability Analytics")
    print("=" * 45)
    print("\nConfiguration:")
    print(f"Interval: {config.interval} seconds")
    print(f"Events: {config.count}")
    print(f"Rare blood probability: {config.rare_probability:.0%}")
    print(f"Camp probability: {config.camp_probability:.0%}")
    print(f"Output: {config.output_dir}")
    print(f"Seed: {config.seed}\n")


def print_summary(stats: dict, config: GeneratorConfig) -> None:
    total_rare = sum(stats["rare_blood_counts"].values())

    print("\n" + "=" * 45)
    print("STREAM GENERATION COMPLETE")
    print("=" * 45)
    print(f"\nEvents generated: {stats['generated']}")
    if stats["skipped_invalid"]:
        print(f"Events skipped (failed validation): {stats['skipped_invalid']}")

    print("\nDonation Status:")
    for status, count in stats["status_counts"].items():
        print(f"{status}: {count}")

    print("\nRare Blood Events:")
    for group, count in stats["rare_blood_counts"].items():
        print(f"{group}: {count}")
    print(f"\nTotal rare blood events: {total_rare}")

    print(f"\nFiles written to:\n{config.output_dir}")
    print("\nReady for Phase 5:")
    print("Spark Structured Streaming Bronze ingestion")
    print("=" * 45)


# ============================================================================
# CLI
# ============================================================================

def parse_arguments() -> GeneratorConfig:
    """Parse command-line arguments into a GeneratorConfig."""
    parser = argparse.ArgumentParser(
        description="Simulate a live blood donation event stream (Phase 4)."
    )
    parser.add_argument("--interval", type=float, default=DEFAULT_INTERVAL,
                         help=f"Seconds to wait between events (default: {DEFAULT_INTERVAL})")
    parser.add_argument("--count", type=int, default=DEFAULT_COUNT,
                         help=f"Number of events to generate (default: {DEFAULT_COUNT})")
    parser.add_argument("--rare-probability", type=float, default=RARE_BLOOD_PROBABILITY,
                         help=f"Probability of prioritizing a rare-blood donor (default: {RARE_BLOOD_PROBABILITY})")
    parser.add_argument("--camp-probability", type=float, default=CAMP_EVENT_PROBABILITY,
                         help=f"Probability an event is linked to a camp (default: {CAMP_EVENT_PROBABILITY})")
    parser.add_argument("--output", type=str, default=str(DEFAULT_LANDING_PATH),
                         help="Output directory for JSON events (default: data/streaming/landing/)")
    parser.add_argument("--seed", type=int, default=SEED,
                         help=f"Random seed for reproducible selection (default: {SEED})")
    parser.add_argument("--clear", action="store_true",
                         help="Remove existing event files before starting (off by default)")

    args = parser.parse_args()

    if args.count <= 0:
        parser.error("--count must be a positive integer")
    if args.interval < 0:
        parser.error("--interval must be zero or positive")
    if not (0.0 <= args.rare_probability <= 1.0):
        parser.error("--rare-probability must be between 0.0 and 1.0")
    if not (0.0 <= args.camp_probability <= 1.0):
        parser.error("--camp-probability must be between 0.0 and 1.0")

    return GeneratorConfig(
        interval=args.interval,
        count=args.count,
        rare_probability=args.rare_probability,
        camp_probability=args.camp_probability,
        output_dir=Path(args.output),
        seed=args.seed,
        clear=args.clear,
    )


# ============================================================================
# MAIN
# ============================================================================

def main() -> None:
    config = parse_arguments()
    print_header(config)
    stats = generate_stream(config)
    print_summary(stats, config)


if __name__ == "__main__":
    main()
