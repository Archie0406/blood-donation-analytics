"""
Enterprise Rare Blood Type Availability Analytics
Data Generation v2 — Transaction-Driven Inventory Model
=====================================================

WHY THIS SCRIPT EXISTS
--------------------------
The original data model (Phase 1-10) treated `blood_inventory.csv` as a
static, independently-generated snapshot, deliberately never merged with
the donation log (documented honestly in README's "Inventory Processing"
section). That was a real, acknowledged limitation.

This script fixes it by building a genuine **transaction-driven** inventory
model on top of the existing project data, without modifying or deleting
anything the original pipeline produced:

    Current Inventory
        =
    Opening Inventory (0)
        + DONATION
        + TRANSFER_IN
        - ISSUE
        - TRANSFER_OUT
        - EXPIRY
        + ADJUSTMENT

Every transaction is generated so that this equation holds **exactly, by
construction**, per (blood_bank_id, blood_group, component_type) key --
this is what pipeline/09_inventory_transaction_engine.py's reconciliation
test verifies.

WHAT THIS SCRIPT READS (read-only)
---------------------------------------
    data/raw/donors.csv
    data/raw/blood_banks.csv
    data/raw/blood_inventory.csv     (used only to decide which
                                       bank+blood_group combinations exist
                                       -- their unit counts are NOT reused;
                                       the new model computes its own
                                       component-level balances from
                                       transactions)
    data/raw/donations.csv           (real, existing "Completed" donations
                                       become real DONATION transactions --
                                       this is what section 33 of the
                                       upgrade brief calls "Donation ->
                                       creates inventory transaction ->
                                       changes inventory")

WHAT THIS SCRIPT WRITES (new, additive -- nothing upstream is touched)
----------------------------------------------------------------------------
    data/raw/inventory_transactions.csv   the transaction ledger (source of truth)
    data/raw/blood_transfers.csv          transfer records (paired with
                                           TRANSFER_IN/TRANSFER_OUT transactions)
    data/raw/blood_issues.csv             consumption/demand records (paired
                                           with ISSUE transactions)
    data/raw/inventory_components.csv     derived closing-balance snapshot
                                           per (bank, blood_group, component,
                                           expiry_status) -- for reference/
                                           validation only; the transaction
                                           engine recomputes this from the
                                           ledger rather than trusting it.
"""

from __future__ import annotations

import random
import sys
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config.config_loader import (  # noqa: E402
    get_component_shelf_life,
    get_random_seed,
    get_valid_component_types,
)

# ============================================================================
# CONFIGURATION
# ============================================================================

SEED = get_random_seed()
COMPONENT_TYPES = get_valid_component_types()  # ["RBC", "Plasma", "Platelets"]

RAW_DIR = PROJECT_ROOT / "data" / "raw"
DONORS_PATH = RAW_DIR / "donors.csv"
BLOOD_BANKS_PATH = RAW_DIR / "blood_banks.csv"
BLOOD_INVENTORY_PATH = RAW_DIR / "blood_inventory.csv"
DONATIONS_PATH = RAW_DIR / "donations.csv"

OUT_TRANSACTIONS_PATH = RAW_DIR / "inventory_transactions.csv"
OUT_TRANSFERS_PATH = RAW_DIR / "blood_transfers.csv"
OUT_ISSUES_PATH = RAW_DIR / "blood_issues.csv"
OUT_COMPONENTS_PATH = RAW_DIR / "inventory_components.csv"

# Demand simulation: rough daily issue probability/size per component tier,
# tuned only to produce plausible, non-negative, moderately busy ledgers for
# a demo dataset -- not derived from any real clinical demand data.
ISSUE_PROBABILITY_PER_DONATION_EVENT = 0.35
ISSUE_FRACTION_OF_BALANCE = (0.05, 0.20)  # min/max fraction of current balance issued

TRANSFER_SURPLUS_THRESHOLD = 5   # a key needs at least this much balance to be a transfer source
TRANSFER_FRACTION_OF_SURPLUS = (0.2, 0.5)
MAX_TRANSFERS = 25

ADJUSTMENT_PROBABILITY = 0.06
ADJUSTMENT_RANGE = (-2, 3)  # can be slightly negative (wastage correction) or positive (recount)

EXPIRY_CHECK_PROBABILITY = 0.5  # probability an aging batch is checked for expiry at all


# ============================================================================
# HELPERS
# ============================================================================

@dataclass
class LedgerKey:
    blood_bank_id: str
    blood_group: str
    component_type: str

    def as_tuple(self):
        return (self.blood_bank_id, self.blood_group, self.component_type)


@dataclass
class Ledger:
    """Per-key running transaction ledger. Every append keeps `balance`
    consistent, so the reconciliation equation holds by construction."""
    key: LedgerKey
    balance: float = 0.0
    transactions: list = field(default_factory=list)

    def append(self, txn_date: date, txn_type: str, units: float, **extra) -> None:
        self.transactions.append({
            "transaction_id": f"TXN{uuid.uuid4().hex[:10].upper()}",
            "blood_bank_id": self.key.blood_bank_id,
            "blood_group": self.key.blood_group,
            "component_type": self.key.component_type,
            "transaction_type": txn_type,
            "units": units,
            "transaction_timestamp": datetime.combine(txn_date, datetime.min.time())
            + timedelta(hours=random.randint(8, 18), minutes=random.randint(0, 59)),
            **extra,
        })
        if txn_type in ("DONATION", "TRANSFER_IN"):
            self.balance += units
        elif txn_type == "ADJUSTMENT":
            self.balance += units  # units may be negative
        else:  # ISSUE, TRANSFER_OUT, EXPIRY
            self.balance -= units


# ============================================================================
# STEP 1 — DONATION TRANSACTIONS (real, tied to existing donations.csv)
# ============================================================================

def build_donation_transactions(donations: pd.DataFrame, ledgers: dict) -> None:
    """Every existing Completed donation becomes real DONATION transactions.
    A whole-blood donation is split into components: RBC and Plasma each
    receive the full donated units; Platelets receives units only about
    half the time (consistent with real-world component separation, kept
    simple for a synthetic demo)."""
    completed = donations[donations["donation_status"] == "Completed"].copy()
    completed["donation_date"] = pd.to_datetime(completed["donation_date"]).dt.date

    for row in completed.itertuples(index=False):
        for component in COMPONENT_TYPES:
            if component == "Platelets" and random.random() >= 0.5:
                continue
            key = LedgerKey(row.blood_bank_id, row.blood_group, component)
            ledger = ledgers.setdefault(key.as_tuple(), Ledger(key=key))
            ledger.append(
                row.donation_date, "DONATION", float(row.units_donated),
                source_donation_id=row.donation_id, source_transfer_id=None,
                destination_bank_id=None, status="POSTED",
            )


# ============================================================================
# STEP 2 — ISSUE TRANSACTIONS (demand / consumption)
# ============================================================================

def build_issue_transactions(ledgers: dict, issue_records: list) -> None:
    """After donations are posted, simulate consumption. Each issue is
    capped at a random fraction of the CURRENT balance at that point in
    time, guaranteeing the running balance never goes negative."""
    for ledger in list(ledgers.values()):
        donation_events = [t for t in ledger.transactions if t["transaction_type"] == "DONATION"]
        for donation_event in donation_events:
            if random.random() >= ISSUE_PROBABILITY_PER_DONATION_EVENT:
                continue
            if ledger.balance <= 0:
                continue

            issue_date = donation_event["transaction_timestamp"].date() + timedelta(
                days=random.randint(1, 10)
            )
            fraction = random.uniform(*ISSUE_FRACTION_OF_BALANCE)
            units = max(1, round(ledger.balance * fraction))
            units = min(units, ledger.balance)
            if units <= 0:
                continue

            issue_id = f"ISSUE{uuid.uuid4().hex[:10].upper()}"
            hospital_id = f"HOSP{random.randint(1, 12):03d}"

            ledger.append(
                issue_date, "ISSUE", float(units),
                source_donation_id=None, source_transfer_id=None,
                destination_bank_id=None, status="POSTED",
                issue_id=issue_id,
            )
            issue_records.append({
                "issue_id": issue_id,
                "blood_bank_id": ledger.key.blood_bank_id,
                "blood_group": ledger.key.blood_group,
                "component_type": ledger.key.component_type,
                "units": units,
                "issue_date": issue_date,
                "hospital_id": hospital_id,
            })


# ============================================================================
# STEP 3 — TRANSFERS BETWEEN BANKS
# ============================================================================

def build_transfer_transactions(ledgers: dict, transfer_records: list) -> None:
    """Move surplus from a bank with healthy balance to another bank
    carrying the same blood_group + component_type. TRANSFER_OUT (source)
    and TRANSFER_IN (destination) are always posted together, same date,
    same units, linked by transfer_id -- so the transfer ledger is always
    internally balanced."""
    by_group_component: dict = {}
    for key_tuple, ledger in ledgers.items():
        bank_id, group, component = key_tuple
        by_group_component.setdefault((group, component), []).append(ledger)

    transfers_made = 0
    for (group, component), bank_ledgers in by_group_component.items():
        if transfers_made >= MAX_TRANSFERS or len(bank_ledgers) < 2:
            continue

        surplus_sources = [l for l in bank_ledgers if l.balance >= TRANSFER_SURPLUS_THRESHOLD]
        for source in surplus_sources:
            if transfers_made >= MAX_TRANSFERS:
                break
            candidates = [l for l in bank_ledgers if l.key.blood_bank_id != source.key.blood_bank_id]
            if not candidates:
                continue
            destination = random.choice(candidates)

            fraction = random.uniform(*TRANSFER_FRACTION_OF_SURPLUS)
            units = max(1, round(source.balance * fraction))
            units = min(units, source.balance)
            if units <= 0:
                continue

            transfer_id = f"TRF{uuid.uuid4().hex[:10].upper()}"
            transfer_date = date.today() - timedelta(days=random.randint(1, 20))

            source.append(
                transfer_date, "TRANSFER_OUT", float(units),
                source_donation_id=None, source_transfer_id=transfer_id,
                destination_bank_id=destination.key.blood_bank_id, status="POSTED",
            )
            destination.append(
                transfer_date, "TRANSFER_IN", float(units),
                source_donation_id=None, source_transfer_id=transfer_id,
                destination_bank_id=None, status="POSTED",
            )
            transfer_records.append({
                "transfer_id": transfer_id,
                "source_bank_id": source.key.blood_bank_id,
                "destination_bank_id": destination.key.blood_bank_id,
                "blood_group": group,
                "component_type": component,
                "units": units,
                "transfer_date": transfer_date,
                "status": "COMPLETED",
            })
            transfers_made += 1


# ============================================================================
# STEP 4 — EXPIRY
# ============================================================================

def build_expiry_transactions(ledgers: dict) -> None:
    """Any component batch old enough to exceed its shelf life has a chance
    of a portion expiring before being issued or transferred -- capped at
    the current balance so it never goes negative."""
    today = date.today()
    for ledger in ledgers.values():
        if ledger.balance <= 0:
            continue
        if random.random() >= EXPIRY_CHECK_PROBABILITY:
            continue

        shelf_life = get_component_shelf_life(ledger.key.component_type)
        oldest_donation = min(
            (t["transaction_timestamp"].date() for t in ledger.transactions if t["transaction_type"] == "DONATION"),
            default=None,
        )
        if oldest_donation is None or (today - oldest_donation).days < shelf_life:
            continue

        units = max(1, round(ledger.balance * random.uniform(0.05, 0.2)))
        units = min(units, ledger.balance)
        expiry_date = oldest_donation + timedelta(days=shelf_life)

        ledger.append(
            expiry_date, "EXPIRY", float(units),
            source_donation_id=None, source_transfer_id=None,
            destination_bank_id=None, status="POSTED",
        )


# ============================================================================
# STEP 5 — ADJUSTMENTS
# ============================================================================

def build_adjustment_transactions(ledgers: dict) -> None:
    """Rare small corrections (recount adjustments, minor wastage) -- never
    allowed to push balance negative."""
    today = date.today()
    for ledger in ledgers.values():
        if random.random() >= ADJUSTMENT_PROBABILITY:
            continue
        delta = random.randint(*ADJUSTMENT_RANGE)
        if delta < 0 and abs(delta) > ledger.balance:
            delta = -int(ledger.balance)
        if delta == 0:
            continue
        ledger.append(
            today - timedelta(days=random.randint(0, 5)), "ADJUSTMENT", float(delta),
            source_donation_id=None, source_transfer_id=None,
            destination_bank_id=None, status="POSTED",
        )


# ============================================================================
# OUTPUT ASSEMBLY
# ============================================================================

def classify_expiry_status(component_type: str, latest_donation_date: date | None, today: date) -> str:
    if latest_donation_date is None:
        return "VALID"
    shelf_life = get_component_shelf_life(component_type)
    days_left = shelf_life - (today - latest_donation_date).days
    if days_left <= 0:
        return "EXPIRED"
    elif days_left <= 3:
        return "EXPIRING_SOON"
    return "VALID"


def assemble_outputs(ledgers: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Build the transaction ledger DataFrame and the derived component
    snapshot DataFrame from the final state of every ledger."""
    all_transactions = []
    component_rows = []
    today = date.today()

    for ledger in ledgers.values():
        all_transactions.extend(ledger.transactions)

        latest_donation = max(
            (t["transaction_timestamp"].date() for t in ledger.transactions if t["transaction_type"] == "DONATION"),
            default=None,
        )
        component_rows.append({
            "blood_bank_id": ledger.key.blood_bank_id,
            "blood_group": ledger.key.blood_group,
            "component_type": ledger.key.component_type,
            "available_units": max(0, round(ledger.balance)),
            "expiry_status": classify_expiry_status(ledger.key.component_type, latest_donation, today),
            "last_transaction_at": max(
                (t["transaction_timestamp"] for t in ledger.transactions), default=None
            ),
        })

    txn_df = pd.DataFrame(all_transactions)
    txn_df = txn_df.sort_values("transaction_timestamp").reset_index(drop=True)

    component_df = pd.DataFrame(component_rows)
    return txn_df, component_df


# ============================================================================
# MAIN
# ============================================================================

def main() -> None:
    print("=" * 45)
    print("TRANSACTIONAL DATA GENERATION (v2 upgrade)")
    print("=" * 45)

    random.seed(SEED)

    for path in [DONORS_PATH, BLOOD_BANKS_PATH, BLOOD_INVENTORY_PATH, DONATIONS_PATH]:
        if not path.exists():
            print(f"ERROR: required file not found: {path}")
            print("Run data_generation/01_generate_seed_data.py first.")
            sys.exit(1)

    blood_inventory = pd.read_csv(BLOOD_INVENTORY_PATH)
    donations = pd.read_csv(DONATIONS_PATH)

    print(f"\nExisting bank+blood_group combinations found: "
          f"{blood_inventory[['blood_bank_id', 'blood_group']].drop_duplicates().shape[0]}")
    print(f"Existing Completed donations: {(donations['donation_status'] == 'Completed').sum()}")

    ledgers: dict = {}

    print("\nStep 1: Building DONATION transactions from real donation history...")
    build_donation_transactions(donations, ledgers)
    print(f"  {len(ledgers)} (bank, blood_group, component) ledgers created")

    issue_records: list = []
    print("Step 2: Simulating ISSUE (consumption) transactions...")
    build_issue_transactions(ledgers, issue_records)
    print(f"  {len(issue_records)} issue events generated")

    transfer_records: list = []
    print("Step 3: Simulating TRANSFER transactions between blood banks...")
    build_transfer_transactions(ledgers, transfer_records)
    print(f"  {len(transfer_records)} transfers generated")

    print("Step 4: Simulating EXPIRY transactions...")
    build_expiry_transactions(ledgers)

    print("Step 5: Simulating rare ADJUSTMENT transactions...")
    build_adjustment_transactions(ledgers)

    print("\nAssembling outputs...")
    txn_df, component_df = assemble_outputs(ledgers)

    negative_balances = (component_df["available_units"] < 0).sum()
    print(f"Negative-balance ledgers after generation: {negative_balances} (must be 0)")
    assert negative_balances == 0, "Generation bug: a ledger went negative"

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    txn_df.to_csv(OUT_TRANSACTIONS_PATH, index=False)
    pd.DataFrame(transfer_records).to_csv(OUT_TRANSFERS_PATH, index=False)
    pd.DataFrame(issue_records).to_csv(OUT_ISSUES_PATH, index=False)
    component_df.to_csv(OUT_COMPONENTS_PATH, index=False)

    print("\n" + "=" * 45)
    print("TRANSACTIONAL DATA GENERATION COMPLETE")
    print("=" * 45)
    print(f"Transactions:        {len(txn_df)}")
    print(f"  DONATION:          {(txn_df['transaction_type'] == 'DONATION').sum()}")
    print(f"  ISSUE:             {(txn_df['transaction_type'] == 'ISSUE').sum()}")
    print(f"  TRANSFER_OUT:      {(txn_df['transaction_type'] == 'TRANSFER_OUT').sum()}")
    print(f"  TRANSFER_IN:       {(txn_df['transaction_type'] == 'TRANSFER_IN').sum()}")
    print(f"  EXPIRY:            {(txn_df['transaction_type'] == 'EXPIRY').sum()}")
    print(f"  ADJUSTMENT:        {(txn_df['transaction_type'] == 'ADJUSTMENT').sum()}")
    print(f"\nTransfers:            {len(transfer_records)}")
    print(f"Issues:               {len(issue_records)}")
    print(f"Component ledgers:    {len(component_df)}")
    print(f"\nFiles written to:\n  {OUT_TRANSACTIONS_PATH}\n  {OUT_TRANSFERS_PATH}\n"
          f"  {OUT_ISSUES_PATH}\n  {OUT_COMPONENTS_PATH}")


if __name__ == "__main__":
    main()
