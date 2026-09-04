"""
Enterprise Rare Blood Type Availability Analytics
Phase 2 — Data Modeling & Synthetic Data Generation
=====================================================

This script generates five relational synthetic CSV datasets that will later
be processed through a PySpark Medallion (Bronze -> Silver -> Gold) pipeline.
No PySpark, Bronze/Silver/Gold, streaming, or dashboard code is created here.

DATA MODEL / RELATIONSHIP DIAGRAM
----------------------------------

    DONORS (PK: donor_id)
        |
        | 1:N
        v
    DONATIONS (PK: donation_id, FK: donor_id, FK: blood_bank_id, FK: camp_id)
        ^                                   ^
        | N:1                               | N:1
        |                                   |
    DONATION CAMPS (PK: camp_id)     BLOOD BANKS (PK: blood_bank_id)
                                              |
                                              | 1:N
                                              v
                                      BLOOD INVENTORY (PK: inventory_id, FK: blood_bank_id)

    Business rule:  donations.blood_group  ==  donors.blood_group  (always)

RARE BLOOD GROUP STRATEGY
--------------------------
For this project, O-, AB-, B-, A- are treated as "rare" blood groups for
demonstration purposes only (NOT a medical guideline). Donor and donation
blood-group generation is weighted so common groups (O+, A+, B+) appear more
often than rare groups, while still guaranteeing enough rare-group records to
support meaningful analytics. Blood-bank inventory further uses a *separate*
population-abundance tiering (common / less_common / rare) purely to decide
how many units a bank stocks -- this is independent of the four business
"rare blood groups" tracked for the dashboard.

INVENTORY STATUS STRATEGY
--------------------------
inventory_status is derived from available_units using thresholds defined
ONCE in the configuration section (CRITICAL_THRESHOLD / LOW_THRESHOLD /
NORMAL_THRESHOLD), never hard-coded elsewhere in the script.

DATA QUALITY STRATEGY
-----------------------
The dataset is kept realistic and mostly clean. A small, controlled number of
duplicate donation rows are intentionally introduced so the future Silver
layer has something meaningful to deduplicate. No broken foreign keys,
invalid blood groups, negative inventory, or invalid IDs are ever introduced.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
from faker import Faker

# ============================================================================
# CONFIGURATION  (single source of truth for all tunable parameters)
# ============================================================================

SEED = 42

# Dataset sizes
NUM_DONORS = 200
NUM_BLOOD_BANKS = 20
NUM_CAMPS = 15
NUM_DONATIONS = 2000

# Donor age range
AGE_MIN, AGE_MAX = 18, 60

# All accepted blood groups
BLOOD_GROUPS = ["O+", "A+", "B+", "AB+", "A-", "B-", "AB-", "O-"]

# The four blood groups this project treats as "rare" for business analytics.
# Demo/project classification only -- NOT a medical guideline.
RARE_BLOOD_GROUPS = ["O-", "AB-", "B-", "A-"]

# Weighted distribution used for DONOR (and therefore DONATION) blood groups.
# Common groups are more frequent; rare groups are less frequent but still
# well represented (weights sum to 100).
BLOOD_GROUP_WEIGHTS = {
    "O+": 30, "A+": 24, "B+": 15, "AB+": 6,
    "A-": 8, "B-": 7, "AB-": 4, "O-": 6,
}

# City -> (State, base_latitude, base_longitude). All city/state pairs are
# fixed and valid -- city and state are never combined randomly.
CITY_STATE_MAP = {
    "Mumbai": ("Maharashtra", 19.0760, 72.8777),
    "Pune": ("Maharashtra", 18.5204, 73.8567),
    "Nashik": ("Maharashtra", 19.9975, 73.7898),
    "Nagpur": ("Maharashtra", 21.1458, 79.0882),
    "Delhi": ("Delhi", 28.7041, 77.1025),
    "Bengaluru": ("Karnataka", 12.9716, 77.5946),
    "Hyderabad": ("Telangana", 17.3850, 78.4867),
    "Chennai": ("Tamil Nadu", 13.0827, 80.2707),
    "Kolkata": ("West Bengal", 22.5726, 88.3639),
    "Ahmedabad": ("Gujarat", 23.0225, 72.5714),
}
CITIES = list(CITY_STATE_MAP.keys())

# Inventory status thresholds -- the ONLY place these numbers are defined.
CRITICAL_THRESHOLD = 0   # units == 0             -> CRITICAL
LOW_THRESHOLD = 5        # 1  <= units <= 5        -> LOW
NORMAL_THRESHOLD = 20    # 6  <= units <= 20       -> NORMAL
                          # units > 20              -> GOOD

# Population-abundance tiers used ONLY to decide how many units a blood bank
# stocks for a given group (separate from the RARE_BLOOD_GROUPS business set).
INVENTORY_TIERS = {
    "O+": "common", "A+": "common", "B+": "common",
    "A-": "less_common", "B-": "less_common", "AB+": "less_common",
    "AB-": "rare", "O-": "rare",
}
TIER_INCLUSION_PROB = {"common": 0.85, "less_common": 0.55, "rare": 0.35}
TIER_UNIT_RANGE = {"common": (10, 150), "less_common": (3, 60), "rare": (0, 25)}

# Reference "today" and historical date ranges for the synthetic timeline
PROJECT_TODAY = date(2026, 8, 8)
REGISTRATION_START = date(2021, 1, 1)
REGISTRATION_END = date(2026, 6, 1)
CAMP_DATE_START = date(2024, 1, 1)
CAMP_DATE_END = date(2026, 7, 1)

# Weighted categorical fields
ELIGIBILITY_WEIGHTS = {"Eligible": 75, "Temporarily Ineligible": 15, "Inactive": 10}
OPERATING_STATUS_WEIGHTS = {"Active": 90, "Temporarily Closed": 10}
DONATION_STATUS_WEIGHTS = {"Completed": 85, "Cancelled": 10, "Rejected": 5}
GENDER_WEIGHTS = {"Male": 48, "Female": 50, "Other": 2}

# Name templates for synthetic blood banks and donation camps
BLOOD_BANK_NAME_TEMPLATES = [
    "{city} CityCare Blood Bank", "LifeLine Blood Center - {city}",
    "Red Hope Blood Bank {city}", "Metro Blood Center {city}",
    "{city} Community Blood Bank", "Sanjeevani Blood Bank {city}",
    "United Blood Services {city}", "{city} Regional Blood Center",
    "HopeLine Blood Bank {city}", "Amrit Blood Bank {city}",
]
CAMP_NAME_TEMPLATES = [
    "{org} Blood Donation Drive", "{org} Lifesaver Camp",
    "{org} Community Blood Camp", "{org} Donate & Save Camp",
    "{org} Annual Blood Donation Camp",
]
CAMP_ORGANIZERS = [
    "Red Cross Society", "Lions Club", "Rotary Club", "City Hospital Trust",
    "Youth Welfare Association", "Corporate CSR Wing", "College Alumni Association",
    "Local Community Center", "Rotary International", "Jaycees Charitable Trust",
]

# Data-quality knobs (kept small and controlled, per project requirements)
DUPLICATE_DONATION_RATE = 0.01     # ~1% of donations duplicated
MISSING_CAMP_LOCATION_RATE = 0.05  # ~5% of camps missing a location description
NO_PRIOR_DONATION_RATE = 0.30      # ~30% of donors have never donated before

# Paths
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"

# Reproducibility
random.seed(SEED)
fake = Faker("en_IN")
Faker.seed(SEED)


# ============================================================================
# HELPERS
# ============================================================================

def weighted_choice(weights: dict) -> str:
    """Pick a single key from a dict of {value: weight} using random.choices."""
    keys = list(weights.keys())
    values = list(weights.values())
    return random.choices(keys, weights=values, k=1)[0]


def random_date(start: date, end: date) -> date:
    """Return a random date between start and end (inclusive)."""
    if end < start:
        start, end = end, start
    delta_days = (end - start).days
    return start + timedelta(days=random.randint(0, delta_days))


def classify_inventory_status(units: int) -> str:
    """Classify available_units into CRITICAL / LOW / NORMAL / GOOD using the
    thresholds defined once in the configuration section."""
    if units <= CRITICAL_THRESHOLD:
        return "CRITICAL"
    elif units <= LOW_THRESHOLD:
        return "LOW"
    elif units <= NORMAL_THRESHOLD:
        return "NORMAL"
    return "GOOD"


# ============================================================================
# DATASET GENERATORS
# ============================================================================

def generate_donors(n: int = NUM_DONORS) -> pd.DataFrame:
    """Generate synthetic donor records with a weighted blood-group mix and
    consistent city/state pairs."""
    groups = list(BLOOD_GROUP_WEIGHTS.keys())
    weights = list(BLOOD_GROUP_WEIGHTS.values())
    records = []

    for i in range(1, n + 1):
        donor_id = f"DON{i:04d}"
        gender = weighted_choice(GENDER_WEIGHTS)
        if gender == "Male":
            full_name = fake.name_male()
        elif gender == "Female":
            full_name = fake.name_female()
        else:
            full_name = fake.name()

        age = random.randint(AGE_MIN, AGE_MAX)
        blood_group = random.choices(groups, weights=weights, k=1)[0]
        city = random.choice(CITIES)
        state, _, _ = CITY_STATE_MAP[city]
        registration_date = random_date(REGISTRATION_START, REGISTRATION_END)
        eligibility_status = weighted_choice(ELIGIBILITY_WEIGHTS)

        # Some donors have never donated -- handled as a null value
        if random.random() < NO_PRIOR_DONATION_RATE:
            last_donation_date = None
        else:
            last_donation_date = random_date(registration_date, PROJECT_TODAY)

        records.append({
            "donor_id": donor_id,
            "full_name": full_name,
            "gender": gender,
            "age": age,
            "blood_group": blood_group,
            "city": city,
            "state": state,
            "registration_date": registration_date,
            "last_donation_date": last_donation_date,
            "eligibility_status": eligibility_status,
        })

    return pd.DataFrame(records)


def generate_blood_banks(n: int = NUM_BLOOD_BANKS) -> pd.DataFrame:
    """Generate synthetic blood bank records with city-consistent coordinates."""
    records = []
    for i in range(1, n + 1):
        blood_bank_id = f"BB{i:03d}"
        city = random.choice(CITIES)
        state, base_lat, base_lon = CITY_STATE_MAP[city]
        blood_bank_name = random.choice(BLOOD_BANK_NAME_TEMPLATES).format(city=city)
        address = f"{fake.street_address()}, {city}, {state} - {random.randint(100000, 999999)}"
        # Jitter coordinates slightly so they stay near the city center
        latitude = round(base_lat + random.uniform(-0.05, 0.05), 6)
        longitude = round(base_lon + random.uniform(-0.05, 0.05), 6)
        storage_capacity = random.randint(50, 500)
        operating_status = weighted_choice(OPERATING_STATUS_WEIGHTS)

        records.append({
            "blood_bank_id": blood_bank_id,
            "blood_bank_name": blood_bank_name,
            "city": city,
            "state": state,
            "address": address,
            "latitude": latitude,
            "longitude": longitude,
            "storage_capacity": storage_capacity,
            "operating_status": operating_status,
        })

    return pd.DataFrame(records)


def _enforce_inventory_business_rules(bank_group_map: dict, blood_banks_df: pd.DataFrame) -> dict:
    """Guarantee the inventory business rules that matter for the rare-blood
    availability use case, regardless of how the random draws landed:

    - Rule 7: at least one blood bank carries 2+ rare blood groups.
    - Rule 8: at least one city has 2+ blood banks carrying the SAME rare group.
    - Rule 9: at least one rare blood group is available in multiple cities.
    """
    bank_ids = blood_banks_df["blood_bank_id"].tolist()
    bank_city = dict(zip(blood_banks_df["blood_bank_id"], blood_banks_df["city"]))

    # Rule 7
    multi_rare_bank = bank_ids[0]
    bank_group_map[multi_rare_bank].update({"O-", "AB-"})

    # Rule 8
    city_to_banks: dict = {}
    for bid in bank_ids:
        city_to_banks.setdefault(bank_city[bid], []).append(bid)
    shared_city = next((c for c, banks in city_to_banks.items() if len(banks) >= 2), None)
    if shared_city:
        b1, b2 = city_to_banks[shared_city][:2]
        bank_group_map[b1].add("B-")
        bank_group_map[b2].add("B-")

    # Rule 9
    distinct_cities = list(dict.fromkeys(bank_city[bid] for bid in bank_ids))
    for city in distinct_cities[:3]:
        candidate_bank = city_to_banks[city][0]
        bank_group_map[candidate_bank].add("O-")

    return bank_group_map


def _apply_forced_inventory_scenarios(df: pd.DataFrame) -> pd.DataFrame:
    """Guarantee at least one CRITICAL and one LOW rare-blood inventory
    record exist, so shortage analytics always have something to show."""
    rare_idx = df[df["blood_group"].isin(RARE_BLOOD_GROUPS)].index.tolist()

    if len(rare_idx) >= 1:
        idx = rare_idx[0]
        df.loc[idx, "available_units"] = 0
        df.loc[idx, "reserved_units"] = 0

    if len(rare_idx) >= 2:
        idx = rare_idx[1]
        forced_units = random.randint(1, LOW_THRESHOLD)
        df.loc[idx, "available_units"] = forced_units
        df.loc[idx, "reserved_units"] = min(int(df.loc[idx, "reserved_units"]), forced_units)

    df["inventory_status"] = df["available_units"].apply(classify_inventory_status)
    return df


def generate_blood_inventory(blood_banks_df: pd.DataFrame) -> pd.DataFrame:
    """Generate inventory records. NOT every blood bank carries every blood
    group -- coverage is randomized by population-abundance tier, then key
    business rules are enforced so the dataset can answer questions like
    "which blood banks currently have O-?" in a meaningful way.

    NOTE: this is a point-in-time snapshot of inventory. Donations are NOT
    used to update this file in this phase -- that logic belongs to a later
    (Silver/Gold) phase. Filtering out Temporarily Closed blood banks from
    "currently available" views (business rule 4) is also deferred to the
    Gold/dashboard layer, not handled in raw data generation.
    """
    bank_group_map: dict = {}
    for _, bank in blood_banks_df.iterrows():
        groups_carried = [
            group for group in BLOOD_GROUPS
            if random.random() < TIER_INCLUSION_PROB[INVENTORY_TIERS[group]]
        ]
        if len(groups_carried) < 2:
            groups_carried = random.sample(BLOOD_GROUPS, 2)
        bank_group_map[bank["blood_bank_id"]] = set(groups_carried)

    bank_group_map = _enforce_inventory_business_rules(bank_group_map, blood_banks_df)

    records = []
    inventory_counter = 1
    for _, bank in blood_banks_df.iterrows():
        for group in sorted(bank_group_map[bank["blood_bank_id"]]):
            tier = INVENTORY_TIERS[group]
            low, high = TIER_UNIT_RANGE[tier]
            available_units = random.randint(low, high)
            reserved_units = random.randint(0, max(0, int(available_units * 0.3)))
            last_updated = datetime.combine(PROJECT_TODAY, datetime.min.time()) - timedelta(
                days=random.randint(0, 45), hours=random.randint(0, 23), minutes=random.randint(0, 59)
            )

            records.append({
                "inventory_id": f"INV{inventory_counter:04d}",
                "blood_bank_id": bank["blood_bank_id"],
                "blood_group": group,
                "available_units": available_units,
                "reserved_units": reserved_units,
                "last_updated": last_updated,
                "inventory_status": classify_inventory_status(available_units),
            })
            inventory_counter += 1

    df = pd.DataFrame(records)
    df = _apply_forced_inventory_scenarios(df)
    return df


def generate_donation_camps(n: int = NUM_CAMPS) -> pd.DataFrame:
    """Generate synthetic donation camp records. Camps use the same city list
    as blood banks, so some camps naturally overlap blood-bank cities."""
    records = []
    for i in range(1, n + 1):
        camp_id = f"CAMP{i:03d}"
        organizer = random.choice(CAMP_ORGANIZERS)
        camp_name = random.choice(CAMP_NAME_TEMPLATES).format(org=organizer.split()[0])
        city = random.choice(CITIES)
        state, _, _ = CITY_STATE_MAP[city]
        camp_date = random_date(CAMP_DATE_START, CAMP_DATE_END)
        location = f"{fake.street_name()} Grounds, {city}"

        # Small controlled data-quality issue: a few camps missing location text
        if random.random() < MISSING_CAMP_LOCATION_RATE:
            location = None

        expected_donors = random.randint(20, 150)

        records.append({
            "camp_id": camp_id,
            "camp_name": camp_name,
            "organizer": organizer,
            "city": city,
            "state": state,
            "camp_date": camp_date,
            "location": location,
            "expected_donors": expected_donors,
        })

    return pd.DataFrame(records)


def generate_donations(
    donors_df: pd.DataFrame,
    blood_banks_df: pd.DataFrame,
    camps_df: pd.DataFrame,
    n: int = NUM_DONATIONS,
) -> pd.DataFrame:
    """Generate donation records. Each donation's blood_group is always taken
    directly from the donor's blood_group -- never assigned independently."""
    donor_lookup = donors_df.set_index("donor_id")
    donor_ids = donors_df["donor_id"].tolist()
    bank_ids = blood_banks_df["blood_bank_id"].tolist()
    camp_ids = camps_df["camp_id"].tolist()

    # Eligible donors are more likely to have donated
    eligibility_weight_map = {"Eligible": 3, "Temporarily Ineligible": 1, "Inactive": 1}
    donor_weights = [
        eligibility_weight_map[donor_lookup.loc[d, "eligibility_status"]] for d in donor_ids
    ]

    records = []
    for i in range(1, n + 1):
        donation_id = f"DONATION{i:04d}"
        donor_id = random.choices(donor_ids, weights=donor_weights, k=1)[0]
        donor = donor_lookup.loc[donor_id]

        blood_group = donor["blood_group"]  # MUST match donor's blood group
        blood_bank_id = random.choice(bank_ids)
        camp_id = random.choice(camp_ids) if random.random() < 0.65 else None

        registration_date = donor["registration_date"]
        donation_date = random_date(registration_date, PROJECT_TODAY)

        units_donated = random.choices([1, 2, 3], weights=[70, 25, 5], k=1)[0]
        donation_status = weighted_choice(DONATION_STATUS_WEIGHTS)

        records.append({
            "donation_id": donation_id,
            "donor_id": donor_id,
            "blood_bank_id": blood_bank_id,
            "camp_id": camp_id,
            "donation_date": donation_date,
            "blood_group": blood_group,
            "units_donated": units_donated,
            "donation_status": donation_status,
        })

    return pd.DataFrame(records)


def apply_data_quality_issues(donations_df: pd.DataFrame) -> pd.DataFrame:
    """Intentionally introduce a small, controlled number of duplicate
    donation rows so the future Silver layer has something realistic to
    deduplicate. Foreign keys, blood groups, and IDs are never made invalid."""
    num_duplicates = max(1, int(len(donations_df) * DUPLICATE_DONATION_RATE))
    duplicate_rows = donations_df.sample(n=num_duplicates, random_state=SEED)
    return pd.concat([donations_df, duplicate_rows], ignore_index=True)


# ============================================================================
# SAVE / VALIDATE / PREVIEW
# ============================================================================

def save_datasets(
    donors: pd.DataFrame,
    blood_banks: pd.DataFrame,
    inventory: pd.DataFrame,
    camps: pd.DataFrame,
    donations: pd.DataFrame,
) -> None:
    """Write all five datasets to data/raw/, creating the directory if needed."""
    try:
        RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
        donors.to_csv(RAW_DATA_DIR / "donors.csv", index=False)
        blood_banks.to_csv(RAW_DATA_DIR / "blood_banks.csv", index=False)
        inventory.to_csv(RAW_DATA_DIR / "blood_inventory.csv", index=False)
        camps.to_csv(RAW_DATA_DIR / "donation_camps.csv", index=False)
        donations.to_csv(RAW_DATA_DIR / "donations.csv", index=False)
        print(f"\nCSV files saved to: {RAW_DATA_DIR}")
    except OSError as exc:
        raise RuntimeError(f"Failed to write CSV files to {RAW_DATA_DIR}: {exc}") from exc


def validate_data(
    donors: pd.DataFrame,
    blood_banks: pd.DataFrame,
    inventory: pd.DataFrame,
    camps: pd.DataFrame,
    donations: pd.DataFrame,
) -> None:
    """Print row counts and rare-blood-focused validation statistics."""
    print("\n" + "=" * 60)
    print("DATA GENERATION COMPLETE")
    print("=" * 60)
    duplicate_count = len(donations) - donations["donation_id"].nunique()
    print(f"Donors:       {len(donors)}")
    print(f"Blood Banks:  {len(blood_banks)}")
    print(f"Camps:        {len(camps)}")
    print(f"Donations:    {len(donations)}  (includes {duplicate_count} intentional duplicate rows)")
    print(f"Inventory:    {len(inventory)} records")

    print("\nBlood Group Distribution — Donors")
    print(donors["blood_group"].value_counts().reindex(BLOOD_GROUPS).fillna(0).astype(int).to_string())

    print("\nBlood Group Distribution — Donations")
    print(donations["blood_group"].value_counts().reindex(BLOOD_GROUPS).fillna(0).astype(int).to_string())

    print("\nRare Blood Availability (from blood_inventory.csv)")
    for group in RARE_BLOOD_GROUPS:
        group_inv = inventory[inventory["blood_group"] == group]
        total_units = int(group_inv["available_units"].sum())
        centers = group_inv[group_inv["available_units"] > 0]["blood_bank_id"].nunique()
        critical_count = int((group_inv["inventory_status"] == "CRITICAL").sum())
        low_count = int((group_inv["inventory_status"] == "LOW").sum())
        print(f"\n{group}:")
        print(f"    Total Units:        {total_units}")
        print(f"    Centers w/ Stock:   {centers}")
        print(f"    Critical Records:   {critical_count}")
        print(f"    Low Records:        {low_count}")

    rare_inventory = inventory[inventory["blood_group"].isin(RARE_BLOOD_GROUPS)]
    total_critical = int((rare_inventory["inventory_status"] == "CRITICAL").sum())
    total_low = int((rare_inventory["inventory_status"] == "LOW").sum())
    print(f"\nTotal CRITICAL rare-blood inventory records: {total_critical}")
    print(f"Total LOW rare-blood inventory records:      {total_low}")
    print("=" * 60)


def preview_data(
    donors: pd.DataFrame,
    blood_banks: pd.DataFrame,
    inventory: pd.DataFrame,
    camps: pd.DataFrame,
    donations: pd.DataFrame,
) -> None:
    """Print a small preview of each dataset (never the full 2,000 donations)."""
    def show(title: str, df: pd.DataFrame, rows: int) -> None:
        print(f"\n--- {title} (first {rows} records) ---")
        print(df.head(rows).to_string(index=False))

    show("DONORS", donors, 5)
    show("BLOOD BANKS", blood_banks, 5)
    show("BLOOD INVENTORY", inventory, 10)
    show("DONATION CAMPS", camps, 5)
    show("DONATIONS", donations, 10)


# ============================================================================
# MAIN
# ============================================================================

def main() -> None:
    print("Generating synthetic data for Enterprise Rare Blood Type Availability Analytics...")
    print(f"(seed={SEED}, donors={NUM_DONORS}, blood_banks={NUM_BLOOD_BANKS}, "
          f"camps={NUM_CAMPS}, donations={NUM_DONATIONS})")

    donors = generate_donors()
    blood_banks = generate_blood_banks()
    inventory = generate_blood_inventory(blood_banks)
    camps = generate_donation_camps()
    donations = generate_donations(donors, blood_banks, camps)
    donations = apply_data_quality_issues(donations)

    save_datasets(donors, blood_banks, inventory, camps, donations)
    validate_data(donors, blood_banks, inventory, camps, donations)
    preview_data(donors, blood_banks, inventory, camps, donations)


if __name__ == "__main__":
    main()
