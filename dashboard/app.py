"""
Enterprise Rare Blood Type Availability Analytics
Phase 9 — Streamlit Dashboard
=====================================================

Run with:
    streamlit run dashboard/app.py

This dashboard reads the Gold Parquet datasets produced by:
    pipeline/08_gold_aggregate.py   (main Gold layer used throughout)

and, optionally, the Bronze streaming donations produced by:
    pipeline/04_bronze_streaming_ingest.py   (for the Live Feed tab)

CENTRAL STORY
---------------
This dashboard is not a generic "blood donation dashboard". Every section
exists in service of one workflow:

    RARE BLOOD TYPE -> IS IT AVAILABLE? -> HOW MANY UNITS? ->
    WHICH BLOOD BANK HAS IT? -> WHICH CITY? -> LOW OR CRITICAL? ->
    ARE THERE RARE-BLOOD DONORS?

DISCLAIMER: this is a synthetic-data analytics demonstration, not a real
medical decision-making system. "Rare" blood groups are a project-defined
classification (see RARE_BLOOD_GROUPS), not a universal medical guideline.
"""

# ============================================================================
# 1. IMPORTS
# ============================================================================

from pathlib import Path
from datetime import datetime

import pandas as pd
import plotly.express as px
import streamlit as st

# ============================================================================
# 2. CONFIGURATION (single source of truth for paths, groups, colors)
# ============================================================================

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
GOLD_DIR = PROJECT_ROOT / "data" / "gold"
BRONZE_STREAMING_PATH = PROJECT_ROOT / "data" / "bronze" / "streaming_donations"

GOLD_PATHS = {
    "daily_donation_summary": GOLD_DIR / "daily_donation_summary",
    "monthly_donation_summary": GOLD_DIR / "monthly_donation_summary",
    "blood_group_distribution": GOLD_DIR / "blood_group_distribution",
    "rare_blood_availability": GOLD_DIR / "rare_blood_availability",
    "rare_blood_summary": GOLD_DIR / "rare_blood_summary",
    "blood_shortage_report": GOLD_DIR / "blood_shortage_report",
    "blood_bank_rare_stock": GOLD_DIR / "blood_bank_rare_stock",
    "city_blood_availability": GOLD_DIR / "city_blood_availability",
    "blood_bank_performance": GOLD_DIR / "blood_bank_performance",
    "donation_camp_performance": GOLD_DIR / "donation_camp_performance",
    "donor_summary": GOLD_DIR / "donor_summary",
    "rare_donor_summary": GOLD_DIR / "rare_donor_summary",
    # Transaction-driven inventory model (upgrade) -- see
    # pipeline/09_inventory_transaction_engine.py
    "gold_inventory_state": GOLD_DIR / "gold_inventory_state",
    "gold_demand_analytics": GOLD_DIR / "gold_demand_analytics",
    "gold_transfer_history": GOLD_DIR / "gold_transfer_history",
}

GOLD_PIPELINE_SCRIPT = "pipeline/08_gold_aggregate.py"
INVENTORY_ENGINE_SCRIPT = "pipeline/09_inventory_transaction_engine.py"

PRIORITY_STYLE = {
    "HEALTHY": ("🟢", "#2E7D32"),
    "MONITOR": ("🔵", "#1565C0"),
    "LOW": ("🟡", "#F9A825"),
    "CRITICAL": ("🟠", "#E65100"),
    "EMERGENCY": ("🔴", "#B71C1C"),
}

VALID_BLOOD_GROUPS = ["A+", "A-", "B+", "B-", "AB+", "AB-", "O+", "O-"]
RARE_BLOOD_GROUPS = ["A-", "B-", "AB-", "O-"]

AVAILABILITY_STATUS_OPTIONS = ["All", "AVAILABLE", "LOW", "CRITICAL", "OUT_OF_STOCK"]

# Consistent status -> (emoji, color) mapping used across every table/chart
STATUS_STYLE = {
    "AVAILABLE": ("🟢", "#2E7D32"),
    "LOW": ("🟡", "#F9A825"),
    "CRITICAL": ("🟠", "#E65100"),
    "OUT_OF_STOCK": ("🔴", "#B71C1C"),
}
DEFAULT_STATUS_STYLE = ("⚪", "#9E9E9E")


# ============================================================================
# 3. PAGE CONFIGURATION
# ============================================================================

st.set_page_config(
    page_title="Rare Blood Type Availability Analytics",
    page_icon="🩸",
    layout="wide",
)


# ============================================================================
# 4. DATA-LOADING FUNCTIONS
# ============================================================================

@st.cache_data
def load_parquet(path_str: str) -> pd.DataFrame | None:
    """Load one Gold/Bronze Parquet dataset as a Pandas DataFrame.
    Returns None (never raises) if the dataset doesn't exist or fails to
    read, so callers can handle the missing-dataset case gracefully."""
    path = Path(path_str)
    if not path.exists():
        return None
    try:
        return pd.read_parquet(path)
    except Exception:
        return None


def load_daily_donation_summary() -> pd.DataFrame | None:
    return load_parquet(str(GOLD_PATHS["daily_donation_summary"]))


def load_monthly_donation_summary() -> pd.DataFrame | None:
    return load_parquet(str(GOLD_PATHS["monthly_donation_summary"]))


def load_blood_group_distribution() -> pd.DataFrame | None:
    return load_parquet(str(GOLD_PATHS["blood_group_distribution"]))


def load_rare_blood_availability() -> pd.DataFrame | None:
    return load_parquet(str(GOLD_PATHS["rare_blood_availability"]))


def load_rare_blood_summary() -> pd.DataFrame | None:
    return load_parquet(str(GOLD_PATHS["rare_blood_summary"]))


def load_shortage_report() -> pd.DataFrame | None:
    return load_parquet(str(GOLD_PATHS["blood_shortage_report"]))


def load_blood_bank_rare_stock() -> pd.DataFrame | None:
    return load_parquet(str(GOLD_PATHS["blood_bank_rare_stock"]))


def load_city_availability() -> pd.DataFrame | None:
    return load_parquet(str(GOLD_PATHS["city_blood_availability"]))


def load_blood_bank_performance() -> pd.DataFrame | None:
    return load_parquet(str(GOLD_PATHS["blood_bank_performance"]))


def load_donation_camp_performance() -> pd.DataFrame | None:
    return load_parquet(str(GOLD_PATHS["donation_camp_performance"]))


def load_donor_summary() -> pd.DataFrame | None:
    return load_parquet(str(GOLD_PATHS["donor_summary"]))


def load_rare_donor_summary() -> pd.DataFrame | None:
    return load_parquet(str(GOLD_PATHS["rare_donor_summary"]))


def load_streaming_donations() -> pd.DataFrame | None:
    return load_parquet(str(BRONZE_STREAMING_PATH))


def load_inventory_state() -> pd.DataFrame | None:
    return load_parquet(str(GOLD_PATHS["gold_inventory_state"]))


def load_demand_analytics() -> pd.DataFrame | None:
    return load_parquet(str(GOLD_PATHS["gold_demand_analytics"]))


def load_transfer_history() -> pd.DataFrame | None:
    return load_parquet(str(GOLD_PATHS["gold_transfer_history"]))


# ============================================================================
# 5. HELPER FUNCTIONS
# ============================================================================

def missing_dataset_warning(dataset_label: str) -> None:
    """Standard, non-crashing warning shown whenever a Gold dataset is
    missing (per Phase 9 spec section 25)."""
    st.warning(
        f"⚠️ **{dataset_label}** dataset not found.\n\n"
        f"Please run:\n\n```\npython {GOLD_PIPELINE_SCRIPT}\n```"
    )


def empty_filter_warning() -> None:
    st.info("No data available for the selected filters.")


def kpi_value_or_na(value) -> str:
    """Format a KPI value, or return 'N/A' if it couldn't be calculated."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "N/A"
    if isinstance(value, float):
        return f"{value:,.0f}"
    return f"{value:,}"


def status_emoji(status: str) -> str:
    emoji, _ = STATUS_STYLE.get(status, DEFAULT_STATUS_STYLE)
    return emoji


def add_status_display_column(df: pd.DataFrame, status_col: str = "availability_status") -> pd.DataFrame:
    """Add a human-friendly 'status_display' column combining an emoji with
    the status text, so CRITICAL/LOW/OUT_OF_STOCK/AVAILABLE are instantly
    distinguishable in any st.dataframe table without relying on styling
    APIs that vary across Streamlit versions."""
    if status_col not in df.columns:
        return df
    df = df.copy()
    df["status_display"] = df[status_col].apply(lambda s: f"{status_emoji(s)} {s}")
    return df


def filter_center_level_df(
    df: pd.DataFrame,
    blood_group: str,
    rare_only: bool,
    state: str,
    city: str,
    blood_bank: str,
    status: str,
) -> pd.DataFrame:
    """Apply the sidebar filters to a center-level dataset (one row per
    blood bank + blood group), shared by rare_blood_availability and
    blood_bank_rare_stock since they share the same grain/columns."""
    filtered = df.copy()

    if rare_only and "blood_group" in filtered.columns:
        filtered = filtered[filtered["blood_group"].isin(RARE_BLOOD_GROUPS)]

    if blood_group != "All" and "blood_group" in filtered.columns:
        filtered = filtered[filtered["blood_group"] == blood_group]

    if state != "All" and "state" in filtered.columns:
        filtered = filtered[filtered["state"] == state]

    if city != "All" and "city" in filtered.columns:
        filtered = filtered[filtered["city"] == city]

    if blood_bank != "All" and "blood_bank_name" in filtered.columns:
        filtered = filtered[filtered["blood_bank_name"] == blood_bank]

    if status != "All" and "availability_status" in filtered.columns:
        filtered = filtered[filtered["availability_status"] == status]

    return filtered


def generate_shortage_alerts(rare_summary: pd.DataFrame) -> list[str]:
    """Dynamically build alert message strings from the Gold data -- never
    hard-coded blood groups or unit counts."""
    messages = []
    for _, row in rare_summary.iterrows():
        group = row["blood_group"]
        units = int(row["total_available_units"])
        status = row["availability_status"]

        if status == "OUT_OF_STOCK":
            messages.append(f"❌ **OUT OF STOCK**: {group} currently has no available units.")
        elif status == "CRITICAL":
            messages.append(f"🚨 **CRITICAL**: {group} has only {units} units available.")
        elif status == "LOW":
            messages.append(f"⚠️ **LOW**: {group} has {units} units available.")
    return messages


# ============================================================================
# 6. SIDEBAR FILTERS
# ============================================================================

def build_sidebar_filters(rare_stock_df: pd.DataFrame | None, daily_df: pd.DataFrame | None) -> dict:
    st.sidebar.title("🔎 Filters")

    if st.sidebar.button("🔄 Refresh Dashboard"):
        st.cache_data.clear()
        st.rerun()

    st.sidebar.markdown("---")

    rare_only = st.sidebar.checkbox("Show rare blood only", value=True)

    blood_group_options = ["All"] + (RARE_BLOOD_GROUPS if rare_only else VALID_BLOOD_GROUPS)
    blood_group = st.sidebar.selectbox("Blood Group", blood_group_options)

    if rare_stock_df is not None and not rare_stock_df.empty:
        state_options = ["All"] + sorted(rare_stock_df["state"].dropna().unique().tolist())
        city_options = ["All"] + sorted(rare_stock_df["city"].dropna().unique().tolist())
        bank_options = ["All"] + sorted(rare_stock_df["blood_bank_name"].dropna().unique().tolist())
    else:
        state_options, city_options, bank_options = ["All"], ["All"], ["All"]

    state = st.sidebar.selectbox("State", state_options)
    city = st.sidebar.selectbox("City", city_options)
    blood_bank = st.sidebar.selectbox("Blood Bank", bank_options)
    status = st.sidebar.selectbox("Availability Status", AVAILABILITY_STATUS_OPTIONS)

    date_range = None
    if daily_df is not None and not daily_df.empty:
        dates = pd.to_datetime(daily_df["donation_date"])
        min_date, max_date = dates.min().date(), dates.max().date()
        date_range = st.sidebar.date_input(
            "Date Range", value=(min_date, max_date), min_value=min_date, max_value=max_date
        )

    st.sidebar.markdown("---")
    st.sidebar.caption(
        "This is a synthetic-data analytics demo. 'Rare' blood groups are a "
        "project-defined classification, not a medical guideline."
    )

    return {
        "rare_only": rare_only,
        "blood_group": blood_group,
        "state": state,
        "city": city,
        "blood_bank": blood_bank,
        "status": status,
        "date_range": date_range,
    }


# ============================================================================
# 7. KPI CALCULATIONS
# ============================================================================

def compute_kpis(
    rare_summary: pd.DataFrame | None,
    rare_stock: pd.DataFrame | None,
    rare_donors: pd.DataFrame | None,
    daily_summary: pd.DataFrame | None,
    blood_group_dist: pd.DataFrame | None,
) -> dict:
    kpis = {
        "rare_units": None, "rare_banks": None, "critical_groups": None,
        "out_of_stock_groups": None, "rare_donors": None,
        "total_donations": None, "total_units": None, "today_donations": None,
    }

    if rare_summary is not None and not rare_summary.empty:
        kpis["rare_units"] = int(rare_summary["total_available_units"].sum())
        kpis["critical_groups"] = int((rare_summary["availability_status"] == "CRITICAL").sum())
        kpis["out_of_stock_groups"] = int((rare_summary["availability_status"] == "OUT_OF_STOCK").sum())

    if rare_stock is not None and not rare_stock.empty:
        kpis["rare_banks"] = int(
            rare_stock.loc[rare_stock["available_units"] > 0, "blood_bank_id"].nunique()
        )

    if rare_donors is not None and not rare_donors.empty:
        kpis["rare_donors"] = int(rare_donors["total_donors"].sum())

    if blood_group_dist is not None and not blood_group_dist.empty:
        kpis["total_donations"] = int(blood_group_dist["total_donations"].sum())
        kpis["total_units"] = int(blood_group_dist["total_units_donated"].sum())

    if daily_summary is not None and not daily_summary.empty:
        today = datetime.now().date()
        daily_summary = daily_summary.copy()
        daily_summary["donation_date"] = pd.to_datetime(daily_summary["donation_date"]).dt.date
        today_row = daily_summary[daily_summary["donation_date"] == today]
        kpis["today_donations"] = int(today_row["total_donations"].sum()) if not today_row.empty else 0

    return kpis


def render_kpi_section(kpis: dict) -> None:
    st.markdown("### Key Metrics")

    row1 = st.columns(5)
    row1[0].metric("🩸 Rare Blood Units Available", kpi_value_or_na(kpis["rare_units"]))
    row1[1].metric("🏥 Blood Banks With Rare Blood", kpi_value_or_na(kpis["rare_banks"]))
    row1[2].metric("⚠️ Critical Blood Groups", kpi_value_or_na(kpis["critical_groups"]))
    row1[3].metric("🚨 Out-of-Stock Blood Groups", kpi_value_or_na(kpis["out_of_stock_groups"]))
    row1[4].metric("👥 Rare Blood Donors", kpi_value_or_na(kpis["rare_donors"]))

    row2 = st.columns(3)
    row2[0].metric("Total Donations", kpi_value_or_na(kpis["total_donations"]))
    row2[1].metric("Total Blood Units", kpi_value_or_na(kpis["total_units"]))
    row2[2].metric("Today's Donations", kpi_value_or_na(kpis["today_donations"]))


# ============================================================================
# 8. OVERVIEW TAB
# ============================================================================

def render_overview_tab(rare_summary: pd.DataFrame | None) -> None:
    st.header("Overview")

    if rare_summary is None:
        missing_dataset_warning("rare_blood_summary")
        return
    if rare_summary.empty:
        empty_filter_warning()
        return

    st.subheader("🩸 Rare Blood Availability Summary")
    display_df = add_status_display_column(rare_summary, "availability_status")
    st.dataframe(
        display_df[["blood_group", "total_available_units", "number_of_banks_with_stock",
                    "number_of_blood_banks", "status_display"]].rename(columns={
            "blood_group": "Blood Group", "total_available_units": "Total Available Units",
            "number_of_banks_with_stock": "Blood Banks With Stock",
            "number_of_blood_banks": "Total Blood Banks", "status_display": "Status",
        }),
        use_container_width=True, hide_index=True,
    )

    st.subheader("🚨 Blood Shortage Alerts")
    alerts = generate_shortage_alerts(rare_summary)
    if alerts:
        for alert in alerts:
            st.markdown(alert)
    else:
        st.success("✅ No rare blood shortages detected. All rare blood groups have healthy stock.")

    st.subheader("Rare Blood Units Currently Available")
    fig = px.bar(
        rare_summary.sort_values("blood_group"),
        x="blood_group", y="total_available_units",
        color="availability_status",
        color_discrete_map={k: v[1] for k, v in STATUS_STYLE.items()},
        labels={"blood_group": "Blood Group", "total_available_units": "Available Units",
                "availability_status": "Status"},
        title="Rare Blood Units Currently Available",
        hover_data={"number_of_banks_with_stock": True},
    )
    st.plotly_chart(fig, use_container_width=True)


# ============================================================================
# 9. RARE BLOOD AVAILABILITY TAB
# ============================================================================

def render_rare_blood_tab(rare_summary: pd.DataFrame | None, city_avail: pd.DataFrame | None,
                           filters: dict) -> None:
    st.header("🩸 Rare Blood Availability")

    if rare_summary is None:
        missing_dataset_warning("rare_blood_summary")
    elif rare_summary.empty:
        empty_filter_warning()
    else:
        display_df = add_status_display_column(rare_summary, "availability_status")
        st.dataframe(
            display_df[["blood_group", "total_available_units", "number_of_banks_with_stock",
                        "number_of_blood_banks", "average_units_per_bank", "status_display"]].rename(columns={
                "blood_group": "Blood Group", "total_available_units": "Total Available Units",
                "number_of_banks_with_stock": "Blood Banks With Stock",
                "number_of_blood_banks": "Total Blood Banks",
                "average_units_per_bank": "Avg Units / Bank", "status_display": "Status",
            }),
            use_container_width=True, hide_index=True,
        )

    st.markdown("---")
    st.subheader("📍 City-wise Rare Blood Availability")

    if city_avail is None:
        missing_dataset_warning("city_blood_availability")
        return
    if city_avail.empty:
        empty_filter_warning()
        return

    city_filtered = city_avail.copy()
    if filters["rare_only"]:
        city_filtered = city_filtered[city_filtered["blood_group"].isin(RARE_BLOOD_GROUPS)]
    if filters["blood_group"] != "All":
        city_filtered = city_filtered[city_filtered["blood_group"] == filters["blood_group"]]
    if filters["state"] != "All":
        city_filtered = city_filtered[city_filtered["state"] == filters["state"]]
    if filters["city"] != "All":
        city_filtered = city_filtered[city_filtered["city"] == filters["city"]]

    if city_filtered.empty:
        empty_filter_warning()
        return

    display_city = add_status_display_column(city_filtered, "availability_status")
    st.dataframe(
        display_city[["city", "state", "blood_group", "total_available_units",
                       "banks_with_stock", "status_display"]].rename(columns={
            "city": "City", "state": "State", "blood_group": "Blood Group",
            "total_available_units": "Available Units", "banks_with_stock": "Blood Banks With Stock",
            "status_display": "Status",
        }),
        use_container_width=True, hide_index=True,
    )

    city_totals = city_filtered.groupby("city", as_index=False)["total_available_units"].sum()
    fig = px.bar(
        city_totals.sort_values("total_available_units", ascending=False),
        x="city", y="total_available_units",
        labels={"city": "City", "total_available_units": "Total Rare Blood Units"},
        title="Rare Blood Units by City",
    )
    st.plotly_chart(fig, use_container_width=True)


# ============================================================================
# 10. BLOOD BANKS TAB
# ============================================================================

def render_blood_banks_tab(rare_stock: pd.DataFrame | None, filters: dict) -> None:
    st.header("🏥 Blood Banks With Rare Blood")
    st.caption('Answers: "Where can I find this rare blood type?"')

    if rare_stock is None:
        missing_dataset_warning("blood_bank_rare_stock")
        return

    filtered = filter_center_level_df(
        rare_stock, filters["blood_group"], filters["rare_only"], filters["state"],
        filters["city"], filters["blood_bank"], filters["status"],
    )

    if filtered.empty:
        empty_filter_warning()
        return

    display_df = add_status_display_column(filtered, "availability_status")
    st.dataframe(
        display_df[["blood_bank_name", "city", "state", "blood_group", "available_units",
                     "status_display", "last_updated"]].sort_values(
            "available_units", ascending=False
        ).rename(columns={
            "blood_bank_name": "Blood Bank", "city": "City", "state": "State",
            "blood_group": "Blood Group", "available_units": "Available Units",
            "status_display": "Status", "last_updated": "Last Updated",
        }),
        use_container_width=True, hide_index=True,
    )

    st.markdown("---")
    st.subheader("Top Blood Banks With Rare Blood")

    ranking_group = filters["blood_group"] if filters["blood_group"] != "All" else RARE_BLOOD_GROUPS[0]
    ranking_df = rare_stock[rare_stock["blood_group"] == ranking_group].sort_values(
        "available_units", ascending=False
    ).head(15)

    if not ranking_df.empty:
        fig = px.bar(
            ranking_df, x="available_units", y="blood_bank_name", orientation="h",
            color="availability_status",
            color_discrete_map={k: v[1] for k, v in STATUS_STYLE.items()},
            labels={"available_units": "Available Units", "blood_bank_name": "Blood Bank"},
            title=f"Top Blood Banks With {ranking_group}",
        )
        fig.update_layout(yaxis=dict(categoryorder="total ascending"))
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")
    st.subheader("📍 Blood Bank Locations")

    map_df = filtered.dropna(subset=["latitude", "longitude"]) if "latitude" in filtered.columns else pd.DataFrame()
    map_df = map_df[map_df["available_units"] > 0] if not map_df.empty else map_df

    if map_df.empty:
        st.info("No coordinate data available for the current selection — showing table only (above).")
    else:
        fig_map = px.scatter_map(
            map_df, lat="latitude", lon="longitude",
            size="available_units", color="blood_group",
            hover_name="blood_bank_name",
            hover_data={"city": True, "available_units": True, "availability_status": True,
                        "latitude": False, "longitude": False},
            zoom=4, height=500,
        )
        fig_map.update_layout(map_style="open-street-map", margin={"r": 0, "t": 0, "l": 0, "b": 0})
        st.plotly_chart(fig_map, use_container_width=True)


# ============================================================================
# 11. SHORTAGES TAB
# ============================================================================

def render_shortages_tab(shortage_report: pd.DataFrame | None) -> None:
    st.header("⚠️ Blood Shortage Analysis")

    if shortage_report is None:
        missing_dataset_warning("blood_shortage_report")
        return
    if shortage_report.empty:
        empty_filter_warning()
        return

    sorted_report = shortage_report.sort_values("severity_score", ascending=False)
    display_df = add_status_display_column(sorted_report, "shortage_status")

    st.dataframe(
        display_df[["blood_group", "total_available_units", "number_of_blood_banks",
                     "banks_with_stock", "status_display", "severity_score"]].rename(columns={
            "blood_group": "Blood Group", "total_available_units": "Total Available Units",
            "number_of_blood_banks": "Total Blood Banks", "banks_with_stock": "Banks With Stock",
            "status_display": "Shortage Status", "severity_score": "Severity Score",
        }),
        use_container_width=True, hide_index=True,
    )

    fig = px.bar(
        sorted_report, x="blood_group", y="total_available_units",
        color="shortage_status",
        color_discrete_map={k: v[1] for k, v in STATUS_STYLE.items()},
        labels={"blood_group": "Blood Group", "total_available_units": "Available Units",
                "shortage_status": "Status"},
        title="Blood Shortage Severity (most critical first)",
        category_orders={"blood_group": sorted_report["blood_group"].tolist()},
    )
    st.plotly_chart(fig, use_container_width=True)


# ============================================================================
# 12. DONATIONS TAB
# ============================================================================

def render_donations_tab(
    daily_summary: pd.DataFrame | None, monthly_summary: pd.DataFrame | None,
    blood_group_dist: pd.DataFrame | None, filters: dict,
) -> None:
    st.header("📈 Donation Trends")

    if daily_summary is None:
        missing_dataset_warning("daily_donation_summary")
    else:
        daily = daily_summary.copy()
        daily["donation_date"] = pd.to_datetime(daily["donation_date"])

        if filters["date_range"] and len(filters["date_range"]) == 2:
            start, end = filters["date_range"]
            daily = daily[(daily["donation_date"].dt.date >= start) & (daily["donation_date"].dt.date <= end)]

        if daily.empty:
            empty_filter_warning()
        else:
            fig = px.line(
                daily.sort_values("donation_date"), x="donation_date", y="total_donations",
                labels={"donation_date": "Date", "total_donations": "Total Donations"},
                title="Daily Donations Over Time",
            )
            st.plotly_chart(fig, use_container_width=True)

    if monthly_summary is None:
        missing_dataset_warning("monthly_donation_summary")
    elif monthly_summary.empty:
        empty_filter_warning()
    else:
        monthly = monthly_summary.copy()
        monthly["period"] = monthly["month_name"].str.slice(0, 3) + " " + monthly["year"].astype(str)
        fig2 = px.line(
            monthly, x="period", y="total_units_donated", markers=True,
            labels={"period": "Month", "total_units_donated": "Total Units Donated"},
            title="Monthly Units Donated Over Time",
        )
        st.plotly_chart(fig2, use_container_width=True)

    st.markdown("---")
    st.subheader("🩸 Blood Group Distribution")

    if blood_group_dist is None:
        missing_dataset_warning("blood_group_distribution")
        return
    if blood_group_dist.empty:
        empty_filter_warning()
        return

    dist = blood_group_dist.copy()
    dist["is_rare"] = dist["blood_group"].isin(RARE_BLOOD_GROUPS)

    col1, col2 = st.columns(2)
    with col1:
        fig3 = px.bar(
            dist.sort_values("total_units_donated", ascending=False),
            x="blood_group", y="total_units_donated", color="is_rare",
            color_discrete_map={True: "#B71C1C", False: "#90A4AE"},
            labels={"blood_group": "Blood Group", "total_units_donated": "Total Units",
                    "is_rare": "Rare Blood Group"},
            title="Units Donated by Blood Group (rare groups highlighted)",
        )
        st.plotly_chart(fig3, use_container_width=True)
    with col2:
        fig4 = px.pie(
            dist, names="blood_group", values="total_units_donated",
            title="Share of Total Units Donated",
            hole=0.4,
        )
        st.plotly_chart(fig4, use_container_width=True)

    st.dataframe(
        dist[["blood_group", "total_donations", "total_units_donated", "percentage_of_total"]].rename(columns={
            "blood_group": "Blood Group", "total_donations": "Total Donations",
            "total_units_donated": "Total Units Donated", "percentage_of_total": "% of Total",
        }),
        use_container_width=True, hide_index=True,
    )


# ============================================================================
# 13. DONORS TAB
# ============================================================================

def render_donors_tab(rare_donor_summary: pd.DataFrame | None, donor_summary: pd.DataFrame | None) -> None:
    st.header("👥 Rare Blood Donor Analysis")
    st.caption('Answers: "How many potential rare-blood donors are available?"')

    if rare_donor_summary is None:
        missing_dataset_warning("rare_donor_summary")
    elif rare_donor_summary.empty:
        empty_filter_warning()
    else:
        st.dataframe(
            rare_donor_summary.rename(columns={
                "blood_group": "Blood Group", "total_donors": "Total Donors",
                "active_donors": "Active Donors", "total_donations": "Total Donations",
                "total_units_donated": "Total Units Donated",
            }),
            use_container_width=True, hide_index=True,
        )

        fig = px.bar(
            rare_donor_summary.sort_values("blood_group"),
            x="blood_group", y="total_donors",
            labels={"blood_group": "Blood Group", "total_donors": "Number of Donors"},
            title="Rare Blood Donors by Blood Group",
            color_discrete_sequence=["#B71C1C"],
        )
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")
    st.subheader("Overall Donor Statistics")

    if donor_summary is None:
        missing_dataset_warning("donor_summary")
        return
    if donor_summary.empty:
        empty_filter_warning()
        return

    # Privacy: only aggregated statistics are shown here, never a full donor
    # roster with contact or personal details (donor_summary already
    # excludes those fields at the Gold layer).
    total_donors = len(donor_summary)
    rare_donor_count = int(donor_summary["is_rare_blood_donor"].sum())
    active_donor_count = int((donor_summary["total_donations"] > 0).sum())

    col1, col2, col3 = st.columns(3)
    col1.metric("Total Registered Donors", f"{total_donors:,}")
    col2.metric("Rare Blood Donors", f"{rare_donor_count:,}")
    col3.metric("Donors With ≥1 Completed Donation", f"{active_donor_count:,}")

    by_group = donor_summary.groupby("blood_group", as_index=False).agg(
        donor_count=("donor_id", "count"), total_units_donated=("total_units_donated", "sum")
    )
    fig2 = px.bar(
        by_group.sort_values("donor_count", ascending=False),
        x="blood_group", y="donor_count",
        labels={"blood_group": "Blood Group", "donor_count": "Number of Donors"},
        title="Donor Count by Blood Group",
    )
    st.plotly_chart(fig2, use_container_width=True)


# ============================================================================
# 14. PERFORMANCE TAB
# ============================================================================

def render_performance_tab(
    bank_performance: pd.DataFrame | None, camp_performance: pd.DataFrame | None
) -> None:
    st.header("🏥 Blood Bank Performance")

    if bank_performance is None:
        missing_dataset_warning("blood_bank_performance")
    elif bank_performance.empty:
        empty_filter_warning()
    else:
        st.dataframe(
            bank_performance.sort_values("total_units_received", ascending=False).rename(columns={
                "blood_bank_name": "Blood Bank", "city": "City", "state": "State",
                "total_donations_received": "Total Donations", "total_units_received": "Total Units Received",
                "unique_donors": "Unique Donors", "rare_blood_units": "Rare Blood Units",
                "rare_blood_types_available": "Rare Blood Types Available",
            }),
            use_container_width=True, hide_index=True,
        )

        top_banks = bank_performance.sort_values("total_units_received", ascending=False).head(10)
        fig = px.bar(
            top_banks, x="total_units_received", y="blood_bank_name", orientation="h",
            labels={"total_units_received": "Total Units Received", "blood_bank_name": "Blood Bank"},
            title="Top Blood Banks by Units Received",
        )
        fig.update_layout(yaxis=dict(categoryorder="total ascending"))
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")
    st.header("🏕️ Donation Camp Performance")

    if camp_performance is None:
        missing_dataset_warning("donation_camp_performance")
        return
    if camp_performance.empty:
        empty_filter_warning()
        return

    st.dataframe(
        camp_performance.sort_values("total_units_collected", ascending=False).rename(columns={
            "camp_name": "Camp", "city": "City", "total_donations": "Total Donations",
            "total_units_collected": "Total Units Collected", "unique_donors": "Unique Donors",
        }),
        use_container_width=True, hide_index=True,
    )

    top_camps = camp_performance.sort_values("total_units_collected", ascending=False).head(10)
    fig2 = px.bar(
        top_camps, x="total_units_collected", y="camp_name", orientation="h",
        labels={"total_units_collected": "Total Units Collected", "camp_name": "Camp"},
        title="Top Donation Camps by Units Collected",
    )
    fig2.update_layout(yaxis=dict(categoryorder="total ascending"))
    st.plotly_chart(fig2, use_container_width=True)


# ============================================================================
# 15. LIVE FEED TAB
# ============================================================================

# ============================================================================
# 15b. INVENTORY INTELLIGENCE TAB (transaction-driven model, upgrade)
# ============================================================================

def priority_display(priority: str) -> str:
    emoji, _ = PRIORITY_STYLE.get(priority, ("⚪", "#9E9E9E"))
    return f"{emoji} {priority}"


def render_inventory_intelligence_tab(filters: dict) -> None:
    st.header("📦 Inventory Intelligence")
    st.caption(
        "Current inventory recomputed from a full transaction ledger "
        "(donations, issues, transfers, expiry, adjustments) rather than a "
        "static snapshot -- see docs/UPGRADE_NOTES.md for the full model."
    )

    inventory_state = load_inventory_state()
    if inventory_state is None:
        missing_dataset_warning("gold_inventory_state")
        st.info(f"Run `python {INVENTORY_ENGINE_SCRIPT}` after generating transactional data "
                f"(`python data_generation/02_generate_transactional_data.py`).")
        return
    if inventory_state.empty:
        empty_filter_warning()
        return

    filtered = inventory_state.copy()
    if filters["rare_only"]:
        filtered = filtered[filtered["blood_group"].isin(RARE_BLOOD_GROUPS)]
    if filters["blood_group"] != "All":
        filtered = filtered[filtered["blood_group"] == filters["blood_group"]]

    # ---- Executive summary (dynamically generated, never hard-coded) ----
    total_units = int(filtered["available_units"].sum())
    emergency_count = int((filtered["priority_level"] == "EMERGENCY").sum())
    critical_count = int((filtered["priority_level"] == "CRITICAL").sum())
    expiring_soon = int((filtered["expiry_status"] == "EXPIRING_SOON").sum())
    expired = int((filtered["expiry_status"] == "EXPIRED").sum())

    st.subheader("Executive Summary")
    summary_lines = [
        f"**{total_units:,} units** currently in stock across **{filtered['blood_bank_id'].nunique()} blood bank(s)** "
        f"and **{filtered['component_type'].nunique()} component type(s)**.",
    ]
    if emergency_count:
        worst = filtered.loc[filtered["priority_level"] == "EMERGENCY"].sort_values("shortage_score", ascending=False).iloc[0]
        summary_lines.append(
            f"🔴 **{emergency_count} inventory line(s) at EMERGENCY priority** — most urgent: "
            f"{worst['blood_group']} {worst['component_type']} at {worst['blood_bank_id']} "
            f"({int(worst['available_units'])} units, shortage score {int(worst['shortage_score'])})."
        )
    if critical_count:
        summary_lines.append(f"🟠 **{critical_count} line(s) at CRITICAL priority.**")
    if expiring_soon or expired:
        summary_lines.append(f"⏳ **{expiring_soon} line(s) expiring soon**, **{expired} already EXPIRED** (lifetime).")
    if not (emergency_count or critical_count):
        summary_lines.append("✅ No EMERGENCY or CRITICAL priority lines in the current selection.")

    for line in summary_lines:
        st.markdown(line)

    st.markdown("---")

    # ---- KPI row ----
    cols = st.columns(4)
    cols[0].metric("Total Units (filtered)", f"{total_units:,}")
    cols[1].metric("🔴 EMERGENCY Lines", emergency_count)
    cols[2].metric("🟠 CRITICAL Lines", critical_count)
    avg_coverage = filtered["coverage_days"].dropna()
    cols[3].metric("Median Coverage (days)", f"{avg_coverage.median():.1f}" if not avg_coverage.empty else "N/A")

    st.markdown("---")

    # ---- Priority distribution chart ----
    st.subheader("Priority Level Distribution")
    priority_order = ["EMERGENCY", "CRITICAL", "LOW", "MONITOR", "HEALTHY"]
    priority_counts = filtered["priority_level"].value_counts().reindex(priority_order).fillna(0).reset_index()
    priority_counts.columns = ["priority_level", "count"]
    fig = px.bar(
        priority_counts, x="priority_level", y="count", color="priority_level",
        color_discrete_map={k: v[1] for k, v in PRIORITY_STYLE.items()},
        category_orders={"priority_level": priority_order},
        labels={"priority_level": "Priority Level", "count": "Number of Inventory Lines"},
        title="Inventory Lines by Priority Level",
    )
    st.plotly_chart(fig, use_container_width=True)

    # ---- Critical alerts table (Section 26.5 of the upgrade brief) ----
    st.subheader("🚨 Critical Alerts")
    alerts = filtered[filtered["priority_level"].isin(["EMERGENCY", "CRITICAL"])].copy()
    if alerts.empty:
        st.success("✅ No EMERGENCY or CRITICAL inventory lines in the current selection.")
    else:
        alerts["priority_display"] = alerts["priority_level"].apply(priority_display)
        alerts["coverage_display"] = alerts["coverage_days"].apply(
            lambda x: f"{x:.1f}" if pd.notna(x) else "No demand data"
        )
        st.dataframe(
            alerts.sort_values("shortage_score", ascending=False)[[
                "blood_group", "component_type", "blood_bank_id", "available_units",
                "coverage_display", "shortage_score", "priority_display", "recommended_action",
            ]].rename(columns={
                "blood_group": "Blood Group", "component_type": "Component", "blood_bank_id": "Blood Bank",
                "available_units": "Available Units", "coverage_display": "Coverage Days",
                "shortage_score": "Shortage Score", "priority_display": "Priority", "recommended_action": "Recommended Action",
            }),
            use_container_width=True, hide_index=True,
        )

    st.markdown("---")

    # ---- Expiry management ----
    st.subheader("⏳ Expiry Management")
    expiry_counts = filtered["expiry_status"].value_counts().reindex(["VALID", "EXPIRING_SOON", "EXPIRED"]).fillna(0)
    ecols = st.columns(3)
    ecols[0].metric("VALID", int(expiry_counts.get("VALID", 0)))
    ecols[1].metric("EXPIRING_SOON", int(expiry_counts.get("EXPIRING_SOON", 0)))
    ecols[2].metric("EXPIRED (lifetime)", int(expiry_counts.get("EXPIRED", 0)))

    st.markdown("---")

    # ---- Demand analytics ----
    st.subheader("📊 Demand Analytics")
    demand = load_demand_analytics()
    if demand is None:
        missing_dataset_warning("gold_demand_analytics")
    elif demand.empty:
        empty_filter_warning()
    else:
        if filters["rare_only"]:
            demand = demand[demand["blood_group"].isin(RARE_BLOOD_GROUPS)]
        top_demand = demand.sort_values("average_daily_demand", ascending=False).head(10)
        fig2 = px.bar(
            top_demand, x="average_daily_demand",
            y=top_demand["blood_bank_id"] + " · " + top_demand["blood_group"] + " " + top_demand["component_type"],
            orientation="h",
            labels={"average_daily_demand": "Avg Daily Demand (units)", "y": "Bank · Group Component"},
            title="Top 10 Highest-Demand Inventory Lines",
        )
        fig2.update_layout(yaxis=dict(categoryorder="total ascending"))
        st.plotly_chart(fig2, use_container_width=True)

    st.markdown("---")

    # ---- Transfer history ----
    st.subheader("🔄 Inter-Bank Transfer History")
    st.caption('Answers: "Which blood bank can fulfill another bank\'s shortage?"')
    transfers = load_transfer_history()
    if transfers is None:
        missing_dataset_warning("gold_transfer_history")
    elif transfers.empty:
        st.info("No transfers recorded in this dataset.")
    else:
        valid_transfers = transfers[transfers["is_valid"]] if "is_valid" in transfers.columns else transfers
        display_transfers = valid_transfers.sort_values("transfer_date", ascending=False).head(25)
        st.dataframe(
            display_transfers[["transfer_id", "source_bank_id", "destination_bank_id",
                                "blood_group", "component_type", "units", "transfer_date", "status"]].rename(columns={
                "transfer_id": "Transfer ID", "source_bank_id": "From", "destination_bank_id": "To",
                "blood_group": "Blood Group", "component_type": "Component", "units": "Units",
                "transfer_date": "Date", "status": "Status",
            }),
            use_container_width=True, hide_index=True,
        )
        st.caption(f"{len(valid_transfers)} valid transfer(s) total "
                   f"({len(transfers) - len(valid_transfers)} flagged invalid and excluded).")


def render_live_feed_tab() -> None:
    st.header("🔴 Live Donation Feed")

    streaming_df = load_streaming_donations()

    if streaming_df is None or streaming_df.empty:
        st.info(
            "Live streaming data is currently unavailable.\n\n"
            "Historical Gold analytics are still available in the other tabs.\n\n"
            "To generate live data, run:\n"
            "```\npython streaming/02_stream_donation_generator.py --interval 2 --count 50\n"
            "python pipeline/04_bronze_streaming_ingest.py --minutes 3\n```"
        )
        return

    recent = streaming_df.copy()
    recent["event_time"] = pd.to_datetime(recent["event_time"])
    recent = recent.sort_values("event_time", ascending=False).head(25)
    recent["is_rare"] = recent["blood_group"].isin(RARE_BLOOD_GROUPS)

    st.caption(f"Showing the {len(recent)} most recent donation events (of {len(streaming_df)} total).")

    for _, row in recent.iterrows():
        rare_tag = "🔴 RARE" if row["is_rare"] else ""
        with st.container(border=True):
            cols = st.columns([2, 1, 2, 1, 1, 1])
            cols[0].markdown(f"**{row['donation_id']}** {rare_tag}")
            cols[1].markdown(f"🩸 {row['blood_group']}")
            cols[2].markdown(f"🏥 {row['blood_bank_id']}")
            cols[3].markdown(f"{row['units_donated']} unit(s)")
            cols[4].markdown(f"{row['donation_status']}")
            cols[5].markdown(f"{row['event_time'].strftime('%H:%M:%S')}")

    st.markdown("---")
    st.subheader("Full Live Feed Table")
    st.dataframe(
        recent[["donation_id", "blood_group", "blood_bank_id", "units_donated",
                "event_time", "donation_status"]].rename(columns={
            "donation_id": "Donation ID", "blood_group": "Blood Group",
            "blood_bank_id": "Blood Bank", "units_donated": "Units",
            "event_time": "Donation Time", "donation_status": "Status",
        }),
        use_container_width=True, hide_index=True,
    )


# ============================================================================
# 16. MAIN
# ============================================================================

def main() -> None:
    st.title("🩸 Rare Blood Type Availability Analytics")
    st.caption(
        "Monitor rare blood availability, identify blood banks with critical blood types, "
        "detect shortages, and analyze donation trends."
    )
    st.caption(
        "Synthetic-data demonstration only — not a real medical decision-making system. "
        "'Rare' blood groups (A-, B-, AB-, O-) are a project-defined classification."
    )

    # ---- Load all Gold datasets up front (cached) ----
    rare_summary = load_rare_blood_summary()
    rare_availability = load_rare_blood_availability()
    rare_stock = load_blood_bank_rare_stock()
    shortage_report = load_shortage_report()
    city_avail = load_city_availability()
    daily_summary = load_daily_donation_summary()
    monthly_summary = load_monthly_donation_summary()
    blood_group_dist = load_blood_group_distribution()
    bank_performance = load_blood_bank_performance()
    camp_performance = load_donation_camp_performance()
    donor_summary = load_donor_summary()
    rare_donor_summary = load_rare_donor_summary()

    # ---- Sidebar filters ----
    filters = build_sidebar_filters(rare_stock, daily_summary)

    # ---- Top KPI section ----
    kpis = compute_kpis(rare_summary, rare_stock, rare_donor_summary, daily_summary, blood_group_dist)
    render_kpi_section(kpis)

    st.markdown("---")

    # ---- Tabbed navigation ----
    tabs = st.tabs([
        "Overview", "Rare Blood Availability", "Blood Banks", "Shortages",
        "Inventory Intelligence", "Donations", "Donors", "Performance", "Live Feed",
    ])

    with tabs[0]:
        render_overview_tab(rare_summary)
    with tabs[1]:
        render_rare_blood_tab(rare_summary, city_avail, filters)
    with tabs[2]:
        render_blood_banks_tab(rare_stock if rare_stock is not None else rare_availability, filters)
    with tabs[3]:
        render_shortages_tab(shortage_report)
    with tabs[4]:
        render_inventory_intelligence_tab(filters)
    with tabs[5]:
        render_donations_tab(daily_summary, monthly_summary, blood_group_dist, filters)
    with tabs[6]:
        render_donors_tab(rare_donor_summary, donor_summary)
    with tabs[7]:
        render_performance_tab(bank_performance, camp_performance)
    with tabs[8]:
        render_live_feed_tab()


if __name__ == "__main__":
    main()
