"""
Report view generation.
Produces three pivoted CSVs + a location snapshot with rolling metrics.
"""
import re
from pathlib import Path

import pandas as pd
import yaml


def _norm(s):
    return re.sub(r"[^A-Z0-9]+", " ", str(s).strip().upper()).strip()


def _load_location_map():
    """Load the location map CSV for enriching output."""
    path = Path("config/location_map.csv")
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path, dtype=str)
    return df


def _enrich(df, loc_map):
    """Merge location metadata onto a DataFrame that has location_code."""
    if loc_map.empty:
        return df
    meta_cols = ["location_code", "chain", "mobi_customer_id", "mobi_customer_name"]
    meta = loc_map[[c for c in meta_cols if c in loc_map.columns]].drop_duplicates(subset=["location_code"])
    return df.merge(meta, on="location_code", how="left")


def _month_columns(df_wide, meta_cols):
    """Extract and sort month columns (YYYY-MM format) from a wide DataFrame."""
    month_cols = sorted([c for c in df_wide.columns if c not in meta_cols and re.match(r"\d{4}-\d{2}", c)])
    return month_cols


def view_sales_by_location_month(transactions_df, loc_map, out_dir):
    """View 1: Sales $ by location by month, pivoted wide."""
    agg = transactions_df.groupby(["location_code", "yyyymm"], as_index=False)["total"].sum()
    pivot = agg.pivot_table(index="location_code", columns="yyyymm", values="total", fill_value=0).reset_index()
    pivot.columns.name = None
    pivot = _enrich(pivot, loc_map)

    # Get customer_name from loc_map
    if not loc_map.empty and "customer_name" in loc_map.columns:
        name_map = loc_map[["location_code", "customer_name"]].drop_duplicates(subset=["location_code"])
        pivot = pivot.merge(name_map, on="location_code", how="left")

    # Reorder: metadata first, then months
    meta = ["location_code", "customer_name", "chain", "mobi_customer_id", "mobi_customer_name"]
    meta = [c for c in meta if c in pivot.columns]
    months = _month_columns(pivot, set(meta))
    pivot = pivot[meta + months]

    path = Path(out_dir) / "sales_by_location_month.csv"
    pivot.to_csv(path, index=False)
    print(f"  View 1: {path} ({len(pivot)} locations, {len(months)} months)")
    return pivot


def _canonical_description(transactions_df):
    """Pick one item_description per vendor_item (longest wins — most descriptive)."""
    desc = transactions_df[["vendor_item", "item_description"]].drop_duplicates()
    desc["_len"] = desc["item_description"].astype(str).str.len()
    desc = desc.sort_values("_len", ascending=False).drop_duplicates(subset="vendor_item", keep="first")
    return desc[["vendor_item", "item_description"]]


def view_sales_by_sku_location_month(transactions_df, loc_map, out_dir):
    """View 2: Sales $ by SKU by location by month, pivoted wide."""
    agg = transactions_df.groupby(
        ["location_code", "vendor_item", "yyyymm"], as_index=False
    )["total"].sum()
    pivot = agg.pivot_table(
        index=["location_code", "vendor_item"],
        columns="yyyymm", values="total", fill_value=0
    ).reset_index()
    # Re-attach a single canonical description per vendor_item
    pivot = pivot.merge(_canonical_description(transactions_df), on="vendor_item", how="left")
    pivot.columns.name = None
    pivot = _enrich(pivot, loc_map)

    meta = ["location_code", "chain", "mobi_customer_id", "vendor_item", "item_description"]
    meta = [c for c in meta if c in pivot.columns]
    months = _month_columns(pivot, set(meta))
    pivot = pivot[meta + months]

    path = Path(out_dir) / "sales_by_sku_location_month.csv"
    pivot.to_csv(path, index=False)
    print(f"  View 2: {path} ({len(pivot)} rows)")
    return pivot


def view_qty_by_sku_location_month(transactions_df, loc_map, out_dir):
    """View 3: Quantity by SKU by location by month, pivoted wide."""
    agg = transactions_df.groupby(
        ["location_code", "vendor_item", "yyyymm"], as_index=False
    )["qty"].sum()
    pivot = agg.pivot_table(
        index=["location_code", "vendor_item"],
        columns="yyyymm", values="qty", fill_value=0
    ).reset_index()
    pivot = pivot.merge(_canonical_description(transactions_df), on="vendor_item", how="left")
    pivot.columns.name = None
    pivot = _enrich(pivot, loc_map)

    meta = ["location_code", "chain", "mobi_customer_id", "vendor_item", "item_description"]
    meta = [c for c in meta if c in pivot.columns]
    months = _month_columns(pivot, set(meta))
    pivot = pivot[meta + months]

    path = Path(out_dir) / "qty_by_sku_location_month.csv"
    pivot.to_csv(path, index=False)
    print(f"  View 3: {path} ({len(pivot)} rows)")
    return pivot


def _window(latest, n):
    """Generate a set of n YYYY-MM strings ending at latest, stepping back by month."""
    y, m = map(int, latest.split("-"))
    out = [latest]
    for _ in range(n - 1):
        m -= 1
        if m == 0:
            m = 12
            y -= 1
        out.append(f"{y}-{m:02d}")
    return set(out)


def _build_sku_group_lookup(cfg):
    """Build SKU variant group lookup from config."""
    groups = cfg.get("sku_variant_groups") or {}
    group_map = {}
    display_map = {}
    for group_name, variants in groups.items():
        group_key = _norm(group_name)
        variant_list = variants if isinstance(variants, list) else []
        if variant_list:
            display_map[group_key] = variant_list[0]
            for variant in variant_list:
                group_map[_norm(variant)] = group_key
        else:
            display_map[group_key] = group_name
        group_map[group_key] = group_key
        display_map.setdefault(group_key, group_key)
    return group_map, display_map


def location_snapshot(transactions_df, loc_map, out_dir, report_month, cfg):
    """Location snapshot: one row per location with rolling metrics and SLA tier.

    Rolling averages divide by actual months with data (not calendar months)
    to handle the missing September 2025 correctly.
    """
    out_dir = Path(out_dir)
    sku_group_map, sku_display_map = _build_sku_group_lookup(cfg)

    last3_months = _window(report_month, 3)
    last6_months = _window(report_month, 6)

    # Filter to months that actually exist in the data
    available_months = set(transactions_df["yyyymm"].dropna().unique())
    actual_3m = last3_months & available_months
    actual_6m = last6_months & available_months
    n3 = len(actual_3m) or 1
    n6 = len(actual_6m) or 1

    df = transactions_df.copy()

    # --- Last month total $ ---
    lm = df[df["yyyymm"] == report_month].groupby("location_code", as_index=False)["total"].sum()
    lm = lm.rename(columns={"total": "last_month_total"})

    # --- 3-month avg monthly $ ---
    m3 = df[df["yyyymm"].isin(last3_months)].groupby("location_code", as_index=False)["total"].sum()
    m3["avg_monthly_3m"] = m3["total"] / n3
    m3 = m3.rename(columns={"total": "total_3m"})

    # --- 6-month avg monthly $ ---
    m6 = df[df["yyyymm"].isin(last6_months)].groupby("location_code", as_index=False)["total"].sum()
    m6["avg_monthly_6m"] = m6["total"] / n6
    m6 = m6.rename(columns={"total": "total_6m"})

    # --- SLA tier based on 3-month avg spend ---
    th = cfg.get("sla_thresholds", {"tier1": 850, "tier2": 600, "tier3": 300})
    vmap = cfg.get("visit_map", {
        "Tier 1": "Monthly", "Tier 2": "Bi-Monthly",
        "Tier 3": "Quarterly", "Tier 4": "Annually"
    })

    def sla(spend):
        if spend >= th["tier1"]: return "Tier 1"
        if spend >= th["tier2"]: return "Tier 2"
        if spend >= th["tier3"]: return "Tier 3"
        return "Tier 4"

    # --- Most popular SKU (by 3-month quantity, using variant groups) ---
    sku_df = df[df["yyyymm"].isin(last3_months)].copy()
    sku_df["sku_group"] = sku_df["vendor_item"].apply(lambda s: sku_group_map.get(_norm(s), _norm(s)))
    sku_agg = sku_df.groupby(["location_code", "sku_group"], as_index=False)["qty"].sum()
    sku_agg = sku_agg.sort_values(["location_code", "qty"], ascending=[True, False])
    top_sku = sku_agg.groupby("location_code", as_index=False).first()
    top_sku["most_popular_sku"] = top_sku["sku_group"].map(
        lambda g: sku_display_map.get(g, g)
    )
    top_sku = top_sku[["location_code", "most_popular_sku"]]

    # --- Assemble snapshot ---
    all_locs = df[["location_code"]].drop_duplicates()
    snap = all_locs.merge(lm, on="location_code", how="left") \
                   .merge(m3[["location_code", "avg_monthly_3m"]], on="location_code", how="left") \
                   .merge(m6[["location_code", "avg_monthly_6m"]], on="location_code", how="left") \
                   .merge(top_sku, on="location_code", how="left")

    snap = snap.fillna({"last_month_total": 0, "avg_monthly_3m": 0, "avg_monthly_6m": 0})
    snap["sla_tier"] = snap["avg_monthly_3m"].apply(sla)
    snap["visit_frequency"] = snap["sla_tier"].map(vmap)
    snap["report_month"] = report_month

    # Enrich with location metadata
    snap = _enrich(snap, loc_map)
    if not loc_map.empty and "customer_name" in loc_map.columns:
        name_map = loc_map[["location_code", "customer_name"]].drop_duplicates(subset=["location_code"])
        snap = snap.merge(name_map, on="location_code", how="left")

    # Order columns
    meta = ["location_code", "customer_name", "chain", "mobi_customer_id", "mobi_customer_name"]
    meta = [c for c in meta if c in snap.columns]
    metric_cols = [
        "last_month_total", "avg_monthly_3m", "avg_monthly_6m",
        "sla_tier", "visit_frequency", "most_popular_sku", "report_month",
    ]
    snap = snap[meta + metric_cols]

    # Round dollar amounts
    for col in ["last_month_total", "avg_monthly_3m", "avg_monthly_6m"]:
        snap[col] = snap[col].round(2)

    path = out_dir / "location_snapshot.csv"
    snap.to_csv(path, index=False)
    print(f"  Snapshot: {path} ({len(snap)} locations)")

    # Print tier distribution
    tier_dist = snap["sla_tier"].value_counts().sort_index()
    print(f"  Tier distribution: {tier_dist.to_dict()}")

    return snap


def top_skus_by_location(transactions_df, loc_map, out_dir, report_month, cfg):
    """Top N SKUs per location with rolling qty metrics.

    Output: one row per (location, SKU) with last-month qty, 3M avg, 6M avg.
    Used by push_to_mobiwork.py for the repeating SKU panel.
    """
    out_dir = Path(out_dir)
    top_n = cfg.get("top_n", 10)
    sku_group_map, sku_display_map = _build_sku_group_lookup(cfg)

    last3_months = _window(report_month, 3)
    last6_months = _window(report_month, 6)
    available = set(transactions_df["yyyymm"].dropna().unique())
    n3 = len(last3_months & available) or 1
    n6 = len(last6_months & available) or 1

    df = transactions_df.copy()

    # Last month qty per location+SKU
    lm = df[df["yyyymm"] == report_month].groupby(
        ["location_code", "vendor_item"], as_index=False
    )["qty"].sum().rename(columns={"qty": "last_month_qty"})

    # 3-month qty
    m3 = df[df["yyyymm"].isin(last3_months)].groupby(
        ["location_code", "vendor_item"], as_index=False
    )["qty"].sum().rename(columns={"qty": "qty_3m_total"})
    m3["avg_qty_3m"] = m3["qty_3m_total"] / n3

    # 6-month qty
    m6 = df[df["yyyymm"].isin(last6_months)].groupby(
        ["location_code", "vendor_item"], as_index=False
    )["qty"].sum().rename(columns={"qty": "qty_6m_total"})
    m6["avg_qty_6m"] = m6["qty_6m_total"] / n6

    # Merge
    top = m3.merge(lm, on=["location_code", "vendor_item"], how="left")
    top = top.merge(
        m6[["location_code", "vendor_item", "avg_qty_6m"]],
        on=["location_code", "vendor_item"], how="left"
    )
    # Attach canonical description
    top = top.merge(_canonical_description(df), on="vendor_item", how="left")
    top = top.fillna({"last_month_qty": 0, "avg_qty_6m": 0})

    # Rank by 3-month total, keep top N per location
    top = top.sort_values(["location_code", "qty_3m_total"], ascending=[True, False])
    top = top.groupby("location_code").head(top_n)

    top["report_month"] = report_month
    top = _enrich(top, loc_map)

    # Round
    for col in ["avg_qty_3m", "avg_qty_6m"]:
        top[col] = top[col].round(1)

    cols = ["location_code", "chain", "mobi_customer_id",
            "vendor_item", "item_description",
            "last_month_qty", "avg_qty_3m", "avg_qty_6m", "report_month"]
    cols = [c for c in cols if c in top.columns]
    top = top[cols]

    path = out_dir / "top_skus_by_location.csv"
    top.to_csv(path, index=False)
    print(f"  Top SKUs: {path} ({len(top)} rows across {top['location_code'].nunique()} locations)")
    return top


def generate_all_views(transactions_df, loc_map, out_dir, report_month, cfg):
    """Generate all three views + location snapshot + top SKUs."""
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    print("\nGenerating views...")
    v1 = view_sales_by_location_month(transactions_df, loc_map, out_dir)
    v2 = view_sales_by_sku_location_month(transactions_df, loc_map, out_dir)
    v3 = view_qty_by_sku_location_month(transactions_df, loc_map, out_dir)
    snap = location_snapshot(transactions_df, loc_map, out_dir, report_month, cfg)
    top = top_skus_by_location(transactions_df, loc_map, out_dir, report_month, cfg)
    return v1, v2, v3, snap, top
