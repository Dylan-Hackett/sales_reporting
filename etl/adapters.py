"""
Source adapters for Imperial, LPQ, and J&TJ sales data.
Each adapter returns a standardized DataFrame with consistent columns.
"""
import re
from pathlib import Path

import pandas as pd


# Standardized output columns
STANDARD_COLS = [
    "source", "vendor_item", "item_description", "location_code",
    "customer_name", "address", "city", "state", "zip",
    "invoice_date", "invoice_num", "yyyymm", "qty", "unit_price", "total",
]


def _normalize_col(name):
    """Strip newlines, lowercase, snake_case."""
    s = str(name).replace("\n", " ").strip()
    s = re.sub(r"[^a-zA-Z0-9]+", "_", s).strip("_").lower()
    return s


def _concat_address(row):
    parts = [str(row.get("address1") or "").strip(),
             str(row.get("address2") or "").strip()]
    return ", ".join(p for p in parts if p and p.lower() != "nan")


def _standardize(df, source, location_col="customer"):
    """Map raw normalized columns to the standard schema."""
    df = df.copy()
    df["source"] = source
    df["address"] = df.apply(_concat_address, axis=1)
    df["invoice_date"] = pd.to_datetime(df["invoice_date"], format="%m/%d/%y", errors="coerce")
    df["yyyymm"] = df["invoice_date"].dt.strftime("%Y-%m")

    col_map = {
        "vendor_item": "vendor_item",
        "item_description": "item_description",
        location_col: "location_code",
        "customer_name": "customer_name",
        "city": "city",
        "state": "state",
        "zipcode": "zip",
        "invoice": "invoice_num",
        "ship_qty": "qty",
        "net_price": "unit_price",
        "total": "total",
    }

    out = pd.DataFrame()
    out["source"] = df["source"]
    out["address"] = df["address"]
    out["invoice_date"] = df["invoice_date"]
    out["yyyymm"] = df["yyyymm"]

    for src_col, dst_col in col_map.items():
        if src_col in df.columns:
            out[dst_col] = df[src_col]
        else:
            out[dst_col] = None

    # Coerce types
    out["qty"] = pd.to_numeric(out["qty"], errors="coerce").fillna(0)
    out["unit_price"] = pd.to_numeric(out["unit_price"], errors="coerce").fillna(0)
    out["total"] = pd.to_numeric(out["total"], errors="coerce").fillna(0)
    out["location_code"] = out["location_code"].astype(str).str.strip()
    out["invoice_num"] = out["invoice_num"].astype(str).str.strip()

    return out[STANDARD_COLS]


def ingest_imperial(data_dir):
    """Read all .xlsx files from the Imperial subdirectory."""
    imp_dir = Path(data_dir) / "Imperial "
    if not imp_dir.exists():
        imp_dir = Path(data_dir) / "Imperial"
    files = sorted(imp_dir.glob("*.xlsx"))
    if not files:
        raise FileNotFoundError(f"No .xlsx files found in {imp_dir}")

    frames = []
    for f in files:
        df = pd.read_excel(f, header=4)
        df.columns = [_normalize_col(c) for c in df.columns]
        # Filter to Record Type 1 only
        if "record_type" in df.columns:
            df["record_type"] = pd.to_numeric(df["record_type"], errors="coerce")
            df = df[df["record_type"] == 1].copy()
        df["sourcefile"] = f.name
        frames.append(df)
        print(f"  Imperial: {f.name} -> {len(df)} rows")

    combined = pd.concat(frames, ignore_index=True)
    return _standardize(combined, source="imperial", location_col="customer")


def ingest_lpq(data_dir):
    """Read LPQ_All_Purchases sheet from LPQ 2025.xlsx."""
    path = Path(data_dir) / "LPQ 2025.xlsx"
    df = pd.read_excel(path, sheet_name="LPQ_All_Purchases")
    df.columns = [_normalize_col(c) for c in df.columns]

    if "record_type" in df.columns:
        df["record_type"] = pd.to_numeric(df["record_type"], errors="coerce")
        df = df[df["record_type"] == 1].copy()

    # Use the Location column as location_code
    print(f"  LPQ: {len(df)} rows, {df['location'].nunique()} locations")
    return _standardize(df, source="lpq", location_col="location")


def ingest_jtj(data_dir):
    """Read Joe_Juice_All_Purchases sheet from 2025 J&TJ.xlsx."""
    path = Path(data_dir) / "2025 J&TJ.xlsx"
    df = pd.read_excel(path, sheet_name="Joe_Juice_All_Purchases")
    df.columns = [_normalize_col(c) for c in df.columns]

    if "record_type" in df.columns:
        df["record_type"] = pd.to_numeric(df["record_type"], errors="coerce")
        df = df[df["record_type"] == 1].copy()

    print(f"  J&TJ: {len(df)} rows, {df['location'].nunique()} locations")
    return _standardize(df, source="jtj", location_col="location")


def load_all(data_dir):
    """Load all three sources and deduplicate.

    LPQ/J&TJ files are authoritative for those chains.
    Remove their location codes from Imperial to avoid double-counting.
    """
    print("Loading Imperial...")
    imp = ingest_imperial(data_dir)
    print("Loading LPQ...")
    lpq = ingest_lpq(data_dir)
    print("Loading J&TJ...")
    jtj = ingest_jtj(data_dir)

    # Get location codes that belong to LPQ/J&TJ
    chain_codes = set(lpq["location_code"].unique()) | set(jtj["location_code"].unique())

    imp_before = len(imp)
    imp = imp[~imp["location_code"].isin(chain_codes)].copy()
    imp_after = len(imp)
    removed = imp_before - imp_after
    print(f"\nDedup: removed {removed} Imperial rows matching LPQ/J&TJ location codes")

    unified = pd.concat([imp, lpq, jtj], ignore_index=True)
    print(f"Unified: {len(unified)} rows, {unified['location_code'].nunique()} unique locations")
    print(f"Date range: {unified['yyyymm'].min()} to {unified['yyyymm'].max()}")
    return unified
