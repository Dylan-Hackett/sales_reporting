#!/usr/bin/env python3
"""
Push per-location sales report data to MobiWork forms.

Reads ETL output CSVs and updates MobiWork "Sales Report" form instances
(template 170770) via the REST API.

Form fields (new template):
  Summary panel:  ReportMonth, TotalSales, TotalUnits
  SKU Detail panel (repeating): SKUCode, ItemDescription, SKUSales, SKUQty

Usage:
    python push_to_mobiwork.py
    python push_to_mobiwork.py --only LPQ100
    python push_to_mobiwork.py --only 145683277  # by filled_form_id
    python push_to_mobiwork.py --dry-run
"""
import argparse
import math
import os
import sys
import xml.etree.ElementTree as ET
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
from pathlib import Path
from xml.sax.saxutils import escape

import pandas as pd
import requests

# ---------------------------------------------------------------------------
# Auth (same pattern as update_edenwald_form.py)
# ---------------------------------------------------------------------------

def _require_env(name, description):
    value = os.getenv(name)
    if not value:
        print(f"Missing env var {name} ({description}). Set in .env or export.", file=sys.stderr)
        sys.exit(1)
    return value


API_LOGIN_ID = _require_env("MOBI_API_LOGIN_ID", "MobiWork API login ID")
API_PASSWORD = _require_env("MOBI_API_PASSWORD", "MobiWork API password")
CLIENT_ID = _require_env("MOBI_CLIENT_ID", "MobiWork client ID")
API_VERSION = os.getenv("MOBI_API_VERSION", "10.4.109")
MOBI_BASE_URL = os.getenv("MOBI_BASE_URL", "https://platform.mobiwork.com").rstrip("/")

TARGETS_PATH = Path(os.getenv("MOBIFORM_TARGETS", "config/mobiform_targets.csv"))
XML_HEADERS = {"Content-Type": "application/xml"}


def authenticate():
    """Authenticate with MobiWork API and return session token."""
    url = f"{MOBI_BASE_URL}/api/rest/{CLIENT_ID}/{API_VERSION}/authenticate.html"
    body = f"""<request>
  <apiLoginId>{API_LOGIN_ID}</apiLoginId>
  <apiPassword>{API_PASSWORD}</apiPassword>
</request>"""
    resp = requests.post(url, data=body, headers=XML_HEADERS, timeout=10)
    if resp.status_code != 200:
        print(f"Auth failed: HTTP {resp.status_code}", file=sys.stderr)
        sys.exit(1)
    root = ET.fromstring(resp.text)
    token = root.findtext("token")
    if not token:
        print("Auth failed: no token in response", file=sys.stderr)
        sys.exit(1)
    print("Authenticated.")
    return token


# ---------------------------------------------------------------------------
# Targets
# ---------------------------------------------------------------------------

def load_targets(path, only=None):
    """Load target list from CSV. Filter by --only if provided."""
    df = pd.read_csv(path)
    df.columns = [c.strip().lower() for c in df.columns]
    required = {"mobi_customer_id", "filled_form_id"}
    missing = required - set(df.columns)
    if missing:
        print(f"Target file missing columns: {missing}", file=sys.stderr)
        sys.exit(1)

    if only:
        selectors = {s.strip().lower() for s in only}
        mask = (
            df["mobi_customer_id"].astype(str).str.strip().str.lower().isin(selectors) |
            df["filled_form_id"].astype(str).str.strip().str.lower().isin(selectors)
        )
        if "customer_label" in df.columns:
            mask |= df["customer_label"].astype(str).str.strip().str.lower().isin(selectors)
        # Also match by location_code if present
        if "location_code" in df.columns:
            mask |= df["location_code"].astype(str).str.strip().str.lower().isin(selectors)
        df = df[mask]
        if df.empty:
            print(f"--only filter matched no targets", file=sys.stderr)
            sys.exit(1)

    return df.to_dict("records")


# ---------------------------------------------------------------------------
# Formatting helpers (ported from update_edenwald_form.py)
# ---------------------------------------------------------------------------

def _fmt_qty(val):
    """Format a quantity value for display."""
    if pd.isna(val):
        return ""
    if isinstance(val, str):
        return val
    if math.isclose(val, round(val)):
        return str(int(round(val)))
    return f"{val:.2f}".rstrip("0").rstrip(".")


def _fmt_int(value):
    """Format a value as an integer string."""
    if value is None:
        return ""
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return ""
    try:
        if pd.isna(value):
            return ""
    except TypeError:
        pass
    try:
        dec = Decimal(str(value))
        dec = dec.quantize(Decimal("1"), rounding=ROUND_HALF_UP)
        return str(int(dec))
    except (InvalidOperation, ValueError):
        return str(value)


# ---------------------------------------------------------------------------
# Form XML building
# ---------------------------------------------------------------------------

# Field type constants (from MobiWork API)
FIELD_TEXT = 1
FIELD_NUMBER = 2

# apiName → (fieldType, display label) for Sales Report template (170770)
FIELD_META = {
    "ReportMonth": (FIELD_TEXT, "Report Month"),
    "TotalSales": (FIELD_TEXT, "Total Sales"),
    "TotalUnits": (FIELD_NUMBER, "Total Units"),
}
# Individual SKU fields (TopSKU1 through TopSKU10)
for _i in range(1, 11):
    FIELD_META[f"TopSKU{_i}"] = (FIELD_TEXT, f"Top SKU #{_i}")


def build_form_xml(filled_form_id, snapshot_row, sku_rows, sku_sales_rows=None, top_n=10, customer_id=None):
    """Build the XML payload for a MobiWork form update.

    Args:
        filled_form_id: The MobiWork filled form instance ID.
        snapshot_row: dict/Series from location_snapshot.csv.
        sku_rows: DataFrame of top SKUs for this location (qty data).
        sku_sales_rows: DataFrame of SKU-level sales $ for this location.
        top_n: Max number of SKU rows to include in the repeating panel.
        customer_id: MobiWork customer ID to associate the form with.
    """
    fields = []

    def add_field(api_name, value, index=0, label_override=None):
        if value is None:
            return
        value_str = str(value).strip()
        if value_str == "" or value_str == "nan":
            return
        field_type, label = FIELD_META.get(api_name, (FIELD_TEXT, api_name))
        if label_override:
            label = label_override
        fields.append(f"""    <formField>
      <fieldType>{field_type}</fieldType>
      <apiName>{api_name}</apiName>
      <index>{index}</index>
      <name>{escape(label)}</name>
      <value>{escape(value_str)}</value>
    </formField>""")

    # --- Summary fields ---
    report_month = snapshot_row.get("report_month", "")
    add_field("ReportMonth", report_month)

    # Total Sales $ (formatted with dollar sign)
    last_month_total = snapshot_row.get("last_month_total")
    if last_month_total is not None and not pd.isna(last_month_total):
        add_field("TotalSales", f"${last_month_total:,.2f}")
    else:
        add_field("TotalSales", "$0.00")

    # Total Units (sum of all SKU quantities for the month)
    total_units = 0
    if sku_rows is not None and not sku_rows.empty:
        qty_col = "last_month_qty"
        if qty_col in sku_rows.columns:
            total_units = int(sku_rows[qty_col].sum())
    add_field("TotalUnits", total_units)

    # --- Individual SKU fields (TopSKU1 through TopSKU10) ---
    # Build a sales $ lookup by vendor_item if we have SKU sales data
    sku_sales_map = {}
    if sku_sales_rows is not None and not sku_sales_rows.empty:
        last_month_col = snapshot_row.get("report_month", "")
        if last_month_col in sku_sales_rows.columns:
            for _, sr in sku_sales_rows.iterrows():
                sku_sales_map[sr.get("vendor_item", "")] = sr[last_month_col]

    if sku_rows is not None and not sku_rows.empty:
        for rank, (_, row) in enumerate(sku_rows.head(top_n).iterrows()):
            vendor_item = row.get("vendor_item", "")
            sku_sale = sku_sales_map.get(vendor_item, 0)
            if not sku_sale or pd.isna(sku_sale):
                sku_sale = 0
            sale_str = f"${float(sku_sale):,.2f}"
            # Use 3-month avg qty (matches ranking order) instead of last month
            qty_raw = row.get("avg_qty_3m", row.get("last_month_qty", 0))
            qty_str = _fmt_qty(qty_raw) if not pd.isna(qty_raw) else "0"
            line = f"{vendor_item} | {sale_str} | {qty_str} units/mo"
            add_field(f"TopSKU{rank + 1}", line)

    fields_block = "\n".join(fields)
    customer_block = ""
    if customer_id:
        customer_block = f"\n  <holderId>{customer_id}</holderId>\n  <holderType>2</holderType>"
    return f"""<mobiForm>
  <filledFormId>{filled_form_id}</filledFormId>{customer_block}
  <name>Sales Report</name>
  <apiName>SalesReport</apiName>
  <status>PROCESSED</status>
  <formFields>
{fields_block}
  </formFields>
</mobiForm>"""


def update_form(token, filled_form_id, xml_payload, dry_run=False):
    """POST the form update to MobiWork API."""
    if dry_run:
        print(f"  [DRY RUN] Would update form {filled_form_id}")
        return True

    url = (f"{MOBI_BASE_URL}/api/rest/{CLIENT_ID}/{API_VERSION}"
           f"/mobiForm/{filled_form_id}/update.html?token={token}")
    resp = requests.post(url, data=xml_payload, headers=XML_HEADERS, timeout=10)
    if resp.status_code != 200:
        print(f"  Update failed: HTTP {resp.status_code}")
        return False
    root = ET.fromstring(resp.text)
    if root.get("statusCode") != "1":
        error = root.find(".//error")
        msg = error.text if error is not None else "Unknown error"
        print(f"  Update failed: {msg}")
        return False
    return True


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Push location reports to MobiWork forms")
    parser.add_argument("--targets", help="Path to mobiform targets CSV")
    parser.add_argument("--only", action="append",
                        help="Limit to specific customer IDs, form IDs, labels, or location codes (repeatable)")
    parser.add_argument("--snapshot", default="out/location_snapshot.csv",
                        help="Path to location_snapshot.csv")
    parser.add_argument("--top-skus", default="out/top_skus_by_location.csv",
                        help="Path to top_skus_by_location.csv")
    parser.add_argument("--sku-sales", default="out/sales_by_sku_location_month.csv",
                        help="Path to sales_by_sku_location_month.csv (per-SKU $ data)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print what would be sent without actually calling the API")
    args = parser.parse_args()

    # Load data
    snapshot = pd.read_csv(args.snapshot, dtype={"mobi_customer_id": str})
    top_skus = pd.read_csv(args.top_skus, dtype={"mobi_customer_id": str})
    sku_sales = pd.read_csv(args.sku_sales, dtype={"mobi_customer_id": str})

    targets_path = Path(args.targets) if args.targets else TARGETS_PATH
    targets = load_targets(targets_path, args.only)
    print(f"Loaded {len(targets)} targets from {targets_path}")

    # Build lookups by both mobi_customer_id and location_code
    snap_by_mobi = {}
    snap_by_loc = {}
    for _, row in snapshot.iterrows():
        mobi_key = str(row.get("mobi_customer_id", "")).strip()
        loc_key = str(row.get("location_code", "")).strip()
        if mobi_key and mobi_key != "nan":
            snap_by_mobi[mobi_key] = row
        if loc_key:
            snap_by_loc[loc_key] = row

    skus_by_mobi = {}
    skus_by_loc = {}
    if "mobi_customer_id" in top_skus.columns:
        top_skus["_mobi"] = top_skus["mobi_customer_id"].astype(str).str.strip()
        for key, grp in top_skus.groupby("_mobi"):
            if key and key != "nan":
                skus_by_mobi[key] = grp
    if "location_code" in top_skus.columns:
        for key, grp in top_skus.groupby("location_code"):
            skus_by_loc[str(key).strip()] = grp

    # Build SKU sales $ lookup by location_code
    sku_sales_by_loc = {}
    if "location_code" in sku_sales.columns:
        for key, grp in sku_sales.groupby("location_code"):
            sku_sales_by_loc[str(key).strip()] = grp

    if not args.dry_run:
        token = authenticate()
    else:
        token = "DRY_RUN"

    successes = 0
    failures = []

    for i, target in enumerate(targets, 1):
        cust_id = str(target["mobi_customer_id"]).strip()
        # Normalize: strip trailing .0 from float-read IDs
        if cust_id.endswith(".0"):
            cust_id = cust_id[:-2]
        form_id = str(target["filled_form_id"]).strip()
        loc_code = str(target.get("location_code", "")).strip()
        label = target.get("customer_label", loc_code or cust_id)

        print(f"\n[{i}/{len(targets)}] {label} (customer={cust_id}, location={loc_code}, form={form_id})")

        # Try location_code first, fall back to mobi_customer_id
        snap_row = None
        sku_rows = pd.DataFrame()
        if loc_code and loc_code != "nan":
            snap_row = snap_by_loc.get(loc_code)
            sku_rows = skus_by_loc.get(loc_code, pd.DataFrame())
        if snap_row is None:
            snap_row = snap_by_mobi.get(cust_id)
            sku_rows = skus_by_mobi.get(cust_id, sku_rows if not sku_rows.empty else pd.DataFrame())
        if snap_row is None:
            print(f"  Skipped: not found in snapshot (tried location={loc_code}, mobi={cust_id})")
            failures.append(target)
            continue

        # Get SKU sales $ data for this location
        sku_sales_rows = sku_sales_by_loc.get(loc_code, pd.DataFrame())

        xml = build_form_xml(form_id, snap_row, sku_rows, sku_sales_rows, customer_id=cust_id)

        if args.dry_run:
            total_units = int(sku_rows["last_month_qty"].sum()) if not sku_rows.empty and "last_month_qty" in sku_rows.columns else 0
            print(f"  Month: {snap_row.get('report_month')} | Sales: ${snap_row.get('last_month_total', 0):,.2f} | Units: {total_units}")
            print(f"  SKU detail rows: {len(sku_rows)}")

        ok = update_form(token, form_id, xml, dry_run=args.dry_run)
        if ok:
            successes += 1
        else:
            failures.append(target)

    print(f"\nDone: {successes}/{len(targets)} updated, {len(failures)} failed/skipped")


if __name__ == "__main__":
    main()
