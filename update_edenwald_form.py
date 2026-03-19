#!/usr/bin/env python3
"""
Update existing MobiWork form with Edenwald Senior Living data.
The script fills the SalesReportingForm with the latest ETL outputs.
"""
import argparse
import math
import os
import re
import sys
import xml.etree.ElementTree as ET
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from xml.sax.saxutils import escape

import pandas as pd
import requests
import yaml

# MobiWork API Configuration
def require_env(name, description):
    value = os.getenv(name)
    if not value:
        print(f"✗ Missing environment variable {name} ({description}).", file=sys.stderr)
        print("  Create a .env file (see .env.example) or export the variable before running.", file=sys.stderr)
        sys.exit(1)
    return value


API_LOGIN_ID = require_env("MOBI_API_LOGIN_ID", "MobiWork API login ID")
API_PASSWORD = require_env("MOBI_API_PASSWORD", "MobiWork API password")
CLIENT_ID = require_env("MOBI_CLIENT_ID", "MobiWork client ID")
API_VERSION = os.getenv("MOBI_API_VERSION", "10.4.109")
FORM_ID = os.getenv("MOBI_FORM_ID", "165008")  # SalesReportingForm template ID
FILLED_FORM_ID = os.getenv("MOBI_FILLED_FORM_ID", "145683277")  # Default filled form instance
MOBI_BASE_URL = os.getenv("MOBI_BASE_URL", "https://platform.mobiwork.com").rstrip("/")

TARGETS_PATH = Path(os.getenv("MOBIFORM_TARGETS", "config/mobiform_targets.csv"))
CONFIG_PATH = Path("config/config.yaml")
XML_HEADERS = {"Content-Type": "application/xml"}


def norm(value):
    return re.sub(r"[^A-Z0-9]+", " ", str(value).strip().upper()).strip()


def load_cfg(path: Path):
    if path.exists():
        with open(path, "r", encoding="utf-8") as fh:
            return yaml.safe_load(fh) or {}
    return {}


def build_variant_lookup(cfg):
    groups = cfg.get("sku_variant_groups") or {}
    lookup = {}
    for group_name, variants in groups.items():
        group_key = norm(group_name)
        lookup[group_key] = group_key
        if isinstance(variants, list):
            for variant in variants:
                lookup[norm(variant)] = group_key
    return lookup


def coerce_customer_id(value):
    if pd.isna(value):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        try:
            return float(str(value).strip())
        except (TypeError, ValueError):
            return None


def customer_key(value):
    coerced = coerce_customer_id(value)
    if coerced is None:
        return ""
    if float(coerced).is_integer():
        return str(int(round(coerced)))
    return str(coerced)


def load_targets(path: Path, only=None):
    if not path.exists():
        print(f"✗ Target list not found: {path}", file=sys.stderr)
        sys.exit(1)

    df = pd.read_csv(path)
    if df.empty:
        print(f"✗ No targets defined in {path}", file=sys.stderr)
        sys.exit(1)

    df.columns = [c.strip().lower() for c in df.columns]
    required = {"mobi_customer_id", "filled_form_id"}
    missing = required - set(df.columns)
    if missing:
        print(f"✗ Target file missing columns: {', '.join(sorted(missing))}", file=sys.stderr)
        sys.exit(1)

    df["mobi_customer_id"] = df["mobi_customer_id"].apply(coerce_customer_id)
    if df["mobi_customer_id"].isnull().any():
        bad = df[df["mobi_customer_id"].isnull()]
        print("✗ Invalid mobi_customer_id values in target file:", file=sys.stderr)
        print(bad.to_string(index=False), file=sys.stderr)
        sys.exit(1)

    df["filled_form_id"] = df["filled_form_id"].astype(str).str.strip()
    label_col = "customer_label" if "customer_label" in df.columns else None

    if only:
        selectors = {str(item).strip().lower() for item in only if str(item).strip()}

        def matches(row):
            candidates = {
                customer_key(row["mobi_customer_id"]).lower(),
                row["filled_form_id"].lower(),
            }
            if label_col:
                label_val = str(row[label_col]).strip().lower()
                if label_val:
                    candidates.add(label_val)
            return any(sel in candidates for sel in selectors)

        df = df[df.apply(matches, axis=1)]
        if df.empty:
            print(f"✗ --only filter {sorted(selectors)} matched no targets", file=sys.stderr)
            sys.exit(1)

    records = df.to_dict("records")
    for rec in records:
        rec.setdefault("customer_label", "")
        rec["customer_label"] = str(rec.get("customer_label") or "").strip()
        rec["mobi_customer_id"] = float(rec["mobi_customer_id"])
        rec["customer_key"] = customer_key(rec["mobi_customer_id"])
        if not rec["filled_form_id"] and FILLED_FORM_ID:
            rec["filled_form_id"] = FILLED_FORM_ID
    return records


def authenticate():
    print("\n1. AUTHENTICATING...")
    auth_url = f"{MOBI_BASE_URL}/api/rest/{CLIENT_ID}/{API_VERSION}/authenticate.html"
    auth_data = f"""<request>
  <apiLoginId>{API_LOGIN_ID}</apiLoginId>
  <apiPassword>{API_PASSWORD}</apiPassword>
</request>"""

    try:
        response = requests.post(auth_url, data=auth_data, headers=XML_HEADERS, timeout=10)
    except Exception as exc:
        print(f"   ✗ Authentication error: {exc}")
        sys.exit(1)

    if response.status_code != 200:
        print(f"   ✗ Authentication failed: {response.status_code}")
        sys.exit(1)

    root = ET.fromstring(response.text)
    token = root.findtext("token")
    if not token:
        print("   ✗ Authentication failed: token not found in response")
        sys.exit(1)

    print("   ✓ Authentication successful!")
    return token


def update_single_customer(
    *,
    target,
    parent_df,
    top_skus_df,
    sku_group_lookup,
    token,
    total,
    index,
    top_sku_limit,
):
    label = target.get("customer_label") or f"Customer {target['customer_key']}"
    customer_id = target["mobi_customer_id"]
    customer_key_value = target["customer_key"]
    filled_form_id = str(target.get("filled_form_id") or "").strip() or FILLED_FORM_ID

    if not filled_form_id:
        print(f"[{index}/{total}] ✗ {label}: Missing filled_form_id")
        return False

    parent_rows = parent_df[parent_df["_customer_key"] == customer_key_value]
    if parent_rows.empty:
        print(f"[{index}/{total}] ✗ {label}: Not found in parent_snapshot.csv")
        return False

    parent_row = parent_rows.iloc[0]
    customer_skus = top_skus_df[top_skus_df["_customer_key"] == customer_key_value].reset_index(drop=True)

    mp_sku = parent_row.get("Most Popular SKU")
    if pd.isna(mp_sku):
        mp_sku = ""

    def fmt_qty(val):
        if pd.isna(val):
            return ""
        if isinstance(val, str):
            return val
        if math.isclose(val, round(val)):
            return str(int(round(val)))
        return f"{val:.2f}".rstrip("0").rstrip(".")

    def fmt_int(value):
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

    summary_last_month = fmt_int(parent_row.get("Last Month Qty"))
    summary_avg_value = None

    mp_group_key = sku_group_lookup.get(norm(mp_sku), norm(mp_sku))
    if not customer_skus.empty:
        grouped = customer_skus.copy()
        grouped["sku_group_key"] = grouped["SKU"].map(lambda s: sku_group_lookup.get(norm(s), norm(s)))
        group_rows = grouped[grouped["sku_group_key"] == mp_group_key]
        if not group_rows.empty:
            if "6M Avg Qty" in group_rows:
                summary_avg_value = float(group_rows["6M Avg Qty"].sum())
            elif "3M Avg Qty" in group_rows:
                summary_avg_value = float(group_rows["3M Avg Qty"].sum())

    if summary_avg_value is None:
        parent_avg = parent_row.get("6-Month Avg")
        if parent_avg is not None and not pd.isna(parent_avg):
            summary_avg_value = float(parent_avg)
    if summary_avg_value is None and "6M Avg Qty" in customer_skus:
        summary_avg_value = float(customer_skus["6M Avg Qty"].sum())
    if summary_avg_value is None and "3M Avg Qty" in customer_skus:
        summary_avg_value = float(customer_skus["3M Avg Qty"].sum())

    summary_avg_month = fmt_int(summary_avg_value)
    customer_id_int = int(round(customer_id)) if float(customer_id).is_integer() else customer_id
    if float(customer_id).is_integer():
        customer_field_value = int(round(customer_id))
    else:
        customer_field_value = customer_key_value

    print("\n" + "=" * 80)
    print(f"[{index}/{total}] UPDATE MOBIWORK FORM - {label}")
    print("=" * 80)
    print("\nData to update:")
    print(f"  Customer: {customer_id_int}")
    print(f"  Agreement Level: {parent_row.get('Agreement Level', '')}")
    print(f"  Visit Frequency: {parent_row.get('Visit Frequency', '')}")
    print(f"  Most Popular SKU: {mp_sku}")
    print(f"  Last Month Qty: {summary_last_month or 'unavailable'}")
    avg_label = "Avg Monthly Qty (6M window)"
    if mp_group_key and mp_group_key != norm(mp_sku):
        avg_label += f" ({mp_group_key} variants)"
    print(f"  {avg_label}: {summary_avg_month or 'unavailable'}")
    print(f"  Report Period: {parent_row.get('ReportPeriod', '')}")

    fields_xml = []

    field_meta = {
        "Customer": (1, "Customer"),
        "AgreementLevel": (6, "Agreement Level"),
        "VisitFrequency": (1, "Visit Frequency"),
        "MostPopularSKU": (1, "Most Popular SKU"),
        "LastMonthQty": (2, "Last Month Qty"),
        "MonthTotal": (2, "Avg monthly"),
        "SKU": (1, "SKU"),
        "ItemName": (1, "Item Name"),
        "LastMonthQty1": (2, "Last Month Qty"),
        "LastMonthQty2": (2, "Last Month Qty"),
        "ExpectedQtyPerMonth": (2, "Expected Qty Per Month"),
    }

    def add_field(api_name, value, index=0):
        if value is None:
            return
        value_str = str(value).strip()
        if value_str == "":
            return
        field_type, label_name = field_meta.get(api_name, (1, api_name))
        fields_xml.append(
            f"""    <formField>
      <fieldType>{field_type}</fieldType>
      <apiName>{api_name}</apiName>
      <index>{index}</index>
      <name>{escape(label_name)}</name>
      <value>{escape(value_str)}</value>
    </formField>"""
        )

    add_field("Customer", customer_field_value)
    add_field("AgreementLevel", parent_row.get("Agreement Level"))
    add_field("VisitFrequency", parent_row.get("Visit Frequency"))
    add_field("MostPopularSKU", mp_sku)
    add_field("LastMonthQty", summary_last_month)
    add_field("MonthTotal", summary_avg_month)

    for idx, row in customer_skus.head(top_sku_limit).iterrows():
        add_field("SKU", row.get("SKU", ""), idx)
        add_field("ItemName", row.get("Item Name", ""), idx)
        add_field("LastMonthQty1", fmt_qty(row.get("Last Month Qty")), idx)

        exp_val = None
        if "3M Avg Qty" in row.index:
            exp_val = fmt_qty(row.get("3M Avg Qty"))
        elif "6M Avg Qty" in row.index:
            exp_val = fmt_qty(row.get("6M Avg Qty"))
        if not exp_val and "Expected Qty/Month" in row.index:
            exp_val = fmt_qty(row.get("Expected Qty/Month"))
        if exp_val:
            add_field("ExpectedQtyPerMonth", exp_val, idx)

    fields_block = "\n".join(fields_xml)

    form_xml = f"""<mobiForm>
  <filledFormId>{filled_form_id}</filledFormId>
  <name>SalesReportingForm</name>
  <apiName>SalesReportingForm</apiName>
  <status>PROCESSED</status>
  <formFields>
{fields_block}
  </formFields>
</mobiForm>"""

    update_url = f"{MOBI_BASE_URL}/api/rest/{CLIENT_ID}/{API_VERSION}/mobiForm/{filled_form_id}/update.html?token={token}"
    print(f"\n2. UPDATING FILLED FORM ID: {filled_form_id}...")
    print(f"   Endpoint: /api/rest/{CLIENT_ID}/{API_VERSION}/mobiForm/{filled_form_id}/update.html")

    try:
        response = requests.post(update_url, data=form_xml, headers=XML_HEADERS, timeout=10)
    except Exception as exc:
        print(f"   ✗ Error sending form update: {exc}")
        return False

    if response.status_code != 200:
        print(f"   ✗ Update failed: HTTP {response.status_code}")
        return False

    root = ET.fromstring(response.text)
    status_code = root.get("statusCode")
    if status_code != "1":
        error = root.find(".//error")
        message = error.text if error is not None else "Unknown error"
        print(f"   ✗ Update failed: {message}")
        return False

    print("   ✓✓✓ SUCCESS! Form updated in MobiWork!")
    print("\n   Updated fields:")
    print(f"     - Agreement Level: {parent_row.get('Agreement Level', '')}")
    print(f"     - Visit Frequency: {parent_row.get('Visit Frequency', '')}")
    print(f"     - Most Popular SKU: {mp_sku or 'n/a'}")
    print(f"     - Last Month Qty: {summary_last_month or 'unavailable'}")
    print(f"     - Avg Monthly Qty: {summary_avg_month or 'unavailable'}")
    print("=" * 80)
    return True


def main(args):
    cfg = load_cfg(CONFIG_PATH)
    sku_group_lookup = build_variant_lookup(cfg)
    top_sku_limit = int(cfg.get("top_n", 10))

    try:
        parent = pd.read_csv("out/parent_snapshot.csv")
        top_skus = pd.read_csv("out/top_skus_lines.csv")
    except FileNotFoundError as exc:
        print(f"✗ Missing ETL output: {exc}")
        print("  Run etl_salesreport_detail.py first.")
        sys.exit(1)

    parent["_customer_key"] = parent["Customer"].apply(customer_key)
    top_skus["_customer_key"] = top_skus["Customer"].apply(customer_key)

    targets_path = Path(args.targets).expanduser() if args.targets else TARGETS_PATH
    targets = load_targets(targets_path, args.only)
    total = len(targets)

    print(f"\nLoaded {total} target{'s' if total != 1 else ''} from {targets_path}")
    token = authenticate()

    successes = 0
    failures = []

    for idx, target in enumerate(targets, start=1):
        ok = update_single_customer(
            target=target,
            parent_df=parent,
            top_skus_df=top_skus,
            sku_group_lookup=sku_group_lookup,
            token=token,
            total=total,
            index=idx,
            top_sku_limit=top_sku_limit,
        )
        if ok:
            successes += 1
        else:
            failures.append(target)

    print("\nSummary")
    print("-------")
    print(f"  Updated: {successes}/{total}")
    if failures:
        print("  Skipped:")
        for target in failures:
            label = target.get("customer_label") or target["customer_key"]
            print(
                f"    - {label} (customer {target['customer_key']}, form {target.get('filled_form_id', '?')})"
            )
    else:
        print("  Skipped: none")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--targets",
        help="Path to the mobiform targets CSV (default: config/mobiform_targets.csv)",
    )
    parser.add_argument(
        "--only",
        action="append",
        help="Limit the update to specific customer IDs, form IDs, or labels (repeatable)",
    )
    main(parser.parse_args())
