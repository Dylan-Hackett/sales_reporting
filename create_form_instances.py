#!/usr/bin/env python3
"""
Create MobiWork form instances for mapped locations.

Reads config/location_map.csv (locations with mobi_customer_id),
creates a SalesReportingForm instance for each via the MobiWork API,
captures the filled_form_id, and writes the updated mobiform_targets.csv.

Usage:
    python create_form_instances.py
    python create_form_instances.py --chain lpq          # only LPQ locations
    python create_form_instances.py --chain jtj --chain imperial
    python create_form_instances.py --dry-run            # preview without API calls
    python create_form_instances.py --limit 5            # create first 5 only (for testing)
"""
import argparse
import os
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from xml.sax.saxutils import escape

import pandas as pd
import requests

# ---------------------------------------------------------------------------
# Auth
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
FORM_ID = os.getenv("MOBI_FORM_ID", "165008")  # SalesReportingForm template ID

XML_HEADERS = {"Content-Type": "application/xml"}


def authenticate():
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
# Form creation
# ---------------------------------------------------------------------------

def create_form_instance(token, mobi_customer_id, customer_name, location_code):
    """Create a new SalesReportingForm instance for a customer.

    Returns the filled_form_id on success, or None on failure.
    """
    # Coerce customer ID to int if whole number
    try:
        cid = float(mobi_customer_id)
        cid = int(cid) if cid == int(cid) else cid
    except (TypeError, ValueError):
        cid = mobi_customer_id

    # Minimal form with just the Customer field set
    form_xml = f"""<mobiForm>
  <formId>{FORM_ID}</formId>
  <name>SalesReportingForm</name>
  <apiName>SalesReportingForm</apiName>
  <status>PROCESSED</status>
  <formFields>
    <formField>
      <fieldType>6</fieldType>
      <apiName>Customer</apiName>
      <index>0</index>
      <name>Customer</name>
      <value>{escape(str(cid))}</value>
    </formField>
  </formFields>
</mobiForm>"""

    url = (f"{MOBI_BASE_URL}/api/rest/{CLIENT_ID}/{API_VERSION}"
           f"/mobiForm/create.html?token={token}")

    try:
        resp = requests.post(url, data=form_xml, headers=XML_HEADERS, timeout=15)
    except Exception as exc:
        print(f"    Error: {exc}")
        return None

    if resp.status_code != 200:
        print(f"    Failed: HTTP {resp.status_code}")
        return None

    root = ET.fromstring(resp.text)
    status_code = root.get("statusCode")

    if status_code != "1":
        error = root.find(".//error")
        msg = error.text if error is not None else "Unknown error"
        print(f"    Failed: {msg}")
        # Print full response for debugging
        print(f"    Response: {resp.text[:300]}")
        return None

    # Extract the filled form ID from response
    filled_id = root.findtext(".//filledFormId")
    if not filled_id:
        # Try alternate locations in the response
        filled_id = root.findtext("filledFormId")
    if not filled_id:
        # Sometimes it's in an id field
        filled_id = root.findtext(".//id")
    if not filled_id:
        print(f"    Created but couldn't extract filled_form_id from response")
        print(f"    Response: {resp.text[:500]}")
        return None

    return filled_id


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Create MobiWork form instances for mapped locations")
    parser.add_argument("--location-map", default="config/location_map.csv",
                        help="Path to location_map.csv")
    parser.add_argument("--targets-out", default="config/mobiform_targets.csv",
                        help="Path to write updated targets CSV")
    parser.add_argument("--chain", action="append",
                        help="Filter to specific chains (repeatable: --chain lpq --chain jtj)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview what would be created without calling the API")
    parser.add_argument("--limit", type=int, default=0,
                        help="Limit to first N locations (for testing)")
    parser.add_argument("--skip-existing", action="store_true", default=True,
                        help="Skip locations that already have a filled_form_id in targets (default: true)")
    args = parser.parse_args()

    # Load location map — only mapped locations
    loc_map = pd.read_csv(args.location_map, dtype=str)
    mapped = loc_map[loc_map["mobi_customer_id"].fillna("").str.strip() != ""].copy()
    print(f"Mapped locations: {len(mapped)}")

    # Filter by chain if specified
    if args.chain:
        mapped = mapped[mapped["chain"].isin(args.chain)]
        print(f"Filtered to chains {args.chain}: {len(mapped)}")

    # Load existing targets to skip already-created forms
    targets_path = Path(args.targets_out)
    existing_targets = pd.DataFrame()
    existing_locs = set()
    if targets_path.exists() and args.skip_existing:
        existing_targets = pd.read_csv(targets_path, dtype=str)
        existing_targets.columns = [c.strip().lower() for c in existing_targets.columns]
        if "location_code" in existing_targets.columns:
            existing_locs = set(existing_targets["location_code"].dropna().str.strip())
        print(f"Existing targets: {len(existing_targets)} ({len(existing_locs)} with location_code)")

    # Filter out already-created
    to_create = mapped[~mapped["location_code"].isin(existing_locs)]
    print(f"New forms to create: {len(to_create)}")

    if args.limit > 0:
        to_create = to_create.head(args.limit)
        print(f"Limited to first {args.limit}")

    if to_create.empty:
        print("Nothing to create.")
        return

    if args.dry_run:
        print(f"\n[DRY RUN] Would create {len(to_create)} form instances:")
        for _, loc in to_create.iterrows():
            print(f"  {loc['location_code']} | {loc['customer_name']} | mobi={loc['mobi_customer_id']}")
        return

    # Authenticate and create forms
    token = authenticate()
    new_targets = []
    failures = []

    for i, (_, loc) in enumerate(to_create.iterrows(), 1):
        code = loc["location_code"]
        mobi_id = loc["mobi_customer_id"]
        name = loc["customer_name"]
        mobi_name = loc.get("mobi_customer_name", "")

        print(f"\n[{i}/{len(to_create)}] {code} - {name} (mobi={mobi_id})")
        filled_id = create_form_instance(token, mobi_id, name, code)

        if filled_id:
            print(f"    Created: filled_form_id={filled_id}")
            new_targets.append({
                "mobi_customer_id": mobi_id,
                "filled_form_id": filled_id,
                "customer_label": mobi_name or name,
                "location_code": code,
                "chain": loc["chain"],
            })
        else:
            failures.append(code)

    # Merge with existing targets and write
    if new_targets:
        new_df = pd.DataFrame(new_targets)
        if not existing_targets.empty:
            # Ensure column compatibility
            for col in new_df.columns:
                if col not in existing_targets.columns:
                    existing_targets[col] = ""
            combined = pd.concat([existing_targets, new_df], ignore_index=True)
        else:
            combined = new_df

        combined.to_csv(targets_path, index=False)
        print(f"\nWrote {targets_path}: {len(combined)} total targets ({len(new_targets)} new)")
    else:
        print("\nNo new targets created.")

    print(f"\nSummary: {len(new_targets)} created, {len(failures)} failed")
    if failures:
        print(f"Failed: {failures}")


if __name__ == "__main__":
    main()
