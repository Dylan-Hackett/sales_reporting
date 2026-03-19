#!/usr/bin/env python3
"""
Delete all SalesReportingForm instances from MobiWork.

Run this before creating new form instances on the redesigned template.
This hits the MobiWork API DELETE endpoint for each filled form ID.

Usage:
    source .env && python delete_old_forms.py
    source .env && python delete_old_forms.py --dry-run
"""
import argparse
import os
import sys
import xml.etree.ElementTree as ET

import pandas as pd
import requests

API_LOGIN_ID = os.getenv("MOBI_API_LOGIN_ID")
API_PASSWORD = os.getenv("MOBI_API_PASSWORD")
CLIENT_ID = os.getenv("MOBI_CLIENT_ID")
API_VERSION = os.getenv("MOBI_API_VERSION", "10.4.109")
MOBI_BASE_URL = os.getenv("MOBI_BASE_URL", "https://platform.mobiwork.com").rstrip("/")
XML_HEADERS = {"Content-Type": "application/xml"}


def authenticate():
    url = f"{MOBI_BASE_URL}/api/rest/{CLIENT_ID}/{API_VERSION}/authenticate.html"
    body = f"""<request>
  <apiLoginId>{API_LOGIN_ID}</apiLoginId>
  <apiPassword>{API_PASSWORD}</apiPassword>
</request>"""
    resp = requests.post(url, data=body, headers=XML_HEADERS, timeout=10)
    root = ET.fromstring(resp.text)
    token = root.findtext("token")
    if not token:
        print("Auth failed", file=sys.stderr)
        sys.exit(1)
    return token


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    # Only delete the 229 forms WE created (from mobiform_targets.csv)
    # Do NOT touch test forms or pre-existing old forms
    form_ids = set()
    targets = pd.read_csv("config/mobiform_targets.csv")
    for fid in targets["filled_form_id"]:
        form_ids.add(str(fid).strip())

    form_ids = sorted(form_ids)
    print(f"Forms to delete: {len(form_ids)}")

    if args.dry_run:
        print("[DRY RUN] Would delete these form IDs:")
        for fid in form_ids:
            print(f"  {fid}")
        return

    token = authenticate()
    deleted = 0
    failed = 0

    for i, fid in enumerate(form_ids, 1):
        url = (f"{MOBI_BASE_URL}/api/rest/{CLIENT_ID}/{API_VERSION}"
               f"/mobiForm/{fid}/delete.html?token={token}")
        try:
            resp = requests.delete(url, headers=XML_HEADERS, timeout=10)
            root = ET.fromstring(resp.text)
            if root.get("statusCode") == "1":
                deleted += 1
            else:
                error = root.find(".//error")
                msg = error.text if error is not None else "Unknown"
                print(f"  [{i}] {fid}: FAILED - {msg}")
                failed += 1
        except Exception as e:
            print(f"  [{i}] {fid}: ERROR - {e}")
            failed += 1

        if i % 50 == 0:
            print(f"  Progress: {i}/{len(form_ids)}")

    print(f"\nDone: {deleted} deleted, {failed} failed")


if __name__ == "__main__":
    main()
