#!/usr/bin/env python3
"""
Update TierLevel custom field for all customers in MobiWork
Reads from parent_snapshot.csv and updates customer records via API
"""
import os
import sys
import xml.etree.ElementTree as ET

import pandas as pd
import requests


def require_env(name, description):
    value = os.getenv(name)
    if not value:
        print(f"✗ Missing environment variable {name} ({description}).", file=sys.stderr)
        print("  Create a .env file (see .env.example) or export the variable before running.", file=sys.stderr)
        sys.exit(1)
    return value


# MobiWork API Configuration (loaded from environment)
API_LOGIN_ID = require_env("MOBI_API_LOGIN_ID", "MobiWork API login ID")
API_PASSWORD = require_env("MOBI_API_PASSWORD", "MobiWork API password")
CLIENT_ID = require_env("MOBI_CLIENT_ID", "MobiWork client ID")
API_VERSION = os.getenv("MOBI_API_VERSION", "10.4.109")
MOBI_BASE_URL = os.getenv("MOBI_BASE_URL", "https://platform.mobiwork.com").rstrip("/")


def authenticate():
    """Authenticate with MobiWork API and return token"""
    auth_url = f"{MOBI_BASE_URL}/api/rest/{CLIENT_ID}/{API_VERSION}/authenticate.html"
    auth_data = f"""<request>
  <apiLoginId>{API_LOGIN_ID}</apiLoginId>
  <apiPassword>{API_PASSWORD}</apiPassword>
</request>"""

    headers = {'Content-Type': 'application/xml'}

    try:
        response = requests.post(auth_url, data=auth_data, headers=headers, timeout=10)
        if response.status_code == 200:
            root = ET.fromstring(response.text)
            token = root.find('token').text
            return token
        else:
            print(f"✗ Authentication failed: {response.status_code}")
            return None
    except Exception as e:
        print(f"✗ Authentication error: {e}")
        return None


def update_customer_tier(token, customer_id, tier_level, visit_frequency):
    """Update a customer's TierLevel custom field"""

    customer_xml = f"""<request>
  <customer>
    <customFields>
      <customField>
        <apiName>TierLevel</apiName>
        <index>0</index>
        <value>{tier_level}</value>
      </customField>
    </customFields>
  </customer>
</request>"""

    url = f"{MOBI_BASE_URL}/api/rest/{CLIENT_ID}/{API_VERSION}/customer/{customer_id}/edit.html?token={token}"
    headers = {'Content-Type': 'application/xml'}

    try:
        response = requests.post(url, data=customer_xml, headers=headers, timeout=10)

        if response.status_code == 200:
            resp_root = ET.fromstring(response.text)
            status_code = resp_root.get('statusCode')

            if status_code == '1':
                return True, "Success"
            else:
                error = resp_root.find('.//error')
                error_code = error.get('code') if error is not None else "Unknown"
                error_text = error.text if error is not None and error.text else f"Error code {error_code}"
                return False, error_text
        else:
            return False, f"HTTP {response.status_code}"
    except Exception as e:
        return False, str(e)


def main():
    print("=" * 80)
    print("MOBIWORK CUSTOMER TIER UPDATE - BULK OPERATION")
    print("=" * 80)

    # Read the output data
    try:
        parent = pd.read_csv('out/parent_snapshot.csv')
    except FileNotFoundError:
        print("✗ Error: out/parent_snapshot.csv not found")
        print("  Please run etl_salesreport_detail.py first to generate the data")
        sys.exit(1)

    # Filter to only customers that have MobiWork Customer IDs and tier data
    customers_to_update = parent[
        parent['Customer'].notna() &
        parent['Agreement Level'].notna()
    ]

    if customers_to_update.empty:
        print("✗ No customers found with MobiWork Customer IDs and tier data")
        print("  Please map customers in config/customer_map.csv")
        sys.exit(1)

    print(f"\nFound {len(customers_to_update)} customers to update")
    print(f"Total customers in data: {len(parent)}")

    # Show preview
    print("\nCustomers to update:")
    for _, row in customers_to_update.head(5).iterrows():
        customer_id = int(row['Customer'])
        customer_key = row['customer_key']
        tier = row['Agreement Level']
        freq = row['Visit Frequency']
        print(f"  - {customer_key}")
        print(f"    ID: {customer_id} | Tier: {tier} | Frequency: {freq}")

    if len(customers_to_update) > 5:
        print(f"  ... and {len(customers_to_update) - 5} more")

    # Confirm
    response = input(f"\nProceed with updating {len(customers_to_update)} customer records? (yes/no): ")
    if response.lower() not in ['yes', 'y']:
        print("Cancelled.")
        sys.exit(0)

    # Authenticate
    print("\n1. AUTHENTICATING...")
    token = authenticate()
    if not token:
        sys.exit(1)
    print(f"   ✓ Authentication successful!")

    # Update each customer
    print(f"\n2. UPDATING {len(customers_to_update)} CUSTOMERS...")
    success_count = 0
    error_count = 0
    errors = []

    for idx, row in customers_to_update.iterrows():
        customer_id = int(row['Customer'])
        customer_key = row['customer_key']
        tier_level = row['Agreement Level']
        visit_frequency = row['Visit Frequency']

        print(f"   [{success_count + error_count + 1}/{len(customers_to_update)}] Customer {customer_id}: {tier_level}...", end=" ")

        success, message = update_customer_tier(token, customer_id, tier_level, visit_frequency)

        if success:
            print(f"✓")
            success_count += 1
        else:
            print(f"✗ {message}")
            error_count += 1
            errors.append({
                'customer_id': customer_id,
                'customer_key': customer_key,
                'error': message
            })

    # Summary
    print("\n" + "=" * 80)
    print("UPDATE SUMMARY")
    print("=" * 80)
    print(f"Total attempted: {len(customers_to_update)}")
    print(f"Successful: {success_count}")
    print(f"Failed: {error_count}")

    if errors:
        print("\nFailed Updates:")
        for err in errors:
            print(f"  - Customer {err['customer_id']}: {err['error']}")

    print("=" * 80)


if __name__ == "__main__":
    main()
