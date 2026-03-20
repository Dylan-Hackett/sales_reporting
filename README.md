# Sales Reporting ETL & MobiWork Push

ETL pipeline that processes Ecologic Solutions sales data from multiple distributors (Imperial, LPQ, J&TJ), generates per-location reports, and pushes to MobiWork customer-linked forms via REST API.

---

## How It Works

1. **ETL** (`run_etl.py`) reads monthly Excel sales reports from `Sales Reports 2025/`, deduplicates across distributors, and outputs per-location summaries.
2. **Push** (`push_to_mobiwork.py`) reads the ETL output and updates MobiWork "Sales Report" forms (template 170770) for each location via the REST API.

Each location gets a form linked to its MobiWork customer record with:
- **Summary panel**: report month, total sales $, total units
- **SKU Detail panel**: top 10 SKUs ranked by 3-month avg quantity, each formatted as `E65-G | $492.84 | 11.3 units/mo`

---

## Repo Layout

```
run_etl.py                  # Run the full ETL pipeline
push_to_mobiwork.py         # Push reports to MobiWork forms
etl/                        # ETL modules (adapters, location mapper, views)
config/
  config.yaml               # Thresholds, SKU variant groups, paths
  customer_map.csv           # Excel customer -> MobiWork customer ID mapping
  sku_aliases.csv            # SKU name normalization
  mobiform_targets_linked.csv  # 169 active customer-linked forms
  mobiwork_customer_locations.xlsx  # Full MobiWork customer export
out/                        # ETL outputs (gitignored)
  location_snapshot.csv     # One row per location, all summary metrics
  top_skus_by_location.csv  # Top SKUs per location with qty data
  sales_by_sku_location_month.csv  # Per-SKU dollar amounts by month
```

---

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install pandas openpyxl pyyaml requests
```

Add MobiWork credentials to `.env`:
```bash
export MOBI_API_LOGIN_ID=...
export MOBI_API_PASSWORD=...
export MOBI_CLIENT_ID=...
```

---

## Running

```bash
source .venv/bin/activate
source .env

# 1. Run ETL
python run_etl.py

# 2. Push to MobiWork (all 169 linked locations)
python push_to_mobiwork.py --targets config/mobiform_targets_linked.csv

# Push a single location
python push_to_mobiwork.py --targets config/mobiform_targets_linked.csv --only LPQ100

# Dry run (no API calls)
python push_to_mobiwork.py --targets config/mobiform_targets_linked.csv --dry-run
```

---

## Current Coverage

- **169 locations** with customer-linked forms, actively receiving data pushes
- **60 locations** still need new MobiWork customer records (shared IDs)
- **~655 locations** completely unmapped (62.3% of total revenue)

---

## Data Sources

- **Imperial**: 14 monthly `.xlsx` files (Jan 2025 - Feb 2026, Sep missing), 506 customer codes
- **LPQ 2025.xlsx**: Le Pain Quotidien, 36 location codes
- **2025 J&TJ.xlsx**: Joe & The Juice, 40+ location codes
- LPQ/J&TJ overlap with Imperial — ETL deduplicates automatically
