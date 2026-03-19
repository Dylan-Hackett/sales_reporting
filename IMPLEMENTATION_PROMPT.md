# Sales Reporting System - Implementation Prompt

Drop this into a fresh Claude Code chat in `/Users/dylanhackett/sales_reporting` with auto-accept permissions.

---

## The Task

Build a new per-location sales reporting ETL pipeline. The existing system (`etl_salesreport_detail.py` and `update_edenwald_form.py`) aggregates to customer-level and pushes to MobiWork forms. The new system needs to work at the **location level** (each store in a chain gets its own data) and produce three report views. Leave the existing files untouched.

## Data Sources

All in `Sales Reports 2025 /` directory:

### 1. Imperial Monthly Reports (`Sales Reports 2025 /Imperial /`)
14 .xlsx files, one per month (Jan 2025 - Feb 2026, Sep 2025 missing). Raw transaction data from Ecologic Solutions via Imperial Bag & Paper distributor. 506 unique customer codes, 200+ locations.

**File format:**
- Header starts at row 4 (rows 0-3 are title block). Use `header=4` when reading.
- Column names have `\n` characters in them: `Ship\nQty`, `Net\nPrice`, `Record\nType`, `Customer\nName`, `Invoice\nDate`, `G/P\nPct`, etc. Normalize these immediately.
- Filter to `Record Type == 1` only (type 2 = subtotal rows where Customer Name == "* Item Total *")
- The `Loc` column is warehouse/distribution center codes (CACI, CAST, etc.), NOT customer location
- The `Customer` column IS the location identifier (e.g. WYT001, PP30835, LPQ201)
- October has both `.xls` and `.xlsx` - use `.xlsx` only, skip the `.xls`
- Key columns after normalization: vendor_item, ibp_item, item_description, loc (warehouse), invoice_num, invoice_date, customer (location code), customer_name, address1, address2, city, state, zipcode, ship_qty, um, net_price, unit_cost, total, gp_pct, company, record_type, sourcefile

### 2. LPQ 2025.xlsx (Le Pain Quotidien)
- Sheet `LPQ_All_Purchases` = detail transactions, same column format as Imperial
- Has a `Location` column with real location codes (LPQ100, LPQ114, LEP0635, etc.)
- Pre-aggregated pivot sheets exist (`Qty_by_Location_Month`, `Sales$_by_Location_Month`) - use these for validation only
- 36 location codes, 40+ SKU codes
- **Important:** LPQ addresses in Imperial data all point to the commissary (56 West 22nd St, NYC), NOT actual store locations. Use the Location column from this file to get real codes.

### 3. 2025 J&TJ.xlsx (Joe & The Juice)
- Sheet `Joe_Juice_All_Purchases` = detail transactions
- Same structure and caveats as LPQ
- Location codes: JOE/JTJ prefixed (JOE0010, JOE08, JTJ067, etc.)
- 40+ location codes
- J&TJ addresses in Imperial all point to 110 Greene St, NYC (commissary)

### Data Overlap
LPQ and J&TJ detail sheets contain the SAME invoices as the Imperial monthly files. **Deduplicate by using LPQ/J&TJ files for those chains, and filtering Imperial to exclude any customer codes that appear in LPQ/J&TJ data.**

### Missing Data
September 2025 is missing from all sources. Rolling averages should divide by actual months with data, not calendar months.

## What to Build

### File Structure
```
etl/
  __init__.py
  adapters.py          # Source adapters
  location_mapper.py   # Location mapping
  views.py             # Report view generation
run_etl.py             # Main entry point
push_to_mobiwork.py    # MobiWork form updater (later)
config/
  location_map.csv     # Generated: source_code → mobi mapping
```

### Stage 1: Source Adapters (`etl/adapters.py`)

Three functions, each returning a standardized pandas DataFrame with these columns:
- `source` (str): "imperial", "lpq", or "jtj"
- `vendor_item` (str): SKU/product code
- `item_description` (str): product name
- `location_code` (str): customer/location identifier code
- `customer_name` (str): full customer name
- `address` (str): concatenated address
- `city`, `state`, `zip` (str)
- `invoice_date` (datetime)
- `invoice_num` (str): invoice number for dedup verification
- `yyyymm` (str): "2025-04" format
- `qty` (int/float): ship quantity
- `unit_price` (float): net price
- `total` (float): total dollar amount

**`ingest_imperial(data_dir)`**: Glob all .xlsx files in the Imperial subdirectory. For each file, read with header=4, normalize column names (strip `\n`, lowercase, snake_case), filter Record Type==1, map to standardized columns. Concatenate all months.

**`ingest_lpq(data_dir)`**: Read LPQ_All_Purchases sheet from LPQ 2025.xlsx. Same normalization and filtering. Use the Location column as location_code.

**`ingest_jtj(data_dir)`**: Read Joe_Juice_All_Purchases sheet from 2025 J&TJ.xlsx. Same as LPQ.

**`load_all(data_dir)`**: Call all three, concatenate. Then deduplicate: get the set of location_codes from LPQ and J&TJ results, filter Imperial to exclude those codes. Return unified DataFrame.

### Stage 2: Location Mapping (`etl/location_mapper.py`)

**`build_location_map(transactions_df, mobiwork_ref_path)`**:
- Extract unique (location_code, customer_name, address, city, state, zip) from the unified transaction data. Imperial addresses are 99.7% consistent across months - just take the most frequent address per code.
- Load `config/mobiwork_customer_locations.xlsx` (2,680 MobiWork customer entries, 59 LPQ, 105 J&TJ already mapped)
- Match by name/address to get mobi_customer_id
- Output `config/location_map.csv` with columns: location_code, chain, customer_name, address, city, state, zip, mobi_customer_id, mobi_customer_name
- Output `out/unmapped_locations.csv` for any that couldn't be matched
- This doesn't need to be perfect on first run - the CSV can be manually edited

### Stage 3: Report Views (`etl/views.py`)

Three output CSVs, all per location:

**View 1: `out/sales_by_location_month.csv`**
- Group by (location_code, yyyymm), sum total
- Pivot to wide format: one row per location, one column per month
- Include location metadata (customer_name, address, chain, mobi_customer_id from location_map)

**View 2: `out/sales_by_sku_location_month.csv`**
- Group by (location_code, vendor_item, item_description, yyyymm), sum total
- Pivot wide: months as columns
- Include location metadata

**View 3: `out/qty_by_sku_location_month.csv`**
- Same grouping as View 2, but sum qty instead of total
- Pivot wide

Also generate a **location snapshot** (`out/location_snapshot.csv`):
- One row per location
- Rolling metrics: last month total $, 3-month avg monthly $, 6-month avg monthly $
- SLA tier based on 3-month avg spend (from config/config.yaml thresholds: Tier 1 $850+, Tier 2 $600-849, Tier 3 $300-599, Tier 4 <$300)
- Visit frequency mapped from tier
- Most popular SKU (by 3-month quantity). Use SKU variant groups from config.yaml to group variants.
- Port the rolling window logic from `etl_salesreport_detail.py` (functions: `window3()`, `window6()`, `build_sku_group_lookup()`, SLA threshold logic)

### Stage 4: Main Entry Point (`run_etl.py`)

```python
# CLI interface
# --data-dir: path to "Sales Reports 2025 /" directory (default: "Sales Reports 2025 /")
# --config: path to config.yaml (default: config/config.yaml)
# --out: output directory (default: out/)
# --report-month: YYYY-MM format, or auto-detect latest month in data
```

Chain stages 1-3. Print summary stats: row counts, unique locations, date range, any unmapped locations.

### Stage 5: MobiWork Push (later - just scaffold for now)

Create `push_to_mobiwork.py` as a scaffold that:
- Reads the new output CSVs
- Has the same auth pattern as `update_edenwald_form.py` (reads .env for API_LOGIN_ID, API_PASSWORD, CLIENT_ID)
- Has placeholder for XML payload building (the actual field names depend on MobiWork form design, which we'll do via Claude for Chrome)
- Reads targets from `config/mobiform_targets.csv`
- Has `--only` flag for single-location testing

## Reference: Existing Code to Port From

`etl_salesreport_detail.py` contains:
- `norm()` - string normalizer for joining/keys
- `window3()`, `window6()` - rolling window month set generators
- `build_sku_group_lookup()` - maps SKU variants to group names using config.yaml
- SLA tier calculation from 3-month avg spend
- Top N SKUs per customer logic

`update_edenwald_form.py` contains:
- MobiWork API auth flow
- XML payload construction for form fields
- POST to `/api/rest/.../mobiForm/{filledFormId}/update.html`
- Response parsing (status code 1 = success)

`config/config.yaml` has SLA thresholds, visit frequency map, SKU variant groups, top_n setting.

## Build Order

1. Create `etl/` directory and `__init__.py`
2. Build `etl/adapters.py` with all three adapters + `load_all()`
3. Test adapters: run them, print row counts, unique locations, date ranges, verify no dupes
4. Build `etl/location_mapper.py`
5. Build `etl/views.py` with all three views + location snapshot
6. Build `run_etl.py` main entry point
7. Run end-to-end, validate View 1 and View 3 against LPQ/J&TJ pre-built pivot sheets
8. Scaffold `push_to_mobiwork.py`

## Verification Steps

After building, run the full pipeline and check:
1. Adapter row counts make sense (should be ~30k+ total transactions)
2. 506 unique location codes from Imperial (minus LPQ/J&TJ overlap)
3. No duplicate invoices between sources after dedup
4. Dollar totals in View 1 match the pre-built `Sales$_by_Location_Month` sheets in LPQ/J&TJ files
5. Quantity totals in View 3 match `Qty_by_Location_Month` sheets
6. Location snapshot SLA tiers look reasonable
7. Spot-check a few locations' monthly totals against source Excel files
