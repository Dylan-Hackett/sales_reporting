# Sales Reporting ETL & MobiWork Tools

Scripts for transforming Acme sales exports into actionable summaries and updating MobiWork assets.

## Project Structure

- `etl_salesreport_detail.py` – main ETL: ingests Excel detail, normalizes customers/SKUs, enriches with configuration lookups, and writes:
  - `out/parent_snapshot.csv`: one row per customer with agreement tier, visit cadence, most-popular SKU (variant aware), and volume metrics.
  - `out/top_skus_lines.csv`: repeating rows for top-N SKUs per customer with monthly quantities, 3M, and 6M averages.
- `update_customer_tiers.py` – reads the parent snapshot and updates the TierLevel custom field in MobiWork for mapped customers.
- `update_edenwald_form.py` – fills a specific SalesReporting MobiForm with Edenwald summary + top SKUs.
- `config/` – YAML + CSV lookups controlling thresholds, mappings, SKU aliases, expected quantities, and SKU variant groups.
- `CUSTOMER_MAPPING_GUIDE.md` – instructions for maintaining the customer map.
- `inbox/` – optional drop zone for new Excel files.
- `out/` – generated output files (gitignored by default).

## Environment Setup

1. **Python environment**
   ```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt  # if you have one; otherwise install pandas, requests, pyyaml
   ```

2. **Secrets**

   Copy the template to a private `.env` (gitignored) and fill in your MobiWork credentials:
   ```bash
   cp .env.example .env
   ```
   Edit `.env` with:
   ```bash
   export MOBI_API_LOGIN_ID=...
   export MOBI_API_PASSWORD=...
   export MOBI_CLIENT_ID=...
   # optional overrides (API version, base URL, form ids, target customer id)
   ```

   Load them in your shell before running scripts:
   ```bash
   source .env
   ```

## Running the ETL

```bash
source .venv/bin/activate
source .env              # load credentials if needed downstream
.venv/bin/python etl_salesreport_detail.py --file "Acme_Sales_2025 w prices (1) (1).xlsx" --detail-sheet "Acme_Sales_2025_with_Updated_Pr"
```

Options:
- `--file` path defaults to latest `*.xlsx` in cwd/inbox.
- `--detail-sheet` auto-detected if omitted.
- `--report-month` override the auto-detected latest month.
- `--config` alternate config file.
- `--top-n` change the number of SKUs per customer.

Outputs land in `out/parent_snapshot.csv` and `out/top_skus_lines.csv`; unmapped customers are listed in `out/unmapped_customers.csv`.

## Updating MobiWork

### Customer Tiers

```bash
source .venv/bin/activate
source .env
.venv/bin/python update_customer_tiers.py
```

The script previews up to five customers, asks for confirmation, and then updates the TierLevel custom field for each mapped customer.

### Edenwald SalesReporting Form

```bash
source .venv/bin/activate
source .env
.venv/bin/python update_edenwald_form.py
```

This pulls Edenwald’s metrics from the ETL outputs, aggregates SKU variants (e.g., `E69` family), and pushes summary + top SKUs into the existing filled form.

## Configuration Notes

- `config/config.yaml` controls:
  - SLA thresholds and visit cadence.
  - `top_n` SKU count.
  - Paths to customer map, SKU aliases, expected quantity lookups.
  - `sku_variant_groups`: declare equivalent SKUs (e.g., `E69-5`, `E69-G`) so the ETL aggregates them for “Most Popular SKU” metrics.
- `config/customer_map.csv`: maps normalized Excel customer/ship-to keys to MobiWork IDs. Maintain using the provided template.
- `config/sku_aliases.csv`: optional Excel SKU → canonical SKU mapping prior to aggregation.
- `config/expected_qty.csv`: optional expected monthly quantity for comparison.

## Git & Deploy

- `.env` is gitignored; never commit real credentials. Share `README.md`, `.env.example`, and config templates instead.
- Main branch is synced to `https://github.com/Dylan-Hackett/sales_reporting`; push via `git push origin main` once changes are committed.

## Troubleshooting

- **Missing credentials:** Scripts exit with a clear message if required environment variables are absent.
- **New Excel layout:** Verify column headers match the ETL expectations; update `rename` mapping in `etl_salesreport_detail.py` if Acme changes names.
- **Unmapped customers:** Check `out/unmapped_customers.csv` and update `config/customer_map.csv` accordingly.
- **API errors (1004/1006):** Typically indicate invalid IDs or payload structure; ensure customer IDs match MobiWork’s internal IDs and that form field `apiName`s align with your MobiWork configuration.
