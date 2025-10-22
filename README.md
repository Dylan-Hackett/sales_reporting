# Sales Reporting ETL & MobiWork Uploads

This repo turns Acme’s monthly Excel sales export into a clean summary and then updates MobiWork forms with the results. The goal of this README is to explain the process from the ground up, so even if you’ve never touched ETL or APIs you can follow along.

---

## ETL and APIs in Plain English

- **ETL stands for Extract → Transform → Load.**
  1. **Extract** – read data from a source (our Excel workbook).
  2. **Transform** – clean it up, regroup it, and calculate new metrics.
  3. **Load** – save the transformed data somewhere else, or hand it to another system.

- **An API (Application Programming Interface)** is a set of rules for two systems to talk to each other. We call the MobiWork REST API to update an existing “Sales Reporting” form. The script sends a request that includes your credentials plus the values we want to write; MobiWork responds with a success or error message.

---

## How This Project Works

1. **Read the Excel file.** `etl_salesreport_detail.py` opens your Acme workbook (sheet `Acme_Sales_2025_with_Updated_Pr`), normalizes the customer and SKU names, and computes rolling quantity metrics.
2. **Apply your configuration.** The script looks at `config/config.yaml` for thresholds and at `config/customer_map.csv` to turn the Excel customer names into MobiWork customer IDs.
3. **Write outputs.**
   - `out/parent_snapshot.csv` → one row per customer/ship-to. Includes agreement tier, visit frequency, most popular SKU, and quantity stats.
   - `out/top_skus_lines.csv` → the top N SKUs per customer, with monthly and multi-month averages.
   - `out/unmapped_customers.csv` → any Excel customers that still need an entry in `config/customer_map.csv`.
4. **Update MobiWork (optional).** `update_edenwald_form.py` reads those CSVs and pushes Edenwald’s metrics into the existing filled form using the API. Once we refactor it, we can do the same for every customer.

---

## Repository Layout

- `etl_salesreport_detail.py` – main ETL pipeline.
- `update_edenwald_form.py` – pushes the Edenwald Sales Reporting form via the MobiWork API.
- `config/`
  - `config.yaml` – thresholds, lookup file paths, SKU variant groups.
  - `customer_map.csv` – Excel customer + ship-to → MobiWork customer ID/name.
  - `sku_aliases.csv` – optional SKU cleanup table.
- `CUSTOMER_MAPPING_GUIDE.md` – how to maintain `customer_map.csv`.
- `inbox/` – drop Excel files here if you like.
- `out/` – ETL results; gitignore keeps the files out of version control.

---

## Getting Started

1. **Create a Python virtual environment.**
   ```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install pandas openpyxl pyyaml requests
   ```

2. **Add your MobiWork credentials.**
   ```bash
   cp .env.example .env
   ```
   Edit `.env` with the API login, password, client ID, and filled-form IDs you have. Load them before running scripts:
   ```bash
   source .env
   ```

3. **Fill the customer map.**
   - Open `out/unmapped_customers.csv` after an ETL run.
   - Look up each row in your MobiWork export.
   - Add lines to `config/customer_map.csv` in the format `excel_customer,excel_shipto,mobi_customer_id,mobi_customer_name`.

---

## Running the ETL

```bash
source .venv/bin/activate
source .env                    # optional, but helps for API follow-up
.venv/bin/python etl_salesreport_detail.py \
  --file "Acme_Sales_2025 w prices (1) (1).xlsx" \
  --detail-sheet "Acme_Sales_2025_with_Updated_Pr"
```

Key options:
- `--file` – defaults to the newest `*.xlsx` in the repo if you skip it.
- `--detail-sheet` – auto-detected if blank.
- `--report-month` – override the auto-detected latest month.
- `--top-n` – change how many SKUs per customer go into `top_skus_lines.csv`.

Always check `out/unmapped_customers.csv` afterward; it tells you which customers still lack a mapping.

---

## Updating the MobiWork Form

For now the script only targets Edenwald. It pulls Edenwald’s row from `parent_snapshot.csv`, grabs its top SKUs from `top_skus_lines.csv`, and posts the values to the MobiWork API.

```bash
source .venv/bin/activate
source .env
.venv/bin/python update_edenwald_form.py
```

What happens under the hood:
1. **Authenticate** – sends your login/password to `authenticate.html` and receives a token.
2. **Compose XML** – builds a payload with the fields MobiWork expects (Agreement Level, Visit Frequency, SKUs, quantities, etc.).
3. **Update** – posts to `mobiForm/{filledFormId}/update.html`. MobiWork replies with status code `1` when everything sticks.

If you see “Missing ETL output,” run the ETL first. If you get `statusCode="0"` or API errors, double-check the customer ID and field names.

---

## Configuration Cheat Sheet

- `config/config.yaml`
  - `sla_thresholds` – dollar cutoffs for Tier 1/2/3/4.
  - `visit_map` – maps tiers to cadence text.
  - `top_n` – number of SKUs per customer in the repeat panel.
  - `customer_map` / `sku_aliases` paths.
  - `sku_variant_groups` – tie similar SKUs together when picking “most popular.”
- `config/customer_map.csv` – the most important lookup. Keep it current.
- `config/sku_aliases.csv` – optional Excel SKU → canonical SKU mapping.

---

## Keeping Things Clean

- `.env` is intentionally ignored by git. Never commit real API credentials.
- Run the ETL before each API push so `parent_snapshot.csv` and `top_skus_lines.csv` reflect the latest month.
- Any time a customer shows up in `out/unmapped_customers.csv`, add it to `config/customer_map.csv`, rerun the ETL, and confirm the list is empty.

---

## Troubleshooting Tips

- **Authentication fails?** Make sure you ran `source .env` in the current shell and the credentials are correct.
- **“Missing ETL output” message?** Re-run `etl_salesreport_detail.py`.
- **Customer still unmapped?** Check that the Excel `Customer` + `ShipTo` pair exactly matches what you entered in `customer_map.csv` (case doesn’t matter; spaces/punctuation do).
- **API status code 1004/1006?** The customer ID, field names, or filled-form ID usually don’t exist in MobiWork. Verify the IDs through the MobiWork portal.

Once all customers are mapped, we can extend the upload script to loop through every customer, not just Edenwald. Until then, save the ETL outputs, keep the customer map growing, and you’ll have everything you need for the full rollout.
