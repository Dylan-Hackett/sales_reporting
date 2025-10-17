# Customer Mapping Guide

## Overview
The ETL script needs to map Acme Excel customer names to your MobiWork customer IDs so the data can be imported into MobiForm.

## Step-by-Step Instructions

### 1. Export Customer List from MobiWork

From MobiWork, export a customer list with these columns:
- **Customer ID** (numeric identifier - required for import)
- **Customer Name** (display name)

Save as CSV or Excel for easy reference.

### 2. Fill Out the Customer Mapping Template

A template has been created for you at: `config/customer_map_template.csv`

**Columns to fill in:**
- `excel_customer` - Already filled (from Acme Excel)
- `excel_shipto` - Already filled (from Acme Excel)
- `mobi_customer_id` - **YOU NEED TO FILL THIS** (from MobiWork export)
- `mobi_customer_name` - **YOU NEED TO FILL THIS** (from MobiWork export)

### 3. Match Customers

For each row in the template:
1. Look at the `excel_customer` and `excel_shipto` columns
2. Find the matching customer in your MobiWork export
3. Copy the MobiWork Customer ID into `mobi_customer_id`
4. Copy the MobiWork Customer Name into `mobi_customer_name`

**Example:**
```csv
excel_customer,excel_shipto,mobi_customer_id,mobi_customer_name
Ben S Next Door,1211 U St Nw,12345,Ben's Next Door - DC Location
Edenwald Senior Living,800 Southerly Road,67890,Edenwald Senior Living
```

### 4. Handle Multiple Locations

If one customer has multiple ship-to locations in the Excel file, you may need to:
- Create separate customer records in MobiWork for each location, OR
- Use the same MobiWork customer ID for all locations if they should be treated as one

**Example - Same Customer, Different Locations:**
```csv
excel_customer,excel_shipto,mobi_customer_id,mobi_customer_name
Eddie S Of Roland Park Eddie S Of Charles St,6213 N Charles Street,11111,Eddie's Roland Park - Location A
Eddie S Of Roland Park Eddie S Of Charles St,6223 1 2 N Charles Street,11112,Eddie's Roland Park - Location B
```

### 5. Save and Replace

Once you've filled in all the MobiWork IDs:
1. Save the file
2. Copy it to replace `config/customer_map.csv`:
   ```bash
   cp config/customer_map_template.csv config/customer_map.csv
   ```

### 6. Re-run the ETL

Run the script again:
```bash
.venv/bin/python3 etl_salesreport_detail.py \
  --file "Acme_Sales_2025 w prices (1) (1).xlsx" \
  --detail-sheet "Acme_Sales_2025_with_Updated_Pr"
```

After this, the `Customer` column in the output CSVs will be populated with MobiWork IDs, ready for import!

## Quick Tips

- **Exact match not required**: The script normalizes names (removes special chars, uppercases), so minor differences are OK
- **Missing customers**: If you skip a customer in the mapping, it will appear in `out/unmapped_customers.csv` again
- **Case doesn't matter**: "Ben's Next Door" vs "BEN'S NEXT DOOR" - both work
- **Use Customer ID not Name**: MobiWork imports work best with numeric IDs in the `Customer` field

## Files Reference

- `config/customer_map_template.csv` - Empty template with all customers to map (START HERE)
- `config/customer_map.csv` - Final mapping file used by the script (COPY YOUR COMPLETED TEMPLATE HERE)
- `out/unmapped_customers.csv` - List of customers still needing mapping (generated after each run)

## Need Help?

Run the script and check `out/unmapped_customers.csv` to see which customers still need mapping.
