import pandas as pd, re, argparse, sys
from pathlib import Path
from datetime import date
import yaml

# ---------- helpers ----------
def norm(s):  # normalize for joining/keys
    return re.sub(r"[^A-Z0-9]+"," ",str(s).strip().upper()).strip()

def latest_closed_month(today=None):
    today = today or date.today()
    y, m = (today.year, today.month-1) if today.month>1 else (today.year-1, 12)
    return f"{y}-{m:02d}"

def window3(latest):
    y, m = map(int, latest.split("-"))
    out = [latest]
    for _ in range(2):
        m = 12 if m==1 else m-1
        y = y-1 if m==12 else y
        out.append(f"{y}-{m:02d}")
    return set(out)

def window6(latest):
    y, m = map(int, latest.split("-"))
    out = [latest]
    for _ in range(5):
        m = 12 if m==1 else m-1
        y = y-1 if m==12 else y
        out.append(f"{y}-{m:02d}")
    return set(out)

def load_cfg(path):
    if path and Path(path).exists():
        with open(path,"r") as f: return yaml.safe_load(f) or {}
    return {}


def build_sku_group_lookup(cfg):
    groups = cfg.get("sku_variant_groups") or {}
    group_map = {}
    display_map = {}
    for group_name, variants in groups.items():
        group_key = norm(group_name)
        variant_list = variants if isinstance(variants, list) else []
        if variant_list:
            display_map[group_key] = variant_list[0]
            for variant in variant_list:
                group_map[norm(variant)] = group_key
        else:
            display_map[group_key] = group_name
        group_map[group_key] = group_key
        display_map.setdefault(group_key, group_key)
    return group_map, display_map

# ---------- main ----------
def main(args):
    cfg = load_cfg(args.config)
    top_n = cfg.get("top_n", args.top_n)
    sku_group_map, sku_display_map = build_sku_group_lookup(cfg)

    # locate file
    if args.file:
        xlsx = Path(args.file)
    else:
        inbox = Path(".")
        files = sorted(inbox.glob("*.xlsx"))
        if not files:
            print("No Excel found. Provide --file.", file=sys.stderr)
            sys.exit(1)
        xlsx = files[-1]

    # Read detail sheet (transactional data)
    xls = pd.ExcelFile(xlsx)

    # Find sheet with detail data (look for one with Invoice, Quantity columns)
    detail_sheet = args.detail_sheet
    if not detail_sheet:
        for sheet in xls.sheet_names:
            df_test = pd.read_excel(xls, sheet, nrows=5)
            if 'Quantity' in df_test.columns and 'Invoice' in ' '.join(df_test.columns):
                detail_sheet = sheet
                break

    if not detail_sheet:
        print(f"Could not find detail sheet. Available: {xls.sheet_names}", file=sys.stderr)
        sys.exit(1)

    print(f"Using detail sheet: {detail_sheet}")
    df = pd.read_excel(xls, detail_sheet)

    # Normalize column names
    rename = {
        "Master Customer Name": "Customer",
        "Customer Ship-to Location Name": "ShipTo",
        "Customer Ship-To Location Name": "ShipTo",
        "Invoice Date": "InvoiceDate",
        "sum of price": "sales",
        "Quantity": "qty",
    }
    df = df.rename(columns=rename)

    # If Month column exists and InvoiceDate doesn't, use Month
    if "InvoiceDate" not in df.columns and "Month" in df.columns:
        df["InvoiceDate"] = df["Month"]

    # Ensure we have required columns
    required = ["Customer", "ShipTo", "SKU", "qty", "InvoiceDate"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        print(f"Missing required columns: {missing}", file=sys.stderr)
        print(f"Available columns: {list(df.columns)}", file=sys.stderr)
        sys.exit(1)

    # Parse invoice date to get year-month (may already be datetime from Excel)
    try:
        df["yyyymm"] = df["InvoiceDate"].dt.strftime("%Y-%m")
    except AttributeError:
        # Not a datetime, convert it
        df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"], errors='coerce')
        df["yyyymm"] = df["InvoiceDate"].dt.strftime("%Y-%m")

    # Build customer key
    df["customer_key"] = df["Customer"].map(norm) + " | " + df["ShipTo"].map(norm)
    df["SKU"] = df["SKU"].map(norm)
    df["SKU_group"] = df["SKU"].map(lambda s: sku_group_map.get(s, s))

    # If no Product Description, create empty
    if "Product Description" not in df.columns:
        df["Product Description"] = ""

    # Handle sales column (sum of price or calculate from qty * price)
    if "sales" not in df.columns:
        if "Acme Price (Effective 4/1/24)" in df.columns:
            df["sales"] = df["qty"] * df["Acme Price (Effective 4/1/24)"]
        else:
            df["sales"] = 0  # fallback

    # ---- maps from config ----
    cust_map_path = Path(cfg.get("customer_map","")) if cfg.get("customer_map") else None
    sku_alias_path = Path(cfg.get("sku_aliases","")) if cfg.get("sku_aliases") else None
    expected_path  = Path(cfg.get("expected_qty","")) if cfg.get("expected_qty") else None

    # SKU aliases (optional)
    if sku_alias_path and sku_alias_path.exists():
        sku_map = pd.read_csv(sku_alias_path)
        sku_map["excel_sku"]  = sku_map["excel_sku"].str.upper().str.strip()
        sku_map["mapped_sku"] = sku_map["mapped_sku"].str.upper().str.strip()
        alias_dict = dict(zip(sku_map["excel_sku"], sku_map["mapped_sku"]))
        df["SKU"] = df["SKU"].map(lambda s: alias_dict.get(s, s))

    # Mobi customer mapping (optional but recommended)
    mobi_cols = ["mobi_customer_id","mobi_customer_name"]
    if cust_map_path and cust_map_path.exists():
        cm = pd.read_csv(cust_map_path)
        cm["key"] = (cm["excel_customer"].map(norm) + " | " + cm["excel_shipto"].map(norm))
        cm = cm[["key"] + mobi_cols].drop_duplicates()
        df = df.merge(cm, left_on="customer_key", right_on="key", how="left").drop(columns=["key"])
    else:
        df[mobi_cols] = None

    # Expected Qty (optional)
    expected = None
    if expected_path and expected_path.exists():
        ex = pd.read_csv(expected_path)
        cols = [c.lower() for c in ex.columns]
        if "customer_key" in cols and "sku" in cols:
            ex.columns = [c.strip() for c in ex.columns]
            expected = ex
            expected["customer_key"] = expected["customer_key"].map(norm)
            expected["SKU"] = expected["SKU"].map(norm)
        elif "sku" in cols:
            ex.columns = [c.strip() for c in ex.columns]
            expected = ex
            expected["SKU"] = expected["SKU"].map(norm)
        else:
            expected = None

    # ---- compute windows ----
    # Use specified month or auto-detect latest month in the data
    if args.report_month:
        latest = args.report_month
    else:
        # Auto-detect: use the latest month in the data
        latest_in_data = df["yyyymm"].max()
        print(f"Auto-detected latest month in data: {latest_in_data}")
        latest = latest_in_data

    last3 = window3(latest)
    last6 = window6(latest)

    # per customer+sku qty
    lm = df[df["yyyymm"]==latest].groupby(["customer_key","SKU"], as_index=False)["qty"].sum()\
         .rename(columns={"qty":"Last Month Qty"})
    l3 = df[df["yyyymm"].isin(last3)].groupby(["customer_key","SKU","Product Description"], as_index=False)["qty"].sum()\
         .rename(columns={"qty":"qty_3m_total"})
    l3["3M Avg Qty"] = l3["qty_3m_total"]/3
    l6 = df[df["yyyymm"].isin(last6)].groupby(["customer_key","SKU","Product Description"], as_index=False)["qty"].sum()\
         .rename(columns={"qty":"qty_6m_total"})
    l6["6M Avg Qty"] = l6["qty_6m_total"]/6

    # aggregated metrics by SKU group (for most-popular summaries)
    lm_group = lm.assign(SKU_group=lm["SKU"].map(lambda s: sku_group_map.get(s, s)))\
                 .groupby(["customer_key","SKU_group"], as_index=False)["Last Month Qty"].sum()
    l3_group = l3.assign(SKU_group=l3["SKU"].map(lambda s: sku_group_map.get(s, s)))\
                 .groupby(["customer_key","SKU_group"], as_index=False)\
                 .agg(qty_3m_total=("qty_3m_total","sum"))
    l3_group["3M Avg Qty"] = l3_group["qty_3m_total"]/3
    l6_group = l6.assign(SKU_group=l6["SKU"].map(lambda s: sku_group_map.get(s, s)))\
                 .groupby(["customer_key","SKU_group"], as_index=False)\
                 .agg(qty_6m_total=("qty_6m_total","sum"))
    l6_group["6M Avg Qty"] = l6_group["qty_6m_total"]/6

    mp_metrics = l3_group.merge(lm_group, on=["customer_key","SKU_group"], how="left")\
                         .merge(l6_group[["customer_key","SKU_group","6M Avg Qty"]],
                                on=["customer_key","SKU_group"], how="left")
    mp_metrics = mp_metrics.fillna({"Last Month Qty":0,"6M Avg Qty":0,"qty_3m_total":0,"3M Avg Qty":0})
    mp_metrics["Most Popular SKU"] = mp_metrics["SKU_group"].map(lambda g: sku_display_map.get(g, g))
    mp_metrics = mp_metrics.sort_values(
        ["customer_key","qty_3m_total","Last Month Qty","Most Popular SKU"],
        ascending=[True,False,False,True]
    )
    mp_group = mp_metrics.groupby("customer_key", as_index=False).first()

    # per customer totals for highlights - using 6 month average
    last_month_total = df[df["yyyymm"]==latest].groupby("customer_key", as_index=False)["qty"].sum()\
                       .rename(columns={"qty":"Last Month Qty (Total)"})
    six_month_total = df[df["yyyymm"].isin(last6)].groupby("customer_key", as_index=False)["qty"].sum()\
                      .rename(columns={"qty":"6-Month Total (All SKUs)"})
    six_month_total["6-Month Avg"] = (six_month_total["6-Month Total (All SKUs)"] / 6).round(0).astype(int)
    six_month_total = six_month_total.rename(columns={"6-Month Avg":"Total 6-Month Avg"})

    # spend for SLA per customer (calculate average monthly spend over 3 months)
    spend = df[df["yyyymm"].isin(last3)].groupby("customer_key", as_index=False)["sales"].sum()\
            .rename(columns={"sales":"spend_3m_total"})
    spend["avg_monthly_spend"] = spend["spend_3m_total"] / 3

    # Service Level Tier thresholds (based on average monthly spend from 3-month total)
    # Tier 1: $850+/mo → Monthly visits
    # Tier 2: $600-$849/mo → Bi-Monthly visits
    # Tier 3: $300-$599/mo → Quarterly visits
    # Tier 4: <$300/mo → Annually visits
    th = cfg.get("sla_thresholds", {"tier1":850,"tier2":600,"tier3":300})
    def sla(sp):
        if sp >= th["tier1"]: return "Tier 1"
        if sp >= th["tier2"]: return "Tier 2"
        if sp >= th["tier3"]: return "Tier 3"
        return "Tier 4"
    spend["Agreement Level"] = spend["avg_monthly_spend"].apply(sla)
    vmap = cfg.get("visit_map", {"Tier 1":"Monthly","Tier 2":"Bi-Monthly","Tier 3":"Quarterly","Tier 4":"Annually"})
    spend["Visit Frequency"] = spend["Agreement Level"].map(vmap)

    # Get ALL unique customers from the data (not just those with recent activity)
    all_customers = df[["customer_key"]].drop_duplicates()

    # -------- parent snapshot (one row per customer_key) --------
    parent = all_customers.merge(
        mp_group[["customer_key","Most Popular SKU","Last Month Qty","6M Avg Qty"]]\
            .rename(columns={"Last Month Qty":"Last Month Qty","6M Avg Qty":"6-Month Avg"}),
        on="customer_key", how="left")\
             .merge(last_month_total.rename(columns={"Last Month Qty (Total)":"Total Last Month Qty"}),
                    on="customer_key", how="left")\
             .merge(six_month_total[["customer_key","Total 6-Month Avg"]],
                    on="customer_key", how="left")\
             .merge(spend[["customer_key","Agreement Level","Visit Frequency"]],
                    on="customer_key", how="left")

    # attach mobi customer id/name to parent
    mobis = df.groupby("customer_key", as_index=False).agg(
        mobi_customer_id=("mobi_customer_id","first"),
        mobi_customer_name=("mobi_customer_name","first")
    )
    parent = parent.merge(mobis, on="customer_key", how="left")
    parent["Customer"] = parent["mobi_customer_id"]

    parent["ReportPeriod"] = latest
    parent = parent[[
        "Customer","customer_key","Agreement Level","Visit Frequency",
        "Most Popular SKU","Last Month Qty","6-Month Avg",
        "Total Last Month Qty","Total 6-Month Avg","ReportPeriod"
    ]]

    # -------- top SKUs (repeating) --------
    top = l3.merge(lm, on=["customer_key","SKU"], how="left").fillna({"Last Month Qty":0})
    top = top.merge(
        l6[["customer_key","SKU","Product Description","6M Avg Qty"]],
        on=["customer_key","SKU","Product Description"],
        how="left"
    )
    top = top.sort_values(["customer_key","qty_3m_total"], ascending=[True,False])\
             .groupby("customer_key").head(top_n)
    top["ReportPeriod"] = latest
    top = top.rename(columns={"Product Description":"Item Name"})[[
        "customer_key","SKU","Item Name","Last Month Qty","3M Avg Qty","6M Avg Qty","ReportPeriod"
    ]]
    top = top.merge(mobis, on="customer_key", how="left")
    top["Customer"] = top["mobi_customer_id"]

    # optional Expected + Delta
    if expected is not None:
        if "customer_key" in [c.lower() for c in expected.columns]:
            expected["customer_key"] = expected["customer_key"].map(norm)
            top = top.merge(expected, on=["customer_key","SKU"], how="left")
        else:
            top = top.merge(expected, on=["SKU"], how="left")
        if "Expected Qty/Month" in top.columns:
            top["Δ vs Expected"] = top["3M Avg Qty"] - top["Expected Qty/Month"]

    # reorder columns
    cols = ["Customer","customer_key","SKU","Item Name","Last Month Qty","3M Avg Qty","6M Avg Qty"]
    if "Expected Qty/Month" in top.columns: cols.append("Expected Qty/Month")
    if "Δ vs Expected" in top.columns: cols.append("Δ vs Expected")
    cols.append("ReportPeriod")
    top = top[cols]

    # safety net: unmapped customers
    outdir = Path(args.out or "out"); outdir.mkdir(parents=True, exist_ok=True)
    unmapped = parent[parent["Customer"].isna()][["customer_key"]].drop_duplicates()
    if not unmapped.empty:
        unmapped.to_csv(outdir/"unmapped_customers.csv", index=False)
        print(f"NOTE: wrote {outdir}/unmapped_customers.csv — add to customer_map.csv and re-run.")

    # write outputs
    parent.to_csv(outdir/"parent_snapshot.csv", index=False)
    top.to_csv(outdir/"top_skus_lines.csv", index=False)

    print(f"Wrote: {outdir}/parent_snapshot.csv and {outdir}/top_skus_lines.csv")
    print(f"Latest closed month used: {latest}")
    print(f"Processed {len(df)} transactions from {detail_sheet}")

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--file", help="Path to Acme Excel (default: latest *.xlsx in current dir)")
    p.add_argument("--out",  help="Output dir (default: ./out)")
    p.add_argument("--config", default="config/config.yaml")
    p.add_argument("--top-n", type=int, default=10)
    p.add_argument("--detail-sheet", help="Name of detail/transactional sheet (auto-detected if not specified)")
    p.add_argument("--report-month", help="Report month in YYYY-MM format (auto-detects latest month in data if not specified)")
    main(p.parse_args())
