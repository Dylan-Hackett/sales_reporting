#!/usr/bin/env python3
"""
Main entry point for the per-location sales reporting ETL pipeline.

Usage:
    python run_etl.py
    python run_etl.py --data-dir "Sales Reports 2025 /" --report-month 2026-02
    python run_etl.py --skip-mapping  # skip location mapping (use existing location_map.csv)
"""
import argparse
from pathlib import Path

import yaml

from etl.adapters import load_all
from etl.location_mapper import build_location_map
from etl.views import generate_all_views


def load_cfg(path):
    p = Path(path)
    if p.exists():
        with open(p) as f:
            return yaml.safe_load(f) or {}
    return {}


def main():
    parser = argparse.ArgumentParser(description="Per-location sales reporting ETL")
    parser.add_argument("--data-dir", default="Sales Reports 2025 /",
                        help="Path to data directory")
    parser.add_argument("--config", default="config/config.yaml",
                        help="Path to config.yaml")
    parser.add_argument("--out", default="out/",
                        help="Output directory")
    parser.add_argument("--report-month",
                        help="YYYY-MM format, or auto-detect latest month in data")
    parser.add_argument("--skip-mapping", action="store_true",
                        help="Skip location mapping (use existing location_map.csv)")
    args = parser.parse_args()

    cfg = load_cfg(args.config)
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Stage 1: Load and deduplicate all sources
    print("=" * 60)
    print("STAGE 1: Loading source data")
    print("=" * 60)
    transactions = load_all(args.data_dir)

    # Stage 2: Location mapping
    print("\n" + "=" * 60)
    print("STAGE 2: Location mapping")
    print("=" * 60)
    mobiwork_ref = Path("config/mobiwork_customer_locations.xlsx")
    if args.skip_mapping:
        import pandas as pd
        map_path = Path("config/location_map.csv")
        if map_path.exists():
            loc_map = pd.read_csv(map_path, dtype=str)
            print(f"  Using existing {map_path} ({len(loc_map)} locations)")
        else:
            print("  No existing location_map.csv found, building...")
            loc_map = build_location_map(transactions, mobiwork_ref, out_dir)
    else:
        loc_map = build_location_map(transactions, mobiwork_ref, out_dir)

    # Auto-detect report month if not specified
    report_month = args.report_month
    if not report_month:
        report_month = transactions["yyyymm"].max()
        print(f"\nAuto-detected report month: {report_month}")

    # Stage 3: Generate views
    print("\n" + "=" * 60)
    print("STAGE 3: Generating report views")
    print("=" * 60)
    v1, v2, v3, snap, top = generate_all_views(transactions, loc_map, out_dir, report_month, cfg)

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"  Total transactions: {len(transactions):,}")
    print(f"  Unique locations: {transactions['location_code'].nunique()}")
    print(f"  Date range: {transactions['yyyymm'].min()} to {transactions['yyyymm'].max()}")
    print(f"  Report month: {report_month}")
    print(f"  Sources: {transactions.groupby('source').size().to_dict()}")

    mapped = loc_map[loc_map["mobi_customer_id"].fillna("").astype(str).str.strip() != ""]
    unmapped = loc_map[loc_map["mobi_customer_id"].fillna("").astype(str).str.strip() == ""]
    print(f"  Mapped locations: {len(mapped)}/{len(loc_map)}")
    if len(unmapped) > 0:
        print(f"  Unmapped: {len(unmapped)} (see out/unmapped_locations.csv)")

    print(f"\nOutputs in {out_dir}/:")
    for f in sorted(out_dir.glob("*.csv")):
        print(f"  {f.name}")
    print("\nDone.")


if __name__ == "__main__":
    main()
