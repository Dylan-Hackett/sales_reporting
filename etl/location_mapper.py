"""
Location mapper: match transaction location codes to MobiWork customer records.
Outputs location_map.csv (matched) and unmapped_locations.csv (unmatched).
"""
import re
from pathlib import Path

import pandas as pd


def _norm(s):
    """Normalize string for fuzzy matching."""
    return re.sub(r"[^A-Z0-9]+", " ", str(s).strip().upper()).strip()


def _extract_location_ref(transactions_df):
    """Extract unique location reference data from transactions.

    Takes the most frequent address per location_code (Imperial is 99.7% consistent).
    """
    locs = transactions_df.copy()
    # Determine chain from source
    locs["chain"] = locs["source"].map({
        "imperial": "imperial",
        "lpq": "lpq",
        "jtj": "jtj",
    })

    # For each location_code, take the most frequent address info
    # Group by location_code and take mode (most frequent) for each field
    ref_rows = []
    for code, grp in locs.groupby("location_code"):
        row = {
            "location_code": code,
            "chain": grp["chain"].iloc[0],
            "customer_name": grp["customer_name"].mode().iloc[0] if not grp["customer_name"].mode().empty else "",
            "address": grp["address"].mode().iloc[0] if not grp["address"].mode().empty else "",
            "city": grp["city"].mode().iloc[0] if not grp["city"].mode().empty else "",
            "state": grp["state"].mode().iloc[0] if not grp["state"].mode().empty else "",
            "zip": grp["zip"].mode().iloc[0] if not grp["zip"].mode().empty else "",
        }
        ref_rows.append(row)

    return pd.DataFrame(ref_rows)


def _load_mobiwork_ref(path):
    """Load MobiWork customer locations reference file."""
    df = pd.read_excel(path, header=4)
    df = df.dropna(subset=["Customer Name"])
    # Clean column names
    col_map = {
        "Customer Name": "mobi_customer_name",
        "Customer ID *": "mobi_customer_id",
        "Location Name": "mobi_location_name",
        "Street Address 1 *": "mobi_address",
        "Street Address 2": "mobi_address2",
        "City *": "mobi_city",
        "State *": "mobi_state",
        "Zip Code *": "mobi_zip",
    }
    df = df.rename(columns=col_map)
    keep = [v for v in col_map.values() if v in df.columns]
    df = df[keep].copy()

    df["mobi_customer_id"] = pd.to_numeric(df["mobi_customer_id"], errors="coerce")
    df["_norm_name"] = df["mobi_customer_name"].apply(_norm)
    df["_norm_addr"] = df["mobi_address"].fillna("").apply(_norm)
    df["_norm_loc"] = df["mobi_location_name"].fillna("").apply(_norm)
    return df


def build_location_map(transactions_df, mobiwork_ref_path, out_dir="out"):
    """Build location_map.csv by matching transaction locations to MobiWork records.

    Matching strategy:
    1. Exact name match (normalized)
    2. Name contained in MobiWork name or vice versa
    3. Address match as tiebreaker
    """
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    print("Extracting location reference data...")
    loc_ref = _extract_location_ref(transactions_df)
    print(f"  {len(loc_ref)} unique locations")

    print("Loading MobiWork reference...")
    mobi = _load_mobiwork_ref(mobiwork_ref_path)
    print(f"  {len(mobi)} MobiWork customer records")

    # Normalize transaction names for matching
    loc_ref["_norm_name"] = loc_ref["customer_name"].apply(_norm)
    loc_ref["_norm_addr"] = loc_ref["address"].apply(_norm)

    results = []
    for _, loc in loc_ref.iterrows():
        match = _find_match(loc, mobi)
        row = {
            "location_code": loc["location_code"],
            "chain": loc["chain"],
            "customer_name": loc["customer_name"],
            "address": loc["address"],
            "city": loc["city"],
            "state": loc["state"],
            "zip": loc["zip"],
            "mobi_customer_id": match["mobi_customer_id"] if match is not None else "",
            "mobi_customer_name": match["mobi_customer_name"] if match is not None else "",
        }
        results.append(row)

    result_df = pd.DataFrame(results)
    mapped = result_df[result_df["mobi_customer_id"] != ""]
    unmapped = result_df[result_df["mobi_customer_id"] == ""]

    # Write outputs
    map_path = Path("config") / "location_map.csv"
    result_df.to_csv(map_path, index=False)
    print(f"\nWrote {map_path}: {len(result_df)} locations ({len(mapped)} mapped, {len(unmapped)} unmapped)")

    if not unmapped.empty:
        unmapped_path = out_dir / "unmapped_locations.csv"
        unmapped.to_csv(unmapped_path, index=False)
        print(f"Wrote {unmapped_path}: {len(unmapped)} unmapped locations")

    return result_df


def _find_match(loc, mobi):
    """Try to match a transaction location to a MobiWork record."""
    norm_name = loc["_norm_name"]
    norm_addr = loc["_norm_addr"]

    if not norm_name:
        return None

    # Strategy 1: Exact normalized name match
    exact = mobi[mobi["_norm_name"] == norm_name]
    if len(exact) == 1:
        return exact.iloc[0]
    if len(exact) > 1:
        # Tiebreak by address
        addr_match = exact[exact["_norm_addr"] == norm_addr]
        if len(addr_match) >= 1:
            return addr_match.iloc[0]
        return exact.iloc[0]

    # Strategy 2: Location name match (MobiWork has a separate location name field)
    loc_match = mobi[mobi["_norm_loc"] == norm_name]
    if len(loc_match) == 1:
        return loc_match.iloc[0]
    if len(loc_match) > 1:
        return loc_match.iloc[0]

    # Strategy 3: Substring containment (transaction name in mobi name or vice versa)
    if len(norm_name) >= 6:
        contains = mobi[
            mobi["_norm_name"].str.contains(norm_name, regex=False, na=False) |
            mobi["_norm_loc"].str.contains(norm_name, regex=False, na=False)
        ]
        if len(contains) == 1:
            return contains.iloc[0]

        # Try the reverse
        reverse = mobi[mobi["_norm_name"].apply(lambda n: n in norm_name if len(n) >= 6 else False)]
        if len(reverse) == 1:
            return reverse.iloc[0]

    # Strategy 4: Address match (if we have address info)
    if norm_addr and len(norm_addr) >= 8:
        addr_hits = mobi[mobi["_norm_addr"] == norm_addr]
        if len(addr_hits) == 1:
            return addr_hits.iloc[0]

    # Strategy 5: Chain-specific matching for LPQ/J&TJ
    # These chains have generic names but street numbers embedded in customer_name
    # e.g., "LEPAIN QUOTIDIEN 1270 FIRST AV" → MobiWork "1270 1st ave"
    chain = loc.get("chain", "")
    if chain in ("lpq", "jtj"):
        return _match_chain_by_address_number(loc, mobi, chain)

    return None


def _extract_street_numbers(text):
    """Extract significant street numbers from text."""
    import re
    # Find numbers that look like street addresses (3+ digits, not years)
    nums = re.findall(r'\b(\d{3,5})\b', text)
    # Filter out things that look like years or zip codes
    return [n for n in nums if not (1900 <= int(n) <= 2030) and not (int(n) >= 10000)]


def _match_chain_by_address_number(loc, mobi, chain):
    """Match chain locations by extracting street numbers from names and matching to MobiWork addresses."""
    norm_name = loc["_norm_name"]

    # Filter MobiWork to the relevant chain
    if chain == "lpq":
        chain_mobi = mobi[mobi["_norm_name"].str.contains("PAIN|QUOTID|LPQ", na=False)]
    elif chain == "jtj":
        chain_mobi = mobi[mobi["_norm_name"].str.contains("JOE|JUICE|JTJ", na=False)]
    else:
        return None

    if chain_mobi.empty:
        return None

    # Extract street numbers from the transaction customer_name
    nums_from_name = _extract_street_numbers(norm_name)
    if not nums_from_name:
        return None

    # Try to match each number against MobiWork addresses
    for num in nums_from_name:
        matches = chain_mobi[chain_mobi["_norm_addr"].str.contains(r'\b' + num + r'\b', regex=True, na=False)]
        if len(matches) == 1:
            return matches.iloc[0]
        # Also check MobiWork customer name for the number
        if len(matches) == 0:
            matches = chain_mobi[chain_mobi["_norm_name"].str.contains(r'\b' + num + r'\b', regex=True, na=False)]
            if len(matches) == 1:
                return matches.iloc[0]

    return None
