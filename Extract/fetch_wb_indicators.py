"""
World Bank indicator fetch -> stg_wb_indicator.csv
Target table: STG_WB_INDICATOR(iso3, year_id, indicator_code, value, obs_status, loaded_at)

Notes on API quirks discovered during development:
  - WGI codes changed: old PV.EST -> new GOV_WGI_PV.EST (source=3 no longer needed)
  - SM.POP.REFG (refugees) moved to source=57 "WDI Archives", no longer queryable
    -> dropped; FACT_SOCIETY.refugees_total will be NULL
  - country/all works for all indicators including new WGI codes
"""

import csv
import sys
import time
from datetime import datetime, timezone

import requests

# ── config ────────────────────────────────────────────────────────────────────

BASE_URL    = "https://api.worldbank.org/v2"
DATE_RANGE  = "2014:2023"
VALID_YEARS = set(range(2014, 2024))
OUTPUT_FILE = "stg_wb_indicator.csv"

REQUEST_DELAY = 1.2    # seconds between requests
RETRY_DELAY   = 10.0   # seconds on 429 / 5xx
MAX_RETRIES   = 4

# WDI indicators (source=2, default)
WDI_INDICATORS = [
    "NY.GDP.PCAP.CD",       # GDP per capita, current USD
    "NY.GDP.PCAP.KD.ZG",    # GDP per capita growth, %
    "IT.NET.USER.ZS",       # Internet users, % population
    "IT.CEL.SETS.P2",       # Mobile cellular subscriptions per 100 — digital proxy where internet data missing (Q3/Q5)
    "IT.NET.BBND.P2",       # Fixed broadband subscriptions per 100 — digital infrastructure dimension (Q3/Q5)
    "SP.URB.TOTL.IN.ZS",    # Urban population, %
    "SE.TER.ENRR",          # Tertiary school enrollment, % — Q7, heavy gaps
    "SE.SEC.ENRR",          # Secondary school enrollment, % — Q7 complement, ~25pp better coverage than tertiary
    "SE.ADT.LITR.ZS",       # Adult literacy rate, %
    "MS.MIL.TOTL.P1",       # Armed forces personnel, total
    "MS.MIL.TOTL.TF.ZS",    # Armed forces personnel, % total labor force
    "MS.MIL.XPND.GD.ZS",    # Military expenditure, % GDP
    "SP.POP.TOTL",           # Population, total
    "SP.POP.1564.TO.ZS",    # Population ages 15-64, % — demographic control (Q5/Q10)
    "SM.POP.RHCR.EA",       # Refugees under UNHCR mandate by country of asylum — replaces retired SM.POP.REFG (Q2/Q9)
]

# WGI indicators — codes renamed from PV.EST -> GOV_WGI_PV.EST in current API.
# source=3 param no longer needed; country/all works.
WGI_INDICATORS = [
    "GOV_WGI_PV.EST",   # Political Stability and Absence of Violence
    "GOV_WGI_VA.EST",   # Voice and Accountability
    "GOV_WGI_GE.EST",   # Government Effectiveness
    "GOV_WGI_CC.EST",   # Control of Corruption
    "GOV_WGI_RL.EST",   # Rule of Law
    "GOV_WGI_RQ.EST",   # Regulatory Quality
]

# ── helpers ───────────────────────────────────────────────────────────────────

def _get(url: str, params: dict) -> list | dict:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, params=params, timeout=40)
            if r.status_code == 429:
                wait = RETRY_DELAY * attempt
                print(f"  429 rate-limited - waiting {wait}s (attempt {attempt}/{MAX_RETRIES})")
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.json()
        except requests.exceptions.RequestException as exc:
            if attempt == MAX_RETRIES:
                raise
            print(f"  Error: {exc} - retry {attempt}/{MAX_RETRIES} in {RETRY_DELAY}s")
            time.sleep(RETRY_DELAY)
    raise RuntimeError("Max retries exceeded")


def _is_api_error(data) -> bool:
    return (
        isinstance(data, list)
        and len(data) == 1
        and isinstance(data[0], dict)
        and "message" in data[0]
    )


def fetch_all_pages(url: str, params: dict) -> list[dict]:
    params = {**params, "page": 1}
    all_records: list[dict] = []

    while True:
        data = _get(url, params)
        time.sleep(REQUEST_DELAY)

        if _is_api_error(data):
            msg = data[0]["message"][0]
            print(f"  API error {msg['id']}: {msg['value']}")
            return []

        if not isinstance(data, list) or len(data) < 2:
            print(f"  Unexpected response: {str(data)[:200]}")
            return []

        meta, page_records = data[0], data[1]

        if not page_records:
            break

        all_records.extend(page_records)

        total_pages  = int(meta.get("pages", 1))
        current_page = int(meta.get("page",  1))

        if current_page >= total_pages:
            break

        params = {**params, "page": current_page + 1}
        print(f"    page {current_page + 1}/{total_pages} ...")

    return all_records


def get_valid_iso3s() -> set[str]:
    print("Fetching country list ...")
    url     = f"{BASE_URL}/country"
    params  = {"format": "json", "per_page": 300}
    records = fetch_all_pages(url, params)

    valid: set[str] = set()
    for rec in records:
        if rec.get("region", {}).get("id") == "NA":
            continue
        iso3 = rec.get("id", "").strip()
        if len(iso3) == 3:
            valid.add(iso3)

    print(f"  {len(valid)} countries found\n")
    return valid


def fetch_indicator(code: str) -> list[dict]:
    print(f"  {code}")
    url    = f"{BASE_URL}/country/all/indicator/{code}"
    params = {"format": "json", "date": DATE_RANGE, "per_page": 1000}
    return fetch_all_pages(url, params)


def parse_records(records: list[dict], code: str, valid_iso3: set[str], loaded_at: str) -> list[dict]:
    rows = []
    for rec in records:
        iso3 = (rec.get("countryiso3code") or "").strip()
        if iso3 not in valid_iso3:
            continue
        try:
            year = int(rec.get("date", ""))
        except (ValueError, TypeError):
            continue
        if year not in VALID_YEARS:
            continue
        value = rec.get("value")
        rows.append({
            "iso3":           iso3,
            "year_id":        year,
            "indicator_code": code,
            "value":          "" if value is None else round(float(value), 6),
            "obs_status":     "M" if value is None else "",
            "loaded_at":      loaded_at,
        })
    return rows


# ── main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    loaded_at  = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    valid_iso3 = get_valid_iso3s()
    rows: list[dict] = []

    print("=== WDI indicators ===")
    for code in WDI_INDICATORS:
        records = fetch_indicator(code)
        rows.extend(parse_records(records, code, valid_iso3, loaded_at))

    print("\n=== WGI indicators (GOV_WGI_* codes) ===")
    for code in WGI_INDICATORS:
        records = fetch_indicator(code)
        rows.extend(parse_records(records, code, valid_iso3, loaded_at))

    fieldnames = ["iso3", "year_id", "indicator_code", "value", "obs_status", "loaded_at"]
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    total   = len(rows)
    present = sum(1 for r in rows if r["value"] != "")
    missing = total - present
    codes   = sorted(set(r["indicator_code"] for r in rows))

    print(f"\nDone -> {OUTPUT_FILE}")
    print(f"  rows total   : {total:,}")
    print(f"  with value   : {present:,}")
    print(f"  obs_status=M : {missing:,}  ({100*missing/total:.1f}%)")
    print(f"  indicators   : {len(codes)}")
    for c in codes:
        print(f"    {c}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted.")
        sys.exit(1)
