"""
World Bank /v2/country -> stg_dim_country.csv
Target table: DIM_COUNTRY(iso3, iso2, name, region, subregion, income_level, lat, lon)
"""

import csv
import sys
import time

import requests

BASE_URL      = "https://api.worldbank.org/v2"
OUTPUT_FILE   = "stg_dim_country.csv"
REQUEST_DELAY = 1.2
RETRY_DELAY   = 10.0
MAX_RETRIES   = 4


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

    return all_records


def main() -> None:
    print("Fetching country list from World Bank ...")
    url     = f"{BASE_URL}/country"
    params  = {"format": "json", "per_page": 300}
    records = fetch_all_pages(url, params)

    rows = []
    for rec in records:
        if rec.get("region", {}).get("id") == "NA":
            continue
        iso3 = rec.get("id", "").strip()
        if len(iso3) != 3:
            continue

        rows.append({
            "iso3":         iso3,
            "iso2":         rec.get("iso2Code", "").strip(),
            "name":         rec.get("name", "").strip(),
            "region":       rec.get("region", {}).get("value", "").strip(),
            "subregion":    rec.get("adminregion", {}).get("value", "").strip(),
            "income_level": rec.get("incomeLevel", {}).get("value", "").strip(),
            "lat":          rec.get("latitude", "").strip() if rec.get("latitude") else "",
            "lon":          rec.get("longitude", "").strip() if rec.get("longitude") else "",
        })

    fieldnames = ["iso3", "iso2", "name", "region", "subregion", "income_level", "lat", "lon"]
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"\nDone -> {OUTPUT_FILE}")
    print(f"  countries: {len(rows)}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted.")
        sys.exit(1)
