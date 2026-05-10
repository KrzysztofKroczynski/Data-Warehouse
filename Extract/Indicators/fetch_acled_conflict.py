"""
ACLED conflict data fetch -> stg_acled_conflict.csv
Target table: FACT_CONFLICT(iso3, year_id, fatalities, events_count,
                             battles_count, protests_count, conflict_type,
                             source, loaded_at)

Fetches raw events per year, aggregates to iso3+year level.
"""

import csv
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone

import pycountry
import requests

# ── credentials ───────────────────────────────────────────────────────────────

ACLED_EMAIL    = "@pjwstk.edu.pl"
ACLED_PASSWORD = "PASS"

AUTH_URL = "https://acleddata.com/oauth/token"

# ── config ────────────────────────────────────────────────────────────────────

BASE_URL    = "https://acleddata.com/api/acled/read"
YEARS       = list(range(2014, 2024))
OUTPUT_FILE = "stg_acled_conflict.csv"

PAGE_LIMIT    = 5000   # events per request (ACLED max)
REQUEST_DELAY = 2.0    # seconds between requests
RETRY_DELAY   = 15.0
MAX_RETRIES   = 4

# ACLED event_type values that map to battles_count / protests_count
BATTLE_TYPES  = {"Battles"}
PROTEST_TYPES = {"Protests", "Riots"}

# ── helpers ───────────────────────────────────────────────────────────────────

def iso_numeric_to_alpha3(numeric) -> str | None:
    """Convert ISO 3166-1 numeric code to alpha-3 string."""
    try:
        c = pycountry.countries.get(numeric=str(int(numeric)).zfill(3))
        return c.alpha_3 if c else None
    except (ValueError, TypeError):
        return None


def get_token() -> str:
    """Exchange email+password for OAuth2 Bearer token (valid 24h)."""
    print("Authenticating with ACLED ...")
    r = requests.post(
        AUTH_URL,
        data={
            "username":   ACLED_EMAIL,
            "password":   ACLED_PASSWORD,
            "grant_type": "password",
            "client_id":  "acled",
        },
        timeout=30,
    )
    r.raise_for_status()
    token = r.json()["access_token"]
    print("  Token obtained.\n")
    return token


def _get(params: dict, token: str, token_holder: list) -> dict:
    """token_holder is a 1-element list so callers can receive refreshed token."""
    for attempt in range(1, MAX_RETRIES + 1):
        headers = {"Authorization": f"Bearer {token_holder[0]}"}
        try:
            r = requests.get(BASE_URL, params=params, headers=headers, timeout=60)
            if r.status_code == 429:
                wait = RETRY_DELAY * attempt
                print(f"  429 rate-limited - waiting {wait}s")
                time.sleep(wait)
                continue
            if r.status_code == 401:
                print(f"  401 Unauthorized - refreshing token (attempt {attempt}) ...")
                token_holder[0] = get_token()
                continue
            r.raise_for_status()
            return r.json()
        except requests.exceptions.RequestException as exc:
            if attempt == MAX_RETRIES:
                raise
            print(f"  Error: {exc} - retry {attempt}/{MAX_RETRIES} in {RETRY_DELAY}s")
            time.sleep(RETRY_DELAY)
    raise RuntimeError("Max retries exceeded")


def fetch_year(year: int, token_holder: list) -> list[dict]:
    """Fetch all events for a given year, handling pagination."""
    all_events: list[dict] = []
    page = 1

    while True:
        params = {
            "event_date":       f"{year}-01-01|{year}-12-31",
            "event_date_where": "BETWEEN",
            "fields":           "iso|event_type|fatalities",
            "limit":            PAGE_LIMIT,
            "page":             page,
            "_format":          "json",
        }

        data = _get(params, token_holder[0], token_holder)
        time.sleep(REQUEST_DELAY)

        # ACLED wraps response in {"status": 200, "success": true, "data": [...], "count": N}
        if not data.get("success"):
            print(f"  API error: {data.get('error', data)}")
            break

        events = data.get("data") or []
        count  = int(data.get("count", 0))

        if not events:
            break

        all_events.extend(events)

        # if fewer than PAGE_LIMIT returned — last page
        if len(events) < PAGE_LIMIT:
            break

        page += 1
        print(f"    page {page} (fetched {len(all_events):,} so far) ...")

    return all_events


def aggregate(events: list[dict], year: int, loaded_at: str) -> list[dict]:
    """Aggregate raw events to iso3+year rows matching FACT_CONFLICT schema."""

    # counters keyed by iso3
    fatalities     = defaultdict(int)
    events_count   = defaultdict(int)
    battles_count  = defaultdict(int)
    protests_count = defaultdict(int)
    type_counts    = defaultdict(lambda: defaultdict(int))  # iso3 -> event_type -> count

    for ev in events:
        iso3 = iso_numeric_to_alpha3(ev.get("iso"))
        if not iso3:
            continue

        etype = (ev.get("event_type") or "").strip()
        try:
            fat = int(ev.get("fatalities") or 0)
        except (ValueError, TypeError):
            fat = 0

        fatalities[iso3]   += fat
        events_count[iso3] += 1
        type_counts[iso3][etype] += 1

        if etype in BATTLE_TYPES:
            battles_count[iso3] += 1
        if etype in PROTEST_TYPES:
            protests_count[iso3] += 1

    rows = []
    for iso3 in events_count:
        # dominant event type for this country-year
        dominant = max(type_counts[iso3], key=type_counts[iso3].get)
        rows.append({
            "iso3":           iso3,
            "year_id":        year,
            "fatalities":     fatalities[iso3],
            "events_count":   events_count[iso3],
            "battles_count":  battles_count[iso3],
            "protests_count": protests_count[iso3],
            "conflict_type":  dominant,
            "source":         "acled",
            "loaded_at":      loaded_at,
        })

    return rows


# ── main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    loaded_at    = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    token_holder = [get_token()]   # mutable so _get can refresh in-place
    all_rows: list[dict] = []

    for year in YEARS:
        print(f"Year {year} ...")
        events = fetch_year(year, token_holder)
        rows   = aggregate(events, year, loaded_at)
        all_rows.extend(rows)
        print(f"  {len(events):,} events -> {len(rows)} country rows")

    fieldnames = [
        "iso3", "year_id", "fatalities", "events_count",
        "battles_count", "protests_count", "conflict_type",
        "source", "loaded_at",
    ]

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_rows)

    total_events = sum(r["events_count"] for r in all_rows)
    countries    = len(set(r["iso3"] for r in all_rows))

    print(f"\nDone -> {OUTPUT_FILE}")
    print(f"  rows (country-year) : {len(all_rows):,}")
    print(f"  countries           : {countries}")
    print(f"  total events        : {total_events:,}")
    print(f"  total fatalities    : {sum(r['fatalities'] for r in all_rows):,}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted.")
        sys.exit(1)
