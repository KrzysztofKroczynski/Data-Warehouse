"""
Google Trends -> stg_google_trends.csv
Target table: FACT_GOOGLE_TRENDS(iso2, year_id, term_id, interest_normalized, interest_raw, anchor_term)

Strategy: global geo="" + interest_by_region(resolution='COUNTRY') returns all countries per call.
50 calls total (5 years x 10 batches), ~8 minutes at 10s delay.
Checkpoint file allows resume after interruption.
"""

import csv
import os
import sys
import time
from pathlib import Path

from pytrends.request import TrendReq

SEARCH_TERM_FILE = "stg_dim_search_term.csv"
OUTPUT_FILE      = "stg_google_trends.csv"
CHECKPOINT_FILE  = "stg_google_trends_checkpoint.csv"

YEARS       = [2019, 2020, 2021, 2022, 2023]
ANCHOR      = "youtube"
BATCH_SIZE  = 4   # keywords per call (+ anchor = 5 total, GT max)
CALL_DELAY  = 10  # seconds between API calls

FIELDNAMES = ["iso2", "year_id", "term_id", "keyword", "interest_raw", "interest_normalized", "anchor_term"]


def load_search_terms(path: str) -> list[dict]:
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def load_checkpoint(path: str) -> tuple[list[dict], set[tuple]]:
    if not Path(path).exists():
        return [], set()
    rows = []
    done = set()
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            rows.append(row)
            done.add((row["year_id"], row["term_id"]))
    return rows, done


def make_batches(terms: list[dict]) -> list[list[dict]]:
    """Split terms into batches of BATCH_SIZE, excluding anchor if present."""
    non_anchor = [t for t in terms if t["keyword"].lower() != ANCHOR]
    batches = []
    for i in range(0, len(non_anchor), BATCH_SIZE):
        batches.append(non_anchor[i:i + BATCH_SIZE])
    return batches


def fetch_batch(pytrends: TrendReq, keywords: list[str], year: int) -> dict | None:
    """Return dict of {keyword: {iso2: value}} or None on failure."""
    kw_list = [ANCHOR] + keywords
    timeframe = f"{year}-01-01 {year}-12-31"

    for attempt in range(1, 4):
        try:
            pytrends.build_payload(kw_list, timeframe=timeframe, geo="")
            df = pytrends.interest_by_region(
                resolution="COUNTRY",
                inc_low_vol=True,
                inc_geo_code=True,
            )
            return df
        except Exception as exc:
            wait = CALL_DELAY * attempt
            print(f"    error: {exc} — retry {attempt}/3 in {wait}s")
            time.sleep(wait)

    print(f"    failed after 3 attempts, skipping batch")
    return None


def main() -> None:
    if not Path(SEARCH_TERM_FILE).exists():
        print(f"ERROR: {SEARCH_TERM_FILE} not found — run fetch_steam_top_games.py first")
        sys.exit(1)

    terms   = load_search_terms(SEARCH_TERM_FILE)
    batches = make_batches(terms)

    term_by_keyword = {t["keyword"]: t for t in terms}

    collected, done_pairs = load_checkpoint(CHECKPOINT_FILE)
    if done_pairs:
        print(f"Resuming from checkpoint: {len(done_pairs)} (year,term_id) pairs already done")

    checkpoint_handle = open(CHECKPOINT_FILE, "a", newline="", encoding="utf-8")
    checkpoint_writer = csv.DictWriter(checkpoint_handle, fieldnames=FIELDNAMES)
    if not done_pairs:
        checkpoint_writer.writeheader()

    pytrends = TrendReq(hl="en-US", tz=0, timeout=(10, 25), retries=2, backoff_factor=2)

    total_calls = len(YEARS) * len(batches)
    call_num    = 0

    for year in YEARS:
        for batch in batches:
            call_num += 1

            batch_keywords  = [t["keyword"] for t in batch]
            batch_term_ids  = [t["term_id"] for t in batch]

            already_done = all((str(year), tid) in done_pairs for tid in batch_term_ids)
            if already_done:
                print(f"[{call_num}/{total_calls}] year={year} terms={batch_term_ids} — skip (checkpoint)")
                continue

            print(f"[{call_num}/{total_calls}] year={year} keywords={batch_keywords}")

            df = fetch_batch(pytrends, batch_keywords, year)

            if df is None or df.empty:
                print("    no data returned")
                time.sleep(CALL_DELAY)
                continue

            geo_col = "geoCode" if "geoCode" in df.columns else None

            for idx, row_data in df.iterrows():
                iso2        = row_data.get("geoCode", idx) if geo_col else str(idx)
                anchor_val  = row_data.get(ANCHOR, 0) or 0

                for term in batch:
                    tid     = term["term_id"]
                    keyword = term["keyword"]

                    raw = row_data.get(keyword)
                    if raw is None:
                        continue

                    raw_int   = int(raw)
                    normalized = (
                        round(raw_int / anchor_val, 4)
                        if anchor_val > 0
                        else None
                    )

                    out_row = {
                        "iso2":                iso2,
                        "year_id":             year,
                        "term_id":             tid,
                        "keyword":             keyword,
                        "interest_raw":        raw_int,
                        "interest_normalized": normalized if normalized is not None else "",
                        "anchor_term":         ANCHOR,
                    }
                    collected.append(out_row)
                    checkpoint_writer.writerow(out_row)

            checkpoint_handle.flush()
            time.sleep(CALL_DELAY)

    checkpoint_handle.close()

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(collected)

    if Path(CHECKPOINT_FILE).exists():
        os.remove(CHECKPOINT_FILE)

    print(f"\nDone -> {OUTPUT_FILE}")
    print(f"  rows: {len(collected):,}")

    non_null = sum(1 for r in collected if r["interest_normalized"] != "")
    print(f"  with normalized value: {non_null:,}")
    print(f"  null (anchor=0):       {len(collected) - non_null:,}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted — checkpoint saved, re-run to resume.")
        sys.exit(1)
