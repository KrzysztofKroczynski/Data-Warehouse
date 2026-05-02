"""
Google Trends per-country extraction -> stg_google_trends.csv
Target table: FACT_GOOGLE_TRENDS(iso2, year_id, term_id, interest_normalized, interest_raw, anchor_term)

Per-country, per-year strategy: avoids the global-query problem where non-dominant
countries round to 0 due to GT global normalization.

~50 x 7 x 5 = 1,750 total calls (~3h at 6s delay).
Failed batches are NOT checkpointed — auto-retried on next run.
"""

import ctypes
import csv
import os
import sys
import time
from pathlib import Path

from pytrends.request import TrendReq

# Prevent Windows from sleeping during a long run
_ES_CONTINUOUS      = 0x80000000
_ES_SYSTEM_REQUIRED = 0x00000001
_keep_awake  = lambda: ctypes.windll.kernel32.SetThreadExecutionState(_ES_CONTINUOUS | _ES_SYSTEM_REQUIRED)
_allow_sleep = lambda: ctypes.windll.kernel32.SetThreadExecutionState(_ES_CONTINUOUS)

SEARCH_TERM_FILE = "stg_dim_search_term.csv"
OUTPUT_FILE      = "stg_google_trends.csv"
CHECKPOINT_FILE  = "stg_google_trends_checkpoint.csv"
ANCHOR_FILE      = "stg_youtube_anchor.csv"

YEARS      = [2019, 2020, 2021, 2022, 2023]
ANCHOR     = "youtube"
BATCH_SIZE = 4    # GT allows 5 keywords max; 1 slot reserved for anchor
CALL_DELAY = 6    # seconds

COUNTRIES = [
    "US", "GB", "DE", "FR", "JP", "KR", "BR", "CA", "AU", "RU",
    "IN", "MX", "IT", "ES", "PL", "NL", "SE", "NO", "DK", "FI",
    "BE", "AT", "CH", "CZ", "HU", "RO", "UA", "TR", "SA", "ZA",
    "EG", "NG", "AR", "CL", "CO", "ID", "PH", "TH", "VN", "MY",
    "SG", "HK", "TW", "NZ", "PT", "GR", "IL", "AE", "IR", "PK",
]

FIELDNAMES        = ["iso2", "year_id", "term_id", "keyword", "interest_raw",
                     "interest_normalized", "anchor_term", "anchor_raw"]
ANCHOR_FIELDNAMES = ["iso2", "year_id", "anchor_term", "interest_raw"]


def load_search_terms(path: str) -> list[dict]:
    with open(path, newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f, delimiter=";"))


def load_done_pairs(path: str) -> set[tuple]:
    """Returns set of (iso2, year_id, term_id) already written to checkpoint.
    Skips truncated last row caused by interrupted run."""
    if not Path(path).exists():
        return set()
    done: set[tuple] = set()
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f, delimiter=";"):
            try:
                done.add((row["iso2"], row["year_id"], row["term_id"]))
            except KeyError:
                pass
    return done


def make_batches(terms: list[dict]) -> list[list[dict]]:
    non_anchor = [t for t in terms if t["keyword"].lower() != ANCHOR]
    return [non_anchor[i:i + BATCH_SIZE] for i in range(0, len(non_anchor), BATCH_SIZE)]


def fetch_country_batch(pytrends: TrendReq, keywords: list[str], iso2: str, year: int) -> dict | None:
    """
    Fetch weekly interest sums for ANCHOR + keywords in iso2 for the given year.
    Per-year timeframe ensures GT normalizes within each year → matches manual GT downloads.
    Returns {keyword: weekly_sum} or None after 3 failed attempts.
    """
    kw_list   = [ANCHOR] + keywords
    timeframe = f"{year}-01-01 {year}-12-31"

    for attempt in range(1, 4):
        try:
            pytrends.build_payload(kw_list, timeframe=timeframe, geo=iso2)
            df = pytrends.interest_over_time()
            if df is None or df.empty:
                return None
            if "isPartial" in df.columns:
                df = df[~df["isPartial"].astype(bool)]
            return {kw: round(float(df[kw].sum()), 4) for kw in kw_list if kw in df.columns}
        except Exception as exc:
            wait = 60 * attempt
            print(f"    error: {exc} — retry {attempt}/3 in {wait}s")
            time.sleep(wait)

    print(f"    failed after 3 attempts — will retry on next run")
    return None


def flush_checkpoint_to_output() -> tuple[int, int, int]:
    """Sort checkpoint and write OUTPUT_FILE + ANCHOR_FILE. Returns (total, non_null, null_count)."""
    with open(CHECKPOINT_FILE, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f, delimiter=";"))

    rows = [r for r in rows if r["year_id"] != "year_id"]  # drop duplicate headers from resume runs
    rows.sort(key=lambda r: (r["iso2"], int(r["year_id"]), r["term_id"]))

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES, delimiter=";")
        writer.writeheader()
        writer.writerows(rows)

    seen: set[tuple] = set()
    anchor_rows: list[dict] = []
    for row in rows:
        key = (row["iso2"], row["year_id"])
        if key not in seen:
            seen.add(key)
            anchor_rows.append({
                "iso2":         row["iso2"],
                "year_id":      row["year_id"],
                "anchor_term":  ANCHOR,
                "interest_raw": row["anchor_raw"],
            })

    with open(ANCHOR_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=ANCHOR_FIELDNAMES, delimiter=";")
        writer.writeheader()
        writer.writerows(anchor_rows)

    total    = len(rows)
    non_null = sum(1 for r in rows if r["interest_normalized"] != "")
    return total, non_null, total - non_null


def main() -> None:
    flush_only = "--flush" in sys.argv

    if flush_only:
        if not Path(CHECKPOINT_FILE).exists():
            print(f"ERROR: {CHECKPOINT_FILE} not found — nothing to flush")
            sys.exit(1)
        print(f"Flushing {CHECKPOINT_FILE} to output files (no download)...")
        total, non_null, null_count = flush_checkpoint_to_output()
        print(f"Done -> {OUTPUT_FILE}")
        print(f"  rows: {total:,}  |  normalized: {non_null:,}  |  null (anchor=0): {null_count:,}")
        print(f"Done -> {ANCHOR_FILE}")
        print(f"Checkpoint NOT deleted — re-run without --flush to continue downloading.")
        return

    _keep_awake()
    print("Sleep blocked — Windows will not suspend during this run.")

    try:
        if not Path(SEARCH_TERM_FILE).exists():
            print(f"ERROR: {SEARCH_TERM_FILE} not found — run fetch_steam_top_games.py first")
            sys.exit(1)

        terms      = load_search_terms(SEARCH_TERM_FILE)
        batches    = make_batches(terms)
        done_pairs = load_done_pairs(CHECKPOINT_FILE)

        if done_pairs:
            print(f"Resuming: {len(done_pairs)} (iso2, year, term_id) entries already done")

        pytrends    = TrendReq(hl="en-US", tz=0, timeout=(10, 25))
        total_calls = len(COUNTRIES) * len(batches) * len(YEARS)
        call_num    = 0

        with open(CHECKPOINT_FILE, "a", newline="", encoding="utf-8") as checkpoint_handle:
            checkpoint_writer = csv.DictWriter(checkpoint_handle, fieldnames=FIELDNAMES, delimiter=";")
            if not done_pairs:
                checkpoint_writer.writeheader()

            for iso2 in COUNTRIES:
                for batch_idx, batch in enumerate(batches):
                    keywords = [t["keyword"] for t in batch]
                    term_ids = [t["term_id"] for t in batch]

                    for year in YEARS:
                        call_num += 1

                        if all((iso2, str(year), tid) in done_pairs for tid in term_ids):
                            print(f"[{call_num}/{total_calls}] {iso2} {year} batch={batch_idx} — skip")
                            continue

                        print(f"[{call_num}/{total_calls}] {iso2} {year} keywords={keywords}")

                        data = fetch_country_batch(pytrends, keywords, iso2, year)

                        if data is None:
                            print(f"    no data — skipping, will retry on re-run")
                            time.sleep(CALL_DELAY)
                            continue

                        anchor_val = data.get(ANCHOR, 0.0)

                        for term in batch:
                            key = (iso2, str(year), term["term_id"])
                            if key in done_pairs:
                                continue
                            raw        = data.get(term["keyword"], 0.0)
                            normalized = round(raw / anchor_val, 4) if anchor_val > 0 else None
                            checkpoint_writer.writerow({
                                "iso2":                iso2,
                                "year_id":             year,
                                "term_id":             term["term_id"],
                                "keyword":             term["keyword"],
                                "interest_raw":        raw,
                                "interest_normalized": normalized if normalized is not None else "",
                                "anchor_term":         ANCHOR,
                                "anchor_raw":          anchor_val,
                            })
                            done_pairs.add(key)

                        checkpoint_handle.flush()
                        time.sleep(CALL_DELAY)

        print("\nAll batches done. Writing output files...")
        total, non_null, null_count = flush_checkpoint_to_output()
        os.remove(CHECKPOINT_FILE)

        print(f"Done -> {OUTPUT_FILE}")
        print(f"  rows: {total:,}  |  normalized: {non_null:,}  |  null (anchor=0): {null_count:,}")
        print(f"Done -> {ANCHOR_FILE}")
    finally:
        _allow_sleep()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted — checkpoint saved, re-run to resume.")
        sys.exit(1)
