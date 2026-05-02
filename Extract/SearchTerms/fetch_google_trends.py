"""
Google Trends per-country extraction -> stg_google_trends.csv
Target table: FACT_GOOGLE_TRENDS(iso2, year_id, term_id, entity_mid, interest_normalized, ...)

Per-country strategy: one call per (country, batch) covering all YEARS.
GT returns monthly data normalized across the full YEARS range (peak over all years = 100).
Results are grouped by year and summed.

Entity discovery: for each keyword, all GT entity variants (plain-text + entity mids)
are fetched via suggestions API at startup and queried separately.

~50 x N_BATCHES total calls (N_BATCHES depends on entity count; ~3 entities/keyword
→ ~19 batches → ~950 calls, ~1.5h).
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

YEARS            = [2019, 2020, 2021, 2022, 2023]
ANCHOR           = "youtube"
BATCH_SIZE          = 4    # GT allows 5 keywords max; 1 slot reserved for anchor
CALL_DELAY          = 6    # seconds
SUGGESTION_DELAY    = 2    # seconds between suggestions API calls
NORMALIZATION_SCALE = 100  # multiplier applied to interest_normalized for readability

COUNTRIES = [
    "US", "GB", "DE", "FR", "JP", "KR", "BR", "CA", "AU", "RU",
    "IN", "MX", "IT", "ES", "PL", "NL", "SE", "NO", "DK", "FI",
    "BE", "AT", "CH", "CZ", "HU", "RO", "UA", "TR", "SA", "ZA",
    "EG", "NG", "AR", "CL", "CO", "ID", "PH", "TH", "VN", "MY",
    "SG", "HK", "TW", "NZ", "PT", "GR", "IL", "AE", "IR", "PK",
]

CHECKPOINT_FIELDNAMES = ["iso2", "year_id", "term_id", "keyword", "entity_mid", "entity_type",
                         "interest_raw", "interest_normalized", "anchor_term", "anchor_raw"]
OUTPUT_FIELDNAMES     = ["iso2", "year_id", "term_id", "keyword", "entity_type",
                         "interest_raw", "interest_normalized", "anchor_term", "anchor_raw"]
ANCHOR_FIELDNAMES     = ["iso2", "year_id", "anchor_term", "interest_raw"]


def load_search_terms(path: str) -> list[dict]:
    with open(path, newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f, delimiter=";"))


def discover_entities(pytrends: TrendReq, terms: list[dict]) -> list[dict]:
    """
    For each search term, fetch all GT entity variants via suggestions API.
    Always includes plain-text "Search term". Adds one entry per entity mid found.
    Returns flat list: [{term_id, keyword, mid, entity_type, query_key}, ...]
    query_key is the mid for entities, or the keyword itself for plain-text.
    """
    entities = []
    for term in terms:
        keyword = term["keyword"]
        term_id = term["term_id"]

        entities.append({
            "term_id":     term_id,
            "keyword":     keyword,
            "mid":         "",
            "entity_type": "Search term",
            "query_key":   keyword,
        })

        try:
            for s in pytrends.suggestions(keyword):
                mid = s.get("mid", "")
                if mid:
                    entities.append({
                        "term_id":     term_id,
                        "keyword":     keyword,
                        "mid":         mid,
                        "entity_type": s.get("type", "Unknown"),
                        "query_key":   mid,
                    })
        except Exception as exc:
            print(f"  suggestions failed for '{keyword}': {exc}")

        time.sleep(SUGGESTION_DELAY)

    return entities


def load_done_pairs(path: str) -> set[tuple]:
    """Returns set of (iso2, year_id, term_id, entity_mid) already written to checkpoint.
    Skips truncated last row caused by interrupted run."""
    if not Path(path).exists():
        return set()
    done: set[tuple] = set()
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f, delimiter=";"):
            try:
                done.add((row["iso2"], row["year_id"], row["term_id"], row["entity_mid"]))
            except KeyError:
                pass
    return done


def make_batches(entities: list[dict]) -> list[list[dict]]:
    non_anchor = [e for e in entities if e["keyword"].lower() != ANCHOR]
    return [non_anchor[i:i + BATCH_SIZE] for i in range(0, len(non_anchor), BATCH_SIZE)]


def fetch_country_batch(pytrends: TrendReq, query_keys: list[str], iso2: str) -> dict[int, dict[str, float]] | None:
    """
    Fetch monthly interest for ANCHOR + query_keys in iso2 across all YEARS.
    GT normalizes across the full YEARS range (peak over all years = 100).
    Returns {year: {query_key: monthly_sum}} or None after 3 failed attempts.
    """
    kw_list   = [ANCHOR] + query_keys
    timeframe = f"{YEARS[0]}-01-01 {YEARS[-1]}-12-31"

    for attempt in range(1, 4):
        try:
            pytrends.build_payload(kw_list, timeframe=timeframe, geo=iso2)
            df = pytrends.interest_over_time()
            if df is None or df.empty:
                return None
            if "isPartial" in df.columns:
                df = df[~df["isPartial"].astype(bool)]
            return {
                year: {
                    kw: float(df.loc[df.index.year == year, kw].sum())
                    for kw in kw_list if kw in df.columns
                }
                for year in YEARS
            }
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
    rows.sort(key=lambda r: (r["iso2"], int(r["year_id"]), r["term_id"], r["entity_mid"]))

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_FIELDNAMES, delimiter=";", extrasaction="ignore")
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

        terms    = load_search_terms(SEARCH_TERM_FILE)
        pytrends = TrendReq(hl="en-US", tz=0, timeout=(10, 25))

        print(f"Discovering entity variants for {len(terms)} keywords...")
        entities = discover_entities(pytrends, terms)
        print(f"  found {len(entities)} entity variants total")

        batches    = make_batches(entities)
        done_pairs = load_done_pairs(CHECKPOINT_FILE)

        if done_pairs:
            print(f"Resuming: {len(done_pairs)} (iso2, year, term_id, entity_mid) entries already done")

        total_calls = len(COUNTRIES) * len(batches)
        call_num    = 0

        with open(CHECKPOINT_FILE, "a", newline="", encoding="utf-8") as checkpoint_handle:
            checkpoint_writer = csv.DictWriter(checkpoint_handle, fieldnames=CHECKPOINT_FIELDNAMES, delimiter=";")
            if not done_pairs:
                checkpoint_writer.writeheader()

            for iso2 in COUNTRIES:
                for batch_idx, batch in enumerate(batches):
                    query_keys = [e["query_key"] for e in batch]
                    entity_ids = [(e["term_id"], e["mid"]) for e in batch]
                    call_num  += 1

                    if all(
                        (iso2, str(year), tid, mid) in done_pairs
                        for year in YEARS for tid, mid in entity_ids
                    ):
                        print(f"[{call_num}/{total_calls}] {iso2} batch={batch_idx} — skip")
                        continue

                    print(f"[{call_num}/{total_calls}] {iso2} query_keys={query_keys}")

                    data = fetch_country_batch(pytrends, query_keys, iso2)

                    if data is None:
                        print(f"    no data — skipping, will retry on re-run")
                        time.sleep(CALL_DELAY)
                        continue

                    for year, year_data in data.items():
                        anchor_val = year_data.get(ANCHOR, 0.0)

                        for entity in batch:
                            key = (iso2, str(year), entity["term_id"], entity["mid"])
                            if key in done_pairs:
                                continue
                            raw        = year_data.get(entity["query_key"], 0.0)
                            normalized = raw / anchor_val * NORMALIZATION_SCALE if anchor_val > 0 else None
                            checkpoint_writer.writerow({
                                "iso2":                iso2,
                                "year_id":             year,
                                "term_id":             entity["term_id"],
                                "keyword":             entity["keyword"],
                                "entity_mid":          entity["mid"],
                                "entity_type":         entity["entity_type"],
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
