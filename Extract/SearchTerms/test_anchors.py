"""
Test anchor candidates for use in fetch_google_trends.py.
All candidates queried in one call per country — GT normalizes them relative to each other,
so values are directly comparable.

Criteria:
  - Zero rate: fraction of (country, year) pairs returning 0 — must be 0%
  - Stability: stdev of yearly values — lower = more consistent normalization
  - Scale: mean value — lower than youtube = better resolution for niche games
"""

import statistics
import time

from pytrends.request import TrendReq

ANCHOR_CANDIDATES = ["gmail", "instagram", "facebook", "amazon", "youtube"]

# Previously tested — for reference:
# youtube:   0.0% zeros, mean=2589.6, stdev=1256.1
# translate: 0.0% zeros, mean=1215.6, stdev=1375.2  ← name varies by language
# weather:   0.0% zeros, mean=986.9,  stdev=1143.2  ← name varies by language
# amazon:    0.0% zeros, mean=906.9,  stdev=988.9
# wikipedia: 4.0% zeros — eliminated

TEST_COUNTRIES = [
    "ET", "TD", "ML", "MZ", "UG",   # very poor Africa — hardest test
    "MG", "ZW", "GH", "SN", "ZM",   # poor Africa
]

YEARS      = [2019, 2020, 2021, 2022, 2023]
CALL_DELAY = 6


def fetch_all_anchors(pytrends: TrendReq, iso2: str) -> dict[str, dict[int, float]]:
    """
    One call per country — returns {anchor: {year: monthly_sum}}.
    All anchors normalized relative to each other within this call.
    """
    timeframe = f"{YEARS[0]}-01-01 {YEARS[-1]}-12-31"
    for attempt in range(1, 4):
        try:
            pytrends.build_payload(ANCHOR_CANDIDATES, timeframe=timeframe, geo=iso2)
            df = pytrends.interest_over_time()
            if df is None or df.empty:
                return {}
            if "isPartial" in df.columns:
                df = df[~df["isPartial"].astype(bool)]
            return {
                anchor: {
                    year: float(df.loc[df.index.year == year, anchor].sum())
                    for year in YEARS
                }
                for anchor in ANCHOR_CANDIDATES
                if anchor in df.columns
            }
        except Exception as exc:
            wait = 60 * attempt
            print(f"  error ({iso2}): {exc} — retry {attempt}/3 in {wait}s")
            time.sleep(wait)
    return {}


def main() -> None:
    pytrends = TrendReq(hl="en-US", tz=0, timeout=(10, 25))

    stats: dict[str, list[float]] = {a: [] for a in ANCHOR_CANDIDATES}
    zeros: dict[str, int]         = {a: 0  for a in ANCHOR_CANDIDATES}
    total = 0

    for iso2 in TEST_COUNTRIES:
        print(f"\n{iso2}")
        data = fetch_all_anchors(pytrends, iso2)
        time.sleep(CALL_DELAY)

        if not data:
            print("  no data")
            continue

        total += len(YEARS)
        for anchor in ANCHOR_CANDIDATES:
            yearly = data.get(anchor, {})
            vals   = [yearly.get(y, 0.0) for y in YEARS]
            zeros[anchor] += sum(1 for v in vals if v == 0.0)
            stats[anchor].extend(vals)
            print(f"  {anchor:<12}: {' '.join(f'{v:6.1f}' for v in vals)}")

    print("\n\n=== SUMMARY ===")
    print(f"{'anchor':<12} {'zero_rate':>10} {'mean':>8} {'stdev':>8}")
    print("-" * 42)
    for anchor in sorted(ANCHOR_CANDIDATES, key=lambda a: (zeros[a], statistics.stdev(stats[a]) if len(stats[a]) > 1 else 0)):
        vals      = stats[anchor]
        zero_rate = zeros[anchor] / total if total else 1.0
        mean      = statistics.mean(vals) if vals else 0.0
        stdev     = statistics.stdev(vals) if len(vals) > 1 else 0.0
        marker    = " ← candidate" if zero_rate == 0 and anchor != "youtube" else ""
        print(f"{anchor:<12} {zero_rate:>9.1%} {mean:>8.1f} {stdev:>8.1f}{marker}")


if __name__ == "__main__":
    main()
