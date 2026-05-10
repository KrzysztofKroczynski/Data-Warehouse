"""
SteamSpy API -> ~250 popular games with genre diversity -> stg_dim_search_term.csv
Target table: DIM_SEARCH_TERM(term_id, keyword, genre)
Uses tag endpoints so genre is known at fetch time — no per-game API calls needed.
"""

import csv
import sys
import time
from collections import defaultdict

import requests

OUTPUT_FILE   = "stg_dim_search_term.csv"
REQUEST_DELAY = 2.0
MAX_RETRIES   = 3
TARGET_GAMES  = 250

# Each entry: (Steam tag name, our genre label)
# Games are fetched via tag endpoint -> genre is authoritative, not detected
TAG_ENDPOINTS = [
    ("Shooter",             "fps"),
    ("First-Person",        "fps"),
    ("Military",            "war"),
    ("World War II",        "war"),
    ("War",                 "war"),
    ("Battle Royale",       "battle_royale"),
    ("MOBA",                "moba"),
    ("RPG",                 "rpg"),
    ("Action RPG",          "rpg"),
    ("JRPG",                "rpg"),
    ("Real Time Strategy",  "strategy"),
    ("Turn-Based Strategy", "strategy"),
    ("Grand Strategy",      "strategy"),
    ("Strategy",            "strategy"),
    ("Simulation",          "simulation"),
    ("City Builder",        "simulation"),
    ("Sports",              "sports"),
    ("Racing",              "racing"),
    ("Survival",            "survival"),
    ("Horror",              "horror"),
    ("Puzzle",              "puzzle"),
    ("Platformer",          "platformer"),
    ("Casual",              "casual"),
    ("Action",              "action"),
]

GENRE_QUOTAS = {
    "fps":           25,
    "war":           20,
    "battle_royale": 10,
    "moba":          10,
    "rpg":           30,
    "strategy":      25,
    "simulation":    15,
    "sports":        10,
    "survival":      15,
    "action":        20,
}

# Generic search terms — keep manual genre, no pool lookup
HARD_CODED_TERMS = [
    ("steam",            None),
    ("online games",     None),
    ("war game",         "war"),
    ("battlefield game", "war"),
    ("online gaming",    None),
    ("revolution game",  "political"),
    ("mobile game",      "mobile"),
    ("battle royale",    "battle_royale"),
    ("MOBA game",        "moba"),
    ("RPG game",         "rpg"),
    ("free to play",     None),
    ("buy game steam",   None),
    ("strategy game",    "strategy"),
    ("puzzle game",      "puzzle"),
    ("casual game",      "casual"),
    ("survival game",    "survival"),
    ("escape game",      "escape"),
]

# Actual game titles — look up by appid in pool to get full genre tags
# appid=None means not on Steam (Valorant), fall back to manual genre
HARD_CODED_GAMES = [
    ("Call of Duty",  1962663, "fps|war"),   # CoD MW II as representative
    ("papers please", 239030,  "political"),
    ("disco elysium", 632470,  "political"),
    ("CS2",           730,     "fps"),
    ("valorant",      None,    "fps"),        # not on Steam
]


def steamspy_get(request: str, extra: dict = None) -> dict:
    params = {"request": request}
    if extra:
        params.update(extra)
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get("https://steamspy.com/api.php", params=params, timeout=30)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.RequestException as exc:
            if attempt == MAX_RETRIES:
                print(f"  SteamSpy {request}: failed ({exc})")
                return {}
            time.sleep(REQUEST_DELAY * attempt)
    return {}


def average_forever(game: dict) -> int:
    try:
        return int(game.get("average_forever") or 0)
    except (ValueError, TypeError):
        return 0


def collect_games() -> list[dict]:
    # appid -> {game data + genres set}
    pool: dict[str, dict] = {}
    game_genres: dict[str, set] = defaultdict(set)

    for tag, genre in TAG_ENDPOINTS:
        print(f"Fetching tag={tag} ...")
        results = steamspy_get("tag", {"tag": tag})
        for appid, game in results.items():
            if appid not in pool:
                pool[appid] = game
            game_genres[appid].add(genre)
        time.sleep(REQUEST_DELAY)

    print(f"Pool: {len(pool)} unique games")

    # Attach genres string to each game
    for appid, game in pool.items():
        genres = game_genres[appid]
        # Order genres by TAG_ENDPOINTS priority
        ordered = []
        seen = set()
        for _, g in TAG_ENDPOINTS:
            if g in genres and g not in seen:
                ordered.append(g)
                seen.add(g)
        game["_genres"] = "|".join(ordered) if ordered else "action"
        game["_primary"] = ordered[0] if ordered else "action"

    sorted_games = sorted(pool.values(), key=average_forever, reverse=True)

    genre_counts: dict[str, int] = defaultdict(int)
    selected: list[dict] = []
    seen_names: set[str] = set()

    # Pass 1: fill genre quotas (most popular first per genre)
    for game in sorted_games:
        name = (game.get("name") or "").strip()
        if not name or name in seen_names:
            continue
        primary = game["_primary"]
        quota = GENRE_QUOTAS.get(primary, 0)
        if quota and genre_counts[primary] < quota:
            selected.append(game)
            seen_names.add(name)
            genre_counts[primary] += 1

    # Pass 2: fill remainder to TARGET_GAMES
    for game in sorted_games:
        if len(selected) >= TARGET_GAMES:
            break
        name = (game.get("name") or "").strip()
        if name and name not in seen_names:
            selected.append(game)
            seen_names.add(name)

    print(f"Selected {len(selected)} games")
    for genre, count in sorted(genre_counts.items()):
        print(f"  {genre:20s}: {count}")

    return selected, pool


def main() -> None:
    rows = []
    term_id = 1

    games, pool = collect_games()

    for keyword, manual_genre in HARD_CODED_TERMS:
        rows.append({
            "term_id": term_id,
            "keyword": keyword,
            "genre":   manual_genre or "",
        })
        term_id += 1

    for keyword, appid, fallback_genre in HARD_CODED_GAMES:
        game = pool.get(str(appid)) if appid else None
        genre = game["_genres"] if game else fallback_genre
        rows.append({
            "term_id": term_id,
            "keyword": keyword,
            "genre":   genre,
        })
        term_id += 1

    seen_lower: set[str] = {r["keyword"].lower() for r in rows}

    games_list = games
    added = 0
    for game in games_list:
        name = (game.get("name") or "").strip()
        if not name or name.lower() in seen_lower:
            continue
        rows.append({
            "term_id": term_id,
            "keyword": name,
            "genre":   game["_genres"],
        })
        seen_lower.add(name.lower())
        term_id += 1
        added += 1

    fieldnames = ["term_id", "keyword", "genre"]
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=";")
        writer.writeheader()
        writer.writerows(rows)

    print(f"\nDone -> {OUTPUT_FILE}")
    print(f"  hard-coded terms : {len(HARD_CODED_TERMS) + len(HARD_CODED_GAMES)}")
    print(f"  dynamic games    : {added}")
    print(f"  total rows       : {len(rows)}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted.")
        sys.exit(1)
