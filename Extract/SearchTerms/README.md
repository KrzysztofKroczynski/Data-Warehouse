# SearchTerms — Extract Pipeline

Skrypty pobierające dane do tabel `DIM_SEARCH_TERM` i `FACT_GOOGLE_TRENDS`.

## Wymagania

```bash
pip install pytrends pandas requests pycountry
```

---

## Kolejność uruchomienia

### 1. Kraje — `stg_dim_country.csv`

```bash
python fetch_country_dim.py
```

Pobiera metadane krajów z World Bank API (`/v2/country`).  
Wyjście: `iso3, iso2, name, region, subregion, income_level, lat, lon` (~214 wierszy)

---

### 2. Słownik wyszukiwań — `stg_dim_search_term.csv`

```bash
python fetch_steam_top_games.py
```

Pobiera ~250 popularnych gier z SteamSpy API z gwarantowaną różnorodnością gatunków (FPS, war, RPG, strategy, simulation, sports itd.) + hard-coded słowa kluczowe.  
Wyjście: `term_id, keyword, genre`

Gatunki przypisywane na podstawie Steam tagów. Gra może mieć wiele gatunków oddzielonych `|` (np. `fps|war|action`).

---

### 3. Google Trends — `stg_google_trends.csv`

```bash
python fetch_google_trends.py
```

**Wymaga** `stg_dim_search_term.csv` (krok 2) i `stg_dim_country.csv` (krok 1).

Pobiera zainteresowanie per kraj per rok (2019–2023), normalizuje względem anchora `amazon`.

#### Jak działa

1. **Entity discovery** — dla każdego słowa kluczowego pobiera warianty encji z GT Suggestions API (np. midy dla gier jako obiektów Knowledge Graph). Każda encja odpytywana osobno.
2. **Batche** — encje grupowane w batche po 4 (GT limit 5 słów kluczowych; 1 slot zajmuje anchor).
3. **Per-kraj per-batch** — jeden call do GT pokrywa wszystkie lata 2019–2023 naraz; GT normalizuje dane do peak=100 w tym zakresie.
4. **Normalizacja** — `interest_normalized = interest_raw / anchor_raw × 100`; przy `anchor_raw = 0` wartość to `NULL`.
5. **Wybór najlepszej encji** — dla każdego `(iso2, year_id, term_id)` zachowywana jest encja z najwyższym `interest_normalized`.

Szacowany czas: ~1.5h (~950 calli, 6s delay między callami + opóźnienia retry).

#### Wyjście

| Plik | Zawartość |
|------|-----------|
| `stg_google_trends.csv` | `iso2, year_id, term_id, keyword, interest_raw, interest_normalized, anchor_term, anchor_raw` |
| `stg_youtube_anchor.csv` | `iso2, year_id, anchor_term, interest_raw` — dane anchora per kraj per rok |

#### Resume po przerwaniu

Skrypt zapisuje każdy ukończony batch do `stg_google_trends_checkpoint.csv`.  
Po przerwaniu wystarczy uruchomić ponownie — skrypt pominie już pobrane dane.

Aby tylko przeliczyć pliki wyjściowe z istniejącego checkpointa (bez pobierania):

```bash
python fetch_google_trends.py --flush
```

#### Znane braki danych

Google Trends nie zwraca wiarygodnych danych dla Chin (CN), Korei Północnej (KP), Iranu (IR) i Rosji (RU po 2022). Dla tych krajów `interest_normalized` będzie `NULL` — oczekiwane zachowanie.

---

## Podsumowanie plików wyjściowych

| Plik | Docelowa tabela | Skrypt |
|------|-----------------|--------|
| `stg_dim_country.csv` | `DIM_COUNTRY` | `fetch_country_dim.py` |
| `stg_dim_search_term.csv` | `DIM_SEARCH_TERM` | `fetch_steam_top_games.py` |
| `stg_google_trends.csv` | `FACT_GOOGLE_TRENDS` | `fetch_google_trends.py` |
| `stg_youtube_anchor.csv` | `FACT_GOOGLE_TRENDS` (anchor) | `fetch_google_trends.py` |
