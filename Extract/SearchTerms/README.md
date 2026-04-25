# Extract — Data Pipeline

Skrypty pobierające dane źródłowe do hurtowni. Wszystkie skrypty uruchamiane z katalogu `Extract/`.

## Wymagania

```bash
pip install requests pycountry pytrends
```

Zależności: `requests`, `pycountry`, `pytrends`

---

## Kolejność uruchomienia

### 1. Kraje — `stg_dim_country.csv`

```bash
python fetch_country_dim.py
```

Pobiera metadane krajów z World Bank API (`/v2/country`).  
Wyjście: `iso3, iso2, name, region, subregion, income_level, lat, lon` (~214 wierszy)

---

### 2. World Bank — `stg_wb_indicator.csv`

```bash
python fetch_wb_indicators.py
```

Pobiera 21 wskaźników WDI + WGI dla wszystkich krajów, lata 2014–2023.  
Wyjście: `iso3, year_id, indicator_code, value, obs_status, loaded_at` (~45 000 wierszy)

---

### 3. ACLED — `stg_acled_conflict.csv`

```bash
python fetch_acled_conflict.py
```

Wymaga konta ACLED — uzupełnij `ACLED_EMAIL` i `ACLED_PASSWORD` w skrypcie przed uruchomieniem.  
Rejestracja: https://acleddata.com/access/  
Wyjście: `iso3, year_id, fatalities, events_count, battles_count, protests_count, conflict_type, source, loaded_at`

---

### 4. Słownik wyszukiwań — `stg_dim_search_term.csv`

```bash
python fetch_steam_top_games.py
```

Pobiera ~250 popularnych gier z SteamSpy API (tag endpoints) z gwarantowaną różnorodnością gatunków (FPS, war, RPG, strategy, simulation, sports itd.) + 22 hard-coded słowa kluczowe.  
Wyjście: `term_id, keyword, genre`

Gatunki przypisywane na podstawie Steam tagów. Gra może mieć wiele gatunków oddzielonych `|` (np. `fps|war|action`).

---

### 5. Google Trends — `stg_google_trends.csv`

```bash
python fetch_google_trends.py
```

**Wymaga** `stg_dim_search_term.csv` (krok 4).  
Pobiera zainteresowanie globalnie per kraj per rok (2019–2023), normalizuje względem anchora `youtube`.  
Czas: ~15 minut (~68 wywołań API × 10s opóźnienia — wzrosło po dodaniu ~250 gier).

Wyjście: `iso2, year_id, term_id, keyword, interest_raw, interest_normalized, anchor_term`

#### Resume po przerwaniu

Skrypt zapisuje checkpoint po każdym batchu do `stg_google_trends_checkpoint.csv`.  
Po przerwaniu wystarczy uruchomić ponownie — skrypt pominie już pobrane dane.

#### Znane braki danych

Google Trends nie zwraca wiarygodnych danych dla: Chin (CN), Korei Północnej (KP), Iranu (IR) i Rosji (RU po 2022). Dla tych krajów `interest_normalized` będzie `NULL` — jest to oczekiwane zachowanie.

---

## Podsumowanie plików wyjściowych

| Plik | Docelowa tabela | Skrypt |
|------|-----------------|--------|
| `stg_dim_country.csv` | `DIM_COUNTRY` | `fetch_country_dim.py` |
| `stg_wb_indicator.csv` | `STG_WB_INDICATOR` → FACT_* | `fetch_wb_indicators.py` |
| `stg_acled_conflict.csv` | `FACT_CONFLICT` | `fetch_acled_conflict.py` |
| `stg_dim_search_term.csv` | `DIM_SEARCH_TERM` | `fetch_steam_top_games.py` |
| `stg_google_trends.csv` | `FACT_GOOGLE_TRENDS` | `fetch_google_trends.py` |
