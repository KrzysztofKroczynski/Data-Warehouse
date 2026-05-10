# SSIS Implementation Plan

## Project Context
This project builds a data warehouse for country-level analytics using:
- ACLED conflict data
- World Bank / WGI indicators
- Google Trends search interest

Current selected stack:
- ETL / orchestration: SSIS + SQL Server Agent
- Data warehouse: SQL Server
- Semantic layer: Power BI Model
- Reporting: Power BI

## Current Inputs (already in repository)
- Extract/Indicators/stg_acled_conflict.csv
- Extract/Indicators/stg_wb_indicator.csv
- Extract/SearchTerms/stg_dim_country.csv
- Extract/SearchTerms/stg_dim_search_term.csv
- Extract/SearchTerms/stg_google_trends.csv
- Extract/SearchTerms/stg_youtube_anchor.csv

## Target Warehouse Objects
Defined in SchematBazyDanych.sql:
- Dimensions: DIM_COUNTRY, DIM_TIME, DIM_SEARCH_TERM
- Facts: FACT_CONFLICT, FACT_ECONOMY, FACT_GOVERNANCE, FACT_MILITARY, FACT_SOCIETY, FACT_GOOGLE_TRENDS
- Staging: STG_WB_INDICATOR

## Recommended SSIS Solution Structure
Create one Visual Studio SSIS solution with 4 packages:
1. DW_00_Prepare.dtsx
2. DW_10_LoadDimensions.dtsx
3. DW_20_LoadIndicatorsAndFacts.dtsx
4. DW_30_LoadGoogleTrends.dtsx

Orchestration order:
1. DW_00_Prepare
2. DW_10_LoadDimensions
3. DW_20_LoadIndicatorsAndFacts
4. DW_30_LoadGoogleTrends

## Package Details

### 1) DW_00_Prepare.dtsx
Goal: initialize technical prerequisites.

Steps:
1. Execute SQL Task: ensure target database exists and is selected.
2. Execute SQL Task: run schema script once (or validate required tables).
3. Execute SQL Task: upsert DIM_TIME for 2014-2023 with half='FY'.
4. File System Task: validate all required CSV files exist.
5. Event handler: on error, write to ETL run log table (optional).

## 2) DW_10_LoadDimensions.dtsx
Goal: load all dimensions before facts.

Steps:
1. Data Flow: load DIM_COUNTRY from stg_dim_country.csv.
2. Data Flow: load DIM_SEARCH_TERM from stg_dim_search_term.csv.
3. Execute SQL Task: deduplicate and enforce business keys.
4. Execute SQL Task: quality checks:
   - DIM_COUNTRY iso3 count > 0
   - DIM_SEARCH_TERM keyword count > 0

Load strategy:
- Prefer MERGE (upsert) via staging temp table for each dimension.
- Keep natural keys:
  - DIM_COUNTRY: iso3
  - DIM_SEARCH_TERM: keyword (or term_id if fixed dictionary policy is required)

## 3) DW_20_LoadIndicatorsAndFacts.dtsx
Goal: load indicator staging and transform to fact tables.

Steps:
1. Data Flow: load STG_WB_INDICATOR from stg_wb_indicator.csv.
2. Data Flow: load FACT_CONFLICT from stg_acled_conflict.csv.
3. Execute SQL Task: transform STG_WB_INDICATOR -> FACT_ECONOMY.
4. Execute SQL Task: transform STG_WB_INDICATOR -> FACT_GOVERNANCE.
5. Execute SQL Task: transform STG_WB_INDICATOR -> FACT_MILITARY.
6. Execute SQL Task: transform STG_WB_INDICATOR -> FACT_SOCIETY.
7. Execute SQL Task: post-load checks for row counts and null ratios.

Transformation approach:
- Use SQL PIVOT or conditional aggregation with MAX(CASE WHEN indicator_code=... THEN value END).
- Join to DIM_COUNTRY and DIM_TIME for key validation.
- Use MERGE into each fact by (iso3, year_id).

## 4) DW_30_LoadGoogleTrends.dtsx
Goal: populate FACT_GOOGLE_TRENDS from Google Trends CSV.

Steps:
1. Data Flow: load trends CSV into technical staging table (recommended: STG_GOOGLE_TRENDS).
2. Execute SQL Task: validate iso2 exists in DIM_COUNTRY.
3. Execute SQL Task: validate term_id exists in DIM_SEARCH_TERM.
4. Execute SQL Task: merge into FACT_GOOGLE_TRENDS on (iso2, year_id, term_id).
5. Execute SQL Task: check outliers and null normalization share.

Notes:
- Source uses semicolon delimiter in SearchTerms CSVs.
- interest_raw may exceed tinyint range; if needed, cast safely before inserting into FACT_GOOGLE_TRENDS.interest_raw.

## SQL Server Agent Job
Create one SQL Agent job: DW_Nightly_Load

Steps in job:
1. Run SSIS package DW_00_Prepare
2. Run SSIS package DW_10_LoadDimensions
3. Run SSIS package DW_20_LoadIndicatorsAndFacts
4. Run SSIS package DW_30_LoadGoogleTrends

Schedule:
- Daily at 02:00 (or as required by course demo)

## Data Quality Checks (minimum)
After each run, verify:
1. No orphan keys in facts:
   - FACT_* rows must match DIM_COUNTRY/DIM_TIME
2. Duplicate key checks:
   - FACT_CONFLICT (iso3, year_id)
   - FACT_GOOGLE_TRENDS (iso2, year_id, term_id)
3. Freshness check:
   - max(loaded_at) from loaded tables is from latest run
4. Completeness checks:
   - expected years present (2014-2023 for indicators/conflict)
   - expected years present (2019-2023 for trends if that range is used)

## Delivery Order (What to do first)
1. Create SQL Server database and run SchematBazyDanych.sql.
2. Build DW_00_Prepare and confirm DIM_TIME load.
3. Build DW_10_LoadDimensions and validate dimension counts.
4. Build DW_20_LoadIndicatorsAndFacts and validate all four indicator facts.
5. Build DW_30_LoadGoogleTrends and validate trends fact keys.
6. Configure SQL Server Agent schedule and run full pipeline.
7. Connect Power BI model to SQL Server and create measures.
8. Build final dashboards based on business questions.

## TODO (Ordered)
1. Add technical staging table STG_GOOGLE_TRENDS in SQL Server.
2. Create SSIS project and environment parameters (file paths, connection strings).
3. Implement DW_00_Prepare package.
4. Implement DW_10_LoadDimensions package.
5. Implement DW_20_LoadIndicatorsAndFacts package.
6. Implement DW_30_LoadGoogleTrends package.
7. Add logging table ETL_RUN_LOG and error output paths.
8. Configure SQL Server Agent job and test rerun behavior.
9. Prepare Power BI semantic model.
10. Deliver at least 3 business-ready report pages.
