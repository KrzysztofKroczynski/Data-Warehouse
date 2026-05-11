# Data Warehouse Project Plan (SSIS + SQL Server + Power BI)

## 1. Project Goal
Build an end-to-end analytics platform for country-level analysis combining:
- conflict data (ACLED)
- socio-economic and governance indicators (World Bank / WGI)
- search behavior trends (Google Trends)

The final result must include:
- working ETL pipeline
- multidimensional data model
- semantic layer
- business reports that answer project questions

## 2. Selected Stack
- ETL / orchestration: SSIS + SQL Server Agent
- Data warehouse: SQL Server
- Semantic layer: Power BI Model
- Reporting: Power BI

## 2.1 SQL Bootstrap Scripts (Run First)
Before SSIS development, run these SQL scripts in SSMS.

### Script A: SchematBazyDanych.sql
Purpose:
1. Creates the core warehouse schema.
2. Creates dimensions, facts, staging table, indexes, and foreign keys.

When to run:
1. First run on a clean database.
2. Re-run only if you intentionally rebuild schema.

### Script B: 01_step_start_sqlserver_setup.sql
Purpose:
1. Creates database DataWarehouse if missing.
2. Validates core tables from SchematBazyDanych.sql.
3. Creates technical stage table STG_GOOGLE_TRENDS if missing.
4. Upserts DIM_TIME for years 2019-2023.
5. Prints validation counts.

When to run:
1. Immediately after Script A.
2. Safe to re-run for validation.

### Script C: 04_step_update_country_coords_from_csv.sql
Purpose:
1. Loads Extract/SearchTerms/stg_dim_country.csv into a temporary staging table.
2. Normalizes iso3, lat, and lon values.
3. Updates DIM_COUNTRY.lat and DIM_COUNTRY.lon directly in SQL Server.
4. Prints validation counts and sample rows with coordinates.

When to run:
1. After DIM_COUNTRY has already been loaded by SSIS.
2. Any time country coordinates need to be refreshed from the source CSV.
3. Preferred over adding complex lat/lon transformations to the SSIS package.

### Exact Execution Order in SSMS
1. Open SSMS and connect to SQL Server instance.
2. Open and execute SchematBazyDanych.sql.
3. Open and execute 01_step_start_sqlserver_setup.sql.
4. Run the SSIS dimension load package.
5. Open and execute 04_step_update_country_coords_from_csv.sql.
6. Confirm there are no blocking errors in Messages.
7. Confirm row counts from validation output are visible.

Expected result after Script B:
1. DIM_TIME has 5 rows (2019-2023).
2. STG_GOOGLE_TRENDS exists.
3. Other tables may still have 0 rows until SSIS loads are built and executed.

Expected result after Script C:
1. DIM_COUNTRY coordinates are updated directly from the country CSV.
2. Countries with missing coordinates in the source remain NULL.
3. The SQL validation output shows how many DIM_COUNTRY rows now have both lat and lon.

If you get "object already exists":
1. It usually means schema was already partially created.
2. Continue only after confirming required tables exist.
3. For clean rerun, rebuild database intentionally, then run Script A and Script B again.

## 3. Source Inputs Available in Repository
- Extract/Indicators/stg_acled_conflict.csv
- Extract/Indicators/stg_wb_indicator.csv
- Extract/SearchTerms/stg_dim_country.csv
- Extract/SearchTerms/stg_dim_search_term.csv
- Extract/SearchTerms/stg_google_trends.csv
- Extract/SearchTerms/stg_youtube_anchor.csv

## 4. Target Warehouse Scope
Schema source: SchematBazyDanych.sql

Dimensions:
- DIM_ANCHOR_TERM
- DIM_CONFLICT_SOURCE
- DIM_CONFLICT_TYPE
- DIM_COUNTRY
- DIM_TIME
- DIM_SEARCH_TERM

Facts:
- FACT_CONFLICT
- FACT_ECONOMY
- FACT_GOVERNANCE
- FACT_MILITARY
- FACT_SOCIETY
- FACT_GOOGLE_TRENDS

Staging:
- STG_WB_INDICATOR
- STG_GOOGLE_TRENDS (technical stage created by bootstrap script)

## 5. Full Delivery Roadmap

### Phase 0 - Project Setup
Objective: ensure reproducible environment and repository readiness.

Tasks:
1. Create SQL Server database for DWH.
2. Apply schema from SchematBazyDanych.sql.
3. Add technical table STG_GOOGLE_TRENDS.
4. Seed static dimensions used by facts:
   - DIM_CONFLICT_TYPE
   - DIM_CONFLICT_SOURCE
   - DIM_ANCHOR_TERM
5. Create folder convention for input, archive, rejects, and logs.
6. Define naming convention for SSIS packages and SQL objects.

Deliverables:
- database created and script applied
- confirmed table list
- technical staging table for trends

Exit criteria:
- all required tables exist
- connection from SSIS to SQL Server works

### Phase 1 - ETL Foundation in SSIS
Objective: build a stable and rerunnable SSIS solution.

SSIS packages:
1. DW_00_Prepare.dtsx
2. DW_10_LoadDimensions.dtsx
3. DW_20_LoadIndicatorsAndFacts.dtsx
4. DW_30_LoadGoogleTrends.dtsx

Tasks:
1. Create SSIS project parameters:
   - SQL connection string
   - input folder path
   - archive folder path
2. Build DW_00_Prepare:
   - validate files
   - load DIM_TIME (2019-2023)
3. Build DW_10_LoadDimensions:
   - load DIM_COUNTRY
   - load DIM_SEARCH_TERM
   - load/seed DIM_CONFLICT_TYPE, DIM_CONFLICT_SOURCE, DIM_ANCHOR_TERM
   - deduplicate and key checks
4. Build DW_20_LoadIndicatorsAndFacts:
   - load STG_WB_INDICATOR
   - load FACT_CONFLICT with conflict_type_id and conflict_source_id mapping
   - transform STG_WB_INDICATOR into FACT_ECONOMY, FACT_GOVERNANCE, FACT_MILITARY, FACT_SOCIETY
5. Build DW_30_LoadGoogleTrends:
   - load STG_GOOGLE_TRENDS
   - key validation against DIM_COUNTRY, DIM_SEARCH_TERM, DIM_ANCHOR_TERM
   - merge into FACT_GOOGLE_TRENDS

Deliverables:
- runnable SSIS solution with 4 packages
- successful full pipeline execution

Exit criteria:
- no failed tasks
- fact and dimension row counts are non-zero
- rerun does not create duplicates

### Phase 2 - Orchestration and Monitoring
Objective: production-like scheduling and run observability.

Tasks:
1. Create SQL Server Agent job DW_Nightly_Load.
2. Add step order:
   - DW_00_Prepare
   - DW_10_LoadDimensions
   - DW_20_LoadIndicatorsAndFacts
   - DW_30_LoadGoogleTrends
3. Configure retries and failure notifications.
4. Create ETL_RUN_LOG table and write status per package.
5. Archive processed files (optional for demo, recommended for final).

Deliverables:
- scheduled job with logs
- execution history visible in SQL Agent and ETL_RUN_LOG

Exit criteria:
- at least two successful full runs
- one forced-failure test with clear error logging

### Phase 3 - Data Quality Layer
Objective: guarantee trustworthiness of analytical outputs.

Checks to implement:
1. Duplicate key checks:
   - FACT_CONFLICT (iso3, year_id)
   - FACT_GOOGLE_TRENDS (iso2, year_id, term_id)
2. Orphan key checks:
   - all fact keys exist in dimensions
3. Freshness checks:
   - loaded_at from latest run
4. Completeness checks:
   - 2019-2023 present for conflict and indicators
   - 2019-2023 present for trends
5. Null and outlier checks:
   - high null share by indicator
   - abnormal spikes in trends and fatalities

Deliverables:
- SQL scripts/views for quality checks
- one quality summary table or report

Exit criteria:
- all critical checks pass
- known exceptions are documented

### Phase 4 - Semantic Model (Power BI)
Objective: create a business-friendly semantic layer.

Tasks:
1. Connect Power BI to SQL Server (import mode recommended for class demo).
2. Build relationships as star schema.
3. Create date/year logic from DIM_TIME.
4. Add core DAX measures, for example:
   - Total Fatalities
   - Events Count
   - Avg GDP per Capita
   - Internet Penetration
   - Governance Composite Score
   - Trends Interest Index
5. Add data category and formatting standards.

Deliverables:
- validated semantic model
- documented measures list

Exit criteria:
- all visuals use model measures
- filters and drill behavior are correct

### Phase 5 - Reporting and Business Questions
Objective: answer required business questions with clear visuals.

Required output:
- 7-12 business questions
- report pages with charts and slicers

Suggested report pages:
1. Conflict Overview by Country and Year
2. Economy vs Conflict Correlation
3. Governance and Stability Map
4. Society and Military Profile
5. Search Trends vs Real-World Indicators

Deliverables:
- Power BI report with at least 3 pages (recommended 5)
- each page mapped to business questions

Exit criteria:
- every business question has at least one visual answer
- report works end-to-end during live demo

### Phase 6 - Finalization and Defense Pack
Objective: prepare final submission and smooth presentation.

Tasks:
1. Freeze dataset version for demo.
2. Prepare architecture diagram.
3. Prepare ETL flow diagram (sources -> SSIS -> DWH -> Power BI).
4. Prepare 5-7 minute demo script.
5. Prepare backup screenshots in case of runtime issues.
6. Prepare Q and A notes (design decisions, limitations, next steps).

Deliverables:
- final presentation deck
- demo script
- validated report file

Exit criteria:
- team can run demo without manual fixes
- all required artifacts are in repository or submission folder

## 6. Definition of Done (Whole Project)
Project is complete when all conditions are true:
1. Data warehouse schema is deployed and populated.
2. SSIS ETL pipeline runs in correct order and is rerunnable.
3. SQL Server Agent job executes successfully on schedule.
4. Data quality checks are implemented and documented.
5. Power BI semantic model is stable and measure-driven.
6. Reports answer 7-12 business questions with visual evidence.
7. Demo package and presentation are ready.

## 7. Risks and Mitigations
1. Data format drift in CSV files.
   Mitigation: strict schema mapping and reject output in SSIS.
2. Inconsistent keys between sources.
   Mitigation: pre-load validation against DIM_COUNTRY and DIM_SEARCH_TERM.
3. Long ETL runtime.
   Mitigation: staged loads, indexes, and incremental rerun strategy.
4. Last-minute demo failure.
   Mitigation: freeze data snapshot and keep offline backup visuals.

## 8. Ordered TODO (Execution Checklist)
1. Deploy schema and create STG_GOOGLE_TRENDS.
2. Build SSIS project parameters and shared connections.
3. Implement DW_00_Prepare.dtsx.
4. Implement DW_10_LoadDimensions.dtsx.
5. Implement DW_20_LoadIndicatorsAndFacts.dtsx.
6. Implement DW_30_LoadGoogleTrends.dtsx.
7. Add ETL_RUN_LOG and package-level error handling.
8. Configure SQL Server Agent job and run full E2E test.
9. Implement quality check SQL scripts/views.
10. Build Power BI model and DAX measures.
11. Build report pages mapped to business questions.
12. Prepare final demo and defense materials.

## 9. Milestone Snapshot (Suggested)
1. Milestone A: Schema + dimensions loaded.
2. Milestone B: All facts loaded and quality checks passing.
3. Milestone C: Agent scheduling + stable reruns.
4. Milestone D: Power BI model complete.
5. Milestone E: Final report + presentation ready.
