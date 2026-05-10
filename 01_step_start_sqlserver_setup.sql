/*
Step 1 bootstrap for SQL Server.
What this script does:
1) Creates database DataWarehouse if it does not exist.
2) Checks that core tables from SchematBazyDanych.sql already exist.
3) Creates STG_GOOGLE_TRENDS if missing.
4) Fills DIM_TIME for years 2014-2023 with half='FY' (upsert).
5) Runs quick validation queries.

Important:
- Run SchematBazyDanych.sql first.
- Then run this script.
*/

SET NOCOUNT ON;

IF DB_ID(N'DataWarehouse') IS NULL
BEGIN
    PRINT 'Creating database [DataWarehouse]...';
    EXEC('CREATE DATABASE [DataWarehouse]');
END
ELSE
BEGIN
    PRINT 'Database [DataWarehouse] already exists.';
END
GO

USE [DataWarehouse];
GO

PRINT 'Checking required core tables...';

IF OBJECT_ID(N'dbo.DIM_COUNTRY', N'U') IS NULL
BEGIN
    RAISERROR('Missing table dbo.DIM_COUNTRY. Run SchematBazyDanych.sql first.', 16, 1);
    RETURN;
END

IF OBJECT_ID(N'dbo.DIM_TIME', N'U') IS NULL
BEGIN
    RAISERROR('Missing table dbo.DIM_TIME. Run SchematBazyDanych.sql first.', 16, 1);
    RETURN;
END

IF OBJECT_ID(N'dbo.DIM_SEARCH_TERM', N'U') IS NULL
BEGIN
    RAISERROR('Missing table dbo.DIM_SEARCH_TERM. Run SchematBazyDanych.sql first.', 16, 1);
    RETURN;
END

IF OBJECT_ID(N'dbo.STG_WB_INDICATOR', N'U') IS NULL
BEGIN
    RAISERROR('Missing table dbo.STG_WB_INDICATOR. Run SchematBazyDanych.sql first.', 16, 1);
    RETURN;
END

PRINT 'Core tables found.';

IF OBJECT_ID(N'dbo.STG_GOOGLE_TRENDS', N'U') IS NULL
BEGIN
    PRINT 'Creating dbo.STG_GOOGLE_TRENDS...';

    CREATE TABLE dbo.STG_GOOGLE_TRENDS
    (
        iso2                char(2)         NOT NULL,
        year_id             int             NOT NULL,
        term_id             int             NOT NULL,
        keyword             nvarchar(100)   NULL,
        interest_raw        int             NULL,
        interest_normalized decimal(14,6)   NULL,
        anchor_term         nvarchar(50)    NULL,
        anchor_raw          int             NULL,
        loaded_at           datetime2       NOT NULL CONSTRAINT DF_STG_GT_loaded_at DEFAULT sysutcdatetime(),
        CONSTRAINT PK_STG_GOOGLE_TRENDS PRIMARY KEY (iso2, year_id, term_id)
    );

    CREATE INDEX IX_STG_GT_term_year ON dbo.STG_GOOGLE_TRENDS(term_id, year_id);
    CREATE INDEX IX_STG_GT_year_iso2 ON dbo.STG_GOOGLE_TRENDS(year_id, iso2);
END
ELSE
BEGIN
    PRINT 'Table dbo.STG_GOOGLE_TRENDS already exists.';
END

PRINT 'Upserting DIM_TIME for years 2014-2023...';

;WITH y AS
(
    SELECT 2014 AS year_id
    UNION ALL
    SELECT year_id + 1
    FROM y
    WHERE year_id < 2023
)
MERGE dbo.DIM_TIME AS tgt
USING
(
    SELECT year_id, CAST(year_id AS smallint) AS [year], CAST('FY' AS char(2)) AS half
    FROM y
) AS src
ON tgt.year_id = src.year_id
WHEN MATCHED THEN
    UPDATE SET tgt.[year] = src.[year], tgt.half = src.half
WHEN NOT MATCHED BY TARGET THEN
    INSERT (year_id, [year], half)
    VALUES (src.year_id, src.[year], src.half)
;

PRINT 'Validation summary:';

SELECT 'DIM_COUNTRY' AS table_name, COUNT(*) AS row_count FROM dbo.DIM_COUNTRY
UNION ALL
SELECT 'DIM_TIME', COUNT(*) FROM dbo.DIM_TIME
UNION ALL
SELECT 'DIM_SEARCH_TERM', COUNT(*) FROM dbo.DIM_SEARCH_TERM
UNION ALL
SELECT 'STG_WB_INDICATOR', COUNT(*) FROM dbo.STG_WB_INDICATOR
UNION ALL
SELECT 'STG_GOOGLE_TRENDS', COUNT(*) FROM dbo.STG_GOOGLE_TRENDS
;

SELECT MIN(year_id) AS min_year, MAX(year_id) AS max_year, COUNT(*) AS total_years
FROM dbo.DIM_TIME
OPTION (MAXRECURSION 100);

PRINT 'Step 1 SQL setup completed.';
