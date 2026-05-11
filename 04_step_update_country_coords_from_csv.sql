/*
Update DIM_COUNTRY coordinates directly from the source CSV.

What this script does:
1) Loads the country CSV into a temp staging table.
2) Normalizes iso3/lat/lon values.
3) Updates dbo.DIM_COUNTRY by iso3.
4) Shows a validation summary.
*/

USE [DataWarehouse];
GO

IF OBJECT_ID('tempdb..#country_coords') IS NOT NULL
    DROP TABLE #country_coords;

CREATE TABLE #country_coords
(
    iso3         varchar(10)    NULL,
    iso2         varchar(10)    NULL,
    name         nvarchar(100)  NULL,
    region       nvarchar(100)  NULL,
    subregion    nvarchar(100)  NULL,
    income_level nvarchar(40)   NULL,
    lat_raw      varchar(32)    NULL,
    lon_raw      varchar(32)    NULL
);

BULK INSERT #country_coords
FROM 'D:\Data-Warehouse\Extract\SearchTerms\stg_dim_country.csv'
WITH
(
    FORMAT = 'CSV',
    FIRSTROW = 2,
    FIELDQUOTE = '"',
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    CODEPAGE = '65001',
    TABLOCK
);

;WITH normalized AS
(
    SELECT
        UPPER(LEFT(LTRIM(RTRIM(REPLACE(iso3, CHAR(13), ''))), 3)) AS iso3,
        TRY_CONVERT(decimal(10,7), NULLIF(LTRIM(RTRIM(REPLACE(lat_raw, CHAR(13), ''))), '')) AS lat_value,
        TRY_CONVERT(decimal(10,7), NULLIF(LTRIM(RTRIM(REPLACE(lon_raw, CHAR(13), ''))), '')) AS lon_value
    FROM #country_coords
)
UPDATE target
SET
    lat = source.lat_value,
    lon = source.lon_value
FROM dbo.DIM_COUNTRY AS target
JOIN normalized AS source
    ON RTRIM(target.iso3) = source.iso3;

SELECT COUNT(*) AS staged_rows FROM #country_coords;

SELECT COUNT(*) AS matched_rows
FROM dbo.DIM_COUNTRY AS target
JOIN
(
    SELECT UPPER(LEFT(LTRIM(RTRIM(REPLACE(iso3, CHAR(13), ''))), 3)) AS iso3
    FROM #country_coords
) AS source
    ON RTRIM(target.iso3) = source.iso3;

SELECT COUNT(*) AS rows_with_coords
FROM dbo.DIM_COUNTRY
WHERE lat IS NOT NULL
  AND lon IS NOT NULL;

SELECT TOP (10)
    iso3,
    iso2,
    name,
    lat,
    lon
FROM dbo.DIM_COUNTRY
WHERE lat IS NOT NULL
  AND lon IS NOT NULL
ORDER BY name;
