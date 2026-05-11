-- Created by Redgate Data Modeler (https://datamodeler.redgate-platform.com)
-- Last modification date: 2026-05-10 20:28:35.359

-- tables
-- Table: DIM_ANCHOR_TERM
CREATE TABLE DIM_ANCHOR_TERM (
    Id int  NOT NULL,
    Term nvarchar(50)  NOT NULL,
    CONSTRAINT DIM_ANCHOR_TERM_pk PRIMARY KEY  (Id)
);

-- Table: DIM_CONFLICT_SOURCE
CREATE TABLE DIM_CONFLICT_SOURCE (
    id int  NOT NULL,
    name nvarchar  NOT NULL,
    CONSTRAINT DIM_CONFLICT_SOURCE_pk PRIMARY KEY  (id)
);

-- Table: DIM_CONFLICT_TYPE
CREATE TABLE DIM_CONFLICT_TYPE (
    id int  NOT NULL,
    name nvarchar  NOT NULL,
    CONSTRAINT DIM_CONFLICT_TYPE_pk PRIMARY KEY  (id)
);

-- Table: DIM_COUNTRY
CREATE TABLE DIM_COUNTRY (
    iso3 char(3)  NOT NULL,
    iso2 char(2)  NOT NULL,
    name nvarchar(100)  NOT NULL,
    region nvarchar(60)  NOT NULL,
    subregion nvarchar(100)  NULL,
    income_level nvarchar(40)  NULL,
    lat decimal(10,7)  NULL,
    lon decimal(10,7)  NULL,
    CONSTRAINT UQ_DIM_COUNTRY_iso2 UNIQUE (iso2),
    CONSTRAINT PK_DIM_COUNTRY PRIMARY KEY  (iso3)
);

-- Table: DIM_SEARCH_TERM
CREATE TABLE DIM_SEARCH_TERM (
    term_id int  NOT NULL IDENTITY(1, 1),
    keyword nvarchar(100)  NOT NULL,
    genre nvarchar(60)  NULL,
    pricing_model nvarchar(20)  NULL,
    CONSTRAINT UQ_DIM_SEARCH_TERM_keyword UNIQUE (keyword),
    CONSTRAINT PK_DIM_SEARCH_TERM PRIMARY KEY  (term_id)
);

-- Table: DIM_TIME
CREATE TABLE DIM_TIME (
    year_id int  NOT NULL,
    year smallint  NOT NULL,
    CONSTRAINT PK_DIM_TIME PRIMARY KEY  (year_id)
);

-- Table: FACT_CONFLICT
CREATE TABLE FACT_CONFLICT (
    iso3 char(3)  NOT NULL,
    year_id int  NOT NULL,
    fatalities int  NOT NULL DEFAULT 0,
    events_count int  NOT NULL DEFAULT 0,
    battles_count int  NOT NULL DEFAULT 0,
    protests_count int  NOT NULL DEFAULT 0,
    loaded_at datetime2  NOT NULL DEFAULT sysutcdatetime(),
    conflict_type_id int  NOT NULL,
    conflict_source_id int  NOT NULL,
    CONSTRAINT PK_FACT_CONFLICT PRIMARY KEY  (iso3,year_id)
);

CREATE INDEX IX_FACT_CON_year on FACT_CONFLICT (year_id ASC,iso3 ASC)
;

-- Table: FACT_ECONOMY
CREATE TABLE FACT_ECONOMY (
    iso3 char(3)  NOT NULL,
    year_id int  NOT NULL,
    gdp_per_capita decimal(14,2)  NULL,
    gdp_growth_pct decimal(8,4)  NULL,
    internet_pct decimal(6,2)  NULL,
    mobile_subscriptions_p100 decimal(8,2)  NULL,
    broadband_subscriptions_p100 decimal(8,2)  NULL,
    urban_pct decimal(6,2)  NULL,
    loaded_at datetime2  NOT NULL DEFAULT sysutcdatetime(),
    CONSTRAINT PK_FACT_ECONOMY PRIMARY KEY  (iso3,year_id)
);

CREATE INDEX IX_FACT_ECO_year on FACT_ECONOMY (year_id ASC)
;

-- Table: FACT_GOOGLE_TRENDS
CREATE TABLE FACT_GOOGLE_TRENDS (
    iso2 char(2)  NOT NULL,
    year_id int  NOT NULL,
    term_id int  NOT NULL,
    interest_normalized decimal(10,4)  NULL,
    interest_raw int  NULL,
    anchor_term_Id int  NOT NULL,
    anchor_raw int  NOT NULL,
    CONSTRAINT PK_FACT_GOOGLE_TRENDS PRIMARY KEY  (iso2,year_id,term_id)
);

CREATE INDEX IX_FACT_GT_term on FACT_GOOGLE_TRENDS (term_id ASC,year_id ASC)
;

CREATE INDEX IX_FACT_GT_year on FACT_GOOGLE_TRENDS (year_id ASC,iso2 ASC)
;

-- Table: FACT_GOVERNANCE
CREATE TABLE FACT_GOVERNANCE (
    iso3 char(3)  NOT NULL,
    year_id int  NOT NULL,
    stability decimal(8,4)  NULL,
    freedom_of_speech decimal(8,4)  NULL,
    goverment_effciency decimal(8,4)  NULL,
    corruption decimal(8,4)  NULL,
    righteousness decimal(8,4)  NULL,
    regulation_quality decimal(8,4)  NULL,
    CONSTRAINT PK_FACT_GOVERNANCE PRIMARY KEY  (iso3,year_id)
);

CREATE INDEX IX_FACT_GOV_year on FACT_GOVERNANCE (year_id ASC)
;

-- Table: FACT_MILITARY
CREATE TABLE FACT_MILITARY (
    iso3 char(3)  NOT NULL,
    year_id int  NOT NULL,
    armed_forces_total int  NULL,
    armed_forces_pct decimal(6,3)  NULL,
    military_expenditure_pct decimal(6,3)  NULL,
    CONSTRAINT PK_FACT_MILITARY PRIMARY KEY  (iso3,year_id)
);

CREATE INDEX IX_FACT_MIL_year on FACT_MILITARY (year_id ASC)
;

-- Table: FACT_SOCIETY
CREATE TABLE FACT_SOCIETY (
    iso3 char(3)  NOT NULL,
    year_id int  NOT NULL,
    tertiary_enrollment_pct decimal(6,2)  NULL,
    secondary_enrollment_pct decimal(6,2)  NULL,
    literacy_rate_pct decimal(6,2)  NULL,
    population_total bigint  NULL,
    pop_working_age_pct decimal(6,2)  NULL,
    refugees_total int  NULL,
    CONSTRAINT PK_FACT_SOCIETY PRIMARY KEY  (iso3,year_id)
);

CREATE INDEX IX_FACT_SOC_year on FACT_SOCIETY (year_id ASC)
;

-- Table: STG_WB_INDICATOR
CREATE TABLE STG_WB_INDICATOR (
    iso3 char(3)  NOT NULL,
    year_id int  NOT NULL,
    indicator_code varchar(30)  NOT NULL,
    value decimal(18,6)  NULL,
    obs_status varchar(10)  NULL,
    loaded_at datetime2  NOT NULL DEFAULT sysutcdatetime(),
    CONSTRAINT PK_STG_WB_INDICATOR PRIMARY KEY  (iso3,year_id,indicator_code)
);

CREATE INDEX IX_STG_WB_code on STG_WB_INDICATOR (indicator_code ASC)
;

-- foreign keys
-- Reference: FACT_CONFLICT_DIM_CONFLICT_SOURCE (table: FACT_CONFLICT)
ALTER TABLE FACT_CONFLICT ADD CONSTRAINT FACT_CONFLICT_DIM_CONFLICT_SOURCE
    FOREIGN KEY (conflict_source_id)
    REFERENCES DIM_CONFLICT_SOURCE (id);

-- Reference: FACT_CONFLICT_DIM_CONFLICT_TYPE (table: FACT_CONFLICT)
ALTER TABLE FACT_CONFLICT ADD CONSTRAINT FACT_CONFLICT_DIM_CONFLICT_TYPE
    FOREIGN KEY (conflict_type_id)
    REFERENCES DIM_CONFLICT_TYPE (id);

-- Reference: FACT_GOOGLE_TRENDS_Anchor_Term (table: FACT_GOOGLE_TRENDS)
ALTER TABLE FACT_GOOGLE_TRENDS ADD CONSTRAINT FACT_GOOGLE_TRENDS_Anchor_Term
    FOREIGN KEY (anchor_term_Id)
    REFERENCES DIM_ANCHOR_TERM (Id);

-- Reference: FK_FACT_CON_country (table: FACT_CONFLICT)
ALTER TABLE FACT_CONFLICT ADD CONSTRAINT FK_FACT_CON_country
    FOREIGN KEY (iso3)
    REFERENCES DIM_COUNTRY (iso3);

-- Reference: FK_FACT_CON_year (table: FACT_CONFLICT)
ALTER TABLE FACT_CONFLICT ADD CONSTRAINT FK_FACT_CON_year
    FOREIGN KEY (year_id)
    REFERENCES DIM_TIME (year_id);

-- Reference: FK_FACT_ECO_country (table: FACT_ECONOMY)
ALTER TABLE FACT_ECONOMY ADD CONSTRAINT FK_FACT_ECO_country
    FOREIGN KEY (iso3)
    REFERENCES DIM_COUNTRY (iso3);

-- Reference: FK_FACT_ECO_year (table: FACT_ECONOMY)
ALTER TABLE FACT_ECONOMY ADD CONSTRAINT FK_FACT_ECO_year
    FOREIGN KEY (year_id)
    REFERENCES DIM_TIME (year_id);

-- Reference: FK_FACT_GOV_country (table: FACT_GOVERNANCE)
ALTER TABLE FACT_GOVERNANCE ADD CONSTRAINT FK_FACT_GOV_country
    FOREIGN KEY (iso3)
    REFERENCES DIM_COUNTRY (iso3);

-- Reference: FK_FACT_GOV_year (table: FACT_GOVERNANCE)
ALTER TABLE FACT_GOVERNANCE ADD CONSTRAINT FK_FACT_GOV_year
    FOREIGN KEY (year_id)
    REFERENCES DIM_TIME (year_id);

-- Reference: FK_FACT_GT_country (table: FACT_GOOGLE_TRENDS)
ALTER TABLE FACT_GOOGLE_TRENDS ADD CONSTRAINT FK_FACT_GT_country
    FOREIGN KEY (iso2)
    REFERENCES DIM_COUNTRY (iso2);

-- Reference: FK_FACT_GT_term (table: FACT_GOOGLE_TRENDS)
ALTER TABLE FACT_GOOGLE_TRENDS ADD CONSTRAINT FK_FACT_GT_term
    FOREIGN KEY (term_id)
    REFERENCES DIM_SEARCH_TERM (term_id);

-- Reference: FK_FACT_GT_year (table: FACT_GOOGLE_TRENDS)
ALTER TABLE FACT_GOOGLE_TRENDS ADD CONSTRAINT FK_FACT_GT_year
    FOREIGN KEY (year_id)
    REFERENCES DIM_TIME (year_id);

-- Reference: FK_FACT_MIL_country (table: FACT_MILITARY)
ALTER TABLE FACT_MILITARY ADD CONSTRAINT FK_FACT_MIL_country
    FOREIGN KEY (iso3)
    REFERENCES DIM_COUNTRY (iso3);

-- Reference: FK_FACT_MIL_year (table: FACT_MILITARY)
ALTER TABLE FACT_MILITARY ADD CONSTRAINT FK_FACT_MIL_year
    FOREIGN KEY (year_id)
    REFERENCES DIM_TIME (year_id);

-- Reference: FK_FACT_SOC_country (table: FACT_SOCIETY)
ALTER TABLE FACT_SOCIETY ADD CONSTRAINT FK_FACT_SOC_country
    FOREIGN KEY (iso3)
    REFERENCES DIM_COUNTRY (iso3);

-- Reference: FK_FACT_SOC_year (table: FACT_SOCIETY)
ALTER TABLE FACT_SOCIETY ADD CONSTRAINT FK_FACT_SOC_year
    FOREIGN KEY (year_id)
    REFERENCES DIM_TIME (year_id);

-- End of file.

