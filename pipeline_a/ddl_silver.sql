-- =============================================================================
-- Pipeline A — Silver DDL
-- =============================================================================
-- Creates SILVER.POSITIONS_INTEGRATED — the naive ETL integration table.
--
-- This table is Pipeline A's primary artifact: it looks normalized (one table,
-- consistent column names) but embeds five structural ambiguities (A7–A11)
-- that make AI queries against it unreliable.
--
-- The table is POPULATED by etl_silver.sql (not by direct CSV load).
-- data/seed_v2/positions_integrated.csv is a validation reference only.
--
-- Run after ddl_bronze.sql. Idempotent (CREATE OR REPLACE).
-- =============================================================================

USE ROLE GSF_ROLE;
USE WAREHOUSE GSF_WH;
USE DATABASE GSF_DEMO;

CREATE OR REPLACE TABLE SILVER.POSITIONS_INTEGRATED (

    -- ── Record identity ────────────────────────────────────────────────────────
    -- A7: LOT-* from Topaz (lot-level) coexist with POS-* from Emerald and
    -- NAV-* from Ruby (position-level). Grain is invisible in the schema.
    RECORD_ID           VARCHAR(50)                 COMMENT 'A7: LOT-* (Topaz lots) | POS-* (Emerald positions) | NAV-* (Ruby positions) — mixed grain, no grain column',

    SOURCE_SYSTEM       VARCHAR(10)                 COMMENT 'TOPAZ | EMERALD | RUBY — present but naive AI queries do not filter on it',

    -- ── Account and security identifiers ──────────────────────────────────────
    -- A4: three different account key formats in one column, no cross-source FK
    ACCOUNT_REF         VARCHAR(20)                 COMMENT 'A4: C-XXXXXX (Topaz) | PORT-XXXX (Emerald) | FND-XXXX (Ruby) — three formats, no canonical account_id',

    -- A1: three different security identifier types in one column
    SECURITY_REF        VARCHAR(20)                 COMMENT 'A1: CUSIP (Topaz) | ticker (Emerald) | ISIN (Ruby) — three ID types, no canonical security_id',
    SECURITY_REF_TYPE   VARCHAR(10)                 COMMENT 'CUSIP | TICKER | ISIN — type flag present but not schema-enforced',

    -- A8: ~15% of rows have NULL here — unmastered securities silently dropped by JOIN
    SECURITY_MASTER_ID  VARCHAR(20)                 COMMENT 'A8: NULL for ~15% of rows — 30 securities absent from security master stub; JOIN miss is silent',

    -- ── Dates ─────────────────────────────────────────────────────────────────
    -- A6: settlement date (Topaz) / trade date (Emerald) / NAV strike date (Ruby)
    POSITION_DATE       DATE                        COMMENT 'A6: settlement date (Topaz) | trade date (Emerald) | NAV strike date (Ruby) — same value, different semantics',

    -- ── Financials ────────────────────────────────────────────────────────────
    -- A7: Topaz rows are lot-level — SUM(QUANTITY) triple-counts positions that
    -- appear in all three sources (Topaz lot × n_lots, Emerald × 1, Ruby × 1)
    QUANTITY            DECIMAL(18,4)               COMMENT 'A7: lot-level for Topaz rows — SUM() overcounts vs Emerald/Ruby position-level rows; average overcount ~3x',

    -- A2: three different price sources blended into one column
    PRICE               DECIMAL(18,4)               COMMENT 'A2: custodian EOD (Topaz) | PM evaluated (Emerald, ±0.3%) | NAV (Ruby, ±0.5%) — blended in one column',

    -- A7+A2: meaningless as an aggregate — mixed grain AND mixed price source
    MARKET_VALUE        DECIMAL(18,2)               COMMENT 'A7+A2: computed from mixed grain (lot vs position) AND mixed price sources — SUM() is economically meaningless',

    -- A9: three incompatible cost accounting methods in one column
    COST_BASIS          DECIMAL(18,2)               COMMENT 'A9: specific lot cost (Topaz) | avg cost × qty (Emerald) | book cost (Ruby) — three methods, not comparable',

    -- A11: NULL for all ~4,886 Ruby rows — silently excluded from aggregates
    UNREALIZED_GL       DECIMAL(18,2)               COMMENT 'A11: NULL for all Ruby rows (~22% of total) — fund accounting has no G/L concept; aggregates silently undercount',

    -- A10: NULL for the ~15% of rows with unmastered securities
    ASSET_CLASS         VARCHAR(30)                 COMMENT 'A10: NULL for unmastered rows (~15%) — sleeve % allocations computed on partial universe, look plausible but are wrong',

    CURRENCY            VARCHAR(3)                  COMMENT 'ISO currency code — intentionally resolved correctly (not every column is broken)',

    ETL_LOADED_AT       TIMESTAMP_NTZ               COMMENT 'Fixed per-source batch timestamp: Topaz=06:00, Emerald=08:00, Ruby=10:00 (2025-01-02)'
)
COMMENT = 'Act 2 Silver — Pipeline A naive ETL output. Looks normalized; embeds ambiguities A7 (mixed grain), A8 (unmastered NULLs), A9 (cost basis fragmentation), A10 (asset class gap), A11 (Ruby G/L NULLs).';


-- ── Verify ────────────────────────────────────────────────────────────────────
SHOW TABLES IN SCHEMA GSF_DEMO.SILVER;
