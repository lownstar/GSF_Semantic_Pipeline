-- =============================================================================
-- Pipeline A — Bronze DDL
-- =============================================================================
-- Creates the four Bronze tables in GSF_DEMO.BRONZE:
--
--   TOPAZ_POSITIONS        — custodian feed (position-level, CUSIP, ACCT_NUM)
--   EMERALD_POSITIONS      — front office / OMS feed (lot-level, ticker, portfolioId)
--   RUBY_POSITIONS         — fund accounting feed (position-level, ISIN, fund_code)
--   SECURITY_MASTER_STUB   — partial security master (170 of 200; 30 absent → A8/A10 NULLs)
--
-- Column names and types match the seed CSV headers exactly.
-- Comments identify which Ambiguity (A1–A6) each column exposes.
--
-- Run after setup.sql. Idempotent (CREATE OR REPLACE).
-- =============================================================================

USE ROLE GSF_ROLE;
USE WAREHOUSE GSF_WH;
USE DATABASE GSF_DEMO;

-- ── Topaz Positions (custodian feed) ─────────────────────────────────────────
-- Grain: position-level — one row per account × security (Ambiguity A5).
-- Security ID: CUSIP only (Ambiguity A1).
-- Account ID: custodian account number, C-XXXXXX format (Ambiguity A3/A4).
-- Price: custodian EOD closing price — the baseline (Ambiguity A2).
-- Date: AS_OF_DT — settlement/custody date (Ambiguity A6).

CREATE OR REPLACE TABLE BRONZE.TOPAZ_POSITIONS (
    ACCT_NUM    VARCHAR(20)     NOT NULL    COMMENT 'Custodian account number — Topaz source key (A4: no FK to Emerald/Ruby keys)',
    SEC_CUSIP   VARCHAR(9)      NOT NULL    COMMENT 'CUSIP — Topaz security identifier (A1: CUSIP only, no ISIN/ticker)',
    AS_OF_DT    DATE            NOT NULL    COMMENT 'Settlement/custody date (A6: differs from Emerald positionDate and Ruby nav_date)',
    UNITS       DECIMAL(18,4)   NOT NULL    COMMENT 'Position-level quantity — lots aggregated to one row per account × security (A5)',
    MKT_PRC     DECIMAL(18,4)   NOT NULL    COMMENT 'Custodian EOD closing price (A2: differs from Emerald PM evaluated and Ruby NAV price)',
    MKT_VAL     DECIMAL(18,2)   NOT NULL    COMMENT 'Market value = UNITS × MKT_PRC (A3: column name differs across sources)',
    COST_BASIS  DECIMAL(18,2)   NOT NULL    COMMENT 'Custodian cost basis — incompatible with Emerald lot cost and Ruby book cost (A9 precursor)',
    UNRLZD_GL   DECIMAL(18,2)   NOT NULL    COMMENT 'Unrealized gain/loss = MKT_VAL − COST_BASIS',
    CCY         VARCHAR(3)      NOT NULL    COMMENT 'ISO currency code'
)
COMMENT = 'Act 1 Bronze — Topaz custodian raw position feed. Position-level grain (A5), CUSIP IDs (A1), custodian EOD price (A2).';


-- ── Emerald Positions (front office / OMS feed) ──────────────────────────────
-- Grain: lot-level — one row per portfolio × security × lot (Ambiguity A5).
-- Security ID: proprietary internal ticker (Ambiguity A1).
-- Account ID: portfolio code, PORT-XXXX format (Ambiguity A3/A4).
-- Price: PM evaluated price — custodian EOD ± 0.3% variance (Ambiguity A2).
-- Date: POSITION_DATE — trade date (Ambiguity A6).

CREATE OR REPLACE TABLE BRONZE.EMERALD_POSITIONS (
    LOT_ID              VARCHAR(20)     NOT NULL    COMMENT 'Trade lot identifier — lot-level grain marker (A7 precursor: becomes record_id in integrated table)',
    PORTFOLIO_ID        VARCHAR(20)     NOT NULL    COMMENT 'Portfolio code — Emerald source key (A4: no FK to Topaz ACCT_NUM or Ruby FUND_CODE)',
    SECURITY_TICKER     VARCHAR(10)     NOT NULL    COMMENT 'Internal ticker — Emerald security identifier (A1: ticker only, no CUSIP/ISIN)',
    POSITION_DATE       DATE            NOT NULL    COMMENT 'Trade date / PM view date (A6: differs from Topaz AS_OF_DT and Ruby NAV_DATE)',
    QUANTITY            DECIMAL(18,4)   NOT NULL    COMMENT 'Lot-level quantity — SUM() required to get position total (A5: lot detail present)',
    UNIT_PRICE          DECIMAL(18,4)   NOT NULL    COMMENT 'PM evaluated price (A2: differs from Topaz EOD and Ruby NAV)',
    MARKET_VALUE        DECIMAL(18,2)   NOT NULL    COMMENT 'Market value = QUANTITY × UNIT_PRICE (A3: named differently from Topaz MKT_VAL and Ruby TOTAL_NAV_VALUE)',
    LOT_COST_BASIS      DECIMAL(18,2)   NOT NULL    COMMENT 'Specific lot total cost — incompatible with Topaz custodian cost and Ruby book cost (A9 precursor)',
    UNREALIZED_PNL      DECIMAL(18,2)   NOT NULL    COMMENT 'Unrealized P&L based on PM evaluated price',
    CCY                 VARCHAR(3)      NOT NULL    COMMENT 'ISO currency code'
)
COMMENT = 'Act 1 Bronze — Emerald front office / OMS raw position feed. Lot-level grain (A5), ticker IDs (A1), PM evaluated price (A2).';


-- ── Ruby Positions (fund accounting feed) ────────────────────────────────────
-- Grain: position-level — one row per fund × security (Ambiguity A5).
-- Security ID: ISIN (Ambiguity A1).
-- Account ID: fund code, FND-XXXX format (Ambiguity A3/A4).
-- Price: NAV-based price — custodian EOD ± 0.5% variance (Ambiguity A2).
-- Date: NAV_DATE — NAV strike date (Ambiguity A6).
-- NOTE: No unrealized G/L column — fund accounting tracks book cost only (A11 precursor).

CREATE OR REPLACE TABLE BRONZE.RUBY_POSITIONS (
    FUND_CODE           VARCHAR(20)     NOT NULL    COMMENT 'Fund code — Ruby source key (A4: no FK to Topaz ACCT_NUM or Emerald PORTFOLIO_ID)',
    ISIN_IDENTIFIER     VARCHAR(12)     NOT NULL    COMMENT 'ISIN — Ruby security identifier (A1: ISIN only, no CUSIP/ticker)',
    NAV_DATE            DATE            NOT NULL    COMMENT 'NAV strike date (A6: differs from Topaz AS_OF_DT and Emerald POSITION_DATE)',
    SHARES_HELD         DECIMAL(18,4)   NOT NULL    COMMENT 'Position-level quantity (A5: no lot detail)',
    NAV_PER_SHARE       DECIMAL(18,4)   NOT NULL    COMMENT 'NAV-based price (A2: differs from Topaz EOD and Emerald PM evaluated price)',
    TOTAL_NAV_VALUE     DECIMAL(18,2)   NOT NULL    COMMENT 'Total NAV value = SHARES_HELD × NAV_PER_SHARE (A3: named differently from Topaz MKT_VAL and Emerald MARKET_VALUE)',
    BOOK_COST           DECIMAL(18,2)   NOT NULL    COMMENT 'Book cost — fund accounting method, not adjusted for partial redemptions (A9 precursor)',
    CURRENCY_CODE       VARCHAR(3)      NOT NULL    COMMENT 'ISO currency code (A3: named differently from Topaz/Emerald CCY)'
)
COMMENT = 'Act 1 Bronze — Ruby fund accounting raw position feed. Position-level grain (A5), ISIN IDs (A1), NAV price (A2). No unrealized G/L (A11 precursor).';


-- ── Security Master Stub ──────────────────────────────────────────────────────
-- Contains 170 of 200 securities — the 30 "unmastered" ones are absent.
-- The ETL LEFT JOINs on this table using security_ref + security_ref_type.
-- The 30 absent securities produce NULL security_master_id and NULL asset_class
-- in SILVER.POSITIONS_INTEGRATED, demonstrating Ambiguities A8 and A10.
-- Loaded as Bronze because it simulates the partial master a real naive pipeline
-- would have access to — not a governed Gold-layer artifact.

CREATE OR REPLACE TABLE BRONZE.SECURITY_MASTER_STUB (
    CUSIP               VARCHAR(9)      NOT NULL    COMMENT 'CUSIP — joins to TOPAZ_POSITIONS.SEC_CUSIP',
    ISIN                VARCHAR(12)     NOT NULL    COMMENT 'ISIN — joins to RUBY_POSITIONS.ISIN_IDENTIFIER',
    TICKER              VARCHAR(10)     NOT NULL    COMMENT 'Ticker — joins to EMERALD_POSITIONS.SECURITY_TICKER',
    SECURITY_MASTER_ID  VARCHAR(20)     NOT NULL    COMMENT 'Canonical security key written into SILVER.POSITIONS_INTEGRATED',
    ASSET_CLASS         VARCHAR(30)     NOT NULL    COMMENT 'Asset class classification — absent for 30 unmastered securities, producing NULL in Silver (A10)'
)
COMMENT = 'Bronze — stub security master for Pipeline A ETL join. 170 of 200 securities; 30 absent → NULL security_master_id and asset_class in Silver (A8/A10).';


-- ── Verify ────────────────────────────────────────────────────────────────────
SHOW TABLES IN SCHEMA GSF_DEMO.BRONZE;
