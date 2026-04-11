-- =============================================================================
-- Pipeline B — Gold DDL
-- =============================================================================
-- Creates the four canonical DW tables in GSF_DEMO.GOLD and an internal stage
-- for both data loading and semantic model staging:
--
--   DW_ACCOUNT     — canonical account master (all three source keys)
--   DW_SECURITY    — canonical security master (CUSIP + ISIN + ticker, 200 rows)
--   DW_POSITION    — canonical position-level grain (lots collapsed)
--   DW_TRADE_LOT   — canonical lot-level detail
--   GSF_GOLD_STAGE — internal stage for CSV loads and semantic model YAML
--
-- These tables are the governed foundation for Act 3 of the demo. They carry
-- no ambiguities: all FKs resolve, all 200 securities are mastered, unrealized
-- G/L is computed for every row, and one price source is authoritative.
--
-- The semantic model (semantic_model/positions_gold.yaml) is staged to
-- @GSF_GOLD_STAGE/semantic/ by load_gold.py and read by Cortex Analyst at
-- query time. No CREATE SEMANTIC VIEW DDL is required for this demo.
--
-- Prerequisites: infrastructure/snowflake_setup.sql (creates GSF_DEMO, GSF_WH, GSF_ROLE)
-- Idempotent: CREATE OR REPLACE throughout.
-- =============================================================================

USE ROLE GSF_ROLE;
USE WAREHOUSE GSF_WH;
USE DATABASE GSF_DEMO;


-- ── Internal stage ────────────────────────────────────────────────────────────
-- Used for both CSV data loads and semantic model YAML staging.
-- Semantic model lives at @GSF_GOLD_STAGE/semantic/positions_gold.yaml

CREATE STAGE IF NOT EXISTS GOLD.GSF_GOLD_STAGE
    COMMENT = 'Pipeline B Gold load stage. CSV data + semantic model YAMLs (semantic/positions_gold.yaml etc).';


-- ── DW_ACCOUNT — canonical account master ─────────────────────────────────────
-- Resolves A3/A4: all three source system account keys in one row.
-- cross-source aggregation is a single GROUP BY account_id.

CREATE OR REPLACE TABLE GOLD.DW_ACCOUNT (
    account_id              VARCHAR(20)     NOT NULL    COMMENT 'Canonical account identifier (ACC-XXXX). FK target for DW_POSITION and DW_TRADE_LOT.',
    account_name            VARCHAR(200)                COMMENT 'Human-readable account name.',
    account_type            VARCHAR(30)                 COMMENT 'Individual | Trust | Institutional | Fund',
    base_currency           VARCHAR(3)                  COMMENT 'ISO base currency for the account.',
    custodian_account_num   VARCHAR(20)                 COMMENT 'Topaz source key (C-XXXXXX). Resolves A4: maps to account_id.',
    portfolio_code          VARCHAR(20)                 COMMENT 'Emerald source key (PORT-XXXX). Resolves A4: maps to account_id.',
    fund_code               VARCHAR(20)                 COMMENT 'Ruby source key (FND-XXXX). Resolves A4: maps to account_id.',
    is_active               BOOLEAN                     COMMENT 'Whether the account is currently active.',
    CONSTRAINT pk_dw_account PRIMARY KEY (account_id)
)
COMMENT = 'Act 3 Gold — canonical account master. Resolves A3 (column name heterogeneity) and A4 (account identifier fragmentation): all three source keys present in one row, account_id is the canonical join key.';


-- ── DW_SECURITY — canonical security master ───────────────────────────────────
-- Resolves A1: CUSIP, ISIN, and ticker all present for every security.
-- Resolves A8/A10: 200 rows — no unmastered securities (vs 170 in Bronze stub).

CREATE OR REPLACE TABLE GOLD.DW_SECURITY (
    security_id     VARCHAR(20)     NOT NULL    COMMENT 'Canonical security identifier (SEC-XXXX). FK target for DW_POSITION and DW_TRADE_LOT.',
    security_name   VARCHAR(200)                COMMENT 'Full security name.',
    cusip           VARCHAR(9)                  COMMENT 'CUSIP — Topaz source identifier. Resolves A1: maps to security_id.',
    isin            VARCHAR(12)                 COMMENT 'ISIN — Ruby source identifier. Resolves A1: maps to security_id.',
    ticker          VARCHAR(10)                 COMMENT 'Ticker — Emerald source identifier. Resolves A1: maps to security_id.',
    asset_class     VARCHAR(30)                 COMMENT 'Asset class classification. Resolves A10: always populated (no unmastered rows).',
    security_type   VARCHAR(50)                 COMMENT 'Security type (e.g., Common Stock, Corporate Bond, ETF).',
    currency        VARCHAR(3)                  COMMENT 'ISO currency of denomination.',
    CONSTRAINT pk_dw_security PRIMARY KEY (security_id)
)
COMMENT = 'Act 3 Gold — canonical security master. Resolves A1 (security ID fragmentation: CUSIP/ISIN/ticker all present), A8 (unmastered NULLs: all 200 securities mastered), A10 (asset class gap: always populated).';


-- ── DW_POSITION — canonical position-level grain ─────────────────────────────
-- Resolves A2: one authoritative price source (custodian EOD).
-- Resolves A5/A7: position-level grain (lots collapsed) — safe for SUM().
-- Resolves A9: cost_basis uses specific identification from lot detail.
-- Resolves A11: unrealized_gain_loss computed for all rows (no Ruby NULL gap).

CREATE OR REPLACE TABLE GOLD.DW_POSITION (
    position_id             VARCHAR(20)     NOT NULL    COMMENT 'Canonical position identifier (POS-XXXXXXX).',
    account_id              VARCHAR(20)     NOT NULL    COMMENT 'FK → DW_ACCOUNT.account_id',
    security_id             VARCHAR(20)     NOT NULL    COMMENT 'FK → DW_SECURITY.security_id',
    position_date           DATE            NOT NULL    COMMENT 'Trade-date basis per GIPS standards. Resolves A6: normalized from settlement/trade/NAV dates.',
    quantity                DECIMAL(18,4)   NOT NULL    COMMENT 'Position-level quantity (lots collapsed). Resolves A5/A7: safe for SUM() across sources.',
    market_price            DECIMAL(18,4)   NOT NULL    COMMENT 'Custodian EOD closing price. Resolves A2: single authoritative price source.',
    market_value            DECIMAL(18,2)   NOT NULL    COMMENT 'quantity × market_price. Resolves A2/A7: one price source, position-level grain.',
    cost_basis              DECIMAL(18,2)   NOT NULL    COMMENT 'Sum of specific lot costs from DW_TRADE_LOT. Resolves A9: specific identification method throughout.',
    unrealized_gain_loss    DECIMAL(18,2)   NOT NULL    COMMENT 'market_value − cost_basis. Resolves A11: computed for ALL positions (no Ruby NULL gap).',
    currency                VARCHAR(3)      NOT NULL    COMMENT 'ISO currency code.',
    CONSTRAINT pk_dw_position PRIMARY KEY (position_id),
    CONSTRAINT fk_position_account  FOREIGN KEY (account_id)  REFERENCES GOLD.DW_ACCOUNT (account_id),
    CONSTRAINT fk_position_security FOREIGN KEY (security_id) REFERENCES GOLD.DW_SECURITY (security_id)
)
COMMENT = 'Act 3 Gold — canonical position-level grain. Resolves A2 (price source: custodian EOD), A5/A7 (grain: lots collapsed, SUM() safe), A6 (date: trade-date basis), A9 (cost basis: specific identification), A11 (unrealized G/L: no NULLs).';


-- ── DW_TRADE_LOT — canonical lot-level detail ────────────────────────────────
-- Lot-level detail for Topaz-originated positions.
-- Used by semantic model to scope lot-level analysis separately from DW_POSITION.

CREATE OR REPLACE TABLE GOLD.DW_TRADE_LOT (
    lot_id              VARCHAR(20)     NOT NULL    COMMENT 'Trade lot identifier (LOT-XXXXXXX).',
    account_id          VARCHAR(20)     NOT NULL    COMMENT 'FK → DW_ACCOUNT.account_id',
    security_id         VARCHAR(20)     NOT NULL    COMMENT 'FK → DW_SECURITY.security_id',
    acquisition_date    DATE                        COMMENT 'Date the lot was acquired.',
    acquisition_price   DECIMAL(18,4)               COMMENT 'Price per unit at acquisition.',
    original_quantity   DECIMAL(18,4)               COMMENT 'Original lot quantity at acquisition.',
    remaining_quantity  DECIMAL(18,4)               COMMENT 'Current remaining quantity (after partial disposals).',
    cost_basis          DECIMAL(18,2)               COMMENT 'Specific lot cost = acquisition_price × remaining_quantity.',
    source_system       VARCHAR(10)                 COMMENT 'DW — canonical origin.',
    CONSTRAINT pk_dw_trade_lot PRIMARY KEY (lot_id),
    CONSTRAINT fk_lot_account  FOREIGN KEY (account_id)  REFERENCES GOLD.DW_ACCOUNT (account_id),
    CONSTRAINT fk_lot_security FOREIGN KEY (security_id) REFERENCES GOLD.DW_SECURITY (security_id)
)
COMMENT = 'Act 3 Gold — canonical lot-level detail. Resolves A5: lot-level grain explicitly scoped here; position-level aggregation uses DW_POSITION. Semantic model surfaces these as separate contexts to prevent double-counting.';


-- ── Verify ────────────────────────────────────────────────────────────────────
SHOW TABLES IN SCHEMA GSF_DEMO.GOLD;
SHOW STAGES IN SCHEMA GSF_DEMO.GOLD;
