-- =============================================================================
-- Pipeline A — Silver ETL (Naive Integration)
-- =============================================================================
-- Populates SILVER.POSITIONS_INTEGRATED by:
--   1. Normalizing each Bronze source table into a common column schema (CTEs)
--   2. UNIONing all three normalized sources
--   3. LEFT JOINing to BRONZE.SECURITY_MASTER_STUB on security_ref + type
--
-- The LEFT JOIN is where A8 and A10 emerge naturally: 30 securities are absent
-- from the stub, so their rows get NULL security_master_id and NULL asset_class.
-- The ETL produces a table that LOOKS correct — same columns, no obvious errors —
-- but embeds all five integration-layer ambiguities (A7–A11).
--
-- Prerequisites: setup.sql, ddl_bronze.sql, ddl_silver.sql, load_bronze.py
-- Idempotent: truncates POSITIONS_INTEGRATED before inserting.
-- =============================================================================

USE ROLE GSF_ROLE;
USE WAREHOUSE GSF_WH;
USE DATABASE GSF_DEMO;

TRUNCATE TABLE SILVER.POSITIONS_INTEGRATED;

INSERT INTO SILVER.POSITIONS_INTEGRATED (
    RECORD_ID,
    SOURCE_SYSTEM,
    ACCOUNT_REF,
    SECURITY_REF,
    SECURITY_REF_TYPE,
    SECURITY_MASTER_ID,
    POSITION_DATE,
    QUANTITY,
    PRICE,
    MARKET_VALUE,
    COST_BASIS,
    UNREALIZED_GL,
    ASSET_CLASS,
    CURRENCY,
    ETL_LOADED_AT
)

WITH

-- ── CTE 1: Normalize Topaz rows ───────────────────────────────────────────────
-- A7: record_id = lot_id (LOT-NNNNNNN) — grain is lot-level, invisible in schema
-- A9: cost_basis = specific lot cost (Topaz tracks individual lot acquisition cost)
-- A2: price = custodian EOD baseline (no variance applied at source)

topaz_norm AS (
    SELECT
        LOT_ID                          AS record_id,
        'TOPAZ'                         AS source_system,
        ACCT_NUM                        AS account_ref,
        SEC_CUSIP                       AS security_ref,
        'CUSIP'                         AS security_ref_type,
        AS_OF_DT                        AS position_date,
        UNITS                           AS quantity,         -- A7: lot-level
        MKT_PRC                         AS price,            -- A2: custodian EOD
        MKT_VAL                         AS market_value,
        COST_BASIS                      AS cost_basis,       -- A9: specific lot cost
        UNRLZD_GL                       AS unrealized_gl,
        CCY                             AS currency,
        '2025-01-02 06:00:00'::TIMESTAMP_NTZ AS etl_loaded_at
    FROM BRONZE.TOPAZ_POSITIONS
),

-- ── CTE 2: Normalize Emerald rows ─────────────────────────────────────────────
-- A7: record_id = POS-{portfolioId}-{ticker} — fabricated composite, not a stable surrogate
-- A9: cost_basis = AVG_COST_BASIS × QUANTITY (convert per-unit avg to total position cost)
-- A2: price = PM evaluated price (already in UNIT_PRICE with ±0.3% variance baked in)

emerald_norm AS (
    SELECT
        CONCAT('POS-', PORTFOLIO_ID, '-', SECURITY_TICKER)  AS record_id,  -- A7
        'EMERALD'                               AS source_system,
        PORTFOLIO_ID                            AS account_ref,              -- A4 survives
        SECURITY_TICKER                         AS security_ref,             -- A1 survives
        'TICKER'                                AS security_ref_type,
        POSITION_DATE                           AS position_date,
        QUANTITY                                AS quantity,                 -- position-level
        UNIT_PRICE                              AS price,                    -- A2: PM evaluated
        MARKET_VALUE                            AS market_value,
        ROUND(AVG_COST_BASIS * QUANTITY, 2)     AS cost_basis,               -- A9: avg cost method
        UNREALIZED_PNL                          AS unrealized_gl,
        CCY                                     AS currency,
        '2025-01-02 08:00:00'::TIMESTAMP_NTZ   AS etl_loaded_at
    FROM BRONZE.EMERALD_POSITIONS
),

-- ── CTE 3: Normalize Ruby rows ────────────────────────────────────────────────
-- A7: record_id = NAV-{fund_code}-{isin} — ISIN embedded, mismatches CUSIP/ticker keys
-- A9: cost_basis = BOOK_COST (Ruby tracks total book cost; method differs from lot and avg)
-- A2: price = NAV per share (±0.5% variance from custodian EOD baked in at source)
-- A11: unrealized_gl = NULL — Ruby fund accounting has no G/L concept

ruby_norm AS (
    SELECT
        CONCAT('NAV-', FUND_CODE, '-', ISIN_IDENTIFIER)    AS record_id,   -- A7
        'RUBY'                                  AS source_system,
        FUND_CODE                               AS account_ref,              -- A4 survives
        ISIN_IDENTIFIER                         AS security_ref,             -- A1 survives
        'ISIN'                                  AS security_ref_type,
        NAV_DATE                                AS position_date,
        SHARES_HELD                             AS quantity,                 -- position-level
        NAV_PER_SHARE                           AS price,                    -- A2: NAV price
        TOTAL_NAV_VALUE                         AS market_value,
        BOOK_COST                               AS cost_basis,               -- A9: book cost
        NULL::DECIMAL(18,2)                     AS unrealized_gl,            -- A11: fund acct has no G/L
        CURRENCY_CODE                           AS currency,
        '2025-01-02 10:00:00'::TIMESTAMP_NTZ   AS etl_loaded_at
    FROM BRONZE.RUBY_POSITIONS
),

-- ── CTE 4: Union all three normalized sources ─────────────────────────────────

all_rows AS (
    SELECT * FROM topaz_norm
    UNION ALL
    SELECT * FROM emerald_norm
    UNION ALL
    SELECT * FROM ruby_norm
)

-- ── Final SELECT: LEFT JOIN to stub security master ───────────────────────────
-- A8: 30 securities absent from stub → security_master_id = NULL in output
-- A10: same 30 securities → asset_class = NULL in output
-- The join is a three-way OR across identifier types so each source's security
-- reference resolves against the right stub column.

SELECT
    r.record_id,
    r.source_system,
    r.account_ref,
    r.security_ref,
    r.security_ref_type,
    s.security_master_id,               -- A8: NULL if security not in stub
    r.position_date,
    r.quantity,
    r.price,
    r.market_value,
    r.cost_basis,
    r.unrealized_gl,
    s.asset_class,                      -- A10: NULL if security not in stub
    r.currency,
    r.etl_loaded_at

FROM all_rows r
LEFT JOIN BRONZE.SECURITY_MASTER_STUB s
    ON  (r.security_ref_type = 'CUSIP'   AND r.security_ref = s.cusip)
     OR (r.security_ref_type = 'TICKER'  AND r.security_ref = s.ticker)
     OR (r.security_ref_type = 'ISIN'    AND r.security_ref = s.isin);


-- ── Post-load summary ─────────────────────────────────────────────────────────

SELECT
    'Total rows'                        AS metric,
    COUNT(*)::VARCHAR                   AS value
FROM SILVER.POSITIONS_INTEGRATED

UNION ALL

SELECT
    'Unmastered rows (A8: NULL security_master_id)',
    COUNT(*)::VARCHAR
FROM SILVER.POSITIONS_INTEGRATED
WHERE SECURITY_MASTER_ID IS NULL

UNION ALL

SELECT
    'Ruby rows with NULL unrealized_gl (A11)',
    COUNT(*)::VARCHAR
FROM SILVER.POSITIONS_INTEGRATED
WHERE SOURCE_SYSTEM = 'RUBY' AND UNREALIZED_GL IS NULL

UNION ALL

SELECT
    'Rows with NULL asset_class (A10)',
    COUNT(*)::VARCHAR
FROM SILVER.POSITIONS_INTEGRATED
WHERE ASSET_CLASS IS NULL

ORDER BY 1;
