-- =============================================================================
-- Silver Model: positions_integrated
-- =============================================================================
-- Normalizes the three Bronze source tables into a unified positions table.
-- Intentionally preserves all five integration-layer ambiguities (A7–A11) —
-- this is the design. The table looks correct but embeds systematic errors.
--
-- Ambiguities embedded by design:
--   A7  (Mixed grain)           — Topaz is lot-level; Emerald/Ruby are position-level
--   A8  (Unmastered securities) — 30 of 200 securities absent from stub → NULL IDs
--   A9  (Cost basis methods)    — Three methods coexist: specific lot / avg cost / book cost
--   A10 (Asset class gaps)      — Same 30 unmastered securities → NULL asset_class
--   A11 (Ruby G/L NULLs)       — Ruby fund accounting has no unrealized G/L concept
-- =============================================================================

WITH

-- ── CTE 1: Normalize Topaz rows ───────────────────────────────────────────────
-- A7: grain is lot-level (LOT-NNNNNNN); invisible in the schema
-- A9: cost_basis = specific lot acquisition cost
-- A2: price = custodian EOD baseline (authoritative, but not distinguished from other sources)

topaz_norm AS (
    SELECT
        LOT_ID                                      AS record_id,
        'TOPAZ'                                     AS source_system,
        ACCT_NUM                                    AS account_ref,
        SEC_CUSIP                                   AS security_ref,
        'CUSIP'                                     AS security_ref_type,
        AS_OF_DT                                    AS position_date,
        UNITS                                       AS quantity,          -- A7: lot-level
        MKT_PRC                                     AS price,             -- A2: custodian EOD
        MKT_VAL                                     AS market_value,
        COST_BASIS                                  AS cost_basis,        -- A9: specific lot
        UNRLZD_GL                                   AS unrealized_gl,
        CCY                                         AS currency,
        '2025-01-02 06:00:00'::TIMESTAMP_NTZ        AS etl_loaded_at
    FROM {{ source('bronze', 'topaz_positions') }}
),

-- ── CTE 2: Normalize Emerald rows ─────────────────────────────────────────────
-- A7: record_id is a fabricated composite key, not a stable surrogate
-- A9: cost_basis = avg_cost_basis × quantity (per-unit avg converted to position total)
-- A2: price = PM evaluated (custodian EOD ±0.3% variance baked in)

emerald_norm AS (
    SELECT
        CONCAT('POS-', PORTFOLIO_ID, '-', SECURITY_TICKER)     AS record_id,   -- A7
        'EMERALD'                                               AS source_system,
        PORTFOLIO_ID                                            AS account_ref,
        SECURITY_TICKER                                         AS security_ref,
        'TICKER'                                                AS security_ref_type,
        POSITION_DATE                                           AS position_date,
        QUANTITY                                                AS quantity,     -- position-level
        UNIT_PRICE                                              AS price,        -- A2: PM evaluated
        MARKET_VALUE                                            AS market_value,
        ROUND(AVG_COST_BASIS * QUANTITY, 2)                     AS cost_basis,   -- A9: avg cost
        UNREALIZED_PNL                                          AS unrealized_gl,
        CCY                                                     AS currency,
        '2025-01-02 08:00:00'::TIMESTAMP_NTZ                   AS etl_loaded_at
    FROM {{ source('bronze', 'emerald_positions') }}
),

-- ── CTE 3: Normalize Ruby rows ────────────────────────────────────────────────
-- A7: record_id embeds ISIN, mismatches CUSIP/ticker-based keys from other sources
-- A9: cost_basis = book_cost (Ruby fund accounting; method differs from above)
-- A2: price = NAV per share (custodian EOD ±0.5% variance baked in)
-- A11: unrealized_gl = NULL — fund accounting tracks no G/L

ruby_norm AS (
    SELECT
        CONCAT('NAV-', FUND_CODE, '-', ISIN_IDENTIFIER)        AS record_id,   -- A7
        'RUBY'                                                  AS source_system,
        FUND_CODE                                               AS account_ref,
        ISIN_IDENTIFIER                                         AS security_ref,
        'ISIN'                                                  AS security_ref_type,
        NAV_DATE                                                AS position_date,
        SHARES_HELD                                             AS quantity,     -- position-level
        NAV_PER_SHARE                                           AS price,        -- A2: NAV
        TOTAL_NAV_VALUE                                         AS market_value,
        BOOK_COST                                               AS cost_basis,   -- A9: book cost
        NULL::DECIMAL(18,2)                                     AS unrealized_gl, -- A11
        CURRENCY_CODE                                           AS currency,
        '2025-01-02 10:00:00'::TIMESTAMP_NTZ                   AS etl_loaded_at
    FROM {{ source('bronze', 'ruby_positions') }}
),

all_rows AS (
    SELECT * FROM topaz_norm
    UNION ALL
    SELECT * FROM emerald_norm
    UNION ALL
    SELECT * FROM ruby_norm
)

-- ── Final SELECT: LEFT JOIN to stub security master ───────────────────────────
-- A8: 30 securities absent from stub → security_master_id = NULL
-- A10: same 30 securities → asset_class = NULL

SELECT
    r.record_id,
    r.source_system,
    r.account_ref,
    r.security_ref,
    r.security_ref_type,
    s.security_master_id,               -- A8: NULL if not in 170-row stub
    r.position_date,
    r.quantity,
    r.price,
    r.market_value,
    r.cost_basis,
    r.unrealized_gl,
    s.asset_class,                      -- A10: NULL if not in stub
    r.currency,
    r.etl_loaded_at

FROM all_rows r
LEFT JOIN {{ source('bronze', 'security_master_stub') }} s
    ON  (r.security_ref_type = 'CUSIP'   AND r.security_ref = s.cusip)
     OR (r.security_ref_type = 'TICKER'  AND r.security_ref = s.ticker)
     OR (r.security_ref_type = 'ISIN'    AND r.security_ref = s.isin)
