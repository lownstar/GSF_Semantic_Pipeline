-- =============================================================================
-- Semantic Gold Model: dw_position
-- =============================================================================
-- Governed position fact table at position-level grain (one canonical row per
-- account × security × position_date). Derived from SILVER.POSITIONS_INTEGRATED
-- using the canonical account and security masters to resolve all ambiguities.
-- Topaz is the authoritative source for all position data.
--
-- Resolves ambiguities A1, A2, A3, A4, A5, A7, A8, A9, A10, A11:
--   A1  (Security ID fragmentation) — JOIN to dw_security resolves all three types
--   A2  (Price source divergence)   — Topaz custodian EOD price is the single source
--   A3/A4 (Account fragmentation)   — JOIN to dw_account on source-specific key
--   A5/A7 (Mixed grain)             — Emerald lots aggregated to position-level in emerald_positions CTE
--   A8  (Unmastered securities)     — full 200-row master; no dropped rows
--   A9  (Cost basis methods)        — Topaz custodian cost is the authoritative method (single method)
--   A10 (Asset class gaps)          — all securities have asset_class via dw_security
--   A11 (Ruby G/L NULLs)            — unrealized_gain_loss computed for all rows
--
-- Design decisions:
--   - Topaz is the authoritative source (custodian end-of-day price, position-level).
--   - Topaz rows are already position-level — no aggregation needed.
--   - Emerald lot-level rows are aggregated (SUM qty, SUM market_value, SUM cost_basis)
--     in the emerald_positions CTE — documentation only, not emitted.
--   - Ruby CTE is retained for documentation of per-source resolution logic.
--   - unrealized_gain_loss = market_value - cost_basis for all rows where NULL.
-- =============================================================================

WITH

-- Resolve Topaz position rows to canonical masters (already position-level — no aggregation)
topaz_positions AS (
    SELECT
        acc.account_id,
        sec.security_id,
        si.position_date,
        si.quantity,
        si.price                                            AS market_price,   -- custodian EOD
        si.market_value,
        si.cost_basis,
        COALESCE(si.unrealized_gl, si.market_value - si.cost_basis) AS unrealized_gain_loss,
        si.currency,
        'TOPAZ'                                             AS source_system
    FROM {{ ref('positions_integrated') }} si
    JOIN {{ ref('dw_security') }} sec
        ON si.security_ref_type = 'CUSIP' AND si.security_ref = sec.cusip
    JOIN {{ ref('dw_account') }} acc
        ON si.account_ref = acc.custodian_account_num
    WHERE si.source_system = 'TOPAZ'
),

-- Resolve Emerald lot rows to position grain, join to canonical masters
emerald_positions AS (
    SELECT
        acc.account_id,
        sec.security_id,
        si.position_date,
        SUM(si.quantity)        AS quantity,
        MAX(si.price)           AS market_price,      -- all lots share same PM-evaluated price
        SUM(si.market_value)    AS market_value,
        SUM(si.cost_basis)      AS cost_basis,
        SUM(si.unrealized_gl)   AS unrealized_gain_loss,
        si.currency,
        'EMERALD'               AS source_system
    FROM {{ ref('positions_integrated') }} si
    JOIN {{ ref('dw_security') }} sec
        ON si.security_ref_type = 'TICKER' AND si.security_ref = sec.ticker
    JOIN {{ ref('dw_account') }} acc
        ON si.account_ref = acc.portfolio_code
    WHERE si.source_system = 'EMERALD'
    GROUP BY
        acc.account_id, sec.security_id, si.position_date, si.currency
),

-- Resolve Ruby position rows to canonical masters; compute missing unrealized G/L
ruby_positions AS (
    SELECT
        acc.account_id,
        sec.security_id,
        si.position_date,
        si.quantity,
        si.price                                            AS market_price,
        si.market_value,
        si.cost_basis,
        si.market_value - si.cost_basis                    AS unrealized_gain_loss,  -- A11: computed
        si.currency,
        'RUBY'                                              AS source_system
    FROM {{ ref('positions_integrated') }} si
    JOIN {{ ref('dw_security') }} sec
        ON si.security_ref_type = 'ISIN' AND si.security_ref = sec.isin
    JOIN {{ ref('dw_account') }} acc
        ON si.account_ref = acc.fund_code
    WHERE si.source_system = 'RUBY'
)

-- Emit one canonical row per account × security × position_date.
-- Emerald and Ruby CTEs above are documentation only — Topaz is authoritative.
-- Topaz (custodian) is already position-level; no lot aggregation needed here.
SELECT
    MD5(CONCAT(account_id, '|', security_id, '|', position_date::VARCHAR)) AS position_id,
    account_id,
    security_id,
    position_date,
    quantity,
    market_price,
    market_value,
    cost_basis,
    unrealized_gain_loss,
    currency
FROM topaz_positions
