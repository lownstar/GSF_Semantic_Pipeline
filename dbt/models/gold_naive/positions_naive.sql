-- =============================================================================
-- Naive Gold Model: positions_naive
-- =============================================================================
-- A well-built Gold layer using Ruby (back office GL) as the authoritative
-- source. Ruby is where GSF accounts are established before they can be traded
-- in the front office (Emerald), and it reconciles against the custodian (Topaz).
-- Treating it as the source of record is a reasonable assumption.
--
-- The model has everything a governed Gold layer should have:
--   - Canonical account IDs via accounts_naive (accounts originate in Ruby)
--   - All 200 securities via securities_naive
--   - Correct position grain (Ruby is position-level, not lot-level)
--   - Dimensional relationships to account and security tables
--
-- It still produces wrong answers for governance-sensitive queries because
-- Ruby is a fund accounting system, and fund accounting answers different
-- questions than custodian or trade-system data:
--
--   A2  (Price authority)    — Ruby NAV ±0.5%; authoritative price is Topaz
--                              custodian EOD (the reconciled settlement price).
--   A9  (Cost basis method)  — Ruby uses book cost (fund accounting standard).
--                              Specific identification (Topaz) is required for
--                              tax lots and performance attribution.
--   A11 (Ruby G/L NULLs)    — Fund accounting tracks no mark-to-market
--                              unrealized G/L. Any P&L or risk query returns NULL.
--
-- Governance decisions a dbt model cannot make:
--   "Which system's prices are authoritative for valuation?"
--   "Which cost basis method applies for this reporting context?"
--   "How do we compute G/L for a system that doesn't track it?"
--
-- Materialized as GOLD_NAIVE.POSITIONS_NAIVE.
-- =============================================================================

SELECT
    a.account_id,
    s.security_id                       AS security_master_id,
    pi.position_date,
    s.asset_class,
    pi.currency,

    -- A2: Ruby NAV price (fund accounting). Authoritative price is Topaz custodian EOD.
    pi.price                            AS avg_price,

    -- Correct grain — Ruby reports one row per position (not lot-level like Emerald).
    pi.quantity                         AS total_quantity,

    -- A2: market value computed from NAV price, not custodian EOD.
    pi.market_value                     AS total_market_value,

    -- A11: NULL — fund accounting does not track mark-to-market unrealized G/L.
    pi.unrealized_gl                    AS total_unrealized_gl,

    -- A9: book cost (fund accounting). Specific identification required for
    --     tax lot reporting and performance attribution.
    pi.cost_basis                       AS total_cost_basis,

    1                                   AS source_row_count

FROM {{ ref('positions_integrated') }} pi

-- Ruby is the back office / GL system; accounts originate here.
-- account_ref in Ruby is the fund_code (FND-XXXX).
INNER JOIN {{ ref('accounts_naive') }} a
    ON pi.source_system = 'RUBY'
    AND pi.account_ref = a.fund_code

-- Full 200-security master via ISIN (Ruby's identifier type).
LEFT JOIN {{ ref('securities_naive') }} s
    ON pi.security_ref_type = 'ISIN'
    AND pi.security_ref = s.isin

WHERE pi.source_system = 'RUBY'
