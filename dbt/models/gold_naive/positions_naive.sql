-- =============================================================================
-- Naive Gold Model: positions_naive
-- =============================================================================
-- A "Gold-looking" positions table derived from Silver with wrong assumptions.
-- This is what a naive dbt pipeline produces when it promotes Silver to Gold
-- without resolving the underlying ambiguities. The table has canonical-looking
-- column names and no obvious NULL red flags — but it is demonstrably worse than
-- Silver for ambiguity-sensitive Cortex queries.
--
-- Wrong assumptions applied:
--   1. All prices are equivalent — no source discrimination applied. Blended prices
--      from Topaz (EOD), Emerald (±0.3%), and Ruby (NAV ±0.5%) are treated as one.
--   2. Grain is uniform — Topaz lot-level rows are summed alongside Emerald/Ruby
--      position-level rows; COUNT and cardinality queries against the Silver input break.
--   3. NULL security_master_id rows are silently dropped — GROUP BY on security_master_id
--      excludes ~15% of positions, systematically understating AUM.
--   4. NULL unrealized_gl is propagated — Ruby G/L gaps are not computed; any
--      aggregation on unrealized_gl excludes 22% of rows silently.
--   5. Account reference is passed through raw — no canonical account_id resolution;
--      queries for a specific account must guess the source format.
--
-- Result: GOLD_NAIVE.POSITIONS_NAIVE — one row per security_master_id × position_date
--         × source_system. Looks like a valid Gold aggregate. Is not.
-- =============================================================================

SELECT
    -- Canonical-looking identifier — but only covers the 85% of mastered securities
    security_master_id,

    -- Account reference passed through raw (A3/A4 unresolved)
    account_ref,
    source_system,
    position_date,
    asset_class,
    currency,

    -- Wrong assumption 1: treat all prices as equivalent, average across sources
    -- This blends custodian EOD, PM evaluated, and NAV prices into one number
    ROUND(AVG(price), 4)                AS avg_price,

    -- Wrong assumption 2: sum quantities without grain adjustment
    -- Topaz lot rows (2-3 per position) coexist with position-level rows; COUNT/cardinality queries break
    SUM(quantity)                       AS total_quantity,

    -- Market value computed from blended prices × mixed-grain quantities
    ROUND(SUM(market_value), 2)         AS total_market_value,

    -- Wrong assumption 4: propagate NULL unrealized_gl (Ruby rows excluded from SUM)
    ROUND(SUM(unrealized_gl), 2)        AS total_unrealized_gl,

    -- Cost basis mixes three methods (specific lot / avg cost / book cost)
    ROUND(SUM(cost_basis), 2)           AS total_cost_basis,

    -- Row count for transparency
    COUNT(*)                            AS source_row_count

FROM {{ ref('positions_integrated') }}

-- Wrong assumption 3: NULL security_master_id rows are dropped by this GROUP BY
-- ~15% of positions (30 unmastered securities) are silently excluded
WHERE security_master_id IS NOT NULL

GROUP BY
    security_master_id,
    account_ref,
    source_system,
    position_date,
    asset_class,
    currency
