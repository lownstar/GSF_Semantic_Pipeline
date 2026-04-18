-- =============================================================================
-- Naive Gold Model: securities_naive
-- =============================================================================
-- The security lookup table for the Naive Gold layer.
-- Direct pass-through of the security_master_full seed — all 200 securities,
-- no transformation. A realistic DBA Gold layer would have exactly this:
-- a complete security master with canonical IDs and all identifier types.
--
-- Materialized as GOLD_NAIVE.SECURITIES_NAIVE.
-- =============================================================================

SELECT
    security_id,
    security_name,
    cusip,
    isin,
    ticker,
    asset_class,
    security_type,
    currency
FROM {{ ref('security_master_full') }}
