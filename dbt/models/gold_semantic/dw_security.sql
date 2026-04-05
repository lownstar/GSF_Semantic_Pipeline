-- =============================================================================
-- Semantic Gold Model: dw_security
-- =============================================================================
-- Complete security master — all 200 securities with all three identifier types.
-- Unlike BRONZE.SECURITY_MASTER_STUB (170 rows), this includes the 30 securities
-- that are absent from the stub, eliminating the NULL gaps in Silver.
--
-- Resolves ambiguities A1, A8, A10:
--   A1  (Security ID fragmentation) — CUSIP + ISIN + ticker all present per row
--   A8  (Unmastered securities)     — all 200 securities present; no NULLs on joins
--   A10 (Asset class gaps)          — asset_class populated for all 200 securities
-- =============================================================================

SELECT
    security_id,
    security_name,
    cusip,         -- Topaz identifier
    isin,          -- Ruby identifier
    ticker,        -- Emerald identifier
    asset_class,   -- No NULLs — resolves A10
    security_type,
    currency
FROM {{ ref('security_master_full') }}
