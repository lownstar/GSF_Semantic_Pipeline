-- =============================================================================
-- Semantic Gold Model: dw_account
-- =============================================================================
-- Canonical account dimension. Sourced directly from the account_master_full seed,
-- which carries all three source-system keys (custodian_account_num, portfolio_code,
-- fund_code) alongside the canonical account_id.
--
-- Resolves ambiguities A3/A4:
--   A3 (Account ID fragmentation) — one row per account; all source keys present
--   A4 (Account reference format) — canonical account_id enables cross-source joins
-- =============================================================================

SELECT
    account_id,
    account_name,
    account_type,
    base_currency,
    custodian_account_num,   -- Topaz source key (C-XXXXXX)
    portfolio_code,          -- Emerald source key (PORT-XXXX)
    fund_code,               -- Ruby source key (FND-XXXX)
    is_active
FROM {{ ref('account_master_full') }}
