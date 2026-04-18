-- =============================================================================
-- Naive Gold Model: accounts_naive
-- =============================================================================
-- The account lookup table for the Naive Gold layer.
-- Direct pass-through of the account_master_full seed — no transformation.
-- A realistic DBA Gold layer would have exactly this: a clean account master
-- with canonical IDs and source-system key mappings.
--
-- Materialized as GOLD_NAIVE.ACCOUNTS_NAIVE.
-- =============================================================================

SELECT
    account_id,
    account_name,
    account_type,
    base_currency,
    custodian_account_num,
    portfolio_code,
    fund_code,
    is_active
FROM {{ ref('account_master_full') }}
