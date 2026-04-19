-- =============================================================================
-- Semantic Gold Model: dw_trade_lot
-- =============================================================================
-- Lot-level detail table scoped exclusively to Emerald (the only lot-level source).
-- Emerald is the front office OMS/trade system that tracks individual order
-- executions as acquisition lots, making it the authoritative source for
-- lot-level cost basis and trade lot information.
--
-- Resolves ambiguity A5/A7 by explicitly scoping lot grain to this table:
--   A5  (Position grain mismatch) — lots are here; position aggregates are in dw_position
--   A7  (Mixed grain in Silver)   — Emerald lot rows are NOT mixed with position rows here
--
-- Joins to canonical masters to resolve A1, A3/A4:
--   A1  (Security ID fragmentation) — ticker → dw_security.security_id
--   A3/A4 (Account fragmentation)   — portfolio_code → dw_account.account_id
-- =============================================================================

SELECT
    si.record_id                                            AS lot_id,
    acc.account_id,
    sec.security_id,
    si.position_date                                        AS acquisition_date,
    si.price                                                AS acquisition_price,
    si.quantity                                             AS remaining_quantity,
    si.cost_basis,
    si.currency,
    'EMERALD'                                               AS source_system
FROM {{ ref('positions_integrated') }} si
JOIN {{ ref('dw_security') }} sec
    ON si.security_ref_type = 'TICKER' AND si.security_ref = sec.ticker
JOIN {{ ref('dw_account') }} acc
    ON si.account_ref = acc.portfolio_code
WHERE si.source_system = 'EMERALD'
