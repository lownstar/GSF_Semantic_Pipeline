-- =============================================================================
-- Semantic Gold Model: dw_trade_lot
-- =============================================================================
-- Lot-level detail table scoped exclusively to Topaz (the only lot-level source).
-- Topaz is the custodian system that tracks individual acquisition lots, making it
-- the authoritative source for lot-level cost basis and tax lot information.
--
-- Resolves ambiguity A5/A7 by explicitly scoping lot grain to this table:
--   A5  (Position grain mismatch) — lots are here; position aggregates are in dw_position
--   A7  (Mixed grain in Silver)   — Topaz lot rows are NOT mixed with position rows here
--
-- Joins to canonical masters to resolve A1, A3/A4:
--   A1  (Security ID fragmentation) — CUSIP → dw_security.security_id
--   A3/A4 (Account fragmentation)   — custodian_account_num → dw_account.account_id
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
    'TOPAZ'                                                 AS source_system
FROM {{ ref('positions_integrated') }} si
JOIN {{ ref('dw_security') }} sec
    ON si.security_ref_type = 'CUSIP' AND si.security_ref = sec.cusip
JOIN {{ ref('dw_account') }} acc
    ON si.account_ref = acc.custodian_account_num
WHERE si.source_system = 'TOPAZ'
