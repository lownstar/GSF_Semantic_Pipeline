"""
Gemstone source system file generators for generator_v2.

Each function derives a source system's position file from the canonical DW
tables by applying the physical schema transformations that represent that
source system's conventions. The transformations are:

  - Column renames (Ambiguity A3)
  - Security identifier substitution: security_id → CUSIP/ticker/ISIN (Ambiguity A1)
  - Account identifier substitution: account_id → ACCT_NUM/portfolioId/fund_code (A4)
  - Price variance noise (Ambiguity A2)
  - Grain: Topaz is lot-level (from DW_TRADE_LOT), Emerald/Ruby are position-level (A5)
  - Date column name and semantic label differ per source (Ambiguity A6)

See docs/ambiguity_registry_v2.md for full ambiguity documentation.
"""

import random

import numpy as np
import pandas as pd

from generator_v2.config import (
    EMERALD_PRICE_VARIANCE,
    INTEGRATED_SOURCE_EMERALD,
    INTEGRATED_SOURCE_RUBY,
    INTEGRATED_SOURCE_TOPAZ,
    POSITION_DATE,
    RANDOM_SEED,
    RUBY_PRICE_VARIANCE,
    UNMASTERED_SECURITY_FRACTION,
)

rng = random.Random(RANDOM_SEED + 1)  # offset so prices differ from canonical rng


# ── Lookup helpers ────────────────────────────────────────────────────────────

def _sec_lookup(dw_security: pd.DataFrame) -> dict:
    """Returns dict: security_id → {cusip, isin, ticker}"""
    return {
        row["security_id"]: {
            "cusip":   row["cusip"],
            "isin":    row["isin"],
            "ticker":  row["ticker"],
        }
        for _, row in dw_security.iterrows()
    }


def _acct_lookup(dw_account: pd.DataFrame) -> dict:
    """Returns dict: account_id → {custodian_account_num, portfolio_code, fund_code}"""
    return {
        row["account_id"]: {
            "custodian_account_num": row["custodian_account_num"],
            "portfolio_code":        row["portfolio_code"],
            "fund_code":             row["fund_code"],
        }
        for _, row in dw_account.iterrows()
    }


# ── Topaz ─────────────────────────────────────────────────────────────────────

def generate_topaz_positions(
    dw_trade_lot: pd.DataFrame,
    dw_position: pd.DataFrame,
    dw_account: pd.DataFrame,
    dw_security: pd.DataFrame,
) -> pd.DataFrame:
    """
    Topaz (custodian) position file.

    Physical schema: UPPERCASE abbreviated column names.
    Grain: lot-level — one row per account × security × lot (Ambiguity A5).
    Security identifier: CUSIP (Ambiguity A1).
    Account identifier: custodian account number (Ambiguity A3/A4).
    Date column: AS_OF_DT — represents settlement/custody date (Ambiguity A6).
    Price: custodian EOD closing price — the baseline (Ambiguity A2).
    No price variance applied; Topaz is the authoritative price source.
    """
    sec = _sec_lookup(dw_security)
    acct = _acct_lookup(dw_account)

    # Build market price lookup from DW_POSITION (custodian baseline price)
    price_map = dict(zip(
        zip(dw_position["account_id"], dw_position["security_id"]),
        dw_position["market_price"],
    ))

    rows = []
    for _, lot in dw_trade_lot.iterrows():
        acct_id = lot["account_id"]
        sec_id  = lot["security_id"]
        key = (acct_id, sec_id)

        if key not in price_map:
            # Lot references a position not in DW_POSITION — skip (shouldn't happen)
            continue

        mkt_prc = price_map[key]
        units   = lot["remaining_quantity"]
        mkt_val = round(units * mkt_prc, 2)
        cost    = lot["cost_basis"]
        unrlzd  = round(mkt_val - cost, 2)

        rows.append({
            # Ambiguity A4 / A3: account identifier is custodian account number
            "ACCT_NUM":   acct[acct_id]["custodian_account_num"],
            # Ambiguity A1: security identified by CUSIP only
            "SEC_CUSIP":  sec[sec_id]["cusip"],
            # Ambiguity A6: date is settlement/custody date
            "AS_OF_DT":   POSITION_DATE,
            # Ambiguity A5: lot-level quantity
            "UNITS":      units,
            # Ambiguity A2: custodian EOD price (no variance)
            "MKT_PRC":    mkt_prc,
            "MKT_VAL":    mkt_val,
            "COST_BASIS": round(cost, 2),
            "UNRLZD_GL":  unrlzd,
            "LOT_ID":     lot["lot_id"],
            "CCY":        dw_security.loc[
                              dw_security["security_id"] == sec_id, "currency"
                          ].iloc[0],
        })

    return pd.DataFrame(rows)


# ── Emerald ───────────────────────────────────────────────────────────────────

def generate_emerald_positions(
    dw_position: pd.DataFrame,
    dw_account: pd.DataFrame,
    dw_security: pd.DataFrame,
) -> pd.DataFrame:
    """
    Emerald (portfolio management system) position file.

    Physical schema: camelCase column names.
    Grain: position-level aggregate — one row per account × security (Ambiguity A5).
    Security identifier: proprietary internal ticker (Ambiguity A1).
    Account identifier: portfolio code (Ambiguity A3/A4).
    Date column: positionDate — represents trade date (Ambiguity A6).
    Price: PM evaluated price — custodian price ± EMERALD_PRICE_VARIANCE (A2).
    Includes unrealized P&L (differs from Topaz due to price variance).
    """
    sec = _sec_lookup(dw_security)
    acct = _acct_lookup(dw_account)

    np_rng = np.random.default_rng(RANDOM_SEED + 10)
    rows = []

    for _, pos in dw_position.iterrows():
        acct_id = pos["account_id"]
        sec_id  = pos["security_id"]

        # Ambiguity A2: PM evaluated price varies from custodian EOD
        variance = np_rng.uniform(-EMERALD_PRICE_VARIANCE, EMERALD_PRICE_VARIANCE)
        unit_price = round(pos["market_price"] * (1 + variance), 4)
        quantity   = pos["quantity"]
        mkt_value  = round(quantity * unit_price, 2)
        cost_basis = pos["cost_basis"]
        avg_cost   = round(cost_basis / quantity, 4) if quantity else 0.0
        unrlzd_pnl = round(mkt_value - cost_basis, 2)

        currency = dw_security.loc[
            dw_security["security_id"] == sec_id, "currency"
        ].iloc[0]

        rows.append({
            # Ambiguity A4 / A3: account identifier is portfolio code
            "portfolioId":    acct[acct_id]["portfolio_code"],
            # Ambiguity A1: security identified by ticker only
            "securityTicker": sec[sec_id]["ticker"],
            # Ambiguity A6: date is trade date (PM view)
            "positionDate":   POSITION_DATE,
            # Ambiguity A5: position-level quantity (lots collapsed)
            "quantity":       quantity,
            # Ambiguity A2: PM evaluated price
            "unitPrice":      unit_price,
            "marketValue":    mkt_value,
            "avgCostBasis":   avg_cost,
            "unrealizedPnL":  unrlzd_pnl,
            "ccy":            currency,
        })

    return pd.DataFrame(rows)


# ── Ruby ──────────────────────────────────────────────────────────────────────

def generate_ruby_positions(
    dw_position: pd.DataFrame,
    dw_account: pd.DataFrame,
    dw_security: pd.DataFrame,
) -> pd.DataFrame:
    """
    Ruby (fund accounting system) position file.

    Physical schema: verbose snake_case column names.
    Grain: position-level — one row per fund × security (Ambiguity A5).
    Security identifier: ISIN (Ambiguity A1).
    Account identifier: fund code (Ambiguity A3/A4).
    Date column: nav_date — represents NAV strike date (Ambiguity A6).
    Price: NAV-based price — custodian price ± RUBY_PRICE_VARIANCE (A2).
    No unrealized G/L column — fund accounting tracks book cost only.
    """
    sec = _sec_lookup(dw_security)
    acct = _acct_lookup(dw_account)

    np_rng = np.random.default_rng(RANDOM_SEED + 20)
    rows = []

    for _, pos in dw_position.iterrows():
        acct_id = pos["account_id"]
        sec_id  = pos["security_id"]

        # Ambiguity A2: NAV price differs from both Topaz and Emerald
        variance = np_rng.uniform(-RUBY_PRICE_VARIANCE, RUBY_PRICE_VARIANCE)
        nav_per_share  = round(pos["market_price"] * (1 + variance), 4)
        shares_held    = pos["quantity"]
        total_nav_value = round(shares_held * nav_per_share, 2)

        currency = dw_security.loc[
            dw_security["security_id"] == sec_id, "currency"
        ].iloc[0]

        rows.append({
            # Ambiguity A4 / A3: account identifier is fund code
            "fund_code":       acct[acct_id]["fund_code"],
            # Ambiguity A1: security identified by ISIN only
            "isin_identifier": sec[sec_id]["isin"],
            # Ambiguity A6: date is NAV strike date (fund accounting view)
            "nav_date":        POSITION_DATE,
            # Ambiguity A5: position-level quantity
            "shares_held":     shares_held,
            # Ambiguity A2: NAV-based price
            "nav_per_share":   nav_per_share,
            "total_nav_value": total_nav_value,
            # Ruby tracks book cost; no unrealized G/L column (fund acct convention)
            "book_cost":       round(pos["cost_basis"], 2),
            "currency_code":   currency,
        })

    return pd.DataFrame(rows)


# ── Naive Integration Table (Pipeline A — Ambiguities A7–A11) ─────────────────

def _get_unmastered_ids(dw_security: pd.DataFrame) -> set:
    """
    Deterministically select the ~15% of securities that are 'unmastered' in the
    naive Pipeline A environment. Uses a fixed seed offset so the same set is
    returned regardless of call order. Both generate_integrated_positions() and
    generate_security_master_stub() must call this to stay in sync.
    """
    unmastered_rng = random.Random(RANDOM_SEED + 30)
    all_sec_ids = list(dw_security["security_id"])
    n_unmastered = round(len(all_sec_ids) * UNMASTERED_SECURITY_FRACTION)
    return set(unmastered_rng.sample(all_sec_ids, n_unmastered))


def generate_security_master_stub(dw_security: pd.DataFrame) -> pd.DataFrame:
    """
    Stub security master for Pipeline A's ETL LEFT JOIN.

    Contains only the ~170 mastered securities (the ~30 unmastered ones are
    absent — not NULL-filled). When the ETL LEFT JOINs on this table, the
    absent rows produce NULL security_master_id and NULL asset_class in
    SILVER.POSITIONS_INTEGRATED, demonstrating Ambiguities A8 and A10.

    Columns: cusip, isin, ticker, security_master_id, asset_class
    """
    unmastered_ids = _get_unmastered_ids(dw_security)
    mastered = dw_security[~dw_security["security_id"].isin(unmastered_ids)].copy()
    return (
        mastered[["cusip", "isin", "ticker", "security_id", "asset_class"]]
        .rename(columns={"security_id": "security_master_id"})
        .reset_index(drop=True)
    )


def generate_integrated_positions(
    dw_trade_lot: pd.DataFrame,
    dw_position: pd.DataFrame,
    dw_account: pd.DataFrame,
    dw_security: pd.DataFrame,
) -> pd.DataFrame:
    """
    Naive ETL integration of all three gemstone source systems into one table.

    This is Pipeline A's primary artifact: a single POSITIONS_INTEGRATED table
    that looks normalized (consistent column names, one row per record) but
    buries all six original ambiguities (A1–A6) and introduces five new ones:

      A7  — Mixed-grain record IDs: lot IDs and position IDs coexist in record_id
      A8  — Unmastered security IDs: ~15% of securities have security_master_id=NULL
      A9  — Cost basis semantic fragmentation: lot cost / avg cost / book cost blended
      A10 — Asset class classification gap: NULL asset_class for unmastered rows
      A11 — NULL unrealized G/L from Ruby: Ruby has no G/L concept, rows silently excluded

    Row counts:
      Topaz:   len(dw_trade_lot) rows  (lot-level — A7 grain trap)
      Emerald: len(dw_position) rows   (position-level)
      Ruby:    len(dw_position) rows   (position-level)

    Prices for Emerald/Ruby use the same RNG seeds as the standalone source files
    so prices are consistent across positions_emerald.csv and this table.
    """
    sec = _sec_lookup(dw_security)
    acct = _acct_lookup(dw_account)

    # Extended security lookup: security_id → asset_class, currency
    sec_meta = {
        row["security_id"]: {
            "asset_class": row["asset_class"],
            "currency":    row["currency"],
        }
        for _, row in dw_security.iterrows()
    }

    # Custodian baseline price lookup: (account_id, security_id) → market_price
    price_map = dict(zip(
        zip(dw_position["account_id"], dw_position["security_id"]),
        dw_position["market_price"],
    ))

    # ── Phase 1: Determine unmastered security set (A8 / A10) ─────────────────
    unmastered_ids = _get_unmastered_ids(dw_security)

    # ── Phase 2: Topaz rows (lot-level) ───────────────────────────────────────
    # A7: record_id = lot_id — grain is invisible in the integrated schema
    # A8/A10: security_master_id and asset_class are NULL for unmastered securities
    # A9: cost_basis = specific lot cost (Topaz method)
    topaz_rows = []
    for _, lot in dw_trade_lot.iterrows():
        acct_id = lot["account_id"]
        sec_id  = lot["security_id"]
        key = (acct_id, sec_id)

        if key not in price_map:
            continue  # skip orphaned lots (shouldn't happen with valid DW data)

        quantity  = lot["remaining_quantity"]
        price     = price_map[key]                        # custodian EOD — no variance
        mkt_value = round(quantity * price, 2)
        cost      = round(lot["cost_basis"], 2)
        unrlzd    = round(mkt_value - cost, 2)

        mastered = sec_id not in unmastered_ids

        topaz_rows.append({
            "record_id":          lot["lot_id"],          # A7: lot-level ID
            "source_system":      INTEGRATED_SOURCE_TOPAZ,
            "account_ref":        acct[acct_id]["custodian_account_num"],  # A4 survives
            "security_ref":       sec[sec_id]["cusip"],   # A1 survives (CUSIP)
            "security_ref_type":  "CUSIP",
            "security_master_id": sec_id if mastered else None,            # A8
            "position_date":      POSITION_DATE,          # A6 survives (settlement date)
            "quantity":           quantity,                # A7: lot-level — trap for SUM
            "price":              price,                   # A2 survives (custodian EOD)
            "market_value":       mkt_value,
            "cost_basis":         cost,                    # A9: specific lot cost
            "unrealized_gl":      unrlzd,
            "asset_class":        sec_meta[sec_id]["asset_class"] if mastered else None,  # A10
            "currency":           sec_meta[sec_id]["currency"],
            "etl_loaded_at":      "2025-01-02 06:00:00",  # Topaz batch loads at 06:00
        })

    # ── Phase 3: Emerald rows (position-level) ────────────────────────────────
    # A7: record_id = fabricated composite key — looks structured, not a surrogate
    # A9: cost_basis = position total cost (Emerald treats as average cost method)
    # Price variance uses same seed as standalone generate_emerald_positions()
    emerald_np_rng = np.random.default_rng(RANDOM_SEED + 10)
    emerald_rows = []
    for _, pos in dw_position.iterrows():
        acct_id = pos["account_id"]
        sec_id  = pos["security_id"]

        variance  = emerald_np_rng.uniform(-EMERALD_PRICE_VARIANCE, EMERALD_PRICE_VARIANCE)
        price     = round(pos["market_price"] * (1 + variance), 4)  # A2: PM evaluated
        quantity  = pos["quantity"]
        mkt_value = round(quantity * price, 2)
        cost      = round(pos["cost_basis"], 2)            # A9: avg cost method
        unrlzd    = round(mkt_value - cost, 2)

        portfolio_code = acct[acct_id]["portfolio_code"]
        ticker         = sec[sec_id]["ticker"]
        mastered       = sec_id not in unmastered_ids

        emerald_rows.append({
            "record_id":          f"POS-{portfolio_code}-{ticker}",  # A7: position ID
            "source_system":      INTEGRATED_SOURCE_EMERALD,
            "account_ref":        portfolio_code,          # A4 survives (PORT-XXXX)
            "security_ref":       ticker,                  # A1 survives (ticker)
            "security_ref_type":  "TICKER",
            "security_master_id": sec_id if mastered else None,       # A8
            "position_date":      POSITION_DATE,           # A6 survives (trade date)
            "quantity":           quantity,                 # position-level
            "price":              price,                    # A2: PM evaluated price
            "market_value":       mkt_value,
            "cost_basis":         cost,                     # A9: average cost method
            "unrealized_gl":      unrlzd,
            "asset_class":        sec_meta[sec_id]["asset_class"] if mastered else None,  # A10
            "currency":           sec_meta[sec_id]["currency"],
            "etl_loaded_at":      "2025-01-02 08:00:00",   # Emerald loads at 08:00
        })

    # ── Phase 4: Ruby rows (position-level) ───────────────────────────────────
    # A7: record_id = NAV-{fund_code}-{isin} — ISIN embedded, mismatches CUSIP/ticker systems
    # A9: cost_basis = book cost (Ruby method — does not adjust for partial redemptions)
    # A11: unrealized_gl = None — Ruby fund accounting has no G/L concept
    ruby_np_rng = np.random.default_rng(RANDOM_SEED + 20)
    ruby_rows = []
    for _, pos in dw_position.iterrows():
        acct_id = pos["account_id"]
        sec_id  = pos["security_id"]

        variance  = ruby_np_rng.uniform(-RUBY_PRICE_VARIANCE, RUBY_PRICE_VARIANCE)
        price     = round(pos["market_price"] * (1 + variance), 4)  # A2: NAV price
        quantity  = pos["quantity"]
        mkt_value = round(quantity * price, 2)
        cost      = round(pos["cost_basis"], 2)             # A9: book cost

        fund_code = acct[acct_id]["fund_code"]
        isin      = sec[sec_id]["isin"]
        mastered  = sec_id not in unmastered_ids

        ruby_rows.append({
            "record_id":          f"NAV-{fund_code}-{isin}",  # A7: NAV record ID
            "source_system":      INTEGRATED_SOURCE_RUBY,
            "account_ref":        fund_code,               # A4 survives (FND-XXXX)
            "security_ref":       isin,                    # A1 survives (ISIN)
            "security_ref_type":  "ISIN",
            "security_master_id": sec_id if mastered else None,        # A8
            "position_date":      POSITION_DATE,           # A6 survives (NAV strike date)
            "quantity":           quantity,
            "price":              price,                    # A2: NAV-based price
            "market_value":       mkt_value,
            "cost_basis":         cost,                     # A9: book cost
            "unrealized_gl":      None,                    # A11: Ruby has no G/L concept
            "asset_class":        sec_meta[sec_id]["asset_class"] if mastered else None,  # A10
            "currency":           sec_meta[sec_id]["currency"],
            "etl_loaded_at":      "2025-01-02 10:00:00",   # Ruby loads at 10:00
        })

    return pd.concat(
        [pd.DataFrame(topaz_rows), pd.DataFrame(emerald_rows), pd.DataFrame(ruby_rows)],
        ignore_index=True,
    )
