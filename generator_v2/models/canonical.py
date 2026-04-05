"""
Canonical DW table generators for generator_v2.

Generates ground-truth data for:
  DW_ACCOUNT   — canonical account master with all three source system keys
  DW_SECURITY  — canonical security master with CUSIP, ISIN, ticker
  DW_TRADE_LOT — lot-level position detail (Topaz grain)
  DW_POSITION  — position-level aggregate (derived from lots)

All source system files (Topaz, Emerald, Ruby) are derived from these tables
in sources.py. This means ground truth is always knowable.
"""

import random
import string
from datetime import date, timedelta

import pandas as pd
from faker import Faker

from generator_v2.config import (
    ACCOUNT_TYPES,
    ASSET_CLASSES,
    AVG_LOTS_PER_POSITION,
    EMERALD_ACCT_PREFIX,
    NUM_ACCOUNTS,
    NUM_SECURITIES,
    POSITION_COVERAGE,
    POSITION_DATE,
    RANDOM_SEED,
    RUBY_ACCT_PREFIX,
    SECURITY_TYPES,
    TICKER_MAX_LEN,
    TICKER_MIN_LEN,
    TOPAZ_ACCT_PREFIX,
)

fake = Faker()
Faker.seed(RANDOM_SEED)
rng = random.Random(RANDOM_SEED)


# ── Identifier generators ─────────────────────────────────────────────────────

def _cusip(rng: random.Random) -> str:
    """Generate a synthetic 9-character CUSIP (not check-digit validated)."""
    chars = string.ascii_uppercase + string.digits
    return "".join(rng.choices(chars, k=9))


def _isin(cusip: str) -> str:
    """Derive a synthetic ISIN from a CUSIP: US + CUSIP + check digit placeholder."""
    # Real ISIN = country code (2) + NSIN (9) + check digit (1)
    # We use US prefix + cusip as NSIN + a fixed trailing digit for simplicity
    nsin = cusip.upper()
    # Luhn-like check digit (simplified for synthetic data)
    digits = "US" + nsin
    total = sum(int(c, 36) for c in digits)
    check = str(total % 10)
    return "US" + nsin + check


def _ticker(security_name: str, existing: set, rng: random.Random) -> str:
    """Generate a unique ticker from the security name initials."""
    words = [w for w in security_name.upper().split() if w.isalpha()]
    base = "".join(w[0] for w in words[:TICKER_MAX_LEN])
    if not base:
        base = "".join(rng.choices(string.ascii_uppercase, k=3))
    candidate = base[:TICKER_MAX_LEN]
    # Ensure uniqueness
    suffix = 0
    original = candidate
    while candidate in existing:
        suffix += 1
        candidate = (original + str(suffix))[:TICKER_MAX_LEN]
    return candidate


def _topaz_acct(seq: int) -> str:
    return f"{TOPAZ_ACCT_PREFIX}-{seq:06d}"


def _emerald_acct(seq: int) -> str:
    return f"{EMERALD_ACCT_PREFIX}-{seq:04d}"


def _ruby_acct(seq: int) -> str:
    return f"{RUBY_ACCT_PREFIX}-{seq:04d}"


# ── DW_ACCOUNT ────────────────────────────────────────────────────────────────

def generate_dw_account() -> pd.DataFrame:
    """
    Canonical account master.
    Each account has three source-system keys (Ambiguity A3/A4):
      custodian_account_num  → used by Topaz
      portfolio_code         → used by Emerald
      fund_code              → used by Ruby
    """
    rows = []
    for i in range(1, NUM_ACCOUNTS + 1):
        acct_type = rng.choice(ACCOUNT_TYPES)
        rows.append({
            "account_id":            f"ACC-{i:04d}",
            "account_name":          fake.company() + " " + rng.choice(["Fund", "Portfolio", "Account", "Trust"]),
            "account_type":          acct_type,
            "base_currency":         "USD",
            "custodian_account_num": _topaz_acct(rng.randint(100000, 999999)),
            "portfolio_code":        _emerald_acct(i),
            "fund_code":             _ruby_acct(i),
            "is_active":             True,
        })
    return pd.DataFrame(rows)


# ── DW_SECURITY ───────────────────────────────────────────────────────────────

def generate_dw_security() -> pd.DataFrame:
    """
    Canonical security master with all identifier types (Ambiguity A1).
    Each security has CUSIP (Topaz), ticker (Emerald), and ISIN (Ruby).
    """
    rows = []
    used_cusips: set = set()
    used_tickers: set = set()

    for i in range(1, NUM_SECURITIES + 1):
        asset_class = rng.choice(ASSET_CLASSES)
        sec_type = rng.choice(SECURITY_TYPES[asset_class])
        name = fake.company() + " " + rng.choice(["Inc", "Corp", "Ltd", "plc", "AG", "SA"])

        # Generate unique CUSIP
        cusip = _cusip(rng)
        while cusip in used_cusips:
            cusip = _cusip(rng)
        used_cusips.add(cusip)

        isin = _isin(cusip)
        ticker = _ticker(name, used_tickers, rng)
        used_tickers.add(ticker)

        rows.append({
            "security_id":   f"SEC-{i:04d}",
            "security_name": name,
            "cusip":         cusip,
            "isin":          isin,
            "ticker":        ticker,
            "asset_class":   asset_class,
            "security_type": sec_type,
            "currency":      "USD",
        })
    return pd.DataFrame(rows)


# ── DW_TRADE_LOT ──────────────────────────────────────────────────────────────

def generate_dw_trade_lot(
    dw_account: pd.DataFrame,
    dw_security: pd.DataFrame,
) -> pd.DataFrame:
    """
    Lot-level position detail — the Topaz grain.
    Each account × security holding may have multiple lots from different
    acquisition dates at different prices. AVG_LOTS_PER_POSITION controls density.
    """
    position_date = date.fromisoformat(POSITION_DATE)
    rows = []
    lot_seq = 1

    account_ids = dw_account["account_id"].tolist()
    security_ids = dw_security["security_id"].tolist()

    for acct_id in account_ids:
        # Each account holds a random subset of securities
        n_holdings = max(1, int(len(security_ids) * POSITION_COVERAGE
                                + rng.gauss(0, len(security_ids) * 0.05)))
        n_holdings = min(n_holdings, len(security_ids))
        held_securities = rng.sample(security_ids, n_holdings)

        for sec_id in held_securities:
            n_lots = max(1, int(rng.gauss(AVG_LOTS_PER_POSITION, 1)))
            for _ in range(n_lots):
                # Acquisition date: 1–5 years before position date
                days_ago = rng.randint(30, 5 * 365)
                acq_date = position_date - timedelta(days=days_ago)

                acq_price = round(rng.uniform(10.0, 500.0), 4)
                orig_qty = round(rng.uniform(100, 5000), 0)
                # Remaining quantity = 50–100% of original (partial sales may have occurred)
                remaining_qty = round(orig_qty * rng.uniform(0.5, 1.0), 0)
                cost_basis = round(remaining_qty * acq_price, 2)

                rows.append({
                    "lot_id":             f"LOT-{lot_seq:07d}",
                    "account_id":         acct_id,
                    "security_id":        sec_id,
                    "acquisition_date":   acq_date.isoformat(),
                    "acquisition_price":  acq_price,
                    "original_quantity":  orig_qty,
                    "remaining_quantity": remaining_qty,
                    "cost_basis":         cost_basis,
                    "source_system":      "DW",
                })
                lot_seq += 1

    return pd.DataFrame(rows)


# ── DW_POSITION ───────────────────────────────────────────────────────────────

def generate_dw_position(
    dw_trade_lot: pd.DataFrame,
    dw_security: pd.DataFrame,
) -> pd.DataFrame:
    """
    Canonical position-level aggregate derived from DW_TRADE_LOT.
    One row per account × security (lots collapsed). Market price is assigned
    here as the custodian EOD baseline price — Emerald and Ruby apply variance
    noise on top of this in sources.py (Ambiguity A2).
    """
    # Build a market price lookup per security (custodian EOD baseline)
    sec_price = {
        row["security_id"]: round(rng.uniform(10.0, 500.0), 4)
        for _, row in dw_security.iterrows()
    }

    rows = []
    pos_seq = 1

    grouped = dw_trade_lot.groupby(["account_id", "security_id"])
    for (acct_id, sec_id), lots in grouped:
        quantity = lots["remaining_quantity"].sum()
        cost_basis = lots["cost_basis"].sum()
        market_price = sec_price[sec_id]
        market_value = round(quantity * market_price, 2)
        unrealized_gl = round(market_value - cost_basis, 2)

        # Retrieve currency from dw_security
        currency = dw_security.loc[
            dw_security["security_id"] == sec_id, "currency"
        ].iloc[0]

        rows.append({
            "position_id":         f"POS-{pos_seq:07d}",
            "account_id":          acct_id,
            "security_id":         sec_id,
            "position_date":       POSITION_DATE,
            "quantity":            round(quantity, 0),
            "market_price":        market_price,
            "market_value":        market_value,
            "cost_basis":          round(cost_basis, 2),
            "unrealized_gain_loss": unrealized_gl,
            "currency":            currency,
            "source_system":       "DW",
        })
        pos_seq += 1

    return pd.DataFrame(rows)
