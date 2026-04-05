"""
Configuration for generator_v2 — Gemstone source system demo (V2).

Three synthetic legacy source systems (Topaz, Emerald, Ruby) each export
position data in their own physical schema. A canonical DW schema serves as
ground truth. All source files are derived from the canonical data with
deterministic transformations + price variance noise.

See docs/ambiguity_registry_v2.md for the six intentional ambiguities (A1–A6).
"""

# ── Reproducibility ───────────────────────────────────────────────────────────

RANDOM_SEED = 42

# ── Volume targets ────────────────────────────────────────────────────────────

NUM_ACCOUNTS = 100      # DW_ACCOUNT rows
NUM_SECURITIES = 200    # DW_SECURITY rows
AVG_LOTS_PER_POSITION = 3   # average trade lots per account × security holding
POSITION_COVERAGE = 0.25    # fraction of securities held by any given account

# ── Position snapshot date ────────────────────────────────────────────────────
# Single month-end snapshot. Each source system calls this date something
# different (Ambiguity A6): Topaz = settlement date, Emerald = trade date,
# Ruby = NAV strike date.

POSITION_DATE = "2024-12-31"

# ── Price variance by source (Ambiguity A2) ───────────────────────────────────
# Topaz = custodian EOD closing price (baseline, no noise)
# Emerald = PM evaluated price (small positive/negative variance)
# Ruby = NAV-based price (slightly larger variance, always rounded to 4dp)

EMERALD_PRICE_VARIANCE = 0.003   # ±0.3% vs custodian
RUBY_PRICE_VARIANCE    = 0.005   # ±0.5% vs custodian

# ── Security identifier formats ───────────────────────────────────────────────
# Used when generating DW_SECURITY. Each source system uses a different ID
# field (Ambiguity A1).

CUSIP_LENGTH = 9   # alphanumeric
ISIN_PREFIX  = "US"
ISIN_LENGTH  = 12  # US + 10 chars
TICKER_MIN_LEN = 2
TICKER_MAX_LEN = 5

# ── Account identifier prefixes (Ambiguity A3 / A4) ──────────────────────────
# Three source systems use three different account identifier formats for the
# same underlying client account.

TOPAZ_ACCT_PREFIX   = "C"       # e.g. C-004291
EMERALD_ACCT_PREFIX = "PORT"    # e.g. PORT-042
RUBY_ACCT_PREFIX    = "FND"     # e.g. FND-042

# ── Asset classes ─────────────────────────────────────────────────────────────

ASSET_CLASSES = [
    "Equity",
    "Fixed Income",
    "Alternatives",
    "Cash",
    "Real Estate",
    "Commodities",
]

SECURITY_TYPES = {
    "Equity":        ["Common Stock", "Preferred Stock", "ETF"],
    "Fixed Income":  ["Government Bond", "Corporate Bond", "Municipal Bond", "ETF"],
    "Alternatives":  ["Hedge Fund", "Private Equity", "Real Assets"],
    "Cash":          ["Money Market", "Cash Equivalent"],
    "Real Estate":   ["REIT", "Real Estate Fund"],
    "Commodities":   ["Commodity ETF", "Futures Fund"],
}

# ── Account types ─────────────────────────────────────────────────────────────

ACCOUNT_TYPES = ["Individual", "Institutional", "Trust", "Endowment", "Fund"]

# ── Output ────────────────────────────────────────────────────────────────────

OUTPUT_DIR = "data/seed_v2"

# DW canonical files
DW_ACCOUNT_FILE   = "dw_account.csv"
DW_SECURITY_FILE  = "dw_security.csv"
DW_TRADE_LOT_FILE = "dw_trade_lot.csv"
DW_POSITION_FILE  = "dw_position.csv"

# Gemstone source system files
TOPAZ_FILE   = "positions_topaz.csv"
EMERALD_FILE = "positions_emerald.csv"
RUBY_FILE    = "positions_ruby.csv"

# ── Naive integration table (Naive Pipeline — Ambiguities A7–A11) ─────────────
# A single table produced by a naive ETL that unions all three sources with
# normalized column names. Looks clean; semantically broken.
# See docs/ambiguity_registry_v2.md for the full A7–A11 documentation.

INTEGRATED_FILE = "positions_integrated.csv"

# Stub security master for Naive Pipeline ETL — contains only the mastered
# securities. The ETL LEFT JOINs on this, producing NULLs for the ~15%
# that are absent (Ambiguities A8 / A10).
SECURITY_MASTER_STUB_FILE = "security_master_stub.csv"

# Fraction of securities to treat as "unmastered" in the integrated table.
# These securities exist in DW_SECURITY (Env B / governed) but the naive ETL
# failed to resolve them — security_master_id is NULL for their rows in
# POSITIONS_INTEGRATED (Env A). Simulates a security master onboarding gap
# (Ambiguity A8 / A10).
UNMASTERED_SECURITY_FRACTION = 0.15   # ~30 of 200 securities

# Source system labels written into POSITIONS_INTEGRATED.source_system
INTEGRATED_SOURCE_TOPAZ   = "TOPAZ"
INTEGRATED_SOURCE_EMERALD = "EMERALD"
INTEGRATED_SOURCE_RUBY    = "RUBY"
