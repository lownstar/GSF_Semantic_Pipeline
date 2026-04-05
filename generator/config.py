"""
Central configuration for the seed data generator.
All constants live here — every other module imports from this file.
"""

RANDOM_SEED = 42

DATE_START = "2020-01-01"
DATE_END = "2024-12-31"

ROW_TARGETS = {
    "dim_account": 200,
    "dim_security": 500,
    "dim_asset_class": 8,       # static — derived from ASSET_CLASSES list
    "dim_benchmark": 10,         # static — derived from BENCHMARKS list
    "fact_position": 36_000,     # approximate; varies with account/security assignments
    "fact_transaction": 15_000,  # approximate; ~75 transactions per account
    "fact_return": 49_000,       # account x month-end x return_period (partial population)
    "fact_benchmark_return": 600,
}

# Asset class definitions — (code, name, is_liquid)
ASSET_CLASSES = [
    ("EQ",    "Equity",          True),
    ("FI",    "Fixed Income",    True),
    ("ALT",   "Alternatives",    False),
    ("CASH",  "Cash",            True),
    ("RE",    "Real Estate",     False),
    ("CMDTY", "Commodities",     True),
    ("HF",    "Hedge Funds",     False),
    ("PE",    "Private Equity",  False),
]

# Asset class weight distribution for security assignment (must sum to 1.0)
ASSET_CLASS_WEIGHTS = {
    "EQ":    0.50,
    "FI":    0.25,
    "ALT":   0.08,
    "CASH":  0.05,
    "RE":    0.04,
    "CMDTY": 0.04,
    "HF":    0.02,
    "PE":    0.02,
}

# Benchmark definitions — (code, name, type)
BENCHMARKS = [
    ("SPX",         "S&P 500 Index",              "EQUITY_INDEX"),
    ("AGG",         "Bloomberg US Aggregate",      "BOND_INDEX"),
    ("MSCI_WORLD",  "MSCI World Index",            "EQUITY_INDEX"),
    ("MSCI_EM",     "MSCI Emerging Markets",       "EQUITY_INDEX"),
    ("RUSSEL2K",    "Russell 2000 Index",          "EQUITY_INDEX"),
    ("NAREIT",      "FTSE NAREIT All Equity",      "EQUITY_INDEX"),
    ("BCOM",        "Bloomberg Commodity Index",   "EQUITY_INDEX"),
    ("T_BILL_90D",  "90-Day T-Bill",               "CASH"),
    ("BLEND_60_40", "60/40 Blended Index",         "BLENDED"),
    ("CUSTOM_PE",   "Custom Private Equity Benchmark", "CUSTOM"),
]

# Monthly return distribution parameters by benchmark type
BENCHMARK_RETURN_PARAMS = {
    "EQUITY_INDEX": {"mean": 0.0080, "std": 0.045},
    "BOND_INDEX":   {"mean": 0.0030, "std": 0.015},
    "CASH":         {"mean": 0.0020, "std": 0.002},
    "BLENDED":      {"mean": 0.0060, "std": 0.030},
    "CUSTOM":       {"mean": 0.0070, "std": 0.035},
}

ACCOUNT_TYPES = ["INDIVIDUAL", "INSTITUTIONAL", "TRUST", "ENDOWMENT"]
ACCOUNT_TYPE_WEIGHTS = [0.40, 0.35, 0.15, 0.10]

SECURITY_TYPES = ["EQUITY", "BOND", "ETF", "MUTUAL_FUND", "CASH_EQUIV"]

# Asset class → typical security types
ASSET_CLASS_SECURITY_TYPES = {
    "EQ":    ["EQUITY", "ETF"],
    "FI":    ["BOND", "ETF", "MUTUAL_FUND"],
    "ALT":   ["ETF", "MUTUAL_FUND"],
    "CASH":  ["CASH_EQUIV"],
    "RE":    ["ETF", "MUTUAL_FUND"],
    "CMDTY": ["ETF"],
    "HF":    ["MUTUAL_FUND"],
    "PE":    ["MUTUAL_FUND"],
}

SECTORS = [
    "Technology", "Healthcare", "Financials", "Consumer Discretionary",
    "Industrials", "Communication Services", "Consumer Staples",
    "Energy", "Utilities", "Real Estate", "Materials",
]

TRANSACTION_TYPES = ["BUY", "SELL", "DIVIDEND", "FEE", "TRANSFER_IN", "TRANSFER_OUT"]
TRANSACTION_TYPE_WEIGHTS = [0.38, 0.28, 0.18, 0.10, 0.03, 0.03]

TAX_LOT_METHODS = ["FIFO", "LIFO", "SPECIFIC_ID"]
TAX_LOT_METHOD_WEIGHTS = [0.70, 0.10, 0.20]

# Settlement lag in business days by security type (Ambiguity #3 mechanism)
SETTLEMENT_LAG = {
    "EQUITY":     2,
    "BOND":       1,
    "ETF":        2,
    "MUTUAL_FUND": 1,
    "CASH_EQUIV": 0,
}

RETURN_PERIODS = ["MTD", "QTD", "YTD", "1YR", "3YR", "5YR", "INCEPTION"]

# Monthly fee drag by account type (annual basis points / 12)
FEE_DRAG_MONTHLY = {
    "INDIVIDUAL":    0.00083,   # ~10bps/year
    "INSTITUTIONAL": 0.00042,   # ~5bps/year
    "TRUST":         0.00063,   # ~7.5bps/year
    "ENDOWMENT":     0.00050,   # ~6bps/year
}

# Holdings per account — (min, max) number of securities
HOLDINGS_PER_ACCOUNT = (8, 35)

# Transactions per account over the full date range
TRANSACTIONS_PER_ACCOUNT = (50, 100)

OUTPUT_DIR = "data/seed"
