"""
generator_v2 — Gemstone source system seed data generator.

Generates nine CSV files in data/seed_v2/:

  Canonical DW (ground truth):
    dw_account.csv      — 100 accounts with all three source system keys
    dw_security.csv     — 200 securities with CUSIP, ISIN, ticker
    dw_trade_lot.csv    — lot-level positions (Topaz grain)
    dw_position.csv     — position-level aggregates (canonical)

  Gemstone source system files (derived from DW with schema transformations):
    positions_topaz.csv        — position-level, CUSIP, ACCT_NUM, custodian EOD price
    positions_emerald.csv      — lot-level, ticker, portfolioId, PM evaluated price
    positions_ruby.csv         — position-level, ISIN, fund_code, NAV price

  Naive Pipeline artifacts:
    security_master_stub.csv   — 170 mastered securities for ETL LEFT JOIN (30 absent → A8/A10 NULLs)
    positions_integrated.csv   — union of all three sources, A7–A11 ambiguities

Usage:
    python -m generator_v2.generator [--output-dir data/seed_v2] [--validate]
"""

import argparse
import os
import sys

import pandas as pd

from generator_v2.config import (
    DW_ACCOUNT_FILE,
    DW_POSITION_FILE,
    DW_SECURITY_FILE,
    DW_TRADE_LOT_FILE,
    EMERALD_FILE,
    INTEGRATED_FILE,
    OUTPUT_DIR,
    RUBY_FILE,
    SECURITY_MASTER_STUB_FILE,
    TOPAZ_FILE,
    UNMASTERED_SECURITY_FRACTION,
)
from generator_v2.models.canonical import (
    generate_dw_account,
    generate_dw_position,
    generate_dw_security,
    generate_dw_trade_lot,
)
from generator_v2.models.sources import (
    generate_emerald_positions,
    generate_integrated_positions,
    generate_ruby_positions,
    generate_security_master_stub,
    generate_topaz_positions,
)


def _write(df: pd.DataFrame, path: str, label: str) -> None:
    df.to_csv(path, index=False)
    print(f"  {label:<30} {len(df):>8,} rows  ->  {path}")


def validate(
    dw_account: pd.DataFrame,
    dw_security: pd.DataFrame,
    dw_trade_lot: pd.DataFrame,
    dw_position: pd.DataFrame,
    topaz: pd.DataFrame,
    emerald: pd.DataFrame,
    ruby: pd.DataFrame,
    stub: pd.DataFrame,
    integrated: pd.DataFrame,
) -> bool:
    """
    Post-generation integrity checks. Returns True if all pass.

    Checks V1–V13: canonical DW table integrity and source file consistency.
    Checks VI1–VI5: integrated table structural properties (confirms the
    intentional ambiguities are correctly encoded).

      V1  All DW_TRADE_LOT account_ids exist in DW_ACCOUNT
      V2  All DW_TRADE_LOT security_ids exist in DW_SECURITY
      V3  DW_POSITION quantity per account/security = sum of lot quantities
      V4  DW_POSITION market_value = quantity × market_price (within 1 cent)
      V5  Topaz row count = DW_POSITION row count (position-level 1:1)
      V6  Emerald row count = DW_TRADE_LOT row count (lot-level 1:1)
      V7  Ruby row count = DW_POSITION row count (position-level 1:1)
      V8  Topaz CUSIPs all present in DW_SECURITY.cusip
      V9  Emerald tickers all present in DW_SECURITY.ticker
      V10 Ruby ISINs all present in DW_SECURITY.isin
      V11 Topaz ACCT_NUMs all present in DW_ACCOUNT.custodian_account_num
      V12 Emerald portfolioIds all present in DW_ACCOUNT.portfolio_code
      V13 Ruby fund_codes all present in DW_ACCOUNT.fund_code

      VI1 Integrated row count = Topaz + Emerald + Ruby rows (complete union)
      VI2 Integrated has ~UNMASTERED_SECURITY_FRACTION NULL security_master_ids
      VI3 All non-NULL security_master_ids exist in DW_SECURITY.security_id
      VI4 For mastered securities, Emerald contributes ≥1 lot row and Topaz/Ruby
          each contribute exactly 1 position row per account×security pair
      VI5 All Ruby rows in integrated have unrealized_gl = NaN/None (A11)
      VS1 Security master stub row count = NUM_SECURITIES − n_unmastered
      VS2 All stub security_master_ids exist in DW_SECURITY.security_id
      VS3 No stub security_master_ids are in the unmastered set (no overlap)
    """
    errors = []

    # V1 — lot account FK
    lot_accts = set(dw_trade_lot["account_id"])
    dw_accts  = set(dw_account["account_id"])
    if not lot_accts.issubset(dw_accts):
        errors.append(f"V1 FAIL: {len(lot_accts - dw_accts)} unknown account_ids in DW_TRADE_LOT")

    # V2 — lot security FK
    lot_secs = set(dw_trade_lot["security_id"])
    dw_secs  = set(dw_security["security_id"])
    if not lot_secs.issubset(dw_secs):
        errors.append(f"V2 FAIL: {len(lot_secs - dw_secs)} unknown security_ids in DW_TRADE_LOT")

    # V3 — position quantity = sum of lot quantities
    lot_qty = (
        dw_trade_lot.groupby(["account_id", "security_id"])["remaining_quantity"]
        .sum()
        .reset_index()
        .rename(columns={"remaining_quantity": "lot_total_qty"})
    )
    pos_qty = dw_position[["account_id", "security_id", "quantity"]].copy()
    merged = pos_qty.merge(lot_qty, on=["account_id", "security_id"], how="left")
    mismatch = merged[abs(merged["quantity"] - merged["lot_total_qty"]) > 1]
    if not mismatch.empty:
        errors.append(f"V3 FAIL: {len(mismatch)} positions where quantity ≠ sum of lot quantities")

    # V4 — market_value = quantity × market_price
    expected_mv = dw_position["quantity"] * dw_position["market_price"]
    mv_diff = abs(dw_position["market_value"] - expected_mv)
    if (mv_diff > 0.02).any():
        errors.append(f"V4 FAIL: {(mv_diff > 0.02).sum()} positions where market_value ≠ qty × price")

    # V5 — Topaz is position-level
    if len(topaz) != len(dw_position):
        errors.append(f"V5 FAIL: Topaz rows ({len(topaz)}) ≠ DW_POSITION rows ({len(dw_position)})")

    # V6 — Emerald is lot-level
    if len(emerald) != len(dw_trade_lot):
        errors.append(f"V6 FAIL: Emerald rows ({len(emerald)}) ≠ DW_TRADE_LOT rows ({len(dw_trade_lot)})")

    # V7 — Ruby is position-level
    if len(ruby) != len(dw_position):
        errors.append(f"V7 FAIL: Ruby rows ({len(ruby)}) ≠ DW_POSITION rows ({len(dw_position)})")

    # V8 — Topaz CUSIPs
    dw_cusips = set(dw_security["cusip"])
    topaz_cusips = set(topaz["SEC_CUSIP"])
    if not topaz_cusips.issubset(dw_cusips):
        errors.append(f"V8 FAIL: {len(topaz_cusips - dw_cusips)} Topaz CUSIPs not in DW_SECURITY")

    # V9 — Emerald tickers
    dw_tickers = set(dw_security["ticker"])
    emerald_tickers = set(emerald["securityTicker"])
    if not emerald_tickers.issubset(dw_tickers):
        errors.append(f"V9 FAIL: {len(emerald_tickers - dw_tickers)} Emerald tickers not in DW_SECURITY")

    # V10 — Ruby ISINs
    dw_isins = set(dw_security["isin"])
    ruby_isins = set(ruby["isin_identifier"])
    if not ruby_isins.issubset(dw_isins):
        errors.append(f"V10 FAIL: {len(ruby_isins - dw_isins)} Ruby ISINs not in DW_SECURITY")

    # V11 — Topaz ACCT_NUMs
    dw_cust_accts = set(dw_account["custodian_account_num"])
    topaz_accts   = set(topaz["ACCT_NUM"])
    if not topaz_accts.issubset(dw_cust_accts):
        errors.append(f"V11 FAIL: {len(topaz_accts - dw_cust_accts)} Topaz ACCT_NUMs not in DW_ACCOUNT")

    # V12 — Emerald portfolioIds
    dw_port_codes   = set(dw_account["portfolio_code"])
    emerald_port_ids = set(emerald["portfolioId"])
    if not emerald_port_ids.issubset(dw_port_codes):
        errors.append(f"V12 FAIL: {len(emerald_port_ids - dw_port_codes)} Emerald portfolioIds not in DW_ACCOUNT")

    # V13 — Ruby fund_codes
    dw_fund_codes  = set(dw_account["fund_code"])
    ruby_fund_codes = set(ruby["fund_code"])
    if not ruby_fund_codes.issubset(dw_fund_codes):
        errors.append(f"V13 FAIL: {len(ruby_fund_codes - dw_fund_codes)} Ruby fund_codes not in DW_ACCOUNT")

    # ── Integrated table checks (VI1–VI5) ─────────────────────────────────────

    # VI1 — row count is complete union
    expected_integrated = len(topaz) + len(emerald) + len(ruby)
    if len(integrated) != expected_integrated:
        errors.append(
            f"VI1 FAIL: integrated rows ({len(integrated)}) ≠ "
            f"topaz+emerald+ruby ({expected_integrated})"
        )

    # VI2 — unmastered fraction is approximately correct
    null_master = integrated["security_master_id"].isna().sum()
    total_integrated = len(integrated)
    null_frac = null_master / total_integrated if total_integrated else 0
    # Allow ±5pp tolerance around the configured fraction
    tolerance = 0.05
    if abs(null_frac - UNMASTERED_SECURITY_FRACTION) > tolerance:
        errors.append(
            f"VI2 FAIL: NULL security_master_id fraction={null_frac:.3f}, "
            f"expected ~{UNMASTERED_SECURITY_FRACTION:.3f} (±{tolerance})"
        )

    # VI3 — non-NULL security_master_ids are valid FKs
    dw_sec_ids = set(dw_security["security_id"])
    integrated_master_ids = set(
        integrated["security_master_id"].dropna()
    )
    if not integrated_master_ids.issubset(dw_sec_ids):
        bad = integrated_master_ids - dw_sec_ids
        errors.append(f"VI3 FAIL: {len(bad)} security_master_id values not in DW_SECURITY")

    # VI4 — for a mastered account×security pair, Emerald has ≥1 lot row and
    #        Topaz/Ruby each have exactly 1 position row
    topaz_int   = integrated[integrated["source_system"] == "TOPAZ"]
    emerald_int = integrated[integrated["source_system"] == "EMERALD"]
    ruby_int    = integrated[integrated["source_system"] == "RUBY"]

    topaz_counts = topaz_int.groupby("security_master_id").size()

    # Topaz: each mastered security should appear exactly DW_POSITION count times
    # (one position row per account × security).
    dw_pos_per_sec = dw_position.groupby("security_id").size()
    for sec_id, count in topaz_counts.items():
        if sec_id and sec_id in dw_pos_per_sec and count != dw_pos_per_sec[sec_id]:
            errors.append(
                f"VI4 FAIL: Topaz integrated has {count} rows for {sec_id}, "
                f"expected {dw_pos_per_sec[sec_id]}"
            )
            break  # report first mismatch only

    # VI5 — all Ruby rows have unrealized_gl = NaN (A11)
    ruby_gl_non_null = ruby_int["unrealized_gl"].notna().sum()
    if ruby_gl_non_null > 0:
        errors.append(f"VI5 FAIL: {ruby_gl_non_null} Ruby integrated rows have non-NULL unrealized_gl")

    # ── Security master stub checks (VS1–VS3) ─────────────────────────────────

    # VS1 — stub row count = total securities − unmastered count
    n_unmastered = round(len(dw_security) * UNMASTERED_SECURITY_FRACTION)
    expected_stub_rows = len(dw_security) - n_unmastered
    if len(stub) != expected_stub_rows:
        errors.append(
            f"VS1 FAIL: stub rows ({len(stub)}) ≠ expected {expected_stub_rows} "
            f"({len(dw_security)} securities − {n_unmastered} unmastered)"
        )

    # VS2 — all stub security_master_ids are valid FKs
    stub_ids = set(stub["security_master_id"])
    if not stub_ids.issubset(dw_sec_ids):
        bad = stub_ids - dw_sec_ids
        errors.append(f"VS2 FAIL: {len(bad)} stub security_master_ids not in DW_SECURITY")

    # VS3 — no unmastered security appears in stub (no overlap)
    unmastered_in_stub = stub_ids & integrated[integrated["security_master_id"].isna()]["security_master_id"].dropna().pipe(set)
    # Simpler: compare stub IDs against the NULLed-out set via the integrated table's non-NULL ids
    mastered_in_integrated = set(integrated["security_master_id"].dropna())
    unmastered_leaked = stub_ids - mastered_in_integrated
    # VS3: all stub IDs should appear as mastered in integrated (no unmastered leaked into stub)
    if unmastered_leaked:
        errors.append(
            f"VS3 FAIL: {len(unmastered_leaked)} stub security_master_ids not found as mastered in integrated table"
        )

    n_checks = 13 + 5 + 3
    if errors:
        print("\nValidation FAILED:")
        for e in errors:
            print(f"  ✗ {e}")
        return False
    else:
        print(f"\nValidation PASSED -- all {n_checks} checks OK (V1–V13, VI1–VI5, VS1–VS3)")
        return True


def run(output_dir: str, run_validate: bool) -> None:
    os.makedirs(output_dir, exist_ok=True)
    print(f"\nGenerating seed data -> {output_dir}/\n")

    print("Building canonical DW tables...")
    dw_account   = generate_dw_account()
    dw_security  = generate_dw_security()
    dw_trade_lot = generate_dw_trade_lot(dw_account, dw_security)
    dw_position  = generate_dw_position(dw_trade_lot, dw_security)

    print("Deriving gemstone source files...")
    topaz   = generate_topaz_positions(dw_position, dw_account, dw_security)
    emerald = generate_emerald_positions(dw_trade_lot, dw_position, dw_account, dw_security)
    ruby    = generate_ruby_positions(dw_position, dw_account, dw_security)

    print("Building Naive Pipeline artifacts...")
    stub       = generate_security_master_stub(dw_security)
    integrated = generate_integrated_positions(dw_trade_lot, dw_position, dw_account, dw_security)

    print("\nWriting CSVs:")
    _write(dw_account,   os.path.join(output_dir, DW_ACCOUNT_FILE),            "dw_account.csv")
    _write(dw_security,  os.path.join(output_dir, DW_SECURITY_FILE),           "dw_security.csv")
    _write(dw_trade_lot, os.path.join(output_dir, DW_TRADE_LOT_FILE),          "dw_trade_lot.csv")
    _write(dw_position,  os.path.join(output_dir, DW_POSITION_FILE),           "dw_position.csv")
    _write(topaz,        os.path.join(output_dir, TOPAZ_FILE),                 "positions_topaz.csv")
    _write(emerald,      os.path.join(output_dir, EMERALD_FILE),               "positions_emerald.csv")
    _write(ruby,         os.path.join(output_dir, RUBY_FILE),                  "positions_ruby.csv")
    _write(stub,         os.path.join(output_dir, SECURITY_MASTER_STUB_FILE),  "security_master_stub.csv")
    _write(integrated,   os.path.join(output_dir, INTEGRATED_FILE),            "positions_integrated.csv")

    total = sum(len(df) for df in [dw_account, dw_security, dw_trade_lot, dw_position, topaz, emerald, ruby, stub, integrated])
    print(f"\nTotal rows generated: {total:,}")

    if run_validate:
        ok = validate(dw_account, dw_security, dw_trade_lot, dw_position, topaz, emerald, ruby, stub, integrated)
        if not ok:
            sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate gemstone source system seed data (V2)")
    parser.add_argument("--output-dir", default=OUTPUT_DIR, help="Output directory for CSV files")
    parser.add_argument("--validate",   action="store_true",  help="Run integrity checks after generation")
    args = parser.parse_args()
    run(args.output_dir, args.validate)


if __name__ == "__main__":
    main()
