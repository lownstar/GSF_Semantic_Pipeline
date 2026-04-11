"""
Question bank for the Silver vs Gold variance comparison.

Each Question defines:
- The natural language prompt sent to Cortex Analyst
- Which ambiguity codes (A1–A11) it exercises
- How Silver is expected to fail
- How to extract and classify the Cortex result (result_type)
- A ground_truth_fn that computes the expected answer from seed CSVs

result_type controls how the runner interprets Cortex's output:
  "scalar"    — extract the first numeric value from the first result row
  "row_count" — count the number of rows returned (question asks for a list)
  "percentage"— same as scalar but tolerance is in percentage points, not relative %
"""

from dataclasses import dataclass
from typing import Callable, Literal


@dataclass
class Question:
    id: str
    natural_language: str
    ambiguity_codes: list[str]
    failure_mode_silver: str
    ground_truth_description: str
    result_type: Literal["scalar", "row_count", "percentage"]
    ground_truth_fn: Callable  # (pos_df, sec_df, acc_df) -> float


# ── Ground truth functions ─────────────────────────────────────────────────────
# Each takes (pos, sec, acc) DataFrames loaded from data/seed_v2/ CSVs.
# Ground truth is always computed from the canonical DW data — never from Snowflake.

def _q01_gt(pos, sec, acc):
    """ACC-0042 total market value — the Epic 4 gate question."""
    return float(pos[pos["account_id"] == "ACC-0042"]["market_value"].sum())


def _q02_gt(pos, sec, acc):
    """Total quantity of RLP (SEC-0018, Rasmussen LLC plc) across all accounts.

    RLP is one of 30 securities absent from BRONZE.SECURITY_MASTER_STUB.
    Silver's POSITIONS_INTEGRATED has no security_name column and no mastered
    ID for this security — Cortex cannot reliably resolve it.
    """
    return float(pos[pos["security_id"] == "SEC-0018"]["quantity"].sum())


def _q03_gt(pos, sec, acc):
    """Total market value of all positions — blended price sources in Silver."""
    return float(pos["market_value"].sum())


def _q04_gt(pos, sec, acc):
    """Total unrealized loss for positions with a loss greater than $10,000.

    Ruby rows (22% of POSITIONS_INTEGRATED) have NULL unrealized_gl.
    Silver silently excludes them from any unrealized G/L calculation.
    """
    losses = pos[pos["unrealized_gain_loss"] < -10_000]
    return float(losses["unrealized_gain_loss"].sum())


def _q05_gt(pos, sec, acc):
    """Total quantity of Fixed Income securities across all portfolios.

    POSITIONS_INTEGRATED contains Topaz lot-level rows alongside Emerald/Ruby
    position-level rows. Ground truth is computed at position grain (lots collapsed),
    which is what the semantic Gold layer provides via DW_POSITION.
    """
    fi_ids = sec[sec["asset_class"] == "Fixed Income"]["security_id"]
    return float(pos[pos["security_id"].isin(fi_ids)]["quantity"].sum())


def _q06_gt(pos, sec, acc):
    """Count of positions as of December 31, 2024.

    Topaz uses AS_OF_DT (settlement), Emerald positionDate (trade), Ruby
    nav_date (NAV strike). At period-end these can diverge.
    """
    return float(len(pos[pos["position_date"] == "2024-12-31"]))


def _q07_gt(pos, sec, acc):
    """Total market value of all Fixed Income positions.

    4 of 27 Fixed Income securities are unmastered in BRONZE.SECURITY_MASTER_STUB.
    Any Silver query joining on security_master_id silently drops those positions.
    """
    fi_ids = sec[sec["asset_class"] == "Fixed Income"]["security_id"]
    return float(pos[pos["security_id"].isin(fi_ids)]["market_value"].sum())


def _q08_gt(pos, sec, acc):
    """Total unrealized gain for the Equity sleeve.

    POSITIONS_INTEGRATED.cost_basis blends three accounting methods:
    Topaz (specific lot identification), Emerald (average cost), Ruby (book cost).
    The resulting unrealized G/L cannot be reconciled to any system of record.
    """
    eq_ids = sec[sec["asset_class"] == "Equity"]["security_id"]
    return float(pos[pos["security_id"].isin(eq_ids)]["unrealized_gain_loss"].sum())


def _q09_gt(pos, sec, acc):
    """Fixed Income allocation as a percentage of total AUM.

    POSITIONS_INTEGRATED excludes ~15% of rows with NULL asset_class.
    Both the numerator and denominator are computed on a partial universe,
    making every sleeve percentage wrong — with no warning.
    """
    fi_ids = sec[sec["asset_class"] == "Fixed Income"]["security_id"]
    fi_mv = pos[pos["security_id"].isin(fi_ids)]["market_value"].sum()
    total_mv = pos["market_value"].sum()
    return float((fi_mv / total_mv) * 100.0)


def _q10_gt(pos, sec, acc):
    """Count of positions with unrealized losses greater than $10,000.

    Ruby is a fund accounting system — it has no unrealized G/L concept.
    All 4,886 Ruby-sourced rows in POSITIONS_INTEGRATED have NULL unrealized_gl.
    A WHERE clause on that column silently excludes 22% of the portfolio.
    """
    return float(len(pos[pos["unrealized_gain_loss"] < -10_000]))


def _q11_gt(pos, sec, acc):
    """Total cost basis for all Fixed Income positions.

    Compounds A1 (security ID fragmentation), A4 (account ID fragmentation),
    and A9 (cost basis method fragmentation): Silver can't resolve security
    identifiers cross-source, can't aggregate across account keys, and blends
    three incompatible cost accounting methods into one column.
    """
    fi_ids = sec[sec["asset_class"] == "Fixed Income"]["security_id"]
    return float(pos[pos["security_id"].isin(fi_ids)]["cost_basis"].sum())


# ── Question bank ──────────────────────────────────────────────────────────────

QUESTIONS: list[Question] = [
    Question(
        id="Q01",
        natural_language="What is the total market value of account ACC-0042?",
        ambiguity_codes=["A4"],
        failure_mode_silver=(
            "Silver stores raw source account keys (C-XXXXXX, PORT-XXXX, FND-XXXX). "
            "There is no canonical account_id in POSITIONS_INTEGRATED, so filtering by "
            "'ACC-0042' returns no data."
        ),
        ground_truth_description="Total market value of account ACC-0042 (George Group Trust)",
        result_type="scalar",
        ground_truth_fn=_q01_gt,
    ),
    Question(
        id="Q02",
        natural_language="What is the total quantity of Rasmussen LLC plc held across all accounts?",
        ambiguity_codes=["A1", "A8"],
        failure_mode_silver=(
            "POSITIONS_INTEGRATED has no security_name column — Cortex cannot resolve "
            "a name lookup. Even if it filters by identifier, SEC-0018 (RLP) is absent "
            "from BRONZE.SECURITY_MASTER_STUB, so security_master_id is NULL and any "
            "join-based lookup returns zero rows."
        ),
        ground_truth_description="Total quantity of Rasmussen LLC plc (ticker: RLP, SEC-0018) across all accounts",
        result_type="scalar",
        ground_truth_fn=_q02_gt,
    ),
    Question(
        id="Q03",
        natural_language="What is the total market value of all positions?",
        ambiguity_codes=["A2"],
        failure_mode_silver=(
            "POSITIONS_INTEGRATED.price blends custodian closing prices (Topaz), "
            "PM evaluated prices (Emerald), and NAV-based prices (Ruby). The total is "
            "internally consistent but computed from three different pricing methodologies "
            "that would never appear together in any system of record."
        ),
        ground_truth_description="Total market value across all 4,886 DW positions (custodian closing price basis)",
        result_type="scalar",
        ground_truth_fn=_q03_gt,
    ),
    Question(
        id="Q04",
        natural_language=(
            "What is the total unrealized loss across all positions "
            "with a loss greater than $10,000?"
        ),
        ambiguity_codes=["A3", "A11"],
        failure_mode_silver=(
            "POSITIONS_INTEGRATED.unrealized_gl is NULL for all Ruby-sourced rows (22% "
            "of the table). Any filter on unrealized_gl silently excludes those positions. "
            "Additionally, three different column names map to 'unrealized gain/loss' across "
            "the source schemas — Cortex may not recognize the concept in all three."
        ),
        ground_truth_description="Sum of unrealized_gain_loss for all positions where value < -$10,000",
        result_type="scalar",
        ground_truth_fn=_q04_gt,
    ),
    Question(
        id="Q05",
        natural_language=(
            "What is the total quantity of Fixed Income securities "
            "held across all portfolios?"
        ),
        ambiguity_codes=["A5", "A7"],
        failure_mode_silver=(
            "POSITIONS_INTEGRATED contains Topaz lot-level rows (LOT-* record_ids) "
            "alongside Emerald/Ruby position-level rows (POS-*, NAV-*). Grain is mixed "
            "and invisible — COUNT queries overcount Topaz positions, and any query "
            "treating one row as one position returns a lot fragment instead of the full "
            "position value. The observable symptom: a list query and a count query for "
            "the same question return different cardinalities with no error."
        ),
        ground_truth_description="Total quantity of Fixed Income securities at position grain (DW_POSITION)",
        result_type="scalar",
        ground_truth_fn=_q05_gt,
    ),
    Question(
        id="Q06",
        natural_language="How many positions exist as of December 31, 2024?",
        ambiguity_codes=["A6"],
        failure_mode_silver=(
            "Topaz uses AS_OF_DT (settlement date), Emerald uses positionDate (trade date), "
            "and Ruby uses nav_date (NAV strike date). At year-end these dates can fall on "
            "different calendar days. The ETL silently chose one convention — the column "
            "looks normalized but the semantic distinction between date types is lost."
        ),
        ground_truth_description="Count of positions in DW_POSITION with position_date = 2024-12-31 (trade-date basis)",
        result_type="row_count",
        ground_truth_fn=_q06_gt,
    ),
    Question(
        id="Q07",
        natural_language="What is the total market value of all Fixed Income positions?",
        ambiguity_codes=["A8"],
        failure_mode_silver=(
            "4 of 27 Fixed Income securities are absent from BRONZE.SECURITY_MASTER_STUB. "
            "Any Silver query that joins on security_master_id to retrieve asset_class "
            "silently drops those 4 securities' positions, understating Fixed Income AUM "
            "by an unknown and unannounced amount."
        ),
        ground_truth_description="Total market value of all Fixed Income positions (DW_POSITION joined to DW_SECURITY)",
        result_type="scalar",
        ground_truth_fn=_q07_gt,
    ),
    Question(
        id="Q08",
        natural_language="What is the total unrealized gain for the Equity sleeve?",
        ambiguity_codes=["A9"],
        failure_mode_silver=(
            "POSITIONS_INTEGRATED.cost_basis blends three incompatible accounting methods: "
            "Topaz specific lot identification, Emerald average cost, and Ruby book cost. "
            "The computed unrealized gain is internally consistent but cannot be reconciled "
            "to any system of record and would fail an audit."
        ),
        ground_truth_description="Sum of unrealized_gain_loss for Equity positions (specific identification cost basis)",
        result_type="scalar",
        ground_truth_fn=_q08_gt,
    ),
    Question(
        id="Q09",
        natural_language="What percentage of total AUM is allocated to Fixed Income?",
        ambiguity_codes=["A10"],
        failure_mode_silver=(
            "~15% of positions in POSITIONS_INTEGRATED have NULL asset_class (unmastered "
            "securities). The percentage is computed on a partial AUM base — both the Fixed "
            "Income numerator and the total denominator exclude unmastered positions, "
            "making every sleeve percentage systematically wrong with no indication of the gap."
        ),
        ground_truth_description="Fixed Income market value / total market value x 100 (complete DW universe)",
        result_type="percentage",
        ground_truth_fn=_q09_gt,
    ),
    Question(
        id="Q10",
        natural_language=(
            "How many positions have unrealized losses greater than $10,000?"
        ),
        ambiguity_codes=["A11"],
        failure_mode_silver=(
            "Ruby is a fund accounting system — it does not publish unrealized gain/loss. "
            "All 4,886 Ruby-sourced rows in POSITIONS_INTEGRATED have unrealized_gl = NULL. "
            "A filter WHERE unrealized_gl < -10000 silently excludes 22% of the portfolio. "
            "The result looks complete but covers only two of three source systems."
        ),
        ground_truth_description="Count of positions in DW_POSITION with unrealized_gain_loss < -$10,000",
        result_type="row_count",
        ground_truth_fn=_q10_gt,
    ),
    Question(
        id="Q11",
        natural_language="What is the total cost basis for all Fixed Income positions?",
        ambiguity_codes=["A1", "A4", "A9"],
        failure_mode_silver=(
            "Compounds three ambiguities: (A1) security identifiers are fragmented across "
            "CUSIP/ticker/ISIN with no shared key; (A4) account identifiers use source keys "
            "not canonical account_id; (A9) cost_basis blends lot identification, average "
            "cost, and book cost methods. The Silver answer is both incomplete and "
            "methodologically unsound."
        ),
        ground_truth_description="Sum of cost_basis for Fixed Income positions (specific identification, DW grain)",
        result_type="scalar",
        ground_truth_fn=_q11_gt,
    ),
]
