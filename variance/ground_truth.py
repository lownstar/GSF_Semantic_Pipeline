"""
Ground truth loader for the variance comparison.

Loads data/seed_v2/ CSVs and computes the expected answer for every question.
Ground truth is always derived from the canonical DW seed data — never from
Snowflake — so the comparison is independent of the systems being evaluated.
"""

import os
from pathlib import Path

import pandas as pd

from variance.questions import QUESTIONS, Question

# Canonical data directory relative to the project root
_DATA_DIR = Path(__file__).parent.parent / "data" / "seed_v2"


def load_dataframes(data_dir: Path = _DATA_DIR) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Load the three canonical DW seed CSVs."""
    pos = pd.read_csv(data_dir / "dw_position.csv")
    sec = pd.read_csv(data_dir / "dw_security.csv")
    acc = pd.read_csv(data_dir / "dw_account.csv")
    return pos, sec, acc


def compute_all(questions: list[Question] = QUESTIONS, data_dir: Path = _DATA_DIR) -> dict[str, float]:
    """Return a dict mapping question ID → expected ground truth value."""
    pos, sec, acc = load_dataframes(data_dir)
    return {q.id: q.ground_truth_fn(pos, sec, acc) for q in questions}


def print_ground_truth(truths: dict | None = None, data_dir: Path = _DATA_DIR) -> None:
    """Print all ground truth values to stdout (useful for validation)."""
    if truths is None:
        truths = compute_all(data_dir=data_dir)
    for q in QUESTIONS:
        val = truths[q.id]
        codes = ", ".join(q.ambiguity_codes)
        if q.result_type == "percentage":
            formatted = f"{val:.4f}%"
        elif q.result_type == "row_count":
            formatted = f"{int(val):,} rows"
        else:
            formatted = f"{val:,.2f}"
        print(f"  {q.id} ({codes:12s})  {formatted:>25}  {q.ground_truth_description}")


if __name__ == "__main__":
    print("Ground truth values (computed from data/seed_v2/ CSVs):\n")
    print_ground_truth()
