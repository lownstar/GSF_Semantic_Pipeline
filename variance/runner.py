"""
Variance runner — asks all 11 questions against both the Silver and Gold
Cortex Analyst semantic models, scores the results against pre-computed ground
truth, and saves a timestamped JSON to variance/results/.

Usage:
  python variance/runner.py              # runs all 11 questions x 2 models
  python variance/runner.py --dry-run    # print questions and ground truth, no API calls
  python variance/runner.py --model gold # run Gold model only

The saved JSON is the input for the Streamlit visualization:
  streamlit run app/streamlit_app.py
"""

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

# Ensure project root is on sys.path so cortex and variance packages are importable
# when this script is executed as `python variance/runner.py` from the project root.
_PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))

from cortex.query_cortex import _get_connection, ask_cortex, execute_sql, _ensure_silver_staged
from variance.comparator import score_run
from variance.ground_truth import compute_all, print_ground_truth
from variance.questions import QUESTIONS

_RESULTS_DIR = _PROJECT_ROOT / "variance" / "results"


def _run_question(question_text: str, model_key: str, conn) -> dict:
    """Ask one question against one model. Returns {sql, rows, error}."""
    try:
        response = ask_cortex(question_text, model_key, conn)
        sql = None
        message_content = response.get("message", {}).get("content", [])
        for block in message_content:
            if block.get("type") == "sql":
                sql = block.get("statement", "")
                break

        if not sql:
            return {"sql": None, "rows": [], "error": "Cortex returned no SQL"}

        rows = execute_sql(sql, conn)
        return {"sql": sql, "rows": rows, "error": None}

    except Exception as exc:  # noqa: BLE001
        return {"sql": None, "rows": [], "error": str(exc)}


def run(models: list[str] = ("gold", "silver"), dry_run: bool = False) -> Path:
    """Run all questions against the specified models and save results JSON."""

    ground_truths = compute_all()
    print("\nGround truth (computed from data/seed_v2/):\n")
    print_ground_truth(ground_truths)

    if dry_run:
        print("\n[dry-run] Skipping Cortex API calls.")
        return None

    conn = _get_connection()

    if "silver" in models:
        print("\nEnsuring Silver YAML is staged...")
        _ensure_silver_staged(conn)

    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    timestamp = datetime.now(timezone.utc).isoformat()

    # Collect raw results: {model: {question_id: {sql, rows, error}}}
    model_raw: dict[str, dict] = {}

    for model in models:
        print(f"\n{'-'*60}")
        print(f"  Model: {model.upper()}")
        print(f"{'-'*60}")
        model_raw[model] = {}
        for q in QUESTIONS:
            codes = ", ".join(q.ambiguity_codes)
            print(f"  [{q.id}] ({codes}) {q.natural_language[:60]}...")
            result = _run_question(q.natural_language, model, conn)
            status_hint = "OK" if result["sql"] else "no SQL"
            row_hint = f"{len(result['rows'])} rows" if result["rows"] is not None else "-"
            err_hint = f"  ERROR: {result['error']}" if result["error"] else ""
            print(f"         -> {status_hint}, {row_hint}{err_hint}")
            model_raw[model][q.id] = result

    conn.close()

    # Score results
    model_scored: dict[str, dict] = {}
    for model in models:
        model_scored[model] = score_run(QUESTIONS, ground_truths, model_raw[model])

    # Build output payload
    questions_output = []
    for q in QUESTIONS:
        entry = {
            "id": q.id,
            "question": q.natural_language,
            "ambiguity_codes": q.ambiguity_codes,
            "failure_mode_silver": q.failure_mode_silver,
            "ground_truth_description": q.ground_truth_description,
            "result_type": q.result_type,
            "ground_truth": ground_truths[q.id],
        }
        for model in ("gold", "silver"):
            if model in models:
                raw = model_raw[model][q.id]
                scored = model_scored[model][q.id]
                entry[model] = {
                    "sql": raw["sql"],
                    "rows": raw["rows"],
                    "error": raw["error"],
                    "status": scored["status"],
                    "variance": scored["variance"],
                    "value": scored["value"],
                }
            else:
                entry[model] = None
        questions_output.append(entry)

    # Summary
    summary: dict = {}
    for model in ("gold", "silver"):
        if model in models:
            statuses = [model_scored[model][q.id]["status"] for q in QUESTIONS]
            correct = statuses.count("CORRECT")
            summary[f"{model}_correct"] = correct
            summary[f"{model}_accuracy_pct"] = round(correct / len(QUESTIONS) * 100.0, 1)
        else:
            summary[f"{model}_correct"] = None
            summary[f"{model}_accuracy_pct"] = None

    summary["total_questions"] = len(QUESTIONS)
    summary["questions_diverge"] = sum(
        1 for q in QUESTIONS
        if model_scored.get("gold", {}).get(q.id, {}).get("status") != "CORRECT"
        or model_scored.get("silver", {}).get(q.id, {}).get("status") != "CORRECT"
    ) if len(models) == 2 else None

    output = {
        "run_id": run_id,
        "run_timestamp": timestamp,
        "models_run": list(models),
        "questions": questions_output,
        "summary": summary,
    }

    _RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    out_path = _RESULTS_DIR / f"{run_id}.json"
    out_path.write_text(json.dumps(output, indent=2, default=str))

    print(f"\n{'='*60}")
    print(f"  Results saved: {out_path}")
    for model in ("gold", "silver"):
        if model in models:
            print(f"  {model.capitalize()}: {summary[f'{model}_correct']}/{len(QUESTIONS)} correct "
                  f"({summary[f'{model}_accuracy_pct']}%)")
    print(f"{'='*60}\n")

    return out_path


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run all variance questions against Cortex Analyst (Gold and/or Silver)"
    )
    parser.add_argument(
        "--model",
        choices=["gold", "silver", "both"],
        default="both",
        help="Which model(s) to run (default: both)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print ground truth values only; do not call Cortex Analyst",
    )
    args = parser.parse_args()

    if args.model == "both":
        models = ["gold", "silver"]
    else:
        models = [args.model]

    run(models=models, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
