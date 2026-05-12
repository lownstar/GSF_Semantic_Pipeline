# Roadmap

Items are classified as **Todo** (design complete or well-scoped, ready to start), **Shelved** (built or designed but not deployed), or **Idea** (worth exploring, no committed design yet).

---

## Shelved

### Epic 7: Interactive "Try It Yourself" Chat Tab

**Status:** Shelved (2026-05-12) — branch `feat/epic7-chat-tab` preserved, not merged to main  
**Reason:** Snowflake Cortex query costs are too high to sustain on a publicly accessible portfolio
site. The implementation is complete; if costs drop or the hosting model changes, the branch is
ready to merge. All code is in `app/cortex_chat.py` and `app/streamlit_app.py` on that branch.

**Motivation:** Portfolio visitors currently take the variance results on faith. A live multi-turn
chat interface lets them ask their own natural language questions directly to Cortex Analyst,
across all four tiers, see the generated SQL, and ask follow-ups. Proves the demo is real, not staged.

**Deliverables (complete on branch):**

| File | Change |
|---|---|
| `app/cortex_chat.py` | New — credential loading, Snowflake connection, multi-turn Cortex API call, SQL execution |
| `app/streamlit_app.py` | Add second tab "Try It Yourself": tier selector, suggested questions, `st.chat_message` history, `st.chat_input` |
| `requirements-app.txt` | Add `snowflake-connector-python>=3.0`, `cryptography>=41.0` |
| `docs/runbook.md` | New section: "Enabling the Live Chat Tab" with Streamlit Secrets `.toml` template |

**To revive:** merge `feat/epic7-chat-tab` to main and add Snowflake Secrets to Streamlit Community Cloud.

---

## Todos

### Epic 6: AI Results Analysis Tool

**Status:** Todo — scoped and deferred; designed as a separate project  
**Motivation:** The variance runner produces structured JSON for all 11 questions across all 4 tiers.
An analysis layer on top would surface patterns in how Cortex fails — which ambiguity types cause
more failures, how failure modes differ by tier, etc. — and make the governance argument quantitative.

**Plug-in point:** `variance/results/*.json` (format is stable; plug-in interface intentionally defined)

**Scope:** Separate project consuming this project's output. No changes needed here to enable it.

**Dependencies:**
- Epic 5 complete (variance runner + JSON output) — already done
- At least one live run of `variance/runner.py` to produce results — `demo_results.json` committed

---

## Ideas

### Semantic Silver Tier

**Status:** Idea  
**Concept:** Add a Silver-level Cortex Analyst YAML with better disambiguation — enough to resolve
some of the raw ambiguities (A1–A6) but not the integrated ones (A7–A11). This would create a
progressive improvement narrative across all five tiers (Bronze → Silver → Silver+ → Naive Gold →
Semantic Gold) rather than the current two-endpoint comparison.

**Open questions:**
- Does the narrative benefit justify the added complexity? The current two-endpoint framing (7/11 vs 11/11) is clean and punchy.
- Would require a new dbt Silver model variant or a YAML-only approach using the existing `POSITIONS_INTEGRATED` table.
- Scoring change is unknown — needs a test run to see if intermediate results are coherent.

**Dependencies:**
- No blockers. Would need a new `semantic_model/positions_silver_governed.yaml` and potentially a new `variance/` model entry.

---

### Live Demo Recording + Narrative Polish

**Status:** Idea  
**Concept:** A Loom walkthrough of the deployed Streamlit app — showing the scorecard, spotlighting
the key questions, and narrating the governance argument — would make the portfolio piece more
accessible to non-technical hiring managers who won't clone the repo.

**Scope:** No code changes. Pure content creation: screen recording, voiceover, and possibly a
polished GitHub README section linking the video.

**Dependencies:**
- Deployed Streamlit app is live now; recording can proceed at any time.
- Epic 7 (chat tab) is shelved — recording proceeds without it.
