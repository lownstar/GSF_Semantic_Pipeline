# Refactoring Changelog

Documents the transition from PoC (Epics 1-5) to portfolio-grade pipeline demo.
The baseline commit (`3e6e175`) captures the full PoC state before any changes.

---

## Step 1: V1 Cleanup + Pipeline Rename (2026-04-04)

### Deleted (V1 deprecated artifacts)

| Path | What it was | Why deleted |
|---|---|---|
| `generator/` | V1 data generator (replaced by `generator_v2/`) | Dead code; V2 is the only active generator |
| `data/seed/` | V1 seed CSVs (~58 MB, 9 files) | Replaced by `data/seed_v2/`; V1 schema was different |
| `data/schema/schema_definition.md` | V1 schema documentation | Superseded by `docs/ambiguity_registry_v2.md` |
| `docs/ambiguity_registry.md` | V1 ambiguity registry | Replaced by `docs/ambiguity_registry_v2.md` (A1-A11) |

All deleted files are preserved in git history (baseline commit).

### Renamed

| From | To | Why |
|---|---|---|
| `pipeline_a/` | `pipeline_naive/` | Aligns with demo narrative: "Naive Pipeline" (no semantic governance) |
| `pipeline_b/` | `pipeline_semantic/` | Aligns with demo narrative: "Semantic Enriched Pipeline" |

### Updated references

All "Pipeline A" references changed to "Naive Pipeline" across:
- `pipeline_naive/load_bronze.py` (docstring, print statements, stage comment)
- `pipeline_naive/validate_silver.py` (docstring, print statement)
- `generator_v2/config.py` (comments)
- `generator_v2/generator.py` (comments)
- `generator_v2/models/sources.py` (comments, docstrings)
- `docs/ambiguity_registry_v2.md` (references)
- `semantic_model/positions.yaml` (descriptions)

All "Pipeline B" references changed to "Semantic Enriched Pipeline" across:
- `pipeline_semantic/load_gold.py` (docstring, print statements)
- `pipeline_semantic/validate_gold.py` (docstring, print statement)
- `docs/ambiguity_registry_v2.md` (references)

`README.md` rewritten with new architecture diagram, updated project structure,
and corrected status (Epic 5 marked complete).
