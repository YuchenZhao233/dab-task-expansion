---
name: dab-benchmark-builder
description: Build or extend DAB-style benchmark datasets from local source data such as CSV, JSON, SQLite, DuckDB, SQL dumps, or prepared benchmark datasets. Use when asked to create query dataset tasks, reproduce DAB benchmark construction, design natural-language data-agent queries, build clean canonical databases, split visible databases, write ground truth and validate.py files, or package local datasets for the DAB repo.
---

# DAB Benchmark Builder

Use this skill to create DAB-compatible training task folders from local data. The goal is a repeatable Codex workflow: scripts handle profiling/checking, while Codex still makes the judgment-heavy decisions about query design, split design, hints, and validation.

This skill is project-local. Do not install or register it globally unless the user explicitly asks.

## Core Rule

Always build in this order:

1. Profile source files.
2. Build a clean canonical DB.
3. Design candidate queries against the clean DB.
4. Compute ground truth from the clean DB.
5. Export visible split DBs across different DBMS types.
6. Write DAB task artifacts using the DAB description format.
7. Verify every task using only visible DBs.
8. Package only the DAB-facing files.

Do not start from final split DBs when raw/clean data is available. Do not compute ground truth from corrupted/split visible DBs unless the user explicitly accepts that limitation.

Hard requirements for generated final tasks:

- Each final query must require at least two logical databases from `db_config.yaml`.
- Each final query must also require key normalization/fuzzy entity matching, unstructured text extraction/classification, domain knowledge, a domain formula, or another documented semantic transformation.
- Visible database splits must use different DBMS types. If one split is SQLite, the next split must be DuckDB, PostgreSQL, or MongoDB. Do not create two logical visible databases with the same `db_type` unless the user explicitly asks for a legacy/simple variant and you label it as such.
- `db_description.txt` must describe databases, collections/tables, fields, and types only. Do not include a "Useful links" section or explicit join-key map.

## When Starting a Dataset

Ask for or infer the minimum missing inputs:

- Source path.
- Target dataset name, usually `query_<dataset>`.
- Desired query count, usually 3-5.
- DB types to use or avoid, for example DuckDB + SQLite, no Mongo.
- Training strictness and desired difficulty band.
- Any domain facts the user knows.
- Any columns/tables that must be hidden, transformed, or not shipped.
- Any reference queries to consider. 

Ask users if some of there input are not provided. 

Create a dataset workspace:

```text
dab_new/query_<dataset>/
  manual_querycode/
  clean/
  query_dataset/
```

Use templates from `assets/templates/` for starter docs.

## Required Final Artifact Contract

Read `references/dab_artifact_contract.md` before packaging or answering whether a dataset is ready for DAB.

Minimum DAB-facing files:

```text
query_<dataset>/
  db_config.yaml
  db_description.txt
  db_description_withhint.txt
  query_dataset/
  query1/
    query.json
    ground_truth.csv
    validate.py
```

Each final query directory must contain only normal DAB artifacts plus any intentional extra files. Keep `clean/`, `logs/`, and `__pycache__/` local-only.

## Workflow

## Token-Efficient Work Habits

Prefer token-efficient inspection patterns when they do not reduce correctness or traceability.

Useful defaults:

- Write profiling and intermediate outputs to files, then inspect the smallest relevant slices.
- Start with row counts, table names, join candidates, and targeted categorical distributions before reading large schemas or samples.
- Use compact probes for candidate query design, such as `COUNT`, `GROUP BY ... LIMIT`, and selected columns.
- Once the dataset shape is clear, prefer reusable scripts over many repeated ad hoc snippets.
- Keep manual notes focused on selected/rejected query rationale and required DAB properties.

These are guidance, not hard constraints. If a full schema, broad sample, verbose log, or extra diagnostic output is needed to avoid mistakes, use it.

For optional agent smoke tests, prefer a quiet run followed by a concise summary from the runner JSONL logs. Stream detailed tool progress only when debugging a stuck run, investigating a failure, or when the user asks for live progress.

### 1. Profile Sources

Run:

```bash
python .codex/skills/dab-benchmark-builder/scripts/profile_sources.py \
  /path/to/source \
  --out dab_new/query_<dataset>/manual_querycode/profile
```

Inspect `schema_profile.md` and `schema_profile.json`. Use this to identify entities, joins, dates, categorical fields, measures, text columns, and possible domain formulas.

### 2. Build Clean Canonical DB

Create `manual_querycode/build_clean.py`. Prefer `clean/clean.sqlite` unless a source needs DuckDB features.

Clean DB rules:

- Preserve raw enough data to compute answers.
- Normalize obvious types.
- Keep stable primary keys or source row IDs.
- Do not split, corrupt, hide, or denormalize solely for difficulty yet.

### 3. Design Candidate Queries

Read `references/query_style_guide.md` before writing query candidates.

Create:

```text
manual_querycode/candidate_queries.md
manual_querycode/selected_queries.md
```

Generate 8-12 candidates, then select 3-5. Favor tasks requiring at least two DAB-style properties:

- Cross-table or cross-DB join.
- Entity/key normalization.
- Aggregation/ranking.
- Domain formula or convention.
- Text/manual extraction.
- Counterfactual/filter transformation.
- Nontrivial date/time logic.

Reject candidates that can be solved from one visible database unless the user explicitly asks for a simple RL-only variant. Even then, the query must require a complex join or semantic transformation, and the variant status must be recorded in `selected_queries.md`.

### 4. Compute Ground Truth

Create `manual_querycode/compute_ground_truth.py` against `clean/clean.sqlite`.

It should write:

```text
queryN/ground_truth.csv
```

Keep the code deterministic and self-contained. Use SQL or Python, but avoid hidden manual constants except query targets.

### 5. Export Visible Split DBs

Read `references/split_patterns.md` before splitting.

Create `manual_querycode/export_visible_dbs.py`, then generate:

```text
query_dataset/
db_config.yaml
```

Good split design makes the query require realistic data work without making it ambiguous. Prefer DuckDB for larger fact tables and SQLite for metadata/context/rules. Avoid Mongo unless the user asks or the source naturally requires nested collections.

Use at least two DBMS types across visible splits. Preferred pairings:

- DuckDB facts + SQLite entity/context tables.
- MongoDB documents/free text + SQLite or PostgreSQL metadata.
- PostgreSQL relational tables + DuckDB analytical facts.

If a source naturally has three split components, use three DBMS types rather than three SQLite files.

### 6. Write Descriptions, Hints, and Validators

Read:

- `references/validation_patterns.md`
- `references/quality_checklist.md`
- `references/db_description_examples.md`

Create:

```text
db_description.txt
db_description_withhint.txt
queryN/query.json
queryN/validate.py
GOLD_ANSWERS.md
```

Hints may define semantic conventions, formulas, bucket parsing, noisy ID rules, and null/list behavior. Hints must not disclose final answers or point to a single winning row.

Descriptions must follow DAB's dataset-level style: numbered logical databases with DBMS type, purpose, tables/collections, fields, and brief field descriptions. Do not include physical paths, SQL snippets, or explicit join maps.

Important: `db_description_withhint.txt` is a hints-only companion file in DAB, not a second full database description. The DAB runner reads `db_description.txt` first and then appends the entire contents of `db_description_withhint.txt` when hints are enabled. Therefore `db_description_withhint.txt` must start with `HINTS:` and contain only hint bullets or short hint definitions. Do not repeat database descriptions, schema listings, physical paths, query-specific solve plans, or answer values in that file.

### 7. Verify Visible Solvability

Create `manual_querycode/verify_visible_solve.py`. It must solve each query using only:

```text
query_dataset/
db_config.yaml
db_description*.txt
```

It must not read `clean/clean.sqlite`.

Run:

```bash
python dab_new/query_<dataset>/manual_querycode/compute_ground_truth.py
python dab_new/query_<dataset>/manual_querycode/verify_visible_solve.py
python .codex/skills/dab-benchmark-builder/scripts/check_dab_package.py \
  dab_new/query_<dataset> --run-visible-verify
```

### 8. Optional Agent Smoke Test

Use `run_dab_new_agent.py` only after deterministic checks pass. Weak-agent results guide difficulty:

- 5/5: likely too easy or too explicit.
- 0/5: inspect for underspecification, brittle parsing, or over-hard composition.
- 1-3/5: often useful for RL training.

Do not auto run unless user specifically request this. 

## Packaging

To create a clean copy for the DAB repo:

```bash
python .codex/skills/dab-benchmark-builder/scripts/package_for_dab.py \
  dab_new/query_<dataset> \
  /path/to/dab/query_<dataset> \
  --include-manual
```

Without `--include-manual`, it copies only DAB-facing task artifacts.

## Reference Loading Guide

- Read `references/dab_artifact_contract.md` when checking readiness or packaging.
- Read `references/query_style_guide.md` when drafting or revising queries.
- Read `references/split_patterns.md` before writing `export_visible_dbs.py`.
- Read `references/db_description_examples.md` before writing or revising `db_description.txt` or `db_description_withhint.txt`.
- Read `references/validation_patterns.md` before writing `validate.py`.
- Read `references/prompt_templates.md` when the user wants manual prompt scaffolding for query brainstorming/review.
- Read `references/quality_checklist.md` before final response.
