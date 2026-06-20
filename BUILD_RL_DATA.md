# Building RL training files from this repo

`build_rl_data.py` turns the local `query_<dataset>/` packages in this folder
into VERL-shaped parquet + jsonl that an RL trainer can consume. No git clone,
no Yuchen skill check, no Qwen eval — and **no copies of DBs, queries, or
validators** are produced. The parquet rows reference the originals in this
folder via `--runtime-bench-root`.

## Quick start

```bash
cd /home/ubuntu/benchmark-reproduction/dab_new
python3 build_rl_data.py
```

This builds two buckets in one run:

- `all` — every package, including Postgres- and Mongo-backed ones. With the
  current packages this is **68 tasks** across 9 datasets.
- `no_postgres` — drops packages whose `db_config.yaml` references a Postgres
  DB (Mongo-only packages are still in). Useful when your sandbox doesn't run
  Postgres. Currently **26 tasks**.

Output lands at `runs/<timestamp>/<bucket>/build_verl/{train,test}.{parquet,jsonl}`.

### Postgres / Mongo

Postgres + Mongo tasks are already in the `all` bucket — no extra flag needed.
Ingest validates that each package's `db_config.yaml` resolves to a real
artifact:

| `db_type` | required artifact (relative to package) |
|---|---|
| `sqlite`, `duckdb` | `db_path` file exists |
| `postgres` | `sql_file` (SQL dump) exists |
| `mongo` | `dump_folder` exists and contains at least one `*.bson` file |

The engine only ever *filters out* a DB type (via `--skip-db-type`); it never
filters one in. So there is no "Mongo-only" bucket — just use `all`, or run
with `--buckets all` explicitly.

**Runtime requirement.** The parquet rows only *record* the DB layout. At RL
training time, your sandbox has to actually load the dumps before running the
task: a running Postgres for `psql -f <sql_file>`, a running MongoDB for
`mongorestore --dir <dump_folder>`, and the package's `query_dataset/` reachable
at the path embedded in `extra_info.dataset_dir`. If your sandbox only handles
SQLite + DuckDB, the `no_postgres` bucket is a safe stepping stone.

## Common variations

Only the lightweight bucket, with a named run dir:

```bash
python3 build_rl_data.py --buckets no_postgres --run-name first_train_set
```

Point parquet rows at a different runtime location (e.g. inside docker where
this folder is mounted at `/workspace/DataAgentBench`):

```bash
python3 build_rl_data.py \
  --runtime-bench-root /workspace/DataAgentBench \
  --sandbox-url http://sandbox:8080
```

Run against a sibling tree of `query_*` packages instead of this folder:

```bash
python3 build_rl_data.py --input-root /path/to/other/packages
```

## What you get

```
runs/<run>/<bucket>/
├── external_dab_candidates.jsonl    # one candidate row per task (audit)
├── sandbox_task_manifest.json       # ingest manifest (audit)
├── ingest_report.json               # accepted/skipped per dataset
└── build_verl/
    ├── train.parquet   train.jsonl  # ← RL training files
    ├── test.parquet    test.jsonl
    └── summary.json
```

A row in `train.parquet` carries the VERL fields
`prompt / reward_model / extra_info / data_source / ability` plus
`extra_info.{dataset, query_id, bench_root, dataset_dir, query_dir, db_config_path, sandbox_url, ...}`.
The path fields are rooted at `--runtime-bench-root` (default = this folder),
so the parquet is self-resolving as long as the packages are still on disk
where the runtime expects them.

## Flags worth knowing

| Flag | Default | Effect |
|---|---|---|
| `--buckets` | `all,no_postgres` | Comma list. Known: `all`, `no_postgres`. |
| `--input-root` | this folder | Where to find `query_<dataset>/` packages. |
| `--output-root` | `./runs` | Parent dir for per-run output. |
| `--run-name` | `build_rl_<timestamp>` | Override the run dir name. |
| `--runtime-bench-root` | this folder | Embedded into parquet rows for the RL runner. |
| `--sandbox-url` | `http://localhost:8080` | Embedded into parquet rows. |
| `--require-final-audit` | on | Drop tasks whose `validate.py` smoke test failed. |
| `--keep-intermediates` | off | Keep `dabench_tasks/` (the heavy package copy). |
| `--write-task-artifacts` | off | Also copy `query.json` / `validate.py` / `ground_truth.csv` into `build_verl/task_artifacts/`. |

## What it intentionally doesn't do

- **No git clone.** Input is local; cloning is the original
  `dab_expansion_pipeline/run_external_dab_pipeline.py`'s job.
- **No skill check.** The Yuchen `dab-benchmark-builder` author-side gate
  (`scripts/check_dab_package.py`) isn't invoked.
- **No synthetic generation or evolution.** Externally-authored packages
  already include the query, the DB split, and the validator — there's
  nothing to synthesize.
- **No Qwen difficulty filter.** That needs a running sandbox and the
  `agents.data_agent_bench_sandboxed.eval` evaluator from a different repo.

## Layout reference

```
dab_new/
├── query_<dataset>/                       # your DAB packages
├── rl_pipeline/
│   ├── synthesize_dabench_rl_data.py      # engine, copied from DataAgent/data_pipeline/
│   └── validator_templates.py
├── build_rl_data.py                       # the runner described here
├── BUILD_RL_DATA.md                       # this file
└── runs/                                  # output, created on first run
```

To regenerate the engine from upstream, copy these two files over:

```bash
cp /path/to/DataAgent/data_pipeline/synthesize_dabench_rl_data.py rl_pipeline/
cp /path/to/DataAgent/data_pipeline/validator_templates.py        rl_pipeline/
```
