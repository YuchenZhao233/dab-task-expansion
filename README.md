# DAB-Style Training Tasks

This repository collects DAB-style data-agent tasks generated from local datasets. It is intended as a shareable data/task repository for training and experimentation, not as a standalone benchmark runner.

The tasks follow the DAB artifact shape: each dataset directory contains database descriptions, visible database files, query folders, ground-truth files, and deterministic validators. The source cleanup scripts, exploratory logs, and intermediate canonical databases are local construction artifacts and are not part of the public task package by default.

## What Is Included

Each `query_*` directory is a dataset package. A typical package contains:

```text
query_<dataset>/
  db_config.yaml
  db_description.txt
  db_description_withhint.txt
  GOLD_ANSWERS.md
  query_dataset/
  query1/
    query.json
    ground_truth.csv
    validate.py
```

The `query_dataset/` directory contains the visible databases used by agents. Query folders contain the natural-language task, expected answer data, and a deterministic validation script.

## Dataset Query Counts

| Dataset | Query count |
| --- | ---: |
| `query_dabstep_payments` | 6 |
| `query_spider2_IPL` | 10 |
| `query_spider2_airlines` | 7 |
| `query_spider2_california_traffic` | 8 |
| `query_spider2_imdb_movies` | 8 |
| `query_spider2_music` | 5 |
| **Total** | **44** |

## What Is Not Included

This repository does not provide a standalone runner.

The local smoke-test runner used during development depends on the original DAB `common_scaffold` code and Docker execution environment. To run these tasks with the same scaffold, use them inside a DAB-compatible checkout or another runner that understands the same task layout.

Ignored local artifacts include:

```text
query_*/clean/
query_*/manual_querycode/
query_*/logs/
**/__pycache__/
```

These files are useful while constructing tasks, but they are not required by the packaged task format.

## Downloading Database Files

Database files may be stored with Git LFS. Before cloning or pulling the repository, make sure Git LFS is installed:

```bash
git lfs version
```

If that command is missing, install Git LFS first, then enable it:

```bash
git lfs install
```

After cloning the repository, download the LFS-backed database files with:

```bash
git lfs pull
```

If you cloned before installing Git LFS, run:

```bash
git lfs install
git lfs pull
```

You can check whether any LFS files are still pointers instead of downloaded database files with:

```bash
git lfs ls-files
```

## Task Generation Skill

A project-local Codex skill is provided to make it easier to generate more tasks in the same style:

```text
.codex/skills/dab-benchmark-builder/
```

The skill is not globally registered. It is meant to be used from this project when creating new DAB-style datasets from local CSV, JSON, SQLite, DuckDB, SQL dump, or prepared benchmark data.

At a high level, the skill workflow is:

1. Profile the source data.
2. Build a clean canonical database.
3. Draft candidate natural-language queries.
4. Compute ground truth from the clean data.
5. Split the visible data across multiple database systems.
6. Write DAB-style descriptions, hints, queries, ground truth, and validators.
7. Verify that each task is solvable using only the visible databases.

The generated tasks are intended for training use. They are not presented as a stronger or replacement public benchmark.
