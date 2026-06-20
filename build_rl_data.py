#!/usr/bin/env python3
"""Standalone DAB RL data builder for benchmark-reproduction/dab_new.

Ingests the local `query_<dataset>/` packages in this folder and produces
VERL-shaped parquet + jsonl + task_artifacts under `runs/<timestamp>/<bucket>/`.

No cloning, no skill check, no Qwen eval — just the two stages that turn
already-authored DAB packages into RL training inputs:

    ingest-dab-package  -> external_dab_candidates.jsonl
                           sandbox_task_manifest.json
                           ingest_report.json
    build-verl          -> build_verl/{train,test}.{parquet,jsonl}
                           build_verl/summary.json

By default no DBs, queries, or validators are copied — the parquet rows
reference the originals under `--runtime-bench-root` (defaults to this
folder). The ingest stage's `dabench_tasks/` copy is removed after build,
and `task_artifacts/` is never written. Pass `--keep-intermediates` to
retain everything.

The engine lives in `rl_pipeline/synthesize_dabench_rl_data.py` (copied
verbatim from DataAgent/data_pipeline/).
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
ENGINE = HERE / "rl_pipeline" / "synthesize_dabench_rl_data.py"

DEFAULT_BUCKETS = {
    "all": [],
    "no_postgres": ["postgres"],
}


def run_cmd(cmd: list[str], *, log_path: Path | None = None) -> None:
    print("$ " + " ".join(cmd), flush=True)
    if log_path:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with log_path.open("w", encoding="utf-8") as handle:
            handle.write("$ " + " ".join(cmd) + "\n")
            handle.flush()
            subprocess.run(cmd, check=True, stdout=handle, stderr=subprocess.STDOUT)
    else:
        subprocess.run(cmd, check=True)


def ingest(args: argparse.Namespace, *, input_root: Path, bucket_dir: Path, skip_db_types: list[str]) -> None:
    cmd = [
        args.python,
        str(ENGINE),
        "ingest-dab-package",
        "--input-root", str(input_root),
        "--output-dir", str(bucket_dir),
        "--source-repo", args.source_label,
    ]
    if args.overwrite:
        cmd.append("--overwrite")
    if not args.self_test:
        cmd.append("--no-self-test")
    if not args.require_validate_pass:
        cmd.append("--no-require-validate-pass")
    if args.leakage_check:
        cmd.append("--leakage-check")
    if not args.require_nonempty_answer:
        cmd.append("--allow-empty-answer")
    for db_type in skip_db_types:
        cmd.extend(["--skip-db-type", db_type])
    run_cmd(cmd, log_path=bucket_dir.parent / f"ingest_{bucket_dir.name}.log")


def build_verl(args: argparse.Namespace, *, bucket_dir: Path, bucket_name: str) -> Path:
    build_dir = bucket_dir / "build_verl"
    cmd = [
        args.python,
        str(ENGINE),
        "build-verl",
        "--candidate-jsonl", str(bucket_dir / "external_dab_candidates.jsonl"),
        "--task-manifest-json", str(bucket_dir / "sandbox_task_manifest.json"),
        "--output-dir", str(build_dir),
        "--output-format", "dab_sandbox",
        "--data-source", f"{args.data_source_prefix}_{bucket_name}",
        "--runtime-bench-root", args.runtime_bench_root,
        "--sandbox-url", args.sandbox_url,
        "--iterations", str(args.iterations),
        "--query-timeout", str(args.query_timeout),
        "--query-row-limit", str(args.query_row_limit),
        "--use-hints",
    ]
    if args.require_final_audit:
        cmd.append("--require-final-audit")
    if not args.write_task_artifacts:
        cmd.append("--no-task-artifacts")
    run_cmd(cmd, log_path=bucket_dir.parent / f"build_verl_{bucket_name}.log")
    return build_dir


def prune_intermediates(bucket_dir: Path) -> list[str]:
    removed: list[str] = []
    dabench_tasks = bucket_dir / "dabench_tasks"
    if dabench_tasks.exists():
        shutil.rmtree(dabench_tasks)
        removed.append(str(dabench_tasks))
    return removed


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--input-root", default=str(HERE),
                   help="Folder containing query_<dataset>/ packages (default: this script's folder).")
    p.add_argument("--output-root", default=str(HERE / "runs"),
                   help="Where per-run output directories are created (default: ./runs).")
    p.add_argument("--run-name", default="",
                   help="Optional run name; default is build_rl_<timestamp>.")
    p.add_argument("--python", default=sys.executable,
                   help="Python interpreter used to launch the engine subprocess (default: current).")
    p.add_argument("--buckets", default="all,no_postgres",
                   help=f"Comma-separated bucket names. Known: {','.join(sorted(DEFAULT_BUCKETS))}.")
    p.add_argument("--source-label", default="local:benchmark-reproduction/dab_new",
                   help="Provenance label embedded in candidate rows (replaces --source-repo).")
    p.add_argument("--data-source-prefix", default="dab_new",
                   help="Used as the VERL data_source field prefix; bucket name is appended.")

    # Runtime / sandbox paths embedded into the parquet rows.
    p.add_argument("--runtime-bench-root", default=str(HERE),
                   help="Path the RL sandbox uses to resolve packages at runtime. Default: this folder.")
    p.add_argument("--sandbox-url", default="http://localhost:8080",
                   help="Sandbox URL recorded in parquet rows (the RL runner reads it).")
    p.add_argument("--iterations", type=int, default=80)
    p.add_argument("--query-timeout", type=int, default=60)
    p.add_argument("--query-row-limit", type=int, default=5000)

    # Ingest hygiene knobs (defaults match dab_expansion_pipeline).
    p.add_argument("--overwrite", action="store_true", default=True)
    p.add_argument("--no-overwrite", dest="overwrite", action="store_false")
    p.add_argument("--self-test", action="store_true", default=True)
    p.add_argument("--no-self-test", dest="self_test", action="store_false")
    p.add_argument("--require-validate-pass", action="store_true", default=True)
    p.add_argument("--no-require-validate-pass", dest="require_validate_pass", action="store_false")
    p.add_argument("--leakage-check", action="store_true", default=False)
    p.add_argument("--require-nonempty-answer", action="store_true", default=True)
    p.add_argument("--allow-empty-answer", dest="require_nonempty_answer", action="store_false")
    p.add_argument("--require-final-audit", action="store_true", default=True,
                   help="Drop tasks whose final_audit did not pass (default: on).")
    p.add_argument("--no-require-final-audit", dest="require_final_audit", action="store_false")

    # Output minimality.
    p.add_argument("--write-task-artifacts", action="store_true", default=False,
                   help="Also copy query/validator/GT into build_verl/task_artifacts/ (default: off).")
    p.add_argument("--keep-intermediates", action="store_true", default=False,
                   help="Keep <bucket>/dabench_tasks/ (the copy ingest makes of each package, "
                        "including DBs). Off by default — we delete it after build-verl finishes.")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    if not ENGINE.exists():
        raise SystemExit(f"engine not found: {ENGINE}\n"
                         "Expected rl_pipeline/synthesize_dabench_rl_data.py next to this script.")

    input_root = Path(args.input_root).resolve()
    if not list(input_root.glob("query_*")):
        raise SystemExit(f"no query_<dataset>/ packages found under {input_root}")

    run_name = args.run_name or f"build_rl_{time.strftime('%Y%m%d_%H%M%S')}"
    run_dir = Path(args.output_root).resolve() / run_name
    run_dir.mkdir(parents=True, exist_ok=True)

    requested = [b.strip() for b in args.buckets.split(",") if b.strip()]
    for b in requested:
        if b not in DEFAULT_BUCKETS:
            raise SystemExit(f"unknown bucket {b!r}; expected one of {sorted(DEFAULT_BUCKETS)}")

    summaries = []
    for bucket_name in requested:
        bucket_dir = run_dir / bucket_name
        bucket_dir.mkdir(parents=True, exist_ok=True)
        ingest(args, input_root=input_root, bucket_dir=bucket_dir, skip_db_types=DEFAULT_BUCKETS[bucket_name])
        build_dir = build_verl(args, bucket_dir=bucket_dir, bucket_name=bucket_name)
        pruned = [] if args.keep_intermediates else prune_intermediates(bucket_dir)
        summaries.append({
            "bucket": bucket_name,
            "bucket_dir": str(bucket_dir),
            "candidate_jsonl": str(bucket_dir / "external_dab_candidates.jsonl"),
            "manifest_json": str(bucket_dir / "sandbox_task_manifest.json"),
            "ingest_report": str(bucket_dir / "ingest_report.json"),
            "build_dir": str(build_dir),
            "train_parquet": str(build_dir / "train.parquet"),
            "test_parquet": str(build_dir / "test.parquet"),
            "train_jsonl": str(build_dir / "train.jsonl"),
            "test_jsonl": str(build_dir / "test.jsonl"),
            "build_summary": str(build_dir / "summary.json"),
            "pruned_paths": pruned,
        })

    summary = {
        "run_name": run_name,
        "run_dir": str(run_dir),
        "input_root": str(input_root),
        "runtime_bench_root": args.runtime_bench_root,
        "sandbox_url": args.sandbox_url,
        "buckets": summaries,
    }
    (run_dir / "pipeline_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError as exc:
        print(f"command failed with exit code {exc.returncode}: {exc.cmd}", file=sys.stderr)
        sys.exit(exc.returncode)
