#!/usr/bin/env python3
"""Build DABench-style synthetic RL data artifacts.

This script is intentionally conservative: it does not call an LLM by default.
It prepares prompt packets for Claude/GPT, performs local quality checks on
generated candidates, writes a global dashboard, and emits VERL-shaped data.
Sandbox execution/registration is kept as a separate verification stage because
synthetic databases and validators need environment-specific wiring.
"""

from __future__ import annotations

import argparse
import ast
import csv
import hashlib
import io
import json
import os
import re
import signal
import shutil
import sqlite3
import time
import urllib.error
import urllib.request
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable


DEFAULT_BENCH_ROOT = Path("/mnt/docker-data/workspace/DataAgentBench")
DEFAULT_OUTPUT_ROOT = Path("/mnt/docker-data/workspace/data_pipeline_runs")
VALIDATOR_TEMPLATE_NAMES = {
    "contains_all",
    "normalized_contains_all",
    "numeric_tolerance",
    "numeric_list_tolerance",
    "ordered_contains",
    "unordered_set_contains",
    "json_exact_fields",
    "name_value_proximity",
}
STRICT_ALLOWED_TASK_TYPES = {
    "sql_only",
    "aggregation_heavy",
    "id_normalization",
    "temporal_filter",
    "format_robustness",
}
STRICT_DISALLOWED_TASK_TYPES = {
    "hard",
    "mixed_sql_mongo",
    "json_heavy",
    "mongo_only",
    "multi_hop_join",
    "negative_trap",
}
STRICT_WEAK_VALIDATORS = {"contains_all"}
EVOLUTION_TYPES = [
    "aggregation_first",
    "multi_table_join",
    "id_normalization",
    "temporal_filter",
    "json_or_mongo_precision",
    "negative_trap",
    "format_robustness",
]
DIFFICULTY_RANK = {"easy": 0, "medium": 1, "hard": 2}
FINAL_AUDIT_VERSION = "2026-06-15.1"
MATERIALIZATION_MARKERS = (
    "__materialize_from_candidate_solution__",
    "<computed",
    "database-computed",
    "computed deterministically from the database",
    "computed from database",
    "benchmark harness should replace",
    "placeholder",
)
AuditCheck = Callable[[dict[str, Any], list[dict[str, Any]], argparse.Namespace], tuple[bool, str]]
AUDIT_EXTENSION_CHECKS: list[tuple[str, AuditCheck]] = []
DEGENERATE_REVIEW_RISK_PATTERNS = (
    "copy_count is only 1",
    "count is only 1",
    "singleton",
    "only one track_id",
    "single normalized",
    "normalized matching returns only one",
    "final sum equals the representative",
    "coincidentally get the correct answer",
    "shortcut",
    "truncated",
    "unordered limit",
    "bounded/truncated",
)
COUNT_LIKE_COLUMN_RE = re.compile(r"(^|_)(count|cnt|copies|copy_count|group_size|match_count|candidate_count|n)$", re.IGNORECASE)
GENERIC_GROUP_KEYS = {
    "n.a.",
    "n/a",
    "na",
    "n.a",
    "unknown",
    "unk.",
    "unk",
    "none",
    "null",
    "[untitled]",
    "[silence]",
    "yes",
    "no",
    "y",
    "n",
    "p",
    "q",
}
GENERIC_GROUP_KEY_SUBSTRINGS = (
    "not applicable",
    "unknown",
    "untitled",
    "silence",
)


def load_pipeline_env() -> None:
    """Load data_pipeline/.env without overriding already-exported variables."""
    env_path = Path(__file__).resolve().parent / ".env"
    if not env_path.exists():
        return
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export "):].strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


@dataclass
class SeedTask:
    dataset: str
    query_id: int
    query: str
    dataset_dir: Path
    query_dir: Path
    db_description: str
    db_description_with_hint: str
    db_config_text: str
    ground_truth_text: str
    validate_text: str

    def to_record(self) -> dict[str, Any]:
        dataset_hints = extract_dataset_hints(self.db_description_with_hint)
        ground_truth_summary = summarize_ground_truth_csv(self.ground_truth_text)
        validate_summary = summarize_validate_py(self.validate_text)
        return {
            "dataset": self.dataset,
            "query_id": self.query_id,
            "query": self.query,
            "dataset_dir": str(self.dataset_dir),
            "query_dir": str(self.query_dir),
            "db_description": self.db_description,
            "db_description_with_hint": self.db_description_with_hint,
            "dataset_hints": dataset_hints,
            "hint_source": str((self.dataset_dir / "db_description_withhint.txt").resolve()),
            "db_config_text": self.db_config_text,
            "db_source_types": detect_db_types(self.db_config_text),
            "query_ops": detect_query_ops(self.query),
            "source_ground_truth_text": self.ground_truth_text,
            "source_ground_truth_summary": ground_truth_summary,
            "source_validate_text": self.validate_text,
            "source_validate_summary": validate_summary,
            "source_validation_style": validate_summary.get("style", "unknown"),
            "fingerprint": stable_hash([self.dataset, self.query_id, self.query]),
        }


def stable_hash(parts: Iterable[Any]) -> str:
    payload = json.dumps(list(parts), ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def read_text(path: Path, default: str = "") -> str:
    try:
        return path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return default


def read_query(query_path: Path) -> str:
    data = json.loads(query_path.read_text(encoding="utf-8"))
    if isinstance(data, str):
        return data
    if isinstance(data, dict) and isinstance(data.get("query"), str):
        return data["query"]
    raise ValueError(f"Unsupported query format: {query_path}")


def summarize_ground_truth_csv(text: str, max_rows: int = 8, max_values: int = 30) -> dict[str, Any]:
    raw = (text or "").lstrip("\ufeff").strip()
    if not raw:
        return {
            "available": False,
            "columns": [],
            "row_count": 0,
            "preview_rows": [],
            "flat_values_preview": [],
        }
    try:
        rows = [
            [cell.strip() for cell in row]
            for row in csv.reader(io.StringIO(raw))
            if any(str(cell).strip() for cell in row)
        ]
    except csv.Error:
        rows = [[line.strip()] for line in raw.splitlines() if line.strip()]
    if not rows:
        return {
            "available": False,
            "columns": [],
            "row_count": 0,
            "preview_rows": [],
            "flat_values_preview": [],
        }

    has_header = len(rows) > 1
    columns = rows[0] if has_header else []
    data_rows = rows[1:] if has_header else rows
    preview_rows: list[Any] = []
    for row in data_rows[:max_rows]:
        if columns and len(columns) == len(row):
            preview_rows.append({col: truncate(cell, 160) for col, cell in zip(columns, row)})
        else:
            preview_rows.append([truncate(cell, 160) for cell in row])
    flat_values = [cell for row in data_rows for cell in row if str(cell).strip()]
    return {
        "available": True,
        "columns": columns,
        "row_count": len(data_rows),
        "preview_rows": preview_rows,
        "flat_values_preview": flat_values[:max_values],
        "has_numeric_values": any(looks_numeric(value) for value in flat_values),
        "raw_preview": truncate(raw, 3000),
    }


def looks_numeric(value: Any) -> bool:
    try:
        float(str(value).strip().replace(",", ""))
        return True
    except ValueError:
        return False


def summarize_validate_py(text: str) -> dict[str, Any]:
    raw = text or ""
    lowered = raw.casefold()
    styles: list[str] = []
    if "round(" in lowered or "float(" in lowered or "numeric" in lowered or "tolerance" in lowered:
        styles.append("numeric_tolerance")
    if "re.findall" in lowered or "regex" in lowered:
        styles.append("regex_extraction")
    if " in llm_output" in lowered or "not in llm_output" in lowered:
        styles.append("substring_contains")
    if "all " in lowered or "all(" in lowered or "missing" in lowered:
        styles.append("contains_all")
    if "window" in lowered or "characters" in lowered or "after" in lowered or "before" in lowered:
        styles.append("name_value_proximity")
    if "json" in lowered:
        styles.append("json_or_structured")
    if "case-insensitive" in lowered or ".lower()" in lowered:
        styles.append("case_insensitive")
    return {
        "available": bool(raw.strip()),
        "style": "+".join(styles) if styles else "custom_or_unknown",
        "styles": styles,
        "uses_programmatic_validate_function": "def validate" in lowered,
        "raw_preview": truncate(raw.strip(), 3000),
    }


def extract_dataset_hints(db_description_with_hint: str) -> list[str]:
    """Parse existing per-dataset hints from db_description_withhint.txt.

    These hints are the only allowed hint source for generated candidates. The
    pipeline may select a task-relevant subset, but it must not ask a model to
    invent new hints or rewrite existing ones.
    """
    text = (db_description_with_hint or "").strip()
    if not text:
        return []
    lines = text.splitlines()
    start = 0
    for idx, line in enumerate(lines):
        if line.strip().casefold().startswith("hints:"):
            start = idx + 1
            break
    hints: list[str] = []
    current: list[str] = []
    for raw_line in lines[start:]:
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith(("-", "*")):
            if current:
                hints.append(" ".join(current).strip())
            current = [line.lstrip("-* ").strip()]
        elif current:
            current.append(line)
        elif not line.casefold().startswith("hints:"):
            current = [line]
    if current:
        hints.append(" ".join(current).strip())
    return [hint for hint in hints if hint]


def make_hint_id(dataset: str, index: int) -> str:
    return f"{dataset}:H{index:02d}"


def hint_records_for_dataset(dataset: str, hints: list[str]) -> list[dict[str, str]]:
    return [
        {"id": make_hint_id(dataset, idx), "text": hint}
        for idx, hint in enumerate(hints, 1)
    ]


def build_hint_catalog(seed_rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_dataset: dict[str, list[str]] = {}
    sources: dict[str, str] = {}
    for row in seed_rows:
        dataset = str(row.get("dataset", ""))
        if not dataset or dataset in by_dataset:
            continue
        hints = row.get("dataset_hints") or extract_dataset_hints(str(row.get("db_description_with_hint", "")))
        by_dataset[dataset] = list(hints)
        sources[dataset] = str(row.get("hint_source", ""))
    return {
        "version": 1,
        "policy": "reuse_existing_dataset_hints_only",
        "datasets": {
            dataset: {
                "source": sources.get(dataset, ""),
                "hints": hint_records_for_dataset(dataset, hints),
            }
            for dataset, hints in sorted(by_dataset.items())
        },
    }


def format_allowed_hints(seed: dict[str, Any]) -> list[dict[str, str]]:
    dataset = str(seed.get("dataset", ""))
    hints = seed.get("dataset_hints") or extract_dataset_hints(str(seed.get("db_description_with_hint", "")))
    return hint_records_for_dataset(dataset, list(hints))


def hint_policy_for_seed(seed: dict[str, Any]) -> dict[str, Any]:
    return {
        "source_dataset": seed.get("dataset"),
        "source_query_id": seed.get("query_id"),
        "rule": "Select only task-relevant hint IDs from allowed_hints. Do not write, paraphrase, or invent hint text.",
        "output_fields": ["hint_refs", "hint_selection_rationale"],
        "allow_empty_hint_refs_if_none_relevant": True,
    }


def redacted_ground_truth_shape(summary: dict[str, Any]) -> dict[str, Any]:
    """Expose answer shape without leaking concrete ground-truth values."""
    if not isinstance(summary, dict) or not summary.get("available"):
        return {
            "available": False,
            "columns": [],
            "row_count": 0,
            "answer_shape": "unknown",
            "value_type_counts": {},
        }
    preview_rows = summary.get("preview_rows") if isinstance(summary.get("preview_rows"), list) else []
    flat_values = summary.get("flat_values_preview") if isinstance(summary.get("flat_values_preview"), list) else []
    value_types: Counter[str] = Counter()
    for value in flat_values:
        if looks_numeric(value):
            value_types["numeric"] += 1
        elif isinstance(value, str) and value.strip():
            value_types["string"] += 1
        else:
            value_types[type(value).__name__] += 1
    row_count = int(summary.get("row_count") or 0)
    columns = summary.get("columns") if isinstance(summary.get("columns"), list) else []
    if row_count == 1 and len(flat_values) == 1:
        shape = "single_scalar"
    elif row_count == 1 and len(columns) > 1:
        shape = "single_row_multi_field"
    elif row_count > 1 and len(columns) <= 1:
        shape = "list_or_set"
    elif row_count > 1:
        shape = "table_or_name_value_pairs"
    else:
        shape = "unknown"
    return {
        "available": True,
        "columns": columns,
        "row_count": row_count,
        "answer_shape": shape,
        "value_count": len(flat_values),
        "value_type_counts": dict(sorted(value_types.items())),
        "has_numeric_values": bool(summary.get("has_numeric_values")),
        "preview_redacted": bool(preview_rows or flat_values),
    }


def redacted_validate_summary(summary: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(summary, dict):
        return {"available": False, "style": "unknown", "styles": []}
    return {
        "available": bool(summary.get("available")),
        "style": summary.get("style", "custom_or_unknown"),
        "styles": summary.get("styles", []),
        "uses_programmatic_validate_function": bool(summary.get("uses_programmatic_validate_function")),
        "raw_preview_redacted": bool(summary.get("raw_preview")),
    }


def strict_generation_policy() -> dict[str, Any]:
    return {
        "mode": "strict_seed_conditioned_evidence_first",
        "answer_policy": [
            "Do not use official ground-truth values as expected_answer or validator_args.",
            "Mine or specify executable evidence first, then write the user-facing query from that evidence.",
            "First write an executable candidate_solution that derives the answer from query_db rows.",
            "The user-facing query must be reverse-written from that executable solution.",
            "Generation has no live database access; expected_answer, validator_args, and evidence_card.observed_answer must use materialization markers.",
            "The local materialize-observed-ground-truth step will execute candidate_solution and fill concrete values.",
            "If the solution cannot call return_answer(answer), emit {\"skip\": true, \"reason\": \"...\"}.",
        ],
        "source_alignment_policy": [
            "Keep the generated task close to the source DABench task signature.",
            "Do not downgrade a join/normalization/ranking task into a single-table lookup or count.",
            "Prefer the same answer shape and validator family as the source task unless the signature explains why a stricter equivalent is used.",
            "Every accepted candidate should include source_task_signature, evidence_card, and signature_alignment.",
        ],
        "allowed_task_types": sorted(STRICT_ALLOWED_TASK_TYPES),
        "disallowed_task_types": sorted(STRICT_DISALLOWED_TASK_TYPES),
        "allowed_difficulty": ["easy", "medium"],
        "query_requirements": [
            "Mention concrete tables/collections or field-level filters from db_description.",
            "Require a real data operation: filter, group/aggregate, rank, temporal filter, or ID normalization.",
            "State an explicit output format such as JSON array, JSON object, number rounded to N decimals, or ordered list.",
            "Avoid external-knowledge words such as latest, current, real-world, internet, or web search.",
            "Avoid tasks whose expected answer is empty, zero by construction, schema-only, or a sentinel/nonexistent entity.",
        ],
        "solution_requirements": [
            "Use only logical db_name values from db_config_summary.",
            "Use aggregation-first SQL/Mongo; do not SELECT * without LIMIT.",
            "Top-k queries must use GROUP BY/ORDER BY/LIMIT or an equivalent aggregation pipeline.",
            "Python must call query_db(...) and end with return_answer(answer).",
            "Python may import only json, re, math, or statistics.",
        ],
        "validator_requirements": {
            "disallow": sorted(STRICT_WEAK_VALIDATORS),
            "numeric": "numeric_tolerance",
            "top_k_names": "ordered_contains",
            "sets": "unordered_set_contains",
            "objects": "json_exact_fields",
            "name_value_pairs": "name_value_proximity",
        },
        "query_transform_policy": {
            "allowed": ["none", "injection", "fuzzing", "obfuscation"],
            "injection": "Add one or two real database-derived constraints or lookup hops that lengthen the evidence path without changing grounding.",
            "fuzzing": "Replace direct literals with normalized forms, IDs, aliases, date windows, or title/name lookups that can be resolved from the database.",
            "obfuscation": "Remove answer-revealing wording while keeping enough database-grounded constraints for a unique executable answer.",
            "safety": [
                "No external knowledge, internet, hidden files, or subjective clues.",
                "The transformed query must still be executable by candidate_solution.",
                "Final answer must be materialized locally after transformation.",
            ],
        },
        "hardness_and_ambiguity_policy": {
            "target": "harder but uniquely replayable tasks",
            "preferred_sources_of_difficulty": [
                "multi-step exploration over real rows",
                "cross-table or cross-collection evidence",
                "ID/name/title normalization",
                "temporal filtering plus aggregation",
                "near-distractor candidates with explicit tie-breakers",
                "set/list answers with deterministic order",
            ],
            "required_for_ambiguous_wording": [
                "candidate_solution must define every broad phrase through concrete filters",
                "evidence_chain must name the disambiguating evidence",
                "validator_args must stay concrete after replay",
            ],
        },
    }


def infer_answer_shape_from_summary(summary: dict[str, Any], validate_summary: dict[str, Any] | None = None) -> str:
    validate_summary = validate_summary or {}
    shape = str(redacted_ground_truth_shape(summary).get("answer_shape") or "unknown")
    styles = set(validate_summary.get("style", []) or [])
    if "numeric_tolerance" in styles or summary.get("has_numeric_values"):
        if shape in {"single_scalar", "single_row_multi_field", "unknown"}:
            return "numeric_scalar" if shape != "single_row_multi_field" else "numeric_record"
    if shape == "single_row_multi_field":
        return "record"
    if shape == "table_or_name_value_pairs":
        return "name_value_pairs"
    return shape



def detect_query_wording_patterns(query: str) -> list[dict[str, str]]:
    """Extract reusable DABench wording styles without copying answers."""
    text = str(query or "").strip()
    lowered = text.casefold()
    patterns: list[dict[str, str]] = []

    def add(name: str, description: str, example: str = "") -> None:
        patterns.append({"name": name, "description": description, "example": truncate(example or text, 220)})

    if any(word in lowered for word in ("corresponding", "associated", "matching", "mapped", "same", "related")):
        add("implicit_join_or_lookup", "The wording implies a lookup/join relationship without spelling out the join path.")
    if any(word in lowered for word in ("valid", "non-empty", "available", "eligible", "recorded", "listed")):
        add("valid_subset_filter", "The query uses a broad validity/subset phrase that must be grounded by database filters.")
    if any(word in lowered for word in ("most", "least", "highest", "lowest", "largest", "smallest", "top", "rank")):
        add("ranking_without_formula", "The query asks for an extremum or ranking, leaving the solver to identify the exact aggregation path.")
    if any(word in lowered for word in ("latest", "earliest", "before", "after", "between", "during", "same month", "same year")):
        add("relative_temporal_window", "The query describes a temporal window that must be resolved from stored dates/timestamps.")
    if any(word in lowered for word in ("id", "identifier", "product", "book", "title", "name", "symbol", "code")):
        add("id_or_name_resolution", "The query may require resolving an internal identifier to a human-readable entity or vice versa.")
    if any(word in lowered for word in ("all", "which", "what are", "list", "return each")):
        add("set_or_list_answer", "The query expects a bounded set/list rather than a single scalar.")
    if any(word in lowered for word in ("rounded", "decimal", "percentage", "ratio", "average", "mean", "sum", "total")):
        add("format_or_numeric_precision", "The query implies numeric calculation and explicit formatting/rounding.")
    if not patterns:
        add("plain_database_lookup", "The wording is direct; add only light ambiguity through database-grounded field names or valid-subset phrasing.")
    return patterns[:6]


def ambiguity_strategy_from_contexts(contexts: list[dict[str, Any]], limit: int = 5) -> dict[str, Any]:
    examples: list[dict[str, Any]] = []
    counts: Counter[str] = Counter()
    for context in contexts:
        source_query = str(context.get("source_query") or context.get("source_task_signature", {}).get("source_query") or "")
        if not source_query:
            continue
        patterns = detect_query_wording_patterns(source_query)
        for pattern in patterns:
            counts[str(pattern.get("name"))] += 1
        if len(examples) < limit:
            examples.append(
                {
                    "source_query_id": context.get("source_query_id"),
                    "source_query_preview": truncate(source_query, 320),
                    "wording_patterns": patterns,
                }
            )
    return {
        "purpose": "Use DABench-style ambiguity only when it remains database-grounded and uniquely replayable.",
        "source_wording_examples": examples,
        "pattern_counts": dict(counts.most_common()),
        "allowed_transform_styles": {
            "injection": [
                "add a real date/status/category constraint discovered during exploration",
                "add a lookup hop through an ID/title/name field",
                "add a deterministic secondary tie-breaker",
            ],
            "fuzzing": [
                "replace a direct literal with a normalized alias, ID, month/year, title/name lookup, or punctuation/case variant",
                "ask for canonical display value after resolving an internal identifier",
            ],
            "obfuscation": [
                "describe the target as matching/associated/corresponding records instead of naming every table",
                "use 'valid non-empty values' or 'eligible records' only when candidate_solution defines the exact filters",
            ],
        },
        "forbidden_ambiguity": [
            "subjective criteria",
            "external knowledge",
            "current/latest real-world state",
            "undefined business terms not tied to query filters",
            "hidden answer leakage in the wording",
        ],
    }

def source_task_signature_from_seed(seed: dict[str, Any]) -> dict[str, Any]:
    query = str(seed.get("query", ""))
    ops = sorted(set(seed.get("query_ops") or detect_query_ops(query)))
    validate_summary = seed.get("source_validate_summary", {}) if isinstance(seed.get("source_validate_summary"), dict) else {}
    answer_shape = infer_answer_shape_from_summary(
        seed.get("source_ground_truth_summary", {}) if isinstance(seed.get("source_ground_truth_summary"), dict) else {},
        validate_summary,
    )
    operation_families = sorted(set(ops + infer_operation_families_from_text(query)))
    db_source_types = sorted(set(map(str, seed.get("db_source_types", []) or [])))
    return {
        "dataset": seed.get("dataset"),
        "query_id": seed.get("query_id"),
        "source_query": query,
        "operation_families": operation_families,
        "answer_shape": answer_shape,
        "validator_style": validate_summary.get("style", ["unknown"]),
        "db_source_types": db_source_types,
        "requires_join_or_lookup": "join_or_lookup" in operation_families or any(word in query.casefold() for word in ("join", "match", "map", "id", "identifier", "lookup")),
        "requires_aggregation": "aggregation" in operation_families,
        "requires_temporal_filter": "temporal" in operation_families,
        "complexity_score": source_complexity_score(operation_families, answer_shape, db_source_types, query),
        "difficulty_band": source_difficulty_band(operation_families, answer_shape, db_source_types, query),
        "wording_patterns": detect_query_wording_patterns(query),
        "ambiguity_seed": {
            "safe_to_reuse": True,
            "policy": "reuse wording style, not answer values",
        },
    }


def infer_operation_families_from_text(text: str) -> list[str]:
    lowered = str(text).casefold()
    families: set[str] = set()
    markers = {
        "aggregation": ("count", "average", "mean", "sum", "total", "highest", "largest", "lowest", "top", "rank", "percentage", "ratio"),
        "temporal": ("date", "year", "month", "before", "after", "between", "during", "latest", "earliest"),
        "join_or_lookup": ("join", "match", "map", "id", "identifier", "title", "name", "lookup", "correspond", "same"),
        "id_normalization": ("id", "identifier", "normalize", "normalization", "fuzzy", "purchase_id", "book_id", "product id"),
        "set_answer": ("which", "list", "all", "return each", "json array"),
        "numeric_answer": ("how many", "number", "ratio", "percentage", "score", "average", "mean"),
        "json_or_nested": ("json", "nested", "dictionary", "array", "mongo", "collection"),
    }
    for family, words in markers.items():
        if any(word in lowered for word in words):
            families.add(family)
    return sorted(families or {"lookup"})


def source_complexity_score(operation_families: list[str], answer_shape: str, db_source_types: list[str], query: str) -> int:
    score = 1
    families = set(operation_families)
    score += len(families & {"aggregation", "temporal", "join_or_lookup", "id_normalization", "json_or_nested"})
    if answer_shape in {"list_or_set", "name_value_pairs", "record"}:
        score += 1
    if len(db_source_types) > 1:
        score += 1
    if any(word in str(query).casefold() for word in ("top", "highest", "largest", "lowest", "rank", "ordered")):
        score += 1
    return score


def source_difficulty_band(operation_families: list[str], answer_shape: str, db_source_types: list[str], query: str) -> str:
    score = source_complexity_score(operation_families, answer_shape, db_source_types, query)
    if score <= 2:
        return "easy"
    if score <= 4:
        return "medium"
    return "hard"


def official_anchor_for_seed(seed: dict[str, Any], max_ground_truth_chars: int, max_validate_chars: int) -> dict[str, Any]:
    redacted_summary = redacted_ground_truth_shape(seed.get("source_ground_truth_summary", {}))
    validate_summary = redacted_validate_summary(seed.get("source_validate_summary", {}))
    return {
        "source_dataset": seed.get("dataset"),
        "source_query_id": seed.get("query_id"),
        "source_query": seed.get("query"),
        "source_ground_truth_summary": redacted_summary,
        "source_ground_truth_preview": (
            "[redacted: concrete ground-truth values are intentionally hidden; "
            "use only source_ground_truth_summary for answer shape.]"
        ),
        "source_validate_summary": validate_summary,
        "source_validate_preview": (
            "[redacted: raw validate.py is hidden because it may contain official answer literals; "
            "use only source_validate_summary for validator style.]"
        ),
        "alignment_rule": (
            "Use this official DABench query and validate.py only as the answer-shape and validation-style "
            "anchor. Concrete ground-truth values are hidden on purpose. A generated task may change the "
            "business question, but its expected answer and validator must be recomputed by the executable "
            "candidate_solution over real data and must not reuse hidden official answers."
        ),
    }


def discover_seed_tasks(bench_root: Path) -> list[SeedTask]:
    tasks: list[SeedTask] = []
    for dataset_dir in sorted(bench_root.glob("query_*")):
        if not dataset_dir.is_dir():
            continue
        dataset = dataset_dir.name.removeprefix("query_")
        db_description = read_text(dataset_dir / "db_description.txt")
        db_description_with_hint = read_text(dataset_dir / "db_description_withhint.txt")
        db_config_text = read_text(dataset_dir / "db_config.yaml")
        for query_dir in sorted(dataset_dir.glob("query*"), key=query_sort_key):
            suffix = query_dir.name.removeprefix("query")
            if not query_dir.is_dir() or not suffix.isdigit():
                continue
            query_path = query_dir / "query.json"
            if not query_path.exists():
                continue
            tasks.append(
                SeedTask(
                    dataset=dataset,
                    query_id=int(suffix),
                    query=read_query(query_path),
                    dataset_dir=dataset_dir,
                    query_dir=query_dir,
                    db_description=db_description,
                    db_description_with_hint=db_description_with_hint,
                    db_config_text=db_config_text,
                    ground_truth_text=read_text(query_dir / "ground_truth.csv"),
                    validate_text=read_text(query_dir / "validate.py"),
                )
            )
    return tasks


def query_sort_key(path: Path) -> tuple[int, str]:
    suffix = path.name.removeprefix("query")
    return (int(suffix) if suffix.isdigit() else 10**9, path.name)


def write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


def write_jsonl_preserve_order(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows = []
    with path.open(encoding="utf-8") as f:
        for line_no, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError as exc:
                raise ValueError(f"{path}:{line_no}: invalid JSONL: {exc}") from exc
    return rows


def inventory_from_seeds(seed_rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_dataset = Counter(row["dataset"] for row in seed_rows)
    by_db_type: Counter[str] = Counter()
    by_query_op: Counter[str] = Counter()
    by_validation_style: Counter[str] = Counter()
    for row in seed_rows:
        by_db_type.update(row.get("db_source_types") or ["unknown"])
        by_query_op.update(row.get("query_ops") or ["unknown"])
        by_validation_style.update([str(row.get("source_validation_style") or "unknown")])
    query_lengths = [len(str(row.get("query", ""))) for row in seed_rows]
    db_desc_lengths = [len(str(row.get("db_description", ""))) for row in seed_rows]
    return {
        "total_seed_tasks": len(seed_rows),
        "datasets": dict(sorted(by_dataset.items())),
        "db_source_types": dict(sorted(by_db_type.items())),
        "query_ops": dict(sorted(by_query_op.items())),
        "source_validation_styles": dict(sorted(by_validation_style.items())),
        "query_length": summarize_numbers(query_lengths),
        "db_description_length": summarize_numbers(db_desc_lengths),
        "task_types_to_expand": [
            "sql_only",
            "mongo_only",
            "mixed_sql_mongo",
            "json_heavy",
            "aggregation_heavy",
            "multi_hop_join",
        ],
    }


def detect_db_types(db_config_text: str) -> list[str]:
    lowered = db_config_text.lower()
    types = []
    for name in ("sqlite", "duckdb", "postgres", "mongo"):
        if name in lowered:
            types.append(name)
    return sorted(set(types)) or ["unknown"]


def detect_query_ops(query: str) -> list[str]:
    lowered = query.lower()
    checks = {
        "aggregation": ["count", "average", "mean", "sum", "total", "largest", "highest", "lowest", "most", "least"],
        "temporal": ["date", "year", "month", "before", "after", "between", "during"],
        "join_or_lookup": ["name", "title", "id", "identifier", "product", "book", "company"],
        "set_answer": ["which", "list", "all", "what are"],
        "numeric_answer": ["how many", "number", "ratio", "percentage", "score"],
    }
    return sorted(name for name, markers in checks.items() if any(marker in lowered for marker in markers)) or ["lookup"]


def summarize_numbers(values: list[int]) -> dict[str, float]:
    if not values:
        return {"min": 0, "max": 0, "mean": 0}
    return {
        "min": min(values),
        "max": max(values),
        "mean": round(sum(values) / len(values), 2),
    }


def collect_seeds(args: argparse.Namespace) -> None:
    seeds = discover_seed_tasks(Path(args.bench_root))
    rows = [seed.to_record() for seed in seeds]
    output_dir = Path(args.output_dir)
    write_jsonl(output_dir / "seed_tasks.jsonl", rows)
    inventory = inventory_from_seeds(rows)
    hint_catalog = build_hint_catalog(rows)
    (output_dir / "global_inventory.json").write_text(
        json.dumps(inventory, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (output_dir / "hint_catalog.json").write_text(
        json.dumps(hint_catalog, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(
        json.dumps(
            {
                "seed_tasks": len(rows),
                "hint_datasets": sum(1 for item in hint_catalog["datasets"].values() if item.get("hints")),
                "output_dir": str(output_dir),
            },
            ensure_ascii=False,
            indent=2,
        )
    )


def make_generation_packets(args: argparse.Namespace) -> None:
    seed_rows = read_jsonl(Path(args.seed_jsonl))
    if args.max_packets:
        seed_rows = seed_rows[: args.max_packets]
    inventory = inventory_from_seeds(seed_rows)
    generator_prompt = read_text(Path(args.generator_prompt))
    packets = []
    for idx, seed in enumerate(seed_rows):
        packet = {
            "packet_id": f"gen_{idx:05d}_{seed['dataset']}_q{seed['query_id']}",
            "system_prompt": generator_prompt,
            "input": {
                "global_inventory": inventory,
                "strict_generation_policy": strict_generation_policy(),
                "seed": {
                    "dataset": seed["dataset"],
                    "query_id": seed["query_id"],
                    "query": seed["query"],
                    "query_ops": seed.get("query_ops", []),
                    "db_source_types": seed.get("db_source_types", []),
                    "db_description": truncate(seed.get("db_description", ""), args.max_db_description_chars),
                    "db_config_summary": truncate(seed.get("db_config_text", ""), args.max_db_config_chars),
                    "official_dabench_anchor": official_anchor_for_seed(
                        seed,
                        args.max_ground_truth_chars,
                        args.max_validate_chars,
                    ),
                    "allowed_hints": format_allowed_hints(seed),
                    "hint_policy": hint_policy_for_seed(seed),
                },
            },
            "expected_output": (
                "One strict JSON object matching schemas/synthetic_task.schema.json, or "
                "{\"skip\": true, \"reason\": \"...\"}. Use hint_refs only from allowed_hints. "
                "Do not reuse official ground-truth values; use materialization markers for expected_answer/validator_args "
                "and provide candidate_solution that computes the answer via query_db(...). "
                "Prefer easy/medium sql_only or aggregation_heavy tasks with explicit output format and strict validator."
            ),
        }
        packets.append(packet)
    write_jsonl(Path(args.output_jsonl), packets)
    print(json.dumps({"packets": len(packets), "output_jsonl": args.output_jsonl}, ensure_ascii=False, indent=2))


def make_evolution_packets(args: argparse.Namespace) -> None:
    seed_rows = read_jsonl(Path(args.seed_jsonl))
    evolution_prompt = read_text(Path(args.evolver_prompt))
    inventory = inventory_from_seeds(seed_rows)
    evolution_types = [x.strip() for x in args.evolution_types.split(",") if x.strip()] or EVOLUTION_TYPES
    packets = []
    for seed in seed_rows:
        for evolution_type in evolution_types:
            packet = {
                "packet_id": f"evolve_{len(packets):05d}_{seed['dataset']}_q{seed['query_id']}_{evolution_type}",
                "system_prompt": evolution_prompt,
                "input": {
                    "global_inventory": inventory,
                    "strict_generation_policy": strict_generation_policy(),
                    "evolution_type": evolution_type,
                    "seed": {
                        "dataset": seed["dataset"],
                        "query_id": seed["query_id"],
                        "query": seed["query"],
                        "query_ops": seed.get("query_ops", []),
                        "db_source_types": seed.get("db_source_types", []),
                        "db_description": truncate(seed.get("db_description", ""), args.max_db_description_chars),
                        "db_config_summary": truncate(seed.get("db_config_text", ""), args.max_db_config_chars),
                        "official_dabench_anchor": official_anchor_for_seed(
                            seed,
                            args.max_ground_truth_chars,
                            args.max_validate_chars,
                        ),
                        "allowed_hints": format_allowed_hints(seed),
                        "hint_policy": hint_policy_for_seed(seed),
                    },
                },
                "expected_output": (
                    "One strict evolved JSON task matching schemas/synthetic_task.schema.json, or "
                    "{\"skip\": true, \"reason\": \"...\"}. Do not reuse official ground-truth values. "
                    "Use materialization markers for expected_answer/validator_args when no executed observations are present. "
                    "Keep the task easy/medium unless explicitly overridden, and use a strict non-contains_all validator."
                ),
            }
            packets.append(packet)
            if args.max_packets and len(packets) >= args.max_packets:
                write_jsonl(Path(args.output_jsonl), packets)
                print(json.dumps({"packets": len(packets), "output_jsonl": args.output_jsonl}, ensure_ascii=False, indent=2))
                return
    write_jsonl(Path(args.output_jsonl), packets)
    print(json.dumps({"packets": len(packets), "output_jsonl": args.output_jsonl}, ensure_ascii=False, indent=2))


def make_solver_packets(args: argparse.Namespace) -> None:
    candidates = read_jsonl(Path(args.candidate_jsonl))
    solver_prompt = read_text(Path(args.solver_prompt))
    packets = []
    for idx, candidate in enumerate(candidates):
        pipeline_id = candidate.get("pipeline_id") or f"candidate_{idx:05d}_{stable_hash([candidate.get('dataset'), candidate.get('query')])}"
        packets.append(
            {
                "packet_id": f"solver_{pipeline_id}",
                "system_prompt": solver_prompt,
                "input": {
                    "candidate": candidate,
                    "requirements": {
                        "must_include_executable_solution": True,
                        "must_include_deterministic_validator_template": True,
                        "must_fill_concrete_validator_args_or_materialization_marker": True,
                        "must_avoid_answer_leakage": True,
                        "must_reuse_existing_hint_refs_only": True,
                        "must_not_generate_or_rewrite_hints": True,
                        "sandbox_manifest_not_required_now": True,
                    },
                },
                "expected_output": "One JSON object with candidate_solution, validator_template, validator_args, verification_plan, hint_refs, hint_selection_rationale, and negative_cases. Use materialization markers for validator_args if no executed observations are present. Do not output freeform validator code or new hint text.",
            }
        )
    write_jsonl(Path(args.output_jsonl), packets)
    print(json.dumps({"solver_packets": len(packets), "output_jsonl": args.output_jsonl}, ensure_ascii=False, indent=2))


def build_task_signatures(args: argparse.Namespace) -> None:
    seed_rows = read_jsonl(Path(args.seed_jsonl))
    if args.max_rows:
        seed_rows = seed_rows[: args.max_rows]
    rows: list[dict[str, Any]] = []
    for seed in seed_rows:
        signature = source_task_signature_from_seed(seed)
        rows.append(
            {
                "dataset": seed.get("dataset"),
                "query_id": seed.get("query_id"),
                "query": seed.get("query"),
                "task_signature": signature,
                "allowed_hints": format_allowed_hints(seed),
                "hint_policy": hint_policy_for_seed(seed),
                "db_source_types": seed.get("db_source_types", []),
                "query_ops": seed.get("query_ops", []),
                "source_validation_style": seed.get("source_validation_style", "unknown"),
                "source_answer_shape": signature.get("answer_shape", "unknown"),
                "official_dabench_anchor": official_anchor_for_seed(seed, args.max_ground_truth_chars, args.max_validate_chars),
                "db_description": truncate(seed.get("db_description", ""), args.max_db_description_chars),
                "db_config_summary": truncate(seed.get("db_config_text", ""), args.max_db_config_chars),
            }
        )
    write_jsonl(Path(args.output_jsonl), rows)
    if args.dashboard:
        write_signature_dashboard(Path(args.dashboard), rows)
    print(
        json.dumps(
            {
                "signatures": len(rows),
                "output_jsonl": args.output_jsonl,
                "dashboard": args.dashboard,
            },
            ensure_ascii=False,
            indent=2,
        )
    )


def write_signature_dashboard(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    by_dataset = Counter(str(row.get("dataset", "unknown")) for row in rows)
    by_shape = Counter(str(row.get("task_signature", {}).get("answer_shape", "unknown")) for row in rows)
    by_difficulty = Counter(str(row.get("task_signature", {}).get("difficulty_band", "unknown")) for row in rows)
    op_counts: Counter[str] = Counter()
    for row in rows:
        for op in row.get("task_signature", {}).get("operation_families", []) or []:
            op_counts[str(op)] += 1
    lines = [
        "# DABench Source Task Signatures",
        "",
        "## Summary",
        "",
        f"- total signatures: {len(rows)}",
        "",
        "## Difficulty Bands",
        "",
    ]
    for key, count in sorted(by_difficulty.items()):
        lines.append(f"- `{key}`: {count}")
    lines.extend(["", "## Answer Shapes", ""])
    for key, count in sorted(by_shape.items()):
        lines.append(f"- `{key}`: {count}")
    lines.extend(["", "## Operation Families", ""])
    for key, count in op_counts.most_common():
        lines.append(f"- `{key}`: {count}")
    lines.extend(["", "## Dataset Coverage", ""])
    for key, count in sorted(by_dataset.items()):
        lines.append(f"- `{key}`: {count}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def make_evidence_mining_packets(args: argparse.Namespace) -> None:
    signature_rows = read_jsonl(Path(args.signature_jsonl))
    if args.max_packets:
        signature_rows = signature_rows[: args.max_packets]
    prompt = read_text(Path(args.evidence_prompt))
    packets = []
    for idx, row in enumerate(signature_rows):
        signature = row.get("task_signature", {}) if isinstance(row.get("task_signature"), dict) else {}
        packet = {
            "packet_id": f"evidence_{idx:05d}_{row.get('dataset')}_q{row.get('query_id')}",
            "system_prompt": prompt,
            "input": {
                "strict_generation_policy": strict_generation_policy(),
                "source_task_signature": signature,
                "source": {
                    "dataset": row.get("dataset"),
                    "query_id": row.get("query_id"),
                    "query": row.get("query"),
                    "db_source_types": row.get("db_source_types", []),
                    "query_ops": row.get("query_ops", []),
                    "db_description": truncate(str(row.get("db_description", "")), args.max_db_description_chars),
                    "db_config_summary": truncate(str(row.get("db_config_summary", "")), args.max_db_config_chars),
                    "official_dabench_anchor": row.get("official_dabench_anchor", {}),
                    "allowed_hints": row.get("allowed_hints", []),
                    "hint_policy": row.get("hint_policy", {}),
                },
                "required_output_fields": [
                    "generation_strategy",
                    "provenance",
                    "source_task_signature",
                    "signature_alignment",
                    "evidence_card",
                    "query",
                    "candidate_solution",
                    "query_transform",
                    "expected_answer",
                    "validator_template",
                    "validator_args",
                    "data_requirements",
                    "evidence_chain",
                    "reward_spec",
                    "hint_refs",
                    "hint_selection_rationale",
                ],
            },
            "expected_output": (
                "One JSON candidate task. It must be seed-conditioned and evidence-first: include an evidence_card "
                "with focused executable probes, keep logic/difficulty close to source_task_signature, optionally add "
                "a safe injection/fuzzing/obfuscation query_transform, and make candidate_solution compute a non-empty "
                "answer with return_answer(answer). Use materialization markers for expected_answer, validator_args, "
                "and evidence_card.observed_answer. If this cannot be done, "
                "return {\"skip\": true, \"reason\": \"...\"}."
            ),
        }
        packets.append(packet)
    write_jsonl(Path(args.output_jsonl), packets)
    print(json.dumps({"evidence_packets": len(packets), "output_jsonl": args.output_jsonl}, ensure_ascii=False, indent=2))


def quote_identifier(identifier: str) -> str:
    return '"' + str(identifier).replace('"', '""') + '"'


def list_sql_tables(logical_name: str, cfg: dict[str, Any], dataset_dir: Path) -> list[str]:
    db_type = str(cfg.get("db_type") or "").casefold()
    if db_type == "sqlite":
        rows, _ = execute_sql_client(logical_name, cfg, dataset_dir, "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name", 1000)
        return [str(row.get("name")) for row in rows if row.get("name")]
    if db_type == "duckdb":
        rows, _ = execute_sql_client(
            logical_name,
            cfg,
            dataset_dir,
            "SELECT table_name FROM information_schema.tables WHERE table_schema NOT IN ('pg_catalog', 'information_schema') ORDER BY table_name",
            1000,
        )
        return [str(row.get("table_name")) for row in rows if row.get("table_name")]
    return []


def list_sql_columns(logical_name: str, cfg: dict[str, Any], dataset_dir: Path, table: str) -> list[dict[str, str]]:
    db_type = str(cfg.get("db_type") or "").casefold()
    if db_type == "sqlite":
        rows, _ = execute_sql_client(logical_name, cfg, dataset_dir, f"PRAGMA table_info({quote_identifier(table)})", 1000)
        return [{"name": str(row.get("name")), "type": str(row.get("type") or "")} for row in rows if row.get("name")]
    if db_type == "duckdb":
        table_literal = str(table).replace("'", "''")
        rows, _ = execute_sql_client(
            logical_name,
            cfg,
            dataset_dir,
            (
                "SELECT column_name, data_type FROM information_schema.columns "
                f"WHERE table_name = '{table_literal}' ORDER BY ordinal_position"
            ),
            1000,
        )
        return [{"name": str(row.get("column_name")), "type": str(row.get("data_type") or "")} for row in rows if row.get("column_name")]
    return []


def column_type_family(type_name: str) -> str:
    text = str(type_name).casefold()
    if any(marker in text for marker in ("int", "real", "double", "float", "decimal", "numeric", "number")):
        return "numeric"
    if any(marker in text for marker in ("char", "text", "string", "varchar")):
        return "text"
    if any(marker in text for marker in ("date", "time")):
        return "temporal"
    return "unknown"


def sql_string_literal(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def sql_column_ref(column: str, qualifier: str = "") -> str:
    if qualifier:
        return f"{quote_identifier(qualifier)}.{quote_identifier(column)}"
    return quote_identifier(column)


def group_key_expr(column: str, qualifier: str = "") -> str:
    return f"TRIM(CAST({sql_column_ref(column, qualifier)} AS VARCHAR))"


def normalized_key_expr(column: str, qualifier: str = "") -> str:
    return f"LOWER({group_key_expr(column, qualifier)})"


def group_key_quality_filter_sql(column: str, args: argparse.Namespace, qualifier: str = "") -> str:
    expr = group_key_expr(column, qualifier)
    min_chars = max(1, int(getattr(args, "min_group_key_chars", 2)))
    sentinel_sql = ", ".join(sql_string_literal(key) for key in sorted(GENERIC_GROUP_KEYS))
    substring_filters = " ".join(
        f"AND LOWER({expr}) NOT LIKE {sql_string_literal('%' + marker + '%')}"
        for marker in GENERIC_GROUP_KEY_SUBSTRINGS
    )
    return (
        f"{sql_column_ref(column, qualifier)} IS NOT NULL "
        f"AND {expr} <> '' "
        f"AND LENGTH({expr}) >= {min_chars} "
        f"AND LOWER({expr}) NOT IN ({sentinel_sql})"
        f" {substring_filters}"
    )


def should_skip_group_column(column: str) -> bool:
    snake = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", str(column)).casefold()
    tokens = [token for token in re.split(r"[^a-z0-9]+", snake) if token]
    lowered = str(column).casefold()
    if not tokens:
        return True
    return (
        tokens[-1] in {"id", "uuid", "guid", "key", "code", "ref"}
        or tokens[-1] in {"text", "description", "comment", "comments", "review", "body", "summary", "message", "content"}
        or lowered in {"id", "rowid"}
        or "source_track_id" in lowered
        or COUNT_LIKE_COLUMN_RE.search(str(column)) is not None
    )


def should_skip_numeric_column(column: str) -> bool:
    snake = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", str(column)).casefold()
    tokens = [token for token in re.split(r"[^a-z0-9]+", snake) if token]
    lowered = str(column).casefold()
    if not tokens:
        return True
    if tokens[-1] in {"id", "uuid", "guid", "key", "code", "ref"}:
        return True
    if tokens[0] in {"is", "has", "was", "were", "can", "should"}:
        return True
    if lowered in {"id", "rowid"} or COUNT_LIKE_COLUMN_RE.search(str(column)):
        return True
    return False


def is_quality_group_key(value: Any, args: argparse.Namespace) -> bool:
    text = str(value or "").strip()
    lowered = text.casefold()
    if not text or lowered in GENERIC_GROUP_KEYS:
        return False
    if any(marker in lowered for marker in GENERIC_GROUP_KEY_SUBSTRINGS):
        return False
    if len(text) < max(1, int(getattr(args, "min_group_key_chars", 2))):
        return False
    if len(text) > max(16, int(getattr(args, "max_group_key_value_chars", 120))):
        return False
    alpha_count = sum(ch.isalpha() for ch in text)
    if alpha_count < max(1, int(getattr(args, "min_group_key_alpha_chars", 2))):
        return False
    return True


def source_signature_for_dataset(signature_rows: list[dict[str, Any]], dataset: str) -> dict[str, Any]:
    for row in signature_rows:
        if str(row.get("dataset")) == dataset and isinstance(row.get("task_signature"), dict):
            return row["task_signature"]
    return {}


def source_context_for_dataset(signature_rows: list[dict[str, Any]], dataset: str) -> dict[str, Any]:
    for row in signature_rows:
        if str(row.get("dataset")) == dataset:
            return {
                "source_task_signature": row.get("task_signature", {}) if isinstance(row.get("task_signature"), dict) else {},
                "allowed_hints": row.get("allowed_hints", []),
                "hint_policy": row.get("hint_policy", {}),
                "source_query_id": row.get("query_id"),
                "source_query": row.get("query"),
                "official_dabench_anchor": row.get("official_dabench_anchor", {}),
            }
    return {
        "source_task_signature": {},
        "allowed_hints": [],
        "hint_policy": {},
        "source_query_id": None,
        "source_query": "",
        "official_dabench_anchor": {},
    }


def source_contexts_for_dataset(signature_rows: list[dict[str, Any]], dataset: str) -> list[dict[str, Any]]:
    contexts: list[dict[str, Any]] = []
    for row in signature_rows:
        if str(row.get("dataset")) != dataset:
            continue
        contexts.append(
            {
                "source_task_signature": row.get("task_signature", {}) if isinstance(row.get("task_signature"), dict) else {},
                "allowed_hints": row.get("allowed_hints", []),
                "hint_policy": row.get("hint_policy", {}),
                "source_query_id": row.get("query_id"),
                "source_query": row.get("query"),
                "official_dabench_anchor": row.get("official_dabench_anchor", {}),
            }
        )
    if contexts:
        return contexts
    return [source_context_for_dataset(signature_rows, dataset)]


def source_context_fields(source_context: dict[str, Any]) -> dict[str, Any]:
    return {
        "source_task_signature": source_context.get("source_task_signature", {}),
        "allowed_hints": source_context.get("allowed_hints", []),
        "hint_policy": source_context.get("hint_policy", {}),
        "official_dabench_anchor": source_context.get("official_dabench_anchor", {}),
        "source_query_id": source_context.get("source_query_id"),
        "source_query": source_context.get("source_query"),
    }


def source_signature_operations(source_context: dict[str, Any]) -> set[str]:
    signature = source_context.get("source_task_signature", {})
    if not isinstance(signature, dict):
        signature = {}
    operations = {str(item).casefold() for item in signature.get("operation_families", []) or []}
    task_type = str(signature.get("task_type") or "").casefold()
    answer_shape = str(signature.get("answer_shape") or "").casefold()
    validator_style = str(signature.get("validator_style") or "").casefold()
    if task_type:
        operations.add(task_type)
    if bool(signature.get("requires_join_or_lookup")):
        operations.update({"join", "lookup", "join_or_lookup", "multi_table_join"})
    if bool(signature.get("requires_aggregation")):
        operations.update({"aggregation", "aggregation_first", "numeric_aggregation"})
    if bool(signature.get("requires_temporal_filter")):
        operations.update({"temporal", "temporal_filter"})
    if "normal" in task_type or "normal" in validator_style or bool(signature.get("requires_normalization")):
        operations.update({"normalization", "id_normalization"})
    if "numeric" in answer_shape or "number" in answer_shape or "numeric" in validator_style:
        operations.update({"numeric_answer", "numeric_aggregation"})
    if any(marker in answer_shape for marker in ("list", "set", "array", "top", "pair")) or any(
        marker in validator_style for marker in ("contains", "ordered", "set", "list", "proximity")
    ):
        operations.update({"set_answer", "list_answer", "topk_answer"})
    return operations


def source_context_from_fact(fact: dict[str, Any]) -> dict[str, Any]:
    return {
        "source_task_signature": fact.get("source_task_signature", {}) if isinstance(fact.get("source_task_signature"), dict) else {},
        "allowed_hints": fact.get("allowed_hints", []),
        "hint_policy": fact.get("hint_policy", {}),
        "official_dabench_anchor": fact.get("official_dabench_anchor", {}),
        "source_query_id": fact.get("source_query_id"),
        "source_query": fact.get("source_query"),
    }


def source_prefers_numeric_answer(source_context: dict[str, Any]) -> bool:
    signature = source_context.get("source_task_signature", {})
    if not isinstance(signature, dict):
        return False
    text = " ".join(
        str(signature.get(key, ""))
        for key in ("answer_shape", "validator_style", "task_type")
    ).casefold()
    return any(marker in text for marker in ("numeric", "number", "float", "integer", "scalar"))


def source_answer_family(source_context: dict[str, Any]) -> str:
    signature = source_context.get("source_task_signature", {})
    if not isinstance(signature, dict):
        return "unknown"
    answer_shape = str(signature.get("answer_shape") or "").casefold()
    validator_style = str(signature.get("validator_style") or "").casefold()
    task_type = str(signature.get("task_type") or "").casefold()
    source_query = str(signature.get("source_query") or "").casefold()
    if any(marker in answer_shape for marker in ("numeric", "number", "float", "integer")):
        return "numeric"
    if answer_shape in {"single_scalar", "scalar"}:
        if "numeric" in validator_style or any(
            marker in source_query for marker in ("how many", "how much", "average", "total", "count", "sum ")
        ):
            return "numeric"
        if any(marker in validator_style for marker in ("case_insensitive", "substring", "text", "contains")):
            return "string"
    if any(marker in answer_shape for marker in ("name_value", "record", "json", "field", "pair", "table")):
        return "record"
    if any(marker in answer_shape for marker in ("list", "set", "array", "top")):
        return "list"
    text = " ".join([answer_shape, validator_style, task_type, source_query])
    if any(marker in text for marker in ("numeric", "number", "float", "integer")):
        return "numeric"
    if any(marker in text for marker in ("string", "text", "case_insensitive", "substring")):
        return "string"
    if any(marker in text for marker in ("list", "set", "array", "top", "ordered", "proximity")):
        return "list"
    return "unknown"


def fact_answer_family(fact: dict[str, Any]) -> str:
    template = str(fact.get("validator_template") or "").casefold()
    expected = fact.get("expected_answer") if isinstance(fact.get("expected_answer"), dict) else {}
    expected_type = str(expected.get("type", "")).casefold()
    if template in {"numeric_tolerance", "numeric_list_tolerance"} or expected_type in {"number", "numeric", "integer", "float"}:
        return "numeric"
    if template in {"ordered_contains", "unordered_set_contains"} or expected_type in {"list", "array", "set"}:
        return "list"
    if template == "json_exact_fields" or expected_type in {"object", "dict", "record"}:
        return "record"
    if template in {"normalized_contains_all", "contains_all", "name_value_proximity"} or expected_type in {"string", "text"}:
        return "string"
    return "unknown"


def answer_family_match_score(source_family: str, fact_family: str) -> int:
    if source_family == "unknown" or fact_family == "unknown":
        return 0
    if source_family == fact_family:
        return 160
    if source_family == "record" and fact_family == "list":
        return -40
    if source_family == "list" and fact_family == "record":
        return -40
    return -180


def source_context_match_score(fact: dict[str, Any], source_context: dict[str, Any]) -> tuple[int, str]:
    base, _, _ = fact_priority_score(fact, source_context)
    source_ops = source_signature_operations(source_context)
    fact_ops = {str(item).casefold() for item in fact.get("operation_tags", []) or []}
    source_family = source_answer_family(source_context)
    fact_family = fact_answer_family(fact)
    score = int(base) + answer_family_match_score(source_family, fact_family)
    reasons = [f"source_family={source_family}", f"fact_family={fact_family}"]
    if source_ops & {"join", "lookup", "join_or_lookup", "multi_table_join"}:
        if fact_ops & {"join", "lookup", "join_or_lookup", "multi_table_join"}:
            score += 80
            reasons.append("join_match")
        else:
            score -= 160
            reasons.append("join_missing")
    if source_ops & {"temporal", "temporal_filter"}:
        if fact_ops & {"temporal", "temporal_filter"}:
            score += 80
            reasons.append("temporal_match")
        else:
            score -= 120
            reasons.append("temporal_missing")
    if source_ops & {"aggregation", "aggregation_first", "numeric_aggregation"}:
        if fact_ops & {"aggregation", "aggregation_first", "numeric_aggregation"}:
            score += 60
            reasons.append("aggregation_match")
        else:
            score -= 100
            reasons.append("aggregation_missing")
    if source_ops & {"normalization", "id_normalization"}:
        if fact_ops & {"normalization", "id_normalization"}:
            score += 50
            reasons.append("normalization_match")
        else:
            score -= 70
            reasons.append("normalization_missing")
    if source_ops & {"set_answer", "list_answer", "topk_answer"}:
        if fact_ops & {"set_answer", "list_answer", "topk_answer"} or fact_family in {"list", "record"}:
            score += 50
            reasons.append("list_or_set_match")
        else:
            score -= 80
            reasons.append("list_or_set_missing")
    return score, ";".join(reasons)


def attach_best_source_context(fact: dict[str, Any], source_contexts: list[dict[str, Any]]) -> dict[str, Any]:
    if not source_contexts:
        return fact
    ranked = sorted(
        ((source_context_match_score(fact, context), context) for context in source_contexts),
        key=lambda item: (item[0][0], str(item[1].get("source_query_id") or "")),
        reverse=True,
    )
    (score, reason), context = ranked[0]
    out = dict(fact)
    out.update(source_context_fields(context))
    out["source_match_score"] = score
    out["source_match_reason"] = reason
    return out


def fact_priority_score(fact: dict[str, Any], source_context: dict[str, Any]) -> tuple[int, float, str]:
    source_ops = source_signature_operations(source_context)
    fact_type = str(fact.get("fact_type") or "")
    fact_ops = {str(item).casefold() for item in fact.get("operation_tags", []) or []}
    type_base = {
        "join_group_count_topk_list": 95,
        "normalized_text_topk_list": 92,
        "join_numeric_sum_value": 90,
        "join_group_count_ranking": 85,
        "normalized_text_group_count": 75,
        "temporal_numeric_sum_value": 72,
        "temporal_group_count_ranking": 70,
        "group_numeric_sum_value": 65,
        "group_count_topk_list": 55,
        "group_count_ranking": 25,
    }.get(fact_type, 10)
    alignment = 35 * len(source_ops & fact_ops)
    if source_prefers_numeric_answer(source_context) and str(fact.get("validator_template")) == "numeric_tolerance":
        alignment += 40
    if source_ops & {"join", "lookup", "join_or_lookup", "multi_table_join"}:
        if fact_ops & {"join", "lookup", "join_or_lookup", "multi_table_join"}:
            alignment += 45
        else:
            alignment -= 90
    if source_ops & {"temporal", "temporal_filter"}:
        if fact_ops & {"temporal", "temporal_filter"}:
            alignment += 30
        else:
            alignment -= 45
    if source_ops & {"set_answer"} and str(fact.get("validator_template")) in {"ordered_contains", "unordered_set_contains"}:
        alignment += 30
    metrics = fact.get("non_degeneracy_metrics", {}) if isinstance(fact.get("non_degeneracy_metrics"), dict) else {}
    candidate_groups = float(metrics.get("candidate_groups") or 0)
    winner_count = float(metrics.get("winner_count") or metrics.get("joined_count") or metrics.get("period_winner_count") or 0)
    if candidate_groups < 2:
        alignment -= 60
    elif candidate_groups >= 3:
        alignment += 15
    return (type_base + alignment, candidate_groups + winner_count / 10.0, str(fact.get("fact_id") or ""))


def select_diverse_mined_facts(candidates: list[dict[str, Any]], source_context: dict[str, Any], limit: int) -> list[dict[str, Any]]:
    if limit <= 0:
        return []
    def rank_key(fact: dict[str, Any]) -> tuple[int, float, str]:
        context = source_context_from_fact(fact)
        if not context.get("source_task_signature"):
            context = source_context
        priority, spread, fact_id = fact_priority_score(fact, context)
        priority += int(fact.get("source_match_score") or 0)
        return priority, spread, fact_id

    ranked = sorted(candidates, key=rank_key, reverse=True)
    preferred_types: list[str] = []
    source_ops = source_signature_operations(source_context)
    if source_ops & {"join", "lookup", "join_or_lookup", "multi_table_join"}:
        preferred_types.extend(["join_group_count_topk_list", "join_numeric_sum_value", "join_group_count_ranking"])
    if source_ops & {"set_answer", "list_answer", "topk_answer"}:
        preferred_types.extend(["join_group_count_topk_list", "normalized_text_topk_list", "group_count_topk_list"])
    if source_ops & {"normalization", "id_normalization"}:
        preferred_types.extend(["normalized_text_topk_list", "normalized_text_group_count"])
    if source_ops & {"temporal", "temporal_filter"}:
        preferred_types.extend(["temporal_numeric_sum_value", "temporal_group_count_ranking"])
    if source_prefers_numeric_answer(source_context) or source_ops & {"numeric_aggregation", "aggregation", "aggregation_first"}:
        preferred_types.extend(["group_numeric_sum_value", "join_numeric_sum_value", "temporal_numeric_sum_value"])
    preferred_types.extend([
        "join_group_count_ranking",
        "join_group_count_topk_list",
        "normalized_text_topk_list",
        "normalized_text_group_count",
        "group_numeric_sum_value",
        "temporal_group_count_ranking",
        "group_count_topk_list",
        "group_count_ranking",
    ])

    selected: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    by_source_query: Counter[str] = Counter()
    max_per_source_query = max(1, (limit + 3) // 4)
    for fact_type in preferred_types:
        for fact in ranked:
            fact_id = str(fact.get("fact_id") or "")
            if fact_id in seen_ids or fact.get("fact_type") != fact_type:
                continue
            source_query_id = str(fact.get("source_query_id") or "")
            if source_query_id and by_source_query[source_query_id] >= max_per_source_query and len(selected) < limit - 1:
                continue
            selected.append(fact)
            seen_ids.add(fact_id)
            if source_query_id:
                by_source_query[source_query_id] += 1
            break
        if len(selected) >= limit:
            return selected

    max_per_type = max(1, (limit + 1) // 2)
    by_type: Counter[str] = Counter(str(fact.get("fact_type") or "") for fact in selected)
    for fact in ranked:
        fact_id = str(fact.get("fact_id") or "")
        fact_type = str(fact.get("fact_type") or "")
        if fact_id in seen_ids:
            continue
        if by_type[fact_type] >= max_per_type and len(selected) < limit - 1:
            continue
        source_query_id = str(fact.get("source_query_id") or "")
        if source_query_id and by_source_query[source_query_id] >= max_per_source_query and len(selected) < limit - 1:
            continue
        selected.append(fact)
        seen_ids.add(fact_id)
        by_type[fact_type] += 1
        if source_query_id:
            by_source_query[source_query_id] += 1
        if len(selected) >= limit:
            break
    return selected


def source_match_family(source_match_reason: str, key: str) -> str:
    marker = f"{key}_family="
    for part in str(source_match_reason or "").split(";"):
        if part.startswith(marker):
            return part.removeprefix(marker).strip()
    return ""


def csv_values(value: str) -> list[str]:
    return [item.strip() for item in str(value or "").split(",") if item.strip()]


def ranked_fact_metric_values(fact: dict[str, Any]) -> list[Any]:
    metrics = fact.get("non_degeneracy_metrics", {}) if isinstance(fact.get("non_degeneracy_metrics"), dict) else {}
    top_rows = metrics.get("top_rows") if isinstance(metrics.get("top_rows"), list) else []
    if not top_rows:
        return []
    for metric_name in ("joined_count", "total_value", "group_count", "value"):
        if isinstance(top_rows[0], dict) and metric_name in top_rows[0]:
            return [row.get(metric_name) for row in top_rows if isinstance(row, dict)]
    return []


def fact_has_rank_tie(fact: dict[str, Any]) -> bool:
    values = ranked_fact_metric_values(fact)
    if len(values) > 1 and values[0] == values[1]:
        return True
    metrics = fact.get("non_degeneracy_metrics", {}) if isinstance(fact.get("non_degeneracy_metrics"), dict) else {}
    answer = fact.get("answer")
    top_k = int(metrics.get("top_k") or (len(answer) if isinstance(answer, list) else 1) or 1)
    return bool(top_k > 0 and len(values) > top_k and values[top_k - 1] == values[top_k])


def join_focus_fact_score(fact: dict[str, Any]) -> int:
    reason = str(fact.get("source_match_reason") or "")
    signature = fact.get("source_task_signature", {}) if isinstance(fact.get("source_task_signature"), dict) else {}
    fact_type = str(fact.get("fact_type") or "")
    score = 0
    if fact_type.startswith("join_"):
        score += 10
    if signature.get("requires_join_or_lookup"):
        score += 7
    if signature.get("requires_aggregation"):
        score += 3
    if "join_match" in reason:
        score += 6
    if "aggregation_match" in reason:
        score += 3
    source_family = source_match_family(reason, "source")
    fact_family = source_match_family(reason, "fact")
    if source_family and source_family == fact_family:
        score += 5
    if "list_or_set_match" in reason:
        score += 2
    if fact_has_rank_tie(fact):
        score -= 8
    penalties = {
        "join_missing": 8,
        "aggregation_missing": 4,
        "temporal_missing": 5,
        "normalization_missing": 2,
    }
    for marker, penalty in penalties.items():
        if marker in reason:
            score -= penalty
    answer = fact.get("answer")
    answer_text = " ".join(map(str, answer)) if isinstance(answer, list) else str(answer)
    if answer_text.casefold() in {"true", "false"}:
        score -= 6
    if "cpe:2.3" in answer_text:
        score -= 5
    return score


def filter_db_facts(args: argparse.Namespace) -> None:
    rows = read_jsonl(Path(args.fact_jsonl))
    selected: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    seen: set[tuple[Any, ...]] = set()
    for fact in sorted(rows, key=join_focus_fact_score, reverse=True):
        reasons: list[str] = []
        reason = str(fact.get("source_match_reason") or "")
        signature = fact.get("source_task_signature", {}) if isinstance(fact.get("source_task_signature"), dict) else {}
        fact_type = str(fact.get("fact_type") or "")
        if args.fact_type_prefix and not fact_type.startswith(args.fact_type_prefix):
            reasons.append("fact_type_prefix_mismatch")
        if args.require_source_join and not signature.get("requires_join_or_lookup"):
            reasons.append("source_does_not_require_join_or_lookup")
        if args.require_source_aggregation and not signature.get("requires_aggregation"):
            reasons.append("source_does_not_require_aggregation")
        if args.require_join_match and "join_match" not in reason:
            reasons.append("source_match_reason_missing_join_match")
        if args.require_aggregation_match and "aggregation_match" not in reason:
            reasons.append("source_match_reason_missing_aggregation_match")
        if args.require_family_match:
            source_family = source_match_family(reason, "source")
            fact_family = source_match_family(reason, "fact")
            if not source_family or source_family != fact_family:
                reasons.append("source_fact_family_mismatch")
        if args.reject_rank_ties and fact_has_rank_tie(fact):
            reasons.append("rank_tie")
        for marker in csv_values(args.reject_reason_markers):
            if marker and marker in reason:
                reasons.append(f"source_match_reason_contains:{marker}")
        answer = fact.get("answer")
        answer_text = " ".join(map(str, answer)) if isinstance(answer, list) else str(answer)
        if args.reject_boolean_answers and answer_text.casefold() in {"true", "false"}:
            reasons.append("boolean_answer")
        if args.reject_cpe_answers and "cpe:2.3" in answer_text:
            reasons.append("cpe_answer")
        dedupe_key = (
            fact.get("dataset"),
            fact.get("source_query_id"),
            fact.get("fact_type"),
            json.dumps(fact.get("answer"), ensure_ascii=False, sort_keys=True),
        )
        if args.dedupe_answers and dedupe_key in seen:
            reasons.append("duplicate_answer_for_source_fact_type")
        if reasons:
            rejected.append({**fact, "filter_rejection_reasons": reasons, "join_focus_score": join_focus_fact_score(fact)})
            continue
        seen.add(dedupe_key)
        selected.append(
            {
                **fact,
                "join_focus_selection": {
                    "score": join_focus_fact_score(fact),
                    "top_metric_values": ranked_fact_metric_values(fact)[:5],
                    "selection_reason": "join-focused mined fact filter passed",
                },
            }
        )
        if args.max_facts and len(selected) >= args.max_facts:
            break
    write_jsonl(Path(args.output_jsonl), selected)
    if args.rejected_jsonl:
        write_jsonl(Path(args.rejected_jsonl), rejected)
    dashboard = {
        "input": len(rows),
        "selected": len(selected),
        "rejected": len(rejected),
        "output_jsonl": str(args.output_jsonl),
        "selected_datasets": dict(Counter(str(row.get("dataset") or "") for row in selected)),
        "selected_fact_types": dict(Counter(str(row.get("fact_type") or "") for row in selected)),
        "rejection_reasons": dict(Counter(reason for row in rejected for reason in row.get("filter_rejection_reasons", []))),
        "policy": {
            "fact_type_prefix": args.fact_type_prefix,
            "require_source_join": args.require_source_join,
            "require_source_aggregation": args.require_source_aggregation,
            "require_join_match": args.require_join_match,
            "require_aggregation_match": args.require_aggregation_match,
            "require_family_match": args.require_family_match,
            "reject_rank_ties": args.reject_rank_ties,
            "reject_reason_markers": csv_values(args.reject_reason_markers),
            "reject_boolean_answers": args.reject_boolean_answers,
            "reject_cpe_answers": args.reject_cpe_answers,
        },
    }
    if args.dashboard:
        Path(args.dashboard).parent.mkdir(parents=True, exist_ok=True)
        Path(args.dashboard).write_text(json.dumps(dashboard, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(dashboard, ensure_ascii=False, indent=2, sort_keys=True))


def quality_filter_summary() -> str:
    return (
        "The evidence SQL excludes null, blank, and generic low-information grouping values. "
        "The user-facing query should describe these as valid values and must not enumerate the exact exclusion list."
    )


def candidate_solution_python(db_name: str, sql: str, answer_key: str, answer_mode: str = "scalar", top_k: int = 0) -> str:
    if answer_mode == "topk_list":
        limit = max(1, int(top_k or 3))
        return (
            f"rows = query_db({db_name!r}, {sql!r})\n"
            "if not rows:\n"
            "    raise ValueError('Evidence query returned no rows')\n"
            f"answer = [row[{answer_key!r}] for row in rows[:{limit}] if row.get({answer_key!r}) is not None]\n"
            "if not answer:\n"
            "    raise ValueError('Evidence query returned no answer values')\n"
            "return_answer(answer)"
        )
    return (
        f"rows = query_db({db_name!r}, {sql!r})\n"
        "if not rows:\n"
        "    raise ValueError('Evidence query returned no rows')\n"
        f"answer = rows[0][{answer_key!r}]\n"
        "return_answer(answer)"
    )


def numeric_validator_args(value: Any) -> dict[str, Any]:
    number = float(value)
    return {"expected": number, "tolerance": max(1e-6, abs(number) * 1e-6)}


def topk_answer_values(rows: list[dict[str, Any]], answer_key: str, args: argparse.Namespace, limit: int = 3) -> list[str]:
    values: list[str] = []
    for row in rows[: max(1, limit)]:
        value = row.get(answer_key)
        if not is_quality_group_key(value, args):
            continue
        text = str(value)
        if text not in values:
            values.append(text)
    return values


def make_sql_fact(
    *,
    dataset: str,
    logical_name: str,
    cfg: dict[str, Any],
    table: str | list[str],
    fact_type: str,
    source_context: dict[str, Any],
    evidence_sql: str,
    observation: dict[str, Any],
    answer: Any,
    answer_key: str,
    validator_template: str,
    validator_args: dict[str, Any],
    expected_answer: dict[str, Any],
    non_degeneracy_metrics: dict[str, Any],
    data_requirements: dict[str, Any],
    operation_tags: list[str],
    query_requirements: list[str],
    answer_mode: str = "scalar",
    top_k: int = 0,
) -> dict[str, Any]:
    fact_id = f"fact_{stable_hash([dataset, logical_name, table, fact_type, evidence_sql, answer_key])}"
    return {
        "fact_id": fact_id,
        "dataset": f"synthetic_{dataset}",
        "source_dataset": dataset,
        "db_name": logical_name,
        "db_type": str(cfg.get("db_type") or ""),
        "table": table,
        "fact_type": fact_type,
        **source_context_fields(source_context),
        "evidence_sql": evidence_sql,
        "evidence_observation": {**observation, "success": True},
        "answer": answer,
        "answer_key": answer_key,
        "expected_answer": expected_answer,
        "validator_template": validator_template,
        "validator_args": validator_args,
        "candidate_solution_python": candidate_solution_python(logical_name, evidence_sql, answer_key, answer_mode, top_k),
        "quality_filter_summary": quality_filter_summary(),
        "query_requirements": query_requirements,
        "data_requirements": data_requirements,
        "operation_tags": operation_tags,
        "non_degeneracy_metrics": non_degeneracy_metrics,
    }


def looks_temporal_column(column: str, type_name: str = "") -> bool:
    lowered = str(column).casefold()
    family = column_type_family(type_name)
    return family == "temporal" or any(
        marker in lowered for marker in ("date", "time", "year", "month", "created", "updated", "published", "released")
    )


def temporal_year_expr(column: str, qualifier: str = "") -> str:
    return f"SUBSTR(TRIM(CAST({sql_column_ref(column, qualifier)} AS VARCHAR)), 1, 4)"


def join_key_score(column: str) -> int:
    snake = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", str(column)).casefold()
    tokens = [token for token in re.split(r"[^a-z0-9]+", snake) if token]
    compact = "".join(tokens)
    if not tokens or compact in {"", "name", "title", "type", "status"}:
        return 0
    score = 0
    if tokens[-1] in {"id", "ref"} or compact in {"id", "uuid", "guid"}:
        score += 4
    if tokens[-1] in {"key", "code", "uuid", "guid"}:
        score += 2
    return score


def common_join_keys(left_columns: list[dict[str, str]], right_columns: list[dict[str, str]], limit: int = 3) -> list[str]:
    left_by_norm = {re.sub(r"[^a-z0-9]+", "", col["name"].casefold()): col["name"] for col in left_columns}
    right_by_norm = {re.sub(r"[^a-z0-9]+", "", col["name"].casefold()): col["name"] for col in right_columns}
    candidates = []
    for norm, left_name in left_by_norm.items():
        right_name = right_by_norm.get(norm)
        if not right_name or left_name != right_name:
            continue
        score = join_key_score(left_name)
        if score <= 0:
            continue
        candidates.append((score, left_name))
    candidates.sort(reverse=True)
    return [name for _, name in candidates[:limit]]


def reached_table_fact_limit(facts: list[dict[str, Any]], args: argparse.Namespace) -> bool:
    return len(facts) >= max(1, int(getattr(args, "max_candidate_facts_per_table", 12)))


def mine_sql_group_facts_for_table(
    dataset: str,
    logical_name: str,
    cfg: dict[str, Any],
    dataset_dir: Path,
    table: str,
    columns: list[dict[str, str]],
    source_context: dict[str, Any],
    args: argparse.Namespace,
) -> list[dict[str, Any]]:
    text_columns = [col["name"] for col in columns if column_type_family(col.get("type", "")) == "text" and not should_skip_group_column(col["name"])]
    numeric_columns = [
        col["name"]
        for col in columns
        if column_type_family(col.get("type", "")) == "numeric" and not should_skip_numeric_column(col["name"])
    ]
    temporal_columns = [col["name"] for col in columns if looks_temporal_column(col["name"], col.get("type", ""))]
    facts: list[dict[str, Any]] = []

    for group_col in text_columns[: args.max_group_columns_per_table]:
        group_filter = group_key_quality_filter_sql(group_col, args)
        sql = (
            f"SELECT {group_key_expr(group_col)} AS group_key, COUNT(*) AS group_count "
            f"FROM {quote_identifier(table)} "
            f"WHERE {group_filter} "
            f"GROUP BY {group_key_expr(group_col)} "
            f"HAVING COUNT(*) >= {int(args.min_group_size)} "
            "ORDER BY group_count DESC, group_key ASC LIMIT 5"
        )
        try:
            rows, observation = execute_sql_client(logical_name, cfg, dataset_dir, sql, args.query_row_limit)
        except Exception:
            continue
        if not rows:
            continue
        top = rows[0]
        if not is_quality_group_key(top.get("group_key"), args):
            continue
        count_value = top.get("group_count")
        if not isinstance(count_value, (int, float)) or float(count_value) < args.min_group_size:
            continue
        facts.append(
            make_sql_fact(
                dataset=dataset,
                logical_name=logical_name,
                cfg=cfg,
                table=table,
                fact_type="group_count_ranking",
                source_context=source_context,
                evidence_sql=sql,
                observation=observation,
                answer=top.get("group_key"),
                answer_key="group_key",
                expected_answer={"type": "string", "value": top.get("group_key"), "normalization": "normalized_contains_all"},
                validator_template="normalized_contains_all",
                validator_args={"items": [str(top.get("group_key"))]},
                data_requirements={
                    "tables": [table],
                    "fields": [group_col],
                    "joins": [],
                    "filters": ["valid non-empty grouping values"],
                    "operations": ["group", "count", "rank"],
                },
                operation_tags=["aggregation", "aggregation_first", "ranking"],
                query_requirements=[
                    "Ask for the highest-count valid group.",
                    "Specify the tie-breaker: descending count, then ascending group value.",
                    "Do not list the internal generic-value exclusion rules in the user query.",
                ],
                non_degeneracy_metrics={
                    "winner_count": count_value,
                    "candidate_groups": len(rows),
                    "min_required_winner_count": args.min_group_size,
                    "top_rows": jsonable(rows[:5]),
                },
            )
        )
        top_items = topk_answer_values(rows, "group_key", args, limit=3)
        if len(top_items) >= 2:
            facts.append(
                make_sql_fact(
                    dataset=dataset,
                    logical_name=logical_name,
                    cfg=cfg,
                    table=table,
                    fact_type="group_count_topk_list",
                    source_context=source_context,
                    evidence_sql=sql,
                    observation=observation,
                    answer=top_items,
                    answer_key="group_key",
                    expected_answer={"type": "list", "value": top_items, "normalization": "ordered_contains"},
                    validator_template="ordered_contains",
                    validator_args={"items": top_items},
                    data_requirements={
                        "tables": [table],
                        "fields": [group_col],
                        "joins": [],
                        "filters": ["valid non-empty grouping values"],
                        "operations": ["group", "count", "rank", "top-k list"],
                    },
                    operation_tags=["aggregation", "aggregation_first", "ranking", "set_answer", "list_answer", "topk_answer"],
                    query_requirements=[
                        f"Ask for the top {len(top_items)} highest-count valid groups as an ordered list.",
                        "Specify the tie-breaker: descending count, then ascending group value.",
                        "Require the final answer to preserve the ranked order.",
                    ],
                    non_degeneracy_metrics={
                        "winner_count": count_value,
                        "candidate_groups": len(rows),
                        "top_k": len(top_items),
                        "top_items": top_items,
                        "min_required_winner_count": args.min_group_size,
                        "top_rows": jsonable(rows[:5]),
                    },
                    answer_mode="topk_list",
                    top_k=len(top_items),
                )
            )
        if reached_table_fact_limit(facts, args):
            return facts

        normalized_sql = (
            f"SELECT {normalized_key_expr(group_col)} AS normalized_key, "
            f"MIN({group_key_expr(group_col)}) AS representative_value, "
            f"COUNT(*) AS group_count "
            f"FROM {quote_identifier(table)} "
            f"WHERE {group_filter} "
            f"GROUP BY {normalized_key_expr(group_col)} "
            f"HAVING COUNT(*) >= {int(args.min_group_size)} "
            "ORDER BY group_count DESC, normalized_key ASC LIMIT 5"
        )
        try:
            rows, observation = execute_sql_client(logical_name, cfg, dataset_dir, normalized_sql, args.query_row_limit)
        except Exception:
            rows = []
            observation = {}
        if rows:
            top = rows[0]
            answer = top.get("representative_value") or top.get("normalized_key")
            count_value = top.get("group_count")
            if is_quality_group_key(answer, args) and isinstance(count_value, (int, float)) and float(count_value) >= args.min_group_size:
                facts.append(
                    make_sql_fact(
                        dataset=dataset,
                        logical_name=logical_name,
                        cfg=cfg,
                        table=table,
                        fact_type="normalized_text_group_count",
                        source_context=source_context,
                        evidence_sql=normalized_sql,
                        observation=observation,
                        answer=answer,
                        answer_key="representative_value",
                        expected_answer={"type": "string", "value": answer, "normalization": "normalized_contains_all"},
                        validator_template="normalized_contains_all",
                        validator_args={"items": [str(answer)]},
                        data_requirements={
                            "tables": [table],
                            "fields": [group_col],
                            "joins": [],
                            "filters": ["valid non-empty grouping values"],
                            "operations": ["trim", "casefold", "group", "count", "rank"],
                        },
                        operation_tags=["normalization", "id_normalization", "aggregation", "ranking"],
                        query_requirements=[
                            "Require trim/case-insensitive normalization before grouping.",
                            "Ask for the representative original value for the largest normalized group.",
                            "Do not list the internal generic-value exclusion rules in the user query.",
                        ],
                        non_degeneracy_metrics={
                            "winner_count": count_value,
                            "candidate_groups": len(rows),
                            "min_required_winner_count": args.min_group_size,
                            "top_rows": jsonable(rows[:5]),
                        },
                    )
                )
                top_items = topk_answer_values(rows, "representative_value", args, limit=3)
                if len(top_items) >= 2:
                    facts.append(
                        make_sql_fact(
                            dataset=dataset,
                            logical_name=logical_name,
                            cfg=cfg,
                            table=table,
                            fact_type="normalized_text_topk_list",
                            source_context=source_context,
                            evidence_sql=normalized_sql,
                            observation=observation,
                            answer=top_items,
                            answer_key="representative_value",
                            expected_answer={"type": "list", "value": top_items, "normalization": "ordered_contains"},
                            validator_template="ordered_contains",
                            validator_args={"items": top_items},
                            data_requirements={
                                "tables": [table],
                                "fields": [group_col],
                                "joins": [],
                                "filters": ["valid non-empty grouping values"],
                                "operations": ["trim", "casefold", "group", "count", "rank", "top-k list"],
                            },
                            operation_tags=[
                                "normalization",
                                "id_normalization",
                                "aggregation",
                                "ranking",
                                "set_answer",
                                "list_answer",
                                "topk_answer",
                            ],
                            query_requirements=[
                                f"Ask for the top {len(top_items)} normalized groups as an ordered list of representative original values.",
                                "Require trim/case-insensitive normalization before grouping.",
                                "Specify the tie-breaker: descending count, then ascending normalized value.",
                                "Require the final answer to preserve the ranked order.",
                            ],
                            non_degeneracy_metrics={
                                "winner_count": count_value,
                                "candidate_groups": len(rows),
                                "top_k": len(top_items),
                                "top_items": top_items,
                                "min_required_winner_count": args.min_group_size,
                                "top_rows": jsonable(rows[:5]),
                            },
                            answer_mode="topk_list",
                            top_k=len(top_items),
                        )
                    )
                if reached_table_fact_limit(facts, args):
                    return facts

    for group_col in text_columns[: args.max_group_columns_per_table]:
        group_filter = group_key_quality_filter_sql(group_col, args)
        for value_col in numeric_columns[: args.max_numeric_columns_per_table]:
            if group_col == value_col:
                continue
            sql = (
                f"SELECT {group_key_expr(group_col)} AS group_key, "
                f"COUNT(*) AS group_count, ROUND(SUM({quote_identifier(value_col)}), 6) AS total_value "
                f"FROM {quote_identifier(table)} "
                f"WHERE {group_filter} "
                f"AND {quote_identifier(value_col)} IS NOT NULL "
                f"GROUP BY {group_key_expr(group_col)} "
                f"HAVING COUNT(*) >= {int(args.min_group_size)} AND SUM({quote_identifier(value_col)}) IS NOT NULL "
                "ORDER BY total_value DESC, group_key ASC LIMIT 5"
            )
            try:
                rows, observation = execute_sql_client(logical_name, cfg, dataset_dir, sql, args.query_row_limit)
            except Exception:
                continue
            if not rows:
                continue
            top = rows[0]
            if not is_quality_group_key(top.get("group_key"), args):
                continue
            total = top.get("total_value")
            count_value = top.get("group_count")
            if not isinstance(total, (int, float)) or not isinstance(count_value, (int, float)) or float(count_value) < args.min_group_size:
                continue
            facts.append(
                make_sql_fact(
                    dataset=dataset,
                    logical_name=logical_name,
                    cfg=cfg,
                    table=table,
                    fact_type="group_numeric_sum_value",
                    source_context=source_context,
                    evidence_sql=sql,
                    observation=observation,
                    answer=total,
                    answer_key="total_value",
                    expected_answer={"type": "number", "value": float(total)},
                    validator_template="numeric_tolerance",
                    validator_args=numeric_validator_args(total),
                    data_requirements={
                        "tables": [table],
                        "fields": [group_col, value_col],
                        "joins": [],
                        "filters": ["valid non-empty grouping values", f"{value_col} is not null"],
                        "operations": ["group", "sum", "rank", "numeric answer"],
                    },
                    operation_tags=["aggregation", "aggregation_first", "numeric_aggregation", "numeric_answer", "ranking"],
                    query_requirements=[
                        "Ask for the numeric total of the winning group, not the group name.",
                        "Specify the tie-breaker: descending total, then ascending group value.",
                        "Require a numeric answer rounded to six decimal places when needed.",
                    ],
                    non_degeneracy_metrics={
                        "winner_count": count_value,
                        "winner_total_value": total,
                        "winner_group_key": top.get("group_key"),
                        "candidate_groups": len(rows),
                        "min_required_winner_count": args.min_group_size,
                        "top_rows": jsonable(rows[:5]),
                    },
                )
            )
            if reached_table_fact_limit(facts, args):
                return facts

    for group_col in text_columns[: args.max_group_columns_per_table]:
        group_filter = group_key_quality_filter_sql(group_col, args)
        for temporal_col in temporal_columns[: args.max_temporal_columns_per_table]:
            if temporal_col == group_col:
                continue
            year_expr = temporal_year_expr(temporal_col)
            year_sql = (
                f"SELECT {year_expr} AS period_key, COUNT(*) AS period_count "
                f"FROM {quote_identifier(table)} "
                f"WHERE {quote_identifier(temporal_col)} IS NOT NULL "
                f"AND LENGTH(TRIM(CAST({quote_identifier(temporal_col)} AS VARCHAR))) >= 4 "
                f"AND {year_expr} >= '1900' AND {year_expr} <= '2100' "
                f"GROUP BY {year_expr} "
                f"HAVING COUNT(*) >= {int(args.min_group_size)} "
                "ORDER BY period_count DESC, period_key ASC LIMIT 3"
            )
            try:
                period_rows, _ = execute_sql_client(logical_name, cfg, dataset_dir, year_sql, args.query_row_limit)
            except Exception:
                continue
            for period in period_rows[:2]:
                period_key = str(period.get("period_key") or "").strip()
                if not re.fullmatch(r"\d{4}", period_key):
                    continue
                sql = (
                    f"SELECT {group_key_expr(group_col)} AS group_key, COUNT(*) AS group_count "
                    f"FROM {quote_identifier(table)} "
                    f"WHERE {group_filter} "
                    f"AND {year_expr} = {sql_string_literal(period_key)} "
                    f"GROUP BY {group_key_expr(group_col)} "
                    f"HAVING COUNT(*) >= {int(args.min_group_size)} "
                    "ORDER BY group_count DESC, group_key ASC LIMIT 5"
                )
                try:
                    rows, observation = execute_sql_client(logical_name, cfg, dataset_dir, sql, args.query_row_limit)
                except Exception:
                    continue
                if not rows:
                    continue
                top = rows[0]
                count_value = top.get("group_count")
                if not is_quality_group_key(top.get("group_key"), args):
                    continue
                if not isinstance(count_value, (int, float)) or float(count_value) < args.min_group_size:
                    continue
                facts.append(
                    make_sql_fact(
                        dataset=dataset,
                        logical_name=logical_name,
                        cfg=cfg,
                        table=table,
                        fact_type="temporal_group_count_ranking",
                        source_context=source_context,
                        evidence_sql=sql,
                        observation=observation,
                        answer=top.get("group_key"),
                        answer_key="group_key",
                        expected_answer={"type": "string", "value": top.get("group_key"), "normalization": "normalized_contains_all"},
                        validator_template="normalized_contains_all",
                        validator_args={"items": [str(top.get("group_key"))]},
                        data_requirements={
                            "tables": [table],
                            "fields": [group_col, temporal_col],
                            "joins": [],
                            "filters": [f"{temporal_col} year equals {period_key}", "valid non-empty grouping values"],
                            "operations": ["temporal filter", "group", "count", "rank"],
                        },
                        operation_tags=["temporal", "temporal_filter", "aggregation", "ranking"],
                        query_requirements=[
                            f"Require filtering records to year {period_key} using the temporal field.",
                            "Ask for the highest-count valid group after applying the temporal filter.",
                            "Specify the tie-breaker: descending count, then ascending group value.",
                        ],
                        non_degeneracy_metrics={
                            "period_key": period_key,
                            "period_row_count": period.get("period_count"),
                            "winner_count": count_value,
                            "candidate_groups": len(rows),
                            "min_required_winner_count": args.min_group_size,
                            "top_rows": jsonable(rows[:5]),
                        },
                    )
                )
                if reached_table_fact_limit(facts, args):
                    return facts
    return facts


def mine_sql_join_facts(
    dataset: str,
    logical_name: str,
    cfg: dict[str, Any],
    dataset_dir: Path,
    table_infos: list[dict[str, Any]],
    source_context: dict[str, Any],
    args: argparse.Namespace,
) -> list[dict[str, Any]]:
    facts: list[dict[str, Any]] = []
    pairs_checked = 0
    for left_index, left in enumerate(table_infos):
        for right in table_infos[left_index + 1 :]:
            if pairs_checked >= args.max_join_pairs_per_db:
                return facts
            join_keys = common_join_keys(left["columns"], right["columns"])
            if not join_keys:
                continue
            pairs_checked += 1
            left_text = [
                col["name"]
                for col in left["columns"]
                if column_type_family(col.get("type", "")) == "text" and not should_skip_group_column(col["name"])
            ][:2]
            right_text = [
                col["name"]
                for col in right["columns"]
                if column_type_family(col.get("type", "")) == "text" and not should_skip_group_column(col["name"])
            ][:2]
            left_numeric = [
                col["name"]
                for col in left["columns"]
                if column_type_family(col.get("type", "")) == "numeric" and not should_skip_numeric_column(col["name"])
            ][:2]
            right_numeric = [
                col["name"]
                for col in right["columns"]
                if column_type_family(col.get("type", "")) == "numeric" and not should_skip_numeric_column(col["name"])
            ][:2]
            group_candidates = [(left["table"], "a", col) for col in left_text] + [(right["table"], "b", col) for col in right_text]
            numeric_candidates = [(left["table"], "a", col) for col in left_numeric] + [(right["table"], "b", col) for col in right_numeric]
            for join_key in join_keys:
                for group_table, group_alias, group_col in group_candidates[: args.max_group_columns_per_table]:
                    if group_col == join_key:
                        continue
                    group_filter = group_key_quality_filter_sql(group_col, args, group_alias)
                    sql = (
                        f"SELECT {group_key_expr(group_col, group_alias)} AS group_key, COUNT(*) AS joined_count "
                        f"FROM {quote_identifier(left['table'])} AS {quote_identifier('a')} "
                        f"JOIN {quote_identifier(right['table'])} AS {quote_identifier('b')} "
                        f"ON {sql_column_ref(join_key, 'a')} = {sql_column_ref(join_key, 'b')} "
                        f"WHERE {group_filter} "
                        f"AND {sql_column_ref(join_key, 'a')} IS NOT NULL "
                        f"AND {sql_column_ref(join_key, 'b')} IS NOT NULL "
                        f"GROUP BY {group_key_expr(group_col, group_alias)} "
                        f"HAVING COUNT(*) >= {int(args.min_group_size)} "
                        "ORDER BY joined_count DESC, group_key ASC LIMIT 5"
                    )
                    try:
                        rows, observation = execute_sql_client(logical_name, cfg, dataset_dir, sql, args.query_row_limit)
                    except Exception:
                        continue
                    if not rows:
                        continue
                    top = rows[0]
                    count_value = top.get("joined_count")
                    if not is_quality_group_key(top.get("group_key"), args):
                        continue
                    if not isinstance(count_value, (int, float)) or float(count_value) < args.min_group_size:
                        continue
                    facts.append(
                        make_sql_fact(
                            dataset=dataset,
                            logical_name=logical_name,
                            cfg=cfg,
                            table=[left["table"], right["table"]],
                            fact_type="join_group_count_ranking",
                            source_context=source_context,
                            evidence_sql=sql,
                            observation=observation,
                            answer=top.get("group_key"),
                            answer_key="group_key",
                            expected_answer={"type": "string", "value": top.get("group_key"), "normalization": "normalized_contains_all"},
                            validator_template="normalized_contains_all",
                            validator_args={"items": [str(top.get("group_key"))]},
                            data_requirements={
                                "tables": [left["table"], right["table"]],
                                "fields": [join_key, group_col],
                                "joins": [{"left": left["table"], "right": right["table"], "on": join_key}],
                                "filters": ["valid non-empty grouping values", "non-null join keys"],
                                "operations": ["join", "group", "count", "rank"],
                            },
                            operation_tags=["join", "join_or_lookup", "multi_table_join", "aggregation", "ranking"],
                            query_requirements=[
                                f"Require joining {left['table']} and {right['table']} on {join_key}.",
                                "Ask for the highest joined-row-count valid group after the join.",
                                "Specify the tie-breaker: descending joined count, then ascending group value.",
                            ],
                            non_degeneracy_metrics={
                                "joined_count": count_value,
                                "candidate_groups": len(rows),
                                "min_required_winner_count": args.min_group_size,
                                "join_key": join_key,
                                "join_tables": [left["table"], right["table"]],
                                "group_table": group_table,
                                "top_rows": jsonable(rows[:5]),
                            },
                        )
                    )
                    top_items = topk_answer_values(rows, "group_key", args, limit=3)
                    if len(top_items) >= 2:
                        facts.append(
                            make_sql_fact(
                                dataset=dataset,
                                logical_name=logical_name,
                                cfg=cfg,
                                table=[left["table"], right["table"]],
                                fact_type="join_group_count_topk_list",
                                source_context=source_context,
                                evidence_sql=sql,
                                observation=observation,
                                answer=top_items,
                                answer_key="group_key",
                                expected_answer={"type": "list", "value": top_items, "normalization": "ordered_contains"},
                                validator_template="ordered_contains",
                                validator_args={"items": top_items},
                                data_requirements={
                                    "tables": [left["table"], right["table"]],
                                    "fields": [join_key, group_col],
                                    "joins": [{"left": left["table"], "right": right["table"], "on": join_key}],
                                    "filters": ["valid non-empty grouping values", "non-null join keys"],
                                    "operations": ["join", "group", "count", "rank", "top-k list"],
                                },
                                operation_tags=[
                                    "join",
                                    "join_or_lookup",
                                    "multi_table_join",
                                    "aggregation",
                                    "ranking",
                                    "set_answer",
                                    "list_answer",
                                    "topk_answer",
                                ],
                                query_requirements=[
                                    f"Require joining {left['table']} and {right['table']} on {join_key}.",
                                    f"Ask for the top {len(top_items)} joined groups by joined-row count as an ordered list.",
                                    "Specify the tie-breaker: descending joined count, then ascending group value.",
                                    "Require the final answer to preserve the ranked order.",
                                ],
                                non_degeneracy_metrics={
                                    "joined_count": count_value,
                                    "candidate_groups": len(rows),
                                    "top_k": len(top_items),
                                    "top_items": top_items,
                                    "min_required_winner_count": args.min_group_size,
                                    "join_key": join_key,
                                    "join_tables": [left["table"], right["table"]],
                                    "group_table": group_table,
                                    "top_rows": jsonable(rows[:5]),
                                },
                                answer_mode="topk_list",
                                top_k=len(top_items),
                            )
                        )
                    if len(facts) >= args.max_candidate_facts_per_dataset:
                        return facts
                    for value_table, value_alias, value_col in numeric_candidates[: args.max_numeric_columns_per_table]:
                        if value_col in {join_key, group_col}:
                            continue
                        numeric_sql = (
                            f"SELECT {group_key_expr(group_col, group_alias)} AS group_key, "
                            f"COUNT(*) AS joined_count, "
                            f"ROUND(SUM({sql_column_ref(value_col, value_alias)}), 6) AS total_value "
                            f"FROM {quote_identifier(left['table'])} AS {quote_identifier('a')} "
                            f"JOIN {quote_identifier(right['table'])} AS {quote_identifier('b')} "
                            f"ON {sql_column_ref(join_key, 'a')} = {sql_column_ref(join_key, 'b')} "
                            f"WHERE {group_filter} "
                            f"AND {sql_column_ref(join_key, 'a')} IS NOT NULL "
                            f"AND {sql_column_ref(join_key, 'b')} IS NOT NULL "
                            f"AND {sql_column_ref(value_col, value_alias)} IS NOT NULL "
                            f"GROUP BY {group_key_expr(group_col, group_alias)} "
                            f"HAVING COUNT(*) >= {int(args.min_group_size)} AND SUM({sql_column_ref(value_col, value_alias)}) IS NOT NULL "
                            "ORDER BY total_value DESC, group_key ASC LIMIT 5"
                        )
                        try:
                            numeric_rows, numeric_observation = execute_sql_client(logical_name, cfg, dataset_dir, numeric_sql, args.query_row_limit)
                        except Exception:
                            continue
                        if not numeric_rows:
                            continue
                        numeric_top = numeric_rows[0]
                        numeric_total = numeric_top.get("total_value")
                        numeric_count = numeric_top.get("joined_count")
                        if not is_quality_group_key(numeric_top.get("group_key"), args):
                            continue
                        if (
                            not isinstance(numeric_total, (int, float))
                            or not isinstance(numeric_count, (int, float))
                            or float(numeric_count) < args.min_group_size
                        ):
                            continue
                        facts.append(
                            make_sql_fact(
                                dataset=dataset,
                                logical_name=logical_name,
                                cfg=cfg,
                                table=[left["table"], right["table"]],
                                fact_type="join_numeric_sum_value",
                                source_context=source_context,
                                evidence_sql=numeric_sql,
                                observation=numeric_observation,
                                answer=numeric_total,
                                answer_key="total_value",
                                expected_answer={"type": "number", "value": float(numeric_total)},
                                validator_template="numeric_tolerance",
                                validator_args=numeric_validator_args(numeric_total),
                                data_requirements={
                                    "tables": [left["table"], right["table"]],
                                    "fields": [join_key, group_col, value_col],
                                    "joins": [{"left": left["table"], "right": right["table"], "on": join_key}],
                                    "filters": ["valid non-empty grouping values", "non-null join keys", f"{value_col} is not null"],
                                    "operations": ["join", "group", "sum", "rank", "numeric answer"],
                                },
                                operation_tags=[
                                    "join",
                                    "join_or_lookup",
                                    "multi_table_join",
                                    "aggregation",
                                    "numeric_aggregation",
                                    "numeric_answer",
                                    "ranking",
                                ],
                                query_requirements=[
                                    f"Require joining {left['table']} and {right['table']} on {join_key}.",
                                    f"Require summing numeric field {value_col} after the join.",
                                    "Ask for the numeric total of the winning joined group, not the group name.",
                                    "Specify the tie-breaker: descending summed total, then ascending group value.",
                                    "Require a numeric answer rounded to six decimal places when needed.",
                                ],
                                non_degeneracy_metrics={
                                    "joined_count": numeric_count,
                                    "winner_total_value": numeric_total,
                                    "winner_group_key": numeric_top.get("group_key"),
                                    "candidate_groups": len(numeric_rows),
                                    "min_required_winner_count": args.min_group_size,
                                    "join_key": join_key,
                                    "join_tables": [left["table"], right["table"]],
                                    "group_table": group_table,
                                    "value_table": value_table,
                                    "top_rows": jsonable(numeric_rows[:5]),
                                },
                            )
                        )
                        if len(facts) >= args.max_candidate_facts_per_dataset:
                            return facts
    return facts


def mine_db_facts(args: argparse.Namespace) -> None:
    signature_rows = read_jsonl(Path(args.signature_jsonl)) if args.signature_jsonl else []
    seed_rows = read_jsonl(Path(args.seed_jsonl)) if args.seed_jsonl else []
    datasets = sorted({str(row.get("dataset")) for row in signature_rows + seed_rows if row.get("dataset")})
    if args.datasets:
        allowed = {item.strip() for item in args.datasets.split(",") if item.strip()}
        datasets = [dataset for dataset in datasets if dataset in allowed]
    if args.max_datasets:
        datasets = datasets[: args.max_datasets]

    facts: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    bench_root = Path(args.bench_root)
    for dataset in datasets:
        dataset_dir = bench_root / f"query_{dataset}"
        if not dataset_dir.exists():
            skipped.append({"dataset": dataset, "reason": f"dataset_dir_missing:{dataset_dir}"})
            continue
        clients = load_db_clients(dataset_dir)
        source_contexts = source_contexts_for_dataset(signature_rows, dataset)
        source_context = source_contexts[0] if source_contexts else source_context_for_dataset(signature_rows, dataset)
        dataset_candidates: list[dict[str, Any]] = []
        for logical_name, cfg in clients.items():
            if str(cfg.get("db_type") or "").casefold() not in {"sqlite", "duckdb"}:
                continue
            table_infos: list[dict[str, Any]] = []
            for table in list_sql_tables(str(logical_name), cfg, dataset_dir)[: args.max_tables_per_db]:
                columns = list_sql_columns(str(logical_name), cfg, dataset_dir, table)
                table_infos.append({"table": table, "columns": columns})
            client_candidates: list[dict[str, Any]] = []
            if not bool(getattr(args, "single_table_first", False)):
                dataset_candidates.extend(
                    mine_sql_join_facts(
                        dataset,
                        str(logical_name),
                        cfg,
                        dataset_dir,
                        table_infos,
                        source_context,
                        args,
                    )
                )
            for table_info in table_infos:
                if len(client_candidates) + len(dataset_candidates) >= args.max_candidate_facts_per_dataset:
                    break
                client_candidates.extend(
                    mine_sql_group_facts_for_table(
                        dataset,
                        str(logical_name),
                        cfg,
                        dataset_dir,
                        table_info["table"],
                        table_info["columns"],
                        source_context,
                        args,
                    )
                )
            if bool(getattr(args, "single_table_first", False)) and len(client_candidates) + len(dataset_candidates) < args.max_candidate_facts_per_dataset:
                client_candidates.extend(
                    mine_sql_join_facts(
                        dataset,
                        str(logical_name),
                        cfg,
                        dataset_dir,
                        table_infos,
                        source_context,
                        args,
                    )
                )
            dataset_candidates.extend(client_candidates)
            if len(dataset_candidates) >= args.max_candidate_facts_per_dataset:
                dataset_candidates = dataset_candidates[: args.max_candidate_facts_per_dataset]
        dataset_candidates = [attach_best_source_context(fact, source_contexts) for fact in dataset_candidates]
        selected = select_diverse_mined_facts(dataset_candidates, source_context, args.max_facts_per_dataset)
        facts.extend(selected)
        if not selected:
            skipped.append({"dataset": dataset, "reason": "no_non_degenerate_sql_facts_found"})
    write_jsonl(Path(args.output_jsonl), facts)
    if args.rejected_jsonl:
        write_jsonl(Path(args.rejected_jsonl), skipped)
    print(
        json.dumps(
            {
                "datasets": len(datasets),
                "facts": len(facts),
                "skipped": len(skipped),
                "output_jsonl": args.output_jsonl,
                "rejected_jsonl": args.rejected_jsonl,
            },
            ensure_ascii=False,
            indent=2,
        )
    )


def fact_source_task_type(fact: dict[str, Any]) -> str:
    signature = fact.get("source_task_signature", {}) if isinstance(fact.get("source_task_signature"), dict) else {}
    task_type = str(signature.get("task_type") or "").strip()
    if task_type:
        return task_type
    families = signature.get("operation_families", [])
    if isinstance(families, list) and families:
        return "+".join(sorted(str(item) for item in families[:4]))
    return "unknown"


def fact_answer_shape(fact: dict[str, Any]) -> str:
    expected = fact.get("expected_answer", {}) if isinstance(fact.get("expected_answer"), dict) else {}
    shape = str(expected.get("type") or "").strip()
    if shape:
        return shape
    signature = fact.get("source_task_signature", {}) if isinstance(fact.get("source_task_signature"), dict) else {}
    return str(signature.get("answer_shape") or "unknown")


def fact_global_priority(fact: dict[str, Any]) -> tuple[int, float, str]:
    type_base = {
        "join_group_count_topk_list": 120,
        "join_numeric_sum_value": 115,
        "join_group_count_ranking": 110,
        "normalized_text_topk_list": 100,
        "temporal_group_count_ranking": 90,
        "group_count_topk_list": 85,
        "group_numeric_sum_value": 80,
        "normalized_text_group_count": 70,
        "group_count_ranking": 40,
    }.get(str(fact.get("fact_type") or ""), 10)
    validator_bonus = {
        "ordered_contains": 30,
        "unordered_set_contains": 25,
        "numeric_tolerance": 20,
        "normalized_contains_all": 5,
    }.get(str(fact.get("validator_template") or ""), 0)
    metrics = fact.get("non_degeneracy_metrics", {}) if isinstance(fact.get("non_degeneracy_metrics"), dict) else {}
    candidate_groups = float(metrics.get("candidate_groups") or 0)
    return (type_base + validator_bonus, candidate_groups, str(fact.get("fact_id") or ""))


def balanced_db_mined_facts(facts: list[dict[str, Any]], limit: int, strategy: str = "balanced") -> list[dict[str, Any]]:
    if limit <= 0:
        return facts
    if strategy == "sequential":
        return facts[:limit]

    ranked = sorted(facts, key=fact_global_priority, reverse=True)
    selected: list[dict[str, Any]] = []
    seen: set[str] = set()

    def add_fact(fact: dict[str, Any]) -> None:
        fact_id = str(fact.get("fact_id") or stable_hash(fact))
        if fact_id in seen or len(selected) >= limit:
            return
        selected.append(fact)
        seen.add(fact_id)

    def cover_dimension(name: str, key_fn: Callable[[dict[str, Any]], str]) -> None:
        buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for fact in ranked:
            buckets[key_fn(fact)].append(fact)
        # Prefer sparse buckets first so rare validator/task families are not
        # drowned out by large datasets.
        for _, bucket in sorted(buckets.items(), key=lambda item: (len(item[1]), item[0])):
            if len(selected) >= limit:
                return
            for fact in bucket:
                fact_id = str(fact.get("fact_id") or stable_hash(fact))
                if fact_id not in seen:
                    add_fact(fact)
                    break

    cover_dimension("validator_template", lambda fact: str(fact.get("validator_template") or "unknown"))
    cover_dimension("source_task_type", fact_source_task_type)
    cover_dimension("fact_type", lambda fact: str(fact.get("fact_type") or "unknown"))
    cover_dimension("answer_shape", fact_answer_shape)
    cover_dimension("source_dataset", lambda fact: str(fact.get("source_dataset") or fact.get("dataset") or "unknown"))

    composite_buckets: dict[tuple[str, str, str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for fact in ranked:
        composite_buckets[
            (
                str(fact.get("validator_template") or "unknown"),
                fact_source_task_type(fact),
                str(fact.get("fact_type") or "unknown"),
                fact_answer_shape(fact),
                str(fact.get("source_dataset") or fact.get("dataset") or "unknown"),
            )
        ].append(fact)
    keys = sorted(composite_buckets, key=lambda key: (len(composite_buckets[key]), key))
    while len(selected) < limit and keys:
        progressed = False
        for key in list(keys):
            bucket = composite_buckets[key]
            while bucket:
                fact = bucket.pop(0)
                fact_id = str(fact.get("fact_id") or stable_hash(fact))
                if fact_id in seen:
                    continue
                add_fact(fact)
                progressed = True
                break
            if not bucket:
                keys.remove(key)
            if len(selected) >= limit:
                break
        if not progressed:
            break
    return selected[:limit]


def mined_fact_coverage(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "datasets": dict(Counter(str(row.get("source_dataset") or row.get("dataset") or "unknown") for row in rows)),
        "source_task_types": dict(Counter(fact_source_task_type(row) for row in rows)),
        "fact_types": dict(Counter(str(row.get("fact_type") or "unknown") for row in rows)),
        "validator_templates": dict(Counter(str(row.get("validator_template") or "unknown") for row in rows)),
        "answer_shapes": dict(Counter(fact_answer_shape(row) for row in rows)),
    }


def make_db_mined_task_packets(args: argparse.Namespace) -> None:
    facts = read_jsonl(Path(args.fact_jsonl))
    if args.max_packets:
        facts = balanced_db_mined_facts(facts, args.max_packets, args.sampling_strategy)
    prompt = read_text(Path(args.prompt))
    packets = []
    for idx, fact in enumerate(facts):
        fact_id = str(fact.get("fact_id") or f"db_fact_{idx:05d}")
        packets.append(
            {
                "packet_id": f"db_mined_task_{fact_id}",
                "system_prompt": prompt,
                "input": {
                    "strict_generation_policy": strict_generation_policy(),
                    "db_mined_fact": fact,
                    "required_output_fields": [
                        "generation_strategy",
                        "provenance",
                        "source_task_signature",
                        "signature_alignment",
                        "evidence_card",
                        "query",
                        "candidate_solution",
                        "expected_answer",
                        "validator_template",
                        "validator_args",
                        "reward_spec",
                        "non_degeneracy_metrics",
                        "hint_refs",
                        "hint_selection_rationale",
                    ],
                },
                "expected_output": (
                    "One JSON candidate task built around the supplied db_mined_fact. Keep the answer and "
                    "validator exactly consistent with the fact, write a non-leaking user query, and preserve "
                    "non_degeneracy_metrics."
                ),
            }
        )
    write_jsonl(Path(args.output_jsonl), packets)
    print(
        json.dumps(
            {
                "db_mined_task_packets": len(packets),
                "output_jsonl": args.output_jsonl,
                "sampling_strategy": args.sampling_strategy,
                "coverage": mined_fact_coverage(facts),
            },
            ensure_ascii=False,
            indent=2,
        )
    )


def truncate(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    return text[:limit] + f"\n...[truncated to {limit} chars]"


def normalize_hint_text(text: str) -> str:
    return re.sub(r"\s+", " ", str(text).casefold()).strip()


def source_dataset_for_candidate(candidate: dict[str, Any]) -> str:
    provenance = candidate.get("provenance") if isinstance(candidate.get("provenance"), dict) else {}
    source = str(provenance.get("source_dataset") or "").strip()
    if source:
        return source
    dataset = str(candidate.get("dataset", "")).strip()
    if dataset.startswith("synthetic_"):
        return dataset.removeprefix("synthetic_")
    return dataset


def load_hint_catalog(path: str = "", bench_root: str = str(DEFAULT_BENCH_ROOT)) -> dict[str, Any]:
    if path:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    root = Path(bench_root)
    if root.exists():
        return build_hint_catalog([seed.to_record() for seed in discover_seed_tasks(root)])
    return {"version": 1, "policy": "reuse_existing_dataset_hints_only", "datasets": {}}


def load_official_anchor_catalog(bench_root: str = str(DEFAULT_BENCH_ROOT)) -> dict[tuple[str, int], dict[str, Any]]:
    root = Path(bench_root)
    if not root.exists():
        return {}
    anchors: dict[tuple[str, int], dict[str, Any]] = {}
    for seed in discover_seed_tasks(root):
        record = seed.to_record()
        anchors[(seed.dataset, seed.query_id)] = {
            "source_query": seed.query,
            "source_ground_truth_summary": record.get("source_ground_truth_summary", {}),
            "source_validate_summary": record.get("source_validate_summary", {}),
            "source_validation_style": record.get("source_validation_style", "unknown"),
            "source_task_signature": source_task_signature_from_seed(record),
        }
    return anchors


def allowed_hints_for_candidate(candidate: dict[str, Any], hint_catalog: dict[str, Any] | None) -> tuple[str, list[dict[str, str]]]:
    source_dataset = source_dataset_for_candidate(candidate)
    datasets = (hint_catalog or {}).get("datasets", {}) if isinstance(hint_catalog, dict) else {}
    record = datasets.get(source_dataset, {}) if isinstance(datasets, dict) else {}
    hints = record.get("hints", []) if isinstance(record, dict) else []
    return source_dataset, [hint for hint in hints if isinstance(hint, dict) and hint.get("id") and hint.get("text")]


def is_db_mining_first_candidate(candidate: dict[str, Any]) -> bool:
    provenance = candidate.get("provenance") if isinstance(candidate.get("provenance"), dict) else {}
    return (
        str(candidate.get("generation_strategy", "")).casefold() == "db_mining_first"
        or str(provenance.get("generation_method", "")).casefold() == "db_mining_first"
        or bool(provenance.get("fact_id"))
        or bool(candidate.get("db_mined_fact"))
        or bool(candidate.get("non_degeneracy_metrics"))
    )


def is_explore_and_validate_candidate(candidate: dict[str, Any]) -> bool:
    provenance = candidate.get("provenance") if isinstance(candidate.get("provenance"), dict) else {}
    return (
        str(candidate.get("generation_strategy", "")).casefold() == "explore_and_validate_dab"
        or str(provenance.get("generation_method", "")).casefold() == "explore_and_validate_dab"
    )


def resolve_candidate_hints(candidate: dict[str, Any], hint_catalog: dict[str, Any] | None) -> dict[str, Any]:
    source_dataset, allowed = allowed_hints_for_candidate(candidate, hint_catalog)
    allowed_by_id = {str(item["id"]): str(item["text"]) for item in allowed}
    allowed_by_norm = {normalize_hint_text(item["text"]): str(item["text"]) for item in allowed}
    risks: list[str] = []
    selected: list[str] = []
    selected_refs: list[str] = []

    raw_refs = candidate.get("hint_refs", [])
    if raw_refs is None:
        raw_refs = []
    if not isinstance(raw_refs, list):
        risks.append("hint_refs_not_list")
        raw_refs = []
    for ref in raw_refs:
        ref_text = str(ref)
        if ref_text not in allowed_by_id:
            risks.append("hint_ref_not_allowed")
            continue
        if allowed_by_id[ref_text] not in selected:
            selected.append(allowed_by_id[ref_text])
            selected_refs.append(ref_text)

    raw_hints = candidate.get("hints", [])
    if raw_hints in (None, ""):
        raw_hints = []
    if not isinstance(raw_hints, list):
        risks.append("hints_not_list")
        raw_hints = []
    for item in raw_hints:
        text = str(item.get("text", "")) if isinstance(item, dict) else str(item)
        norm = normalize_hint_text(text)
        if not norm:
            continue
        if norm not in allowed_by_norm:
            risks.append("candidate_hint_not_allowed")
            continue
        canonical = allowed_by_norm[norm]
        if canonical not in selected:
            selected.append(canonical)
            for hint_id, hint_text in allowed_by_id.items():
                if hint_text == canonical and hint_id not in selected_refs:
                    selected_refs.append(hint_id)
                    break

    rationale_allowed = (
        (is_db_mining_first_candidate(candidate) or is_explore_and_validate_candidate(candidate))
        and candidate.get("hint_selection_rationale")
    )
    if allowed and not selected and not rationale_allowed:
        risks.append("missing_dataset_hint_refs")
    if not allowed and (raw_refs or raw_hints):
        risks.append("no_dataset_hint_catalog")
    if len(allowed) > 3 and len(selected) == len(allowed):
        risks.append("overbroad_hint_selection")

    rationale = candidate.get("hint_selection_rationale", {})
    if selected_refs and not rationale:
        risks.append("missing_hint_selection_rationale")
    if rationale and not isinstance(rationale, (dict, list, str)):
        risks.append("invalid_hint_selection_rationale")

    return {
        "source_dataset": source_dataset,
        "allowed_hint_ids": list(allowed_by_id.keys()),
        "selected_hint_refs": selected_refs,
        "selected_hints": selected,
        "risks": sorted(set(risks)),
    }


def has_placeholder_text(value: Any) -> bool:
    text = json.dumps(value, ensure_ascii=False).casefold() if isinstance(value, (dict, list)) else str(value).casefold()
    markers = [
        "<computed",
        "<integer",
        "<license",
        "<top",
        "<repo",
        "<project",
        "database-computed",
        "computed deterministically from the database",
        "computed from database",
        "benchmark harness should replace",
        "placeholder",
        "same semantic target",
    ]
    return any(marker in text for marker in markers)


def validate_expected_answer(expected: Any) -> list[str]:
    risks: list[str] = []
    if not isinstance(expected, dict):
        return ["expected_answer_not_object"]
    if "type" not in expected or "value" not in expected:
        risks.append("expected_answer_missing_type_or_value")
    value = expected.get("value")
    if value in (None, "", [], {}):
        risks.append("expected_answer_empty")
    if has_placeholder_text(value):
        risks.append("expected_answer_placeholder")
    return risks


def validate_template_validator_spec(candidate: dict[str, Any]) -> list[str]:
    risks: list[str] = []
    template = candidate.get("validator_template")
    args = candidate.get("validator_args", {})
    if template is None:
        # Backward-compatible path for old rows, but new generation should not use it.
        return ["validator_template_missing"]
    if template not in VALIDATOR_TEMPLATE_NAMES:
        risks.append("unknown_validator_template")
        return risks
    if template in STRICT_WEAK_VALIDATORS:
        risks.append("weak_contains_all_validator_disallowed")
    if not isinstance(args, dict):
        return ["validator_args_not_object"]
    if has_placeholder_text(args):
        risks.append("validator_args_placeholder")
    if template in {"contains_all", "normalized_contains_all", "ordered_contains", "unordered_set_contains"}:
        if not isinstance(args.get("items"), list) or not args.get("items"):
            risks.append("validator_items_missing")
    if template == "numeric_tolerance":
        if "expected" not in args:
            risks.append("validator_expected_missing")
        if "tolerance" not in args:
            risks.append("validator_tolerance_missing")
    if template == "numeric_list_tolerance":
        if not isinstance(args.get("expected"), list) or not args.get("expected"):
            risks.append("validator_expected_list_missing")
        if "tolerance" not in args:
            risks.append("validator_tolerance_missing")
    if template == "json_exact_fields":
        if not isinstance(args.get("expected"), dict) or not args.get("expected"):
            risks.append("validator_expected_object_missing")
    if template == "name_value_proximity":
        if not isinstance(args.get("pairs"), list) or not args.get("pairs"):
            risks.append("validator_pairs_missing")
    return risks


def validate_strict_task_policy(candidate: dict[str, Any]) -> list[str]:
    risks: list[str] = []
    query = str(candidate.get("query", "")).strip()
    query_l = query.casefold()
    difficulty = str(candidate.get("difficulty", "")).casefold()
    task_type = str(candidate.get("task_type", "")).casefold()
    source_signature = candidate.get("source_task_signature", {}) if isinstance(candidate.get("source_task_signature"), dict) else {}
    source_ops = set(map(str, source_signature.get("operation_families", []) or []))
    source_types = set(map(str, source_signature.get("db_source_types", []) or []))
    source_difficulty = str(source_signature.get("difficulty_band", "")).casefold()
    template = str(candidate.get("validator_template", "")).casefold()
    expected = candidate.get("expected_answer", {}) if isinstance(candidate.get("expected_answer"), dict) else {}
    expected_type = str(expected.get("type", "")).casefold()
    validator_args = candidate.get("validator_args", {}) if isinstance(candidate.get("validator_args"), dict) else {}
    solution = candidate.get("candidate_solution", {}) if isinstance(candidate.get("candidate_solution"), dict) else {}
    python_code = str(solution.get("python") or "")
    sql_parts = [
        str(item.get("query") if isinstance(item, dict) else item)
        for item in (solution.get("sql") or [])
    ]
    for call in extract_query_db_calls(python_code):
        query_text = call.get("query", "")
        if query_text:
            sql_parts.append(str(query_text))
    sql_text = " ".join(sql_parts).casefold()
    operations = " ".join(
        map(str, (candidate.get("data_requirements", {}) or {}).get("operations", []))
    ).casefold() if isinstance(candidate.get("data_requirements", {}), dict) else ""

    if difficulty == "hard" and source_difficulty != "hard" and not is_explore_and_validate_candidate(candidate):
        risks.append("strict_disallows_hard_difficulty_without_hard_source")
    if task_type in STRICT_DISALLOWED_TASK_TYPES:
        allowed_by_source = (
            (task_type == "multi_hop_join" and "join_or_lookup" in source_ops)
            or (task_type == "mongo_only" and any("mongo" in item for item in source_types))
            or (task_type == "mixed_sql_mongo" and (len(source_types) > 1 or str(candidate.get("generation_strategy")) == "explore_and_validate_dab"))
            or (task_type == "json_heavy" and "json_or_nested" in source_ops)
        )
        if not allowed_by_source:
            risks.append(f"strict_disallows_task_type:{task_type}")
    if task_type and task_type not in STRICT_ALLOWED_TASK_TYPES and not (
        task_type == "multi_hop_join" and "join_or_lookup" in source_ops
    ) and not (
        task_type == "mongo_only" and any("mongo" in item for item in source_types)
    ) and not (
        task_type == "mixed_sql_mongo" and (len(source_types) > 1 or str(candidate.get("generation_strategy")) == "explore_and_validate_dab")
    ) and not (
        task_type == "json_heavy" and "json_or_nested" in source_ops
    ):
        risks.append(f"strict_task_type_not_allowed:{task_type}")

    format_markers = [
        "return only",
        "output only",
        "output exactly",
        "return a json",
        "json array",
        "json object",
        "json string",
        "rounded",
        "decimal",
        "ordered",
        "as a number",
        "as an integer",
        "single string",
        "one string",
        "only the",
        "return just",
        "just the",
        "only a",
        "single number",
        "one number",
        "plain string",
        "plain text",
        "comma-separated",
        "sorted alphabetically",
        "with keys",
        "format",
    ]
    if query and not any(marker in query_l for marker in format_markers):
        risks.append("query_missing_explicit_output_format")

    if template in STRICT_WEAK_VALIDATORS:
        risks.append("strict_disallows_contains_all_validator")
    if any(word in query_l for word in ("top ", "highest", "largest", "lowest", "most ", "least ", "rank", "ordered")):
        scalar_string_top1 = (
            template == "normalized_contains_all"
            and expected_type == "string"
            and isinstance(validator_args.get("items"), list)
            and len(validator_args.get("items") or []) == 1
        )
        if template not in {"ordered_contains", "name_value_proximity", "json_exact_fields", "numeric_tolerance"} and not scalar_string_top1:
            risks.append("ranking_query_needs_strict_ordered_validator")
        ordered_text = f"{sql_text} {operations}"
        if not any(marker in ordered_text for marker in ("order by", "sort", "rank", "row_number", "dense_rank", "top")):
            risks.append("ranking_query_missing_ordered_data_operation")
    if any(word in query_l for word in ("how many", "count", "average", "mean", "sum", "ratio", "percentage")):
        aggregation_text = f"{sql_text} {operations}"
        if not any(marker in aggregation_text for marker in ("count(", "avg(", "sum(", "min(", "max(", "group by", "aggregate", "total", "average", "mean", "count")):
            risks.append("aggregation_query_missing_db_side_aggregation")

    if python_code.strip() and "return_answer(" not in python_code:
        risks.append("python_must_call_return_answer")
    if "select *" in sql_text and "limit" not in sql_text:
        risks.append("strict_disallows_select_star_without_limit")
    return sorted(set(risks))


def validate_data_requirements(candidate: dict[str, Any]) -> list[str]:
    risks: list[str] = []
    req = candidate.get("data_requirements", {})
    if not isinstance(req, dict):
        return ["data_requirements_missing"]
    tables = req.get("tables") or req.get("collections") or []
    fields = req.get("fields") or []
    operations = req.get("operations") or []
    if not isinstance(tables, list) or not tables:
        risks.append("data_requirements_missing_tables")
    if not isinstance(fields, list) or not fields:
        risks.append("data_requirements_missing_fields")
    if not isinstance(operations, list) or not operations:
        risks.append("data_requirements_missing_operations")
    op_text = " ".join(map(str, operations)).casefold()
    operation_markers = (
        "filter", "join", "aggregate", "group", "rank", "parse", "normalize", "lookup", "compare",
        "select", "where", "order", "sort", "count", "sum", "avg", "average", "mean", "min", "max",
        "match", "project", "unwind", "compute",
    )
    if not any(word in op_text for word in operation_markers):
        risks.append("data_requirements_no_data_operation")
    return risks


def is_mixed_sql_mongo_candidate(candidate: dict[str, Any]) -> bool:
    provenance = candidate.get("provenance") if isinstance(candidate.get("provenance"), dict) else {}
    target_mix = candidate.get("target_source_mix") if isinstance(candidate.get("target_source_mix"), dict) else {}
    return (
        str(candidate.get("task_type", "")).casefold() == "mixed_sql_mongo"
        or str(candidate.get("target_task_type", "")).casefold() == "mixed_sql_mongo"
        or str(provenance.get("target_task_type", "")).casefold() == "mixed_sql_mongo"
        or str(target_mix.get("target_task_type", "")).casefold() == "mixed_sql_mongo"
    )


def candidate_successful_observations(candidate: dict[str, Any]) -> list[dict[str, Any]]:
    observations: list[dict[str, Any]] = []
    ev = candidate.get("evidence_verification") if isinstance(candidate.get("evidence_verification"), dict) else {}
    for obs in ev.get("observations") or []:
        if isinstance(obs, dict) and obs.get("success"):
            observations.append(obs)
    card = candidate.get("evidence_card") if isinstance(candidate.get("evidence_card"), dict) else {}
    for obs in (card.get("executed_observations") or card.get("observations") or []):
        if isinstance(obs, dict) and obs.get("success"):
            observations.append(obs)
    trace = candidate.get("explore_trace") if isinstance(candidate.get("explore_trace"), list) else []
    for item in trace:
        obs = item.get("observation") if isinstance(item, dict) and isinstance(item.get("observation"), dict) else {}
        if obs.get("success"):
            observations.append(obs)
    return observations


def observation_source_kind(obs: dict[str, Any]) -> str:
    db_type = str(obs.get("db_type") or "").casefold()
    if "mongo" in db_type:
        return "mongo"
    if db_type in SQL_DB_TYPES:
        return "sql"
    query = obs.get("query")
    if isinstance(query, dict):
        return "mongo"
    if isinstance(query, str) and re.search(r"\bselect\b|\bwith\b", query.casefold()):
        return "sql"
    return db_type or "unknown"


def validate_mixed_sql_mongo_usage(candidate: dict[str, Any]) -> list[str]:
    if not is_mixed_sql_mongo_candidate(candidate):
        return []
    risks: list[str] = []
    req = candidate.get("data_requirements", {}) if isinstance(candidate.get("data_requirements"), dict) else {}
    tables = req.get("tables") if isinstance(req.get("tables"), list) else []
    collections = req.get("collections") if isinstance(req.get("collections"), list) else []
    if not tables:
        risks.append("mixed_sql_mongo_missing_sql_tables")
    if not collections:
        risks.append("mixed_sql_mongo_missing_mongo_collections")

    observations = candidate_successful_observations(candidate)
    source_kinds = {observation_source_kind(obs) for obs in observations}
    if "sql" not in source_kinds:
        risks.append("mixed_sql_mongo_missing_successful_sql_observation")
    if "mongo" not in source_kinds:
        risks.append("mixed_sql_mongo_missing_successful_mongo_observation")

    solution = candidate.get("candidate_solution", {}) if isinstance(candidate.get("candidate_solution"), dict) else {}
    python_code = str(solution.get("python") or "")
    query_call_db_names = {str(call.get("db_name")) for call in extract_query_db_calls(python_code) if call.get("db_name")}
    successful_db_names = {str(obs.get("db_name")) for obs in observations if obs.get("db_name")}
    if len(query_call_db_names | successful_db_names) < 2:
        risks.append("mixed_sql_mongo_needs_two_logical_sources")

    ops_text = " ".join(map(str, req.get("operations") or [])).casefold()
    evidence_text = json.dumps(candidate.get("evidence_chain", []), ensure_ascii=False).casefold()
    solution_text = json.dumps(solution, ensure_ascii=False).casefold()
    bridge_markers = (
        "join", "lookup", "bridge", "cross", "source", "map", "mapping", "resolve", "normalize",
        "id", "identifier", "name", "title", "date", "vendor", "business", "article", "repo", "agency", "match", "compare",
    )
    if not any(marker in f"{ops_text} {evidence_text} {solution_text}" for marker in bridge_markers):
        risks.append("mixed_sql_mongo_no_cross_source_bridge")
    return sorted(set(risks))


def validate_evidence_chain(candidate: dict[str, Any]) -> list[str]:
    risks: list[str] = []
    chain = candidate.get("evidence_chain", [])
    explore_mode = is_explore_and_validate_candidate(candidate)
    min_steps = 2 if (is_db_mining_first_candidate(candidate) or explore_mode) else 3
    if not isinstance(chain, list) or len(chain) < min_steps:
        if explore_mode:
            evidence_card = candidate.get("evidence_card") if isinstance(candidate.get("evidence_card"), dict) else {}
            observations = evidence_card.get("executed_observations") or evidence_card.get("observations") or []
            trace = candidate.get("explore_trace") if isinstance(candidate.get("explore_trace"), list) else []
            trace_observations = [
                item.get("observation")
                for item in trace
                if isinstance(item, dict) and isinstance(item.get("observation"), dict) and item.get("observation", {}).get("success")
            ]
            if observations or len(trace_observations) >= 2:
                return []
        return ["evidence_chain_too_short"]
    saw_data_access = False
    saw_transform = False
    saw_verify = False
    for idx, step in enumerate(chain):
        if not isinstance(step, dict):
            risks.append("evidence_chain_step_not_object")
            continue
        action = str(step.get("action", "") or step.get("operation", "")).casefold()
        source = step.get("source") or step.get("table") or step.get("collection") or step.get("fields")
        review = step.get("review_check") or step.get("expected_intermediate") or step.get("why_needed")
        if not source:
            risks.append("evidence_chain_step_missing_source")
        if not action:
            risks.append("evidence_chain_step_missing_action")
        if not review:
            risks.append("evidence_chain_step_missing_review_check")
        if any(word in action for word in ("query", "read", "filter", "join", "aggregate", "group", "parse", "lookup", "normalize")):
            saw_data_access = True
        if any(word in action for word in ("join", "aggregate", "group", "rank", "parse", "normalize", "compare", "compute")):
            saw_transform = True
        if any(word in action for word in ("verify", "validate", "check", "compare")) or idx == len(chain) - 1:
            saw_verify = True
    if not saw_data_access:
        risks.append("evidence_chain_no_data_access")
    if not saw_transform:
        risks.append("evidence_chain_no_transform")
    if not saw_verify:
        risks.append("evidence_chain_no_verification")
    return sorted(set(risks))


def is_zero_like(value: Any) -> bool:
    if isinstance(value, bool) or value is None:
        return False
    if isinstance(value, (int, float)):
        return float(value) == 0.0
    if isinstance(value, str):
        text = value.strip().casefold()
        if text in {"0", "0.0", "zero", "none", "null", "empty", "[]", "{}"}:
            return True
        try:
            return float(text.replace(",", "")) == 0.0
        except ValueError:
            return False
    if isinstance(value, list):
        return len(value) == 0
    if isinstance(value, dict):
        if not value:
            return True
        numeric_values = [v for v in value.values() if isinstance(v, (int, float)) and not isinstance(v, bool)]
        return bool(numeric_values) and all(float(v) == 0.0 for v in numeric_values)
    return False


def validate_training_value_quality(candidate: dict[str, Any]) -> list[str]:
    risks: list[str] = []
    query = str(candidate.get("query", "")).casefold()
    solution = candidate.get("candidate_solution", {}) if isinstance(candidate.get("candidate_solution"), dict) else {}
    sql_text = " ".join(
        str(item.get("query") if isinstance(item, dict) else item)
        for item in (solution.get("sql") or [])
    ).casefold()
    python_text = str(solution.get("python") or "").casefold()
    evidence_text = json.dumps(candidate.get("evidence_chain", []), ensure_ascii=False).casefold()
    expected = candidate.get("expected_answer", {})
    expected_value = expected.get("value") if isinstance(expected, dict) else None
    validator_args = candidate.get("validator_args") if isinstance(candidate.get("validator_args"), dict) else {}
    validator_expected = validator_args.get("expected", expected_value)
    text = " ".join([query, sql_text, python_text, evidence_text])
    sentinel_scan_text = " ".join([query, evidence_text])

    sentinel_markers = [
        "sentinel",
        "nonexistent",
        "non-existent",
        "does not exist",
        "deliberately empty",
        "intentionally empty",
        "no matching",
        "no rows",
        "empty result",
        "not present",
        "00000",
    ]
    if any(marker in sentinel_scan_text for marker in sentinel_markers):
        risks.append("sentinel_or_deliberately_empty_task")

    zeroish = is_zero_like(expected_value) or is_zero_like(validator_expected)
    zero_task_markers = [
        "symmetric difference",
        "difference between",
        "directional_difference",
        "mismatched",
        "mismatch",
        "same artifact identity",
        "same full file identity",
        "forward_count",
        "reverse_count",
        "a_minus_b",
        "b_minus_a",
        "should be zero",
        "equals zero",
    ]
    if zeroish:
        if any(marker in text for marker in zero_task_markers):
            risks.append("zero_valued_identity_or_empty_stat")
        else:
            risks.append("zero_expected_answer_requires_manual_review")

    if "count(*)" in sql_text and zeroish and any(marker in sql_text for marker in ("where", "except", "<>", " not ")):
        risks.append("zero_count_filter_or_difference")
    return sorted(set(risks))


def has_schema_only_task_smell(candidate: dict[str, Any]) -> bool:
    query = str(candidate.get("query", "")).casefold()
    solution = candidate.get("candidate_solution", {}) if isinstance(candidate.get("candidate_solution"), dict) else {}
    sql_text = " ".join(map(str, solution.get("sql") or [])).casefold()
    schema_query_markers = [
        "how many columns",
        "how many tables",
        "column named",
        "columns named",
        "table has a column",
        "tables include",
        "across the schema",
        "database schemas",
    ]
    schema_sql_markers = [
        "pragma_table_info",
        "information_schema.columns",
        "sqlite_master",
        "show tables",
        "describe ",
    ]
    return any(marker in query for marker in schema_query_markers) or (
        any(marker in sql_text for marker in schema_sql_markers)
        and not any(word in query for word in ("record", "row", "entry", "user", "repo", "project", "package", "review", "stock", "business"))
    )


def source_query_id_for_candidate(candidate: dict[str, Any]) -> int | None:
    provenance = candidate.get("provenance") if isinstance(candidate.get("provenance"), dict) else {}
    for value in (provenance.get("source_query_id"), candidate.get("source_query_id"), candidate.get("query_id")):
        try:
            return int(value)
        except (TypeError, ValueError):
            continue
    return None


def normalized_tokens(text: str) -> set[str]:
    stop = {"the", "and", "for", "with", "from", "that", "this", "only", "their", "which", "what", "among"}
    return {
        token
        for token in re.split(r"[^a-zA-Z0-9_]+", str(text).casefold())
        if len(token) >= 3 and token not in stop
    }


def query_similarity(left: str, right: str) -> float:
    a = normalized_tokens(left)
    b = normalized_tokens(right)
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def official_ground_truth_values(anchor: dict[str, Any]) -> list[str]:
    summary = anchor.get("source_ground_truth_summary", {}) if isinstance(anchor, dict) else {}
    values = summary.get("flat_values_preview", []) if isinstance(summary, dict) else []
    out = []
    for value in values:
        text = str(value).strip()
        if len(text) >= 3 and text.casefold() not in {"none", "null", "true", "false"}:
            out.append(text)
    return out


def validate_official_anchor_usage(
    candidate: dict[str, Any],
    official_anchors: dict[tuple[str, int], dict[str, Any]] | None,
) -> list[str]:
    if not official_anchors:
        return []
    source_dataset = source_dataset_for_candidate(candidate)
    source_query_id = source_query_id_for_candidate(candidate)
    if not source_dataset or source_query_id is None:
        return ["official_anchor_provenance_missing"]
    anchor = official_anchors.get((source_dataset, source_query_id))
    if not anchor:
        return ["official_anchor_not_found"]
    risks: list[str] = []
    usage = candidate.get("official_anchor_usage")
    if not isinstance(usage, dict) and not is_db_mining_first_candidate(candidate):
        risks.append("official_anchor_usage_missing")
    source_query = str(anchor.get("source_query") or "")
    similarity = query_similarity(str(candidate.get("query", "")), source_query)
    candidate_answer_text = json.dumps(
        {
            "expected_answer": candidate.get("expected_answer", {}),
            "validator_args": candidate.get("validator_args", {}),
        },
        ensure_ascii=False,
        sort_keys=True,
    ).casefold()
    source_values = official_ground_truth_values(anchor)
    copied_values = [
        value for value in source_values
        if normalize_text(value) and normalize_text(value) in normalize_text(candidate_answer_text)
    ]
    copied_enough = len(copied_values) >= min(2, max(1, len(source_values)))
    numeric_source = bool(source_values) and all(looks_numeric(value) for value in source_values[: min(3, len(source_values))])
    if copied_enough and similarity < 0.45:
        risks.append("copied_official_ground_truth_for_changed_task")
    if numeric_source and copied_values and similarity < 0.45:
        risks.append("copied_official_numeric_answer_for_changed_task")
    return sorted(set(risks))


def candidate_task_signature(candidate: dict[str, Any]) -> dict[str, Any]:
    query = str(candidate.get("query", ""))
    req = candidate.get("data_requirements", {}) if isinstance(candidate.get("data_requirements"), dict) else {}
    operations = [str(x) for x in req.get("operations", []) or []]
    op_text = " ".join(operations + [query])
    operation_families = sorted(set(detect_query_ops(query) + infer_operation_families_from_text(op_text)))
    template = str(candidate.get("validator_template", "") or "")
    expected = candidate.get("expected_answer", {}) if isinstance(candidate.get("expected_answer"), dict) else {}
    answer_type = str(expected.get("type", "") or "").casefold()
    if template in {"numeric_tolerance", "numeric_list_tolerance"} or answer_type in {"number", "numeric", "float", "integer"}:
        answer_shape = "numeric_scalar" if template != "numeric_list_tolerance" else "numeric_list"
    elif template == "normalized_contains_all" and answer_type in {"string", "str"}:
        answer_shape = "single_scalar"
    elif template in {"ordered_contains", "unordered_set_contains", "normalized_contains_all", "contains_all"} or answer_type in {"list", "set", "array"}:
        answer_shape = "list_or_set"
    elif template in {"json_exact_fields", "name_value_proximity"} or answer_type in {"object", "dict", "record"}:
        answer_shape = "name_value_pairs"
    else:
        answer_shape = answer_type or "unknown"
    tables = req.get("tables") or []
    collections = req.get("collections") or []
    source_types = []
    if tables:
        source_types.append("sql")
    if collections:
        source_types.append("mongo")
    return {
        "operation_families": operation_families,
        "answer_shape": answer_shape,
        "validator_style": template or "unknown",
        "db_source_types": sorted(set(source_types)),
        "requires_join_or_lookup": "join_or_lookup" in operation_families or "id_normalization" in operation_families,
        "requires_aggregation": "aggregation" in operation_families,
        "requires_temporal_filter": "temporal" in operation_families,
        "complexity_score": candidate_complexity_score(candidate, operation_families, answer_shape),
        "difficulty_band": str(candidate.get("difficulty") or "").casefold() or "unknown",
    }


def candidate_complexity_score(candidate: dict[str, Any], operation_families: list[str], answer_shape: str) -> int:
    score = 1
    families = set(operation_families)
    score += len(families & {"aggregation", "temporal", "join_or_lookup", "id_normalization", "json_or_nested"})
    if answer_shape in {"list_or_set", "name_value_pairs", "record", "numeric_list"}:
        score += 1
    req = candidate.get("data_requirements", {}) if isinstance(candidate.get("data_requirements"), dict) else {}
    tables = req.get("tables") or []
    collections = req.get("collections") or []
    if len(tables) + len(collections) > 1:
        score += 1
    query = str(candidate.get("query", "")).casefold()
    if any(word in query for word in ("top", "highest", "largest", "lowest", "rank", "ordered")):
        score += 1
    return score


def compatible_answer_shape(source_shape: str, candidate_shape: str) -> bool:
    if not source_shape or source_shape == "unknown" or candidate_shape == "unknown":
        return True
    groups = [
        {"single_scalar", "numeric_scalar", "numeric_record"},
        {"list_or_set", "numeric_list"},
        {"record", "name_value_pairs", "table_or_name_value_pairs"},
    ]
    if source_shape == candidate_shape:
        return True
    return any(source_shape in group and candidate_shape in group for group in groups)


def signature_alignment_report(
    candidate: dict[str, Any],
    official_anchors: dict[tuple[str, int], dict[str, Any]] | None,
) -> dict[str, Any]:
    source_signature = candidate.get("source_task_signature") if isinstance(candidate.get("source_task_signature"), dict) else {}
    source_dataset = source_dataset_for_candidate(candidate)
    source_query_id = source_query_id_for_candidate(candidate)
    if not source_signature and official_anchors and source_dataset and source_query_id is not None:
        anchor = official_anchors.get((source_dataset, source_query_id), {})
        source_signature = anchor.get("source_task_signature", {}) if isinstance(anchor.get("source_task_signature"), dict) else {}
    candidate_signature = candidate_task_signature(candidate)
    explore_mode = is_explore_and_validate_candidate(candidate)
    risks: list[str] = []
    if not source_signature:
        risks.append("explore_source_signature_missing" if explore_mode else "source_signature_missing")
    else:
        source_ops = set(map(str, source_signature.get("operation_families", []) or []))
        candidate_ops = set(map(str, candidate_signature.get("operation_families", []) or []))
        required_ops = source_ops & {"aggregation", "temporal", "join_or_lookup", "id_normalization", "json_or_nested", "numeric_answer", "set_answer"}
        missing_ops = sorted(required_ops - candidate_ops)
        if missing_ops:
            risks.append("explore_signature_operation_shift" if explore_mode else "signature_operation_family_mismatch")
        if not compatible_answer_shape(str(source_signature.get("answer_shape", "")), str(candidate_signature.get("answer_shape", ""))):
            risks.append("explore_signature_answer_shape_shift" if explore_mode else "signature_answer_shape_mismatch")
        source_score = int(source_signature.get("complexity_score") or 0)
        candidate_score = int(candidate_signature.get("complexity_score") or 0)
        if source_score and candidate_score < max(1, source_score - 1):
            risks.append("difficulty_downgraded_from_source")
    needs_evidence_card = bool(source_signature) or explore_mode or str(candidate.get("generation_strategy", "")).casefold().find("evidence") >= 0
    evidence_card = candidate.get("evidence_card")
    if needs_evidence_card and not isinstance(evidence_card, dict):
        risks.append("evidence_card_missing")
    elif isinstance(evidence_card, dict):
        probes = evidence_card.get("probes") or evidence_card.get("executed_probes") or evidence_card.get("observations") or []
        if not isinstance(probes, list) or not probes:
            risks.append("evidence_card_missing_executable_probes")
        if is_zero_like(evidence_card.get("observed_answer")) or is_zero_like(evidence_card.get("candidate_answer")):
            risks.append("evidence_card_empty_or_zero_answer")
    source_ops = set(map(str, source_signature.get("operation_families", []) or [])) if source_signature else set()
    candidate_ops = set(map(str, candidate_signature.get("operation_families", []) or []))
    op_overlap = len(source_ops & candidate_ops) / max(1, len(source_ops | candidate_ops)) if source_ops else 0.0
    score = 1.0
    if "signature_operation_family_mismatch" in risks:
        score -= 0.35
    if "explore_signature_operation_shift" in risks:
        score -= 0.08
    if "signature_answer_shape_mismatch" in risks:
        score -= 0.25
    if "explore_signature_answer_shape_shift" in risks:
        score -= 0.08
    if "difficulty_downgraded_from_source" in risks:
        score -= 0.25
    if "evidence_card_missing" in risks:
        score -= 0.15
    return {
        "score": round(max(0.0, score), 4),
        "operation_overlap": round(op_overlap, 4),
        "source_signature": source_signature,
        "candidate_signature": candidate_signature,
        "risks": sorted(set(risks)),
    }


def judge_candidate(
    candidate: dict[str, Any],
    seen_queries: set[str],
    hint_catalog: dict[str, Any] | None = None,
    official_anchors: dict[tuple[str, int], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    risks: list[str] = []
    explore_mode = is_explore_and_validate_candidate(candidate)
    required = [
        "dataset",
        "query",
        "expected_answer",
        "validator_template",
        "validator_args",
        "solution_plan",
        "candidate_solution",
        "data_requirements",
        "evidence_chain",
        "hint_refs",
        "hint_selection_rationale",
        "reward_spec",
    ]
    missing = [key for key in required if key not in candidate]
    if missing:
        risks.append("missing_fields:" + ",".join(missing))

    query = str(candidate.get("query", "")).strip()
    if not query:
        risks.append("empty_query")
    if len(query) > 2000:
        risks.append("query_too_long")
    normalized_query = normalize_text(query)
    if normalized_query in seen_queries:
        risks.append("duplicate_query")
    seen_queries.add(normalized_query)

    expected = candidate.get("expected_answer", {})
    risks.extend(validate_expected_answer(expected))
    answer_text = answer_to_leakage_text(expected.get("value", "") if isinstance(expected, dict) else "")
    hint_resolution = resolve_candidate_hints(candidate, hint_catalog)
    risks.extend(hint_resolution["risks"])
    hints_text = " ".join(hint_resolution["selected_hints"])
    db_description_text = str(candidate.get("db_description", ""))
    if len(hints_text) > 3000:
        risks.append("hints_too_long")
    if len(db_description_text) > 20000:
        risks.append("db_description_too_long")
    if leaks_answer(answer_text, hints_text):
        risks.append("answer_leakage_in_hint")
    if leaks_answer(answer_text, query):
        risks.append("answer_leakage_in_query")
    if leaks_answer(answer_text, db_description_text):
        risks.append("answer_leakage_in_db_description")

    risks.extend(validate_template_validator_spec(candidate))
    risks.extend(validate_strict_task_policy(candidate))
    validator = candidate.get("validator", {})
    if validator:
        risks.append("freeform_validator_present")

    solution_plan = candidate.get("solution_plan", [])
    if not isinstance(solution_plan, list) or len(solution_plan) < 2:
        risks.append("solution_plan_too_short")

    solution = candidate.get("candidate_solution", {})
    if not isinstance(solution, dict) or not any(solution.get(k) for k in ("sql", "mongo", "python")):
        risks.append("no_executable_candidate_solution")
    else:
        risks.extend(scan_solution_risks(solution, candidate))

    risks.extend(validate_data_requirements(candidate))
    risks.extend(validate_mixed_sql_mongo_usage(candidate))
    risks.extend(validate_evidence_chain(candidate))
    risks.extend(validate_training_value_quality(candidate))
    if has_schema_only_task_smell(candidate):
        risks.append("schema_only_task")
    risks.extend(validate_official_anchor_usage(candidate, official_anchors))
    signature_report = signature_alignment_report(candidate, official_anchors)
    risks.extend(signature_report.get("risks", []))

    reward_spec = candidate.get("reward_spec", {})
    if not isinstance(reward_spec, dict):
        risks.append("reward_spec_not_object")
    elif str(reward_spec.get("primary", "programmatic_validator")) not in {"programmatic_validator", "validator", "rule"}:
        risks.append("reward_primary_not_programmatic_validator")

    if has_external_knowledge_smell(query):
        risks.append("possible_external_knowledge")

    evidence_verification = candidate.get("evidence_verification") if isinstance(candidate.get("evidence_verification"), dict) else {}
    if explore_mode and evidence_verification and not evidence_verification.get("verified"):
        risks.append("explore_evidence_verification_failed")

    source_alignment_score = float(signature_report.get("score", 0.0))
    if explore_mode:
        source_alignment_score = max(source_alignment_score, 0.75)

    dimension_scores = {
        "solvability": 0.2 if "no_executable_candidate_solution" in risks else 0.85,
        "validator_stability": 0.25 if any("validator" in risk for risk in risks) else 0.9,
        "schema_grounding": 0.45 if any(r in risks for r in ("unbounded_sql_select_star", "unbounded_sql_no_limit_or_aggregation", "unbounded_mongo_query")) else 0.75,
        "hint_policy": 0.2 if any("hint" in risk for risk in risks) else 0.95,
        "evidence_chain": 0.2 if any("evidence_chain" in risk for risk in risks) else 0.9,
        "data_task_quality": 0.2 if "schema_only_task" in risks else 0.85,
        "no_answer_leakage": 0.1 if any(r.startswith("answer_leakage") for r in risks) else 0.95,
        "source_signature_alignment": source_alignment_score,
        "difficulty": difficulty_score(candidate),
        "diversity": 0.4 if "duplicate_query" in risks else 0.75,
        "reward_clarity": 0.35 if any(r.startswith("missing_fields") for r in risks) else 0.85,
    }
    score = round(sum(dimension_scores.values()) / len(dimension_scores), 4)
    accepted = score >= 0.72 and not hard_reject(risks)
    if explore_mode and evidence_verification:
        accepted = accepted and bool(evidence_verification.get("verified"))
    return {
        "accepted": accepted,
        "judge_mode": "explore_replay_gated" if explore_mode else "seed_aligned_static",
        "score": score,
        "dimension_scores": dimension_scores,
        "risks": sorted(set(risks)),
        "hint_policy": {
            "source_dataset": hint_resolution["source_dataset"],
            "allowed_hint_ids": hint_resolution["allowed_hint_ids"],
            "selected_hint_refs": hint_resolution["selected_hint_refs"],
            "reuse_existing_dataset_hints_only": True,
        },
        "resolved_hints": hint_resolution["selected_hints"],
        "source_signature_alignment": signature_report,
        "reward_recommendation": {
            "primary": "programmatic_validator",
            "secondary": ["format_reward"],
            "do_not_use_llm_judge_as_primary_reward": True,
            "sandbox_manifest_not_required_now": True,
        },
    }

def answer_to_leakage_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        text = json.dumps(value, ensure_ascii=False, sort_keys=True)
    else:
        text = str(value)
    text = text.strip().strip("\"'")
    if text.lower() in {"", "null", "none", "true", "false", "0", "1"}:
        return ""
    return text


def leaks_answer(answer_text: str, text: str) -> bool:
    if not answer_text or len(answer_text) < 3:
        return False
    answer_norm = normalize_text(answer_text)
    text_norm = normalize_text(text)
    if answer_norm and answer_norm in text_norm:
        return True
    if answer_text.startswith("[") or answer_text.startswith("{"):
        return False
    tokens = [tok for tok in re.split(r"[^A-Za-z0-9_]+", answer_text) if len(tok) >= 4]
    return bool(tokens) and sum(1 for tok in tokens if tok.casefold() in text_norm) >= min(2, len(tokens))


def scan_solution_risks(solution: dict[str, Any], candidate: dict[str, Any] | None = None) -> list[str]:
    risks: list[str] = []
    explore_mode = is_explore_and_validate_candidate(candidate or {})
    sql_items = solution.get("sql") or []
    if isinstance(sql_items, str):
        sql_items = [sql_items]
    for sql_item in sql_items:
        sql = sql_item.get("query") if isinstance(sql_item, dict) else sql_item
        db_name = sql_item.get("db_name") if isinstance(sql_item, dict) else None
        if isinstance(sql_item, dict) and not db_name:
            risks.append("explore_sql_object_missing_db_name_review" if explore_mode else "sql_object_missing_db_name")
        lowered = str(sql).lower()
        if "select *" in lowered and "limit" not in lowered:
            risks.append("unbounded_sql_select_star")
        if (
            re.search(r"\bselect\b", lowered)
            and re.search(r"\bfrom\b", lowered)
            and " limit " not in f" {lowered} "
            and not any(marker in lowered for marker in ("count(", "sum(", "avg(", "min(", "max(", "group by"))
        ):
            risks.append("explore_unbounded_sql_review" if explore_mode else "unbounded_sql_no_limit_or_aggregation")
        if any(word in lowered for word in ("drop ", "delete ", "update ", "insert ", "alter ")):
            risks.append("mutating_sql")
    mongo_items = solution.get("mongo") or []
    if isinstance(mongo_items, str):
        mongo_items = [mongo_items]
    for mongo in mongo_items:
        text = json.dumps(mongo, ensure_ascii=False).lower() if not isinstance(mongo, str) else mongo.lower()
        if '"limit"' not in text and "'limit'" not in text:
            risks.append("unbounded_mongo_query")
    python_code = str(solution.get("python") or "")
    if python_code.strip():
        risks.extend(validate_candidate_python(python_code))
        if "query_result" in python_code:
            risks.append("python_placeholder_variable_query_result")
        query_calls = extract_query_db_calls(python_code)
        if not query_calls and not sql_items and not mongo_items:
            risks.append("python_no_query_db_or_sql")
        if "return_answer(" not in python_code:
            risks.append("python_missing_final_answer")
    return sorted(set(risks))


def difficulty_score(candidate: dict[str, Any]) -> float:
    difficulty = str(candidate.get("difficulty", "")).casefold()
    task_type = str(candidate.get("task_type", "")).casefold()
    if difficulty == "hard" or task_type in {"multi_hop_join", "mixed_sql_mongo", "json_heavy"}:
        return 0.85
    if difficulty == "easy":
        return 0.55
    return 0.7


def normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", text.casefold()).strip()


def has_external_knowledge_smell(text: str) -> bool:
    lowered = text.lower()
    markers = ["current", "latest", "today", "real-world", "internet", "web search", "outside the database"]
    return any(marker in lowered for marker in markers)


def hard_reject(risks: list[str]) -> bool:
    hard = {
        "empty_query",
        "answer_leakage_in_hint",
        "answer_leakage_in_query",
        "answer_leakage_in_db_description",
        "candidate_hint_not_allowed",
        "hint_ref_not_allowed",
        "missing_dataset_hint_refs",
        "no_dataset_hint_catalog",
        "no_executable_candidate_solution",
        "possible_external_knowledge",
        "sentinel_or_deliberately_empty_task",
        "zero_valued_identity_or_empty_stat",
        "zero_count_filter_or_difference",
        "mutating_sql",
        "unbounded_sql_no_limit_or_aggregation",
        "sql_object_missing_db_name",
        "python_no_query_db_or_sql",
        "python_missing_final_answer",
        "python_placeholder_variable_query_result",
        "reward_primary_not_programmatic_validator",
        "expected_answer_placeholder",
        "validator_expected_placeholder",
        "custom_validator_placeholder_or_tautology",
        "validator_template_missing",
        "unknown_validator_template",
        "validator_args_not_object",
        "validator_args_placeholder",
        "weak_contains_all_validator_disallowed",
        "strict_disallows_contains_all_validator",
        "strict_disallows_hard_difficulty_without_hard_source",
        "query_missing_explicit_output_format",
        "ranking_query_needs_strict_ordered_validator",
        "ranking_query_missing_ordered_data_operation",
        "aggregation_query_missing_db_side_aggregation",
        "python_must_call_return_answer",
        "strict_disallows_select_star_without_limit",
        "validator_items_missing",
        "validator_expected_missing",
        "validator_tolerance_missing",
        "validator_expected_list_missing",
        "validator_expected_object_missing",
        "validator_pairs_missing",
        "freeform_validator_present",
        "data_requirements_missing",
        "data_requirements_missing_tables",
        "data_requirements_missing_fields",
        "data_requirements_missing_operations",
        "data_requirements_no_data_operation",
        "evidence_chain_too_short",
        "evidence_chain_step_not_object",
        "evidence_chain_step_missing_source",
        "evidence_chain_step_missing_action",
        "evidence_chain_step_missing_review_check",
        "evidence_chain_no_data_access",
        "evidence_chain_no_transform",
        "evidence_chain_no_verification",
        "schema_only_task",
        "source_signature_missing",
        "signature_operation_family_mismatch",
        "evidence_card_missing",
        "evidence_card_missing_executable_probes",
        "evidence_card_empty_or_zero_answer",
        "mixed_sql_mongo_missing_sql_tables",
        "mixed_sql_mongo_missing_mongo_collections",
        "mixed_sql_mongo_missing_successful_sql_observation",
        "mixed_sql_mongo_missing_successful_mongo_observation",
        "mixed_sql_mongo_needs_two_logical_sources",
        "mixed_sql_mongo_no_cross_source_bridge",
    }
    hard_prefixes = (
        "missing_fields",
        "strict_disallows_task_type:",
        "strict_task_type_not_allowed:",
        "python_import_blocked:",
        "python_attr_blocked:",
        "python_call_blocked:",
        "python_syntax_error:",
    )
    return any(risk in hard or risk.startswith(hard_prefixes) for risk in risks)


def dataset_dir_for_candidate(candidate: dict[str, Any], bench_root: Path) -> Path:
    source = source_dataset_for_candidate(candidate)
    if not source:
        source = str(candidate.get("dataset") or "").removeprefix("synthetic_")
    direct = bench_root / f"query_{source}"
    if direct.exists():
        return direct
    target = f"query_{source}".casefold()
    for path in bench_root.glob("query_*"):
        if path.name.casefold() == target:
            return path
    return direct


def load_db_clients(dataset_dir: Path) -> dict[str, dict[str, Any]]:
    config_path = dataset_dir / "db_config.yaml"
    if not config_path.exists():
        return {}
    import yaml

    data = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    clients = data.get("db_clients")
    return clients if isinstance(clients, dict) else {}


SQL_DB_TYPES = {"sqlite", "duckdb", "postgres", "postgresql"}


def db_client_source_kind(cfg: dict[str, Any]) -> str:
    db_type = str(cfg.get("db_type") or cfg.get("type") or "").casefold()
    if "mongo" in db_type:
        return "mongo"
    if db_type in SQL_DB_TYPES:
        return "sql"
    return db_type or "unknown"


def db_source_mix(clients: dict[str, dict[str, Any]]) -> dict[str, Any]:
    sql_clients = sorted(name for name, cfg in clients.items() if db_client_source_kind(cfg) == "sql")
    mongo_clients = sorted(name for name, cfg in clients.items() if db_client_source_kind(cfg) == "mongo")
    return {
        "sql_clients": sql_clients,
        "mongo_clients": mongo_clients,
        "has_sql": bool(sql_clients),
        "has_mongo": bool(mongo_clients),
        "supports_mixed_sql_mongo": bool(sql_clients and mongo_clients),
    }


def normalized_target_task_type(args: argparse.Namespace) -> str:
    return str(getattr(args, "target_task_type", "") or "").strip().casefold()


def target_source_mix_for_dataset(target_task_type: str, dataset: str, clients: dict[str, dict[str, Any]]) -> dict[str, Any]:
    mix = db_source_mix(clients)
    if target_task_type != "mixed_sql_mongo":
        return {"target_task_type": target_task_type or "auto", **mix}
    return {
        "target_task_type": "mixed_sql_mongo",
        "required": "sql_plus_mongo",
        "must_query": ["at least one SQL logical db", "at least one Mongo logical db"],
        "must_bridge": "The final answer must depend on evidence from both SQL and Mongo sources.",
        "candidate_requirements": [
            "Set task_type to mixed_sql_mongo.",
            "data_requirements.tables must name at least one SQL table.",
            "data_requirements.collections must name at least one Mongo collection.",
            "candidate_solution.python must call query_db against at least one SQL logical db and one Mongo logical db.",
            "Use a bridge key or normalization step such as id/name/title/date/vendor/business/article/repo/agency mapping.",
        ],
        "dataset": dataset,
        **mix,
    }


def dataset_supports_target_task_type(dataset_dir: Path, target_task_type: str) -> bool:
    if target_task_type != "mixed_sql_mongo":
        return True
    return bool(db_source_mix(load_db_clients(dataset_dir)).get("supports_mixed_sql_mongo"))


def is_readonly_sql(sql: str) -> bool:
    stripped = re.sub(r"/\*.*?\*/", " ", str(sql), flags=re.DOTALL).strip()
    stripped = re.sub(r"^\s*--.*?$", "", stripped, flags=re.MULTILINE).strip()
    if not stripped:
        return False
    lowered = stripped.casefold()
    if re.search(r"\b(insert|update|delete|drop|alter|create|replace|truncate|attach|detach|copy|vacuum)\b", lowered):
        return False
    return lowered.startswith(("select", "with", "pragma", "show", "describe", "explain"))


def jsonable(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, dict):
        return {str(k): jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [jsonable(v) for v in value]
    return str(value)


def summarize_rows(rows: list[dict[str, Any]], row_limit: int = 5) -> dict[str, Any]:
    preview = [jsonable(row) for row in rows[:row_limit]]
    columns = list(rows[0].keys()) if rows else []
    return {
        "row_count_observed": len(rows),
        "columns": columns,
        "preview": preview,
        "preview_truncated": len(rows) > row_limit,
    }


def execute_sql_client(
    logical_name: str,
    cfg: dict[str, Any],
    dataset_dir: Path,
    sql: str,
    row_limit: int,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if not is_readonly_sql(sql):
        raise ValueError("Only read-only SQL is allowed in evidence verification.")
    db_type = str(cfg.get("db_type") or "").casefold()
    db_path = cfg.get("db_path")
    if not db_path:
        raise ValueError(f"SQL source {logical_name} has no db_path.")
    path = Path(str(db_path))
    if not path.is_absolute():
        path = dataset_dir / path
    if db_type == "sqlite":
        conn = sqlite3.connect(str(path))
        conn.row_factory = sqlite3.Row
        try:
            cur = conn.execute(sql)
            rows = [dict(row) for row in cur.fetchmany(row_limit + 1)]
        finally:
            conn.close()
    elif db_type == "duckdb":
        try:
            import duckdb
        except ModuleNotFoundError as exc:
            raise RuntimeError("duckdb is not installed; run with the dabbench venv.") from exc
        conn = duckdb.connect(str(path), read_only=True)
        try:
            cur = conn.execute(sql)
            columns = [col[0] for col in (cur.description or [])]
            rows = [dict(zip(columns, row)) for row in cur.fetchmany(row_limit + 1)]
        finally:
            conn.close()
    else:
        raise NotImplementedError(f"Unsupported SQL db_type for evidence verification: {db_type or 'unknown'}")
    return rows, {
        "tool": "query_db",
        "db_name": logical_name,
        "db_type": db_type,
        "query": sql,
        "summary": summarize_rows(rows, row_limit=min(5, row_limit)),
    }


def execute_sql_any_client(
    clients: dict[str, dict[str, Any]],
    dataset_dir: Path,
    sql: str,
    row_limit: int,
) -> dict[str, Any]:
    errors = []
    for logical_name, cfg in clients.items():
        db_type = str(cfg.get("db_type") or "").casefold()
        if db_type not in {"sqlite", "duckdb"}:
            continue
        try:
            rows, observation = execute_sql_client(logical_name, cfg, dataset_dir, sql, row_limit)
            observation["rows"] = rows
            observation["success"] = True
            return observation
        except Exception as exc:
            errors.append({"db_name": logical_name, "error": f"{type(exc).__name__}: {exc}"})
    return {"tool": "query_db", "query": sql, "success": False, "errors": errors}


def execute_mongo_query(
    logical_name: str,
    cfg: dict[str, Any],
    query: Any,
    row_limit: int,
    mongo_uri: str,
) -> dict[str, Any]:
    try:
        from pymongo import MongoClient
    except ModuleNotFoundError as exc:
        raise RuntimeError("pymongo is not installed; run with the dabbench venv.") from exc
    if isinstance(query, str):
        query_obj = json.loads(query)
    elif isinstance(query, dict):
        query_obj = query
    else:
        raise ValueError("Mongo query must be a JSON string or object.")
    collection_name = query_obj.get("collection")
    if not collection_name:
        raise ValueError("Mongo query is missing `collection`.")
    limit = min(int(query_obj.get("limit") or row_limit), row_limit)
    db_name = str(cfg.get("db_name") or logical_name)
    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=3000)
    try:
        cursor = client[db_name][collection_name].find(
            query_obj.get("filter") or {},
            query_obj.get("projection"),
        )
        if query_obj.get("sort"):
            cursor = cursor.sort(query_obj["sort"])
        rows = [jsonable(row) for row in cursor.limit(limit)]
    finally:
        client.close()
    return {
        "tool": "query_db",
        "db_name": logical_name,
        "db_type": "mongo",
        "query": query_obj,
        "rows": rows,
        "summary": summarize_rows(rows if all(isinstance(row, dict) for row in rows) else [{"value": row} for row in rows]),
        "success": True,
    }


def extract_query_db_calls(code: str) -> list[dict[str, str]]:
    calls: list[dict[str, str]] = []
    if not code.strip():
        return calls
    try:
        tree = ast.parse(code)
    except SyntaxError:
        return calls
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func_name = node.func.id if isinstance(node.func, ast.Name) else ""
        if func_name != "query_db":
            continue
        db_name = None
        query = None
        if len(node.args) >= 2:
            if isinstance(node.args[0], ast.Constant) and isinstance(node.args[0].value, str):
                db_name = node.args[0].value
            if isinstance(node.args[1], ast.Constant) and isinstance(node.args[1].value, str):
                query = node.args[1].value
        for kw in node.keywords:
            if kw.arg in {"db_name", "database"} and isinstance(kw.value, ast.Constant) and isinstance(kw.value.value, str):
                db_name = kw.value.value
            if kw.arg == "query" and isinstance(kw.value, ast.Constant) and isinstance(kw.value.value, str):
                query = kw.value.value
        if db_name and query:
            calls.append({"db_name": db_name, "query": query})
    return calls


def validate_candidate_python(code: str) -> list[str]:
    risks: list[str] = []
    if not code.strip():
        return risks
    try:
        tree = ast.parse(code)
    except SyntaxError as exc:
        return [f"python_syntax_error:{exc.msg}"]
    blocked_names = {"open", "eval", "exec", "compile", "input", "globals", "locals", "vars"}
    blocked_attrs = {
        "system", "popen", "remove", "unlink", "rmdir", "rename", "chmod",
        "chown", "kill", "connect", "request", "urlopen",
    }
    allowed_imports = {"json", "re", "math", "statistics"}
    for node in ast.walk(tree):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            names = [alias.name.split(".", 1)[0] for alias in getattr(node, "names", [])]
            if isinstance(node, ast.ImportFrom) and node.module:
                names.append(node.module.split(".", 1)[0])
            for name in names:
                if name not in allowed_imports:
                    risks.append(f"python_import_blocked:{name}")
        if isinstance(node, ast.Call):
            if isinstance(node.func, ast.Name) and node.func.id in blocked_names:
                risks.append(f"python_call_blocked:{node.func.id}")
            if isinstance(node.func, ast.Attribute) and node.func.attr in blocked_attrs:
                risks.append(f"python_attr_blocked:{node.func.attr}")
    return sorted(set(risks))


class PythonExecutionTimeout(Exception):
    pass


class ReturnAnswerSignal(Exception):
    def __init__(self, answer: Any):
        self.answer = answer


def execute_candidate_python(
    code: str,
    clients: dict[str, dict[str, Any]],
    dataset_dir: Path,
    row_limit: int,
    timeout_sec: int,
    mongo_uri: str,
) -> tuple[Any, list[dict[str, Any]], list[str]]:
    risks = validate_candidate_python(code)
    if risks:
        return None, [], risks
    observations: list[dict[str, Any]] = []

    def query_db(db_name: str, query: Any) -> list[dict[str, Any]]:
        cfg = clients.get(str(db_name))
        if not isinstance(cfg, dict):
            raise KeyError(f"Unknown logical db_name: {db_name}")
        db_type = str(cfg.get("db_type") or "").casefold()
        if db_type in {"sqlite", "duckdb"}:
            rows, observation = execute_sql_client(str(db_name), cfg, dataset_dir, str(query), row_limit)
            observation["success"] = True
            observations.append(observation)
            return rows
        if db_type == "mongo":
            observation = execute_mongo_query(str(db_name), cfg, query, row_limit, mongo_uri)
            observations.append(observation)
            return observation.get("rows", [])
        raise NotImplementedError(f"Unsupported db_type in query_db: {db_type}")

    def return_answer(answer: Any) -> None:
        raise ReturnAnswerSignal(answer)

    def timeout_handler(signum, frame):
        raise PythonExecutionTimeout(f"Python execution timed out after {timeout_sec}s")

    allowed_modules = {
        "json": json,
        "re": re,
        "math": __import__("math"),
        "statistics": __import__("statistics"),
    }

    def safe_import(name, globals=None, locals=None, fromlist=(), level=0):
        root = str(name).split(".", 1)[0]
        if root not in allowed_modules:
            raise ImportError(f"Import blocked during evidence verification: {name}")
        return __import__(name, globals, locals, fromlist, level)

    safe_builtins = {
        "__import__": safe_import,
        "len": len,
        "sum": sum,
        "min": min,
        "max": max,
        "sorted": sorted,
        "set": set,
        "list": list,
        "dict": dict,
        "tuple": tuple,
        "str": str,
        "int": int,
        "float": float,
        "round": round,
        "abs": abs,
        "any": any,
        "all": all,
        "enumerate": enumerate,
        "range": range,
        "zip": zip,
        "isinstance": isinstance,
        "print": print,
        "Exception": Exception,
        "ValueError": ValueError,
        "KeyError": KeyError,
        "TypeError": TypeError,
    }
    globals_dict = {
        "__builtins__": safe_builtins,
        "query_db": query_db,
        "return_answer": return_answer,
        **allowed_modules,
    }
    old_handler = signal.getsignal(signal.SIGALRM)
    signal.signal(signal.SIGALRM, timeout_handler)
    signal.setitimer(signal.ITIMER_REAL, max(1, int(timeout_sec)))
    try:
        exec(compile(code, "<candidate_solution.python>", "exec"), globals_dict, globals_dict)
    except ReturnAnswerSignal as exc:
        return exc.answer, observations, []
    except Exception as exc:
        return None, observations, [f"python_execution_error:{type(exc).__name__}: {exc}"]
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, old_handler)
    if "answer" in globals_dict:
        return globals_dict["answer"], observations, []
    return None, observations, ["python_return_answer_missing"]


def answer_to_validator_output(answer: Any) -> str:
    if isinstance(answer, (dict, list)):
        return json.dumps(jsonable(answer), ensure_ascii=False, sort_keys=True)
    return "" if answer is None else str(answer)


def validate_observed_answer(answer: Any, template: str, args: dict[str, Any]) -> dict[str, Any]:
    if answer is None:
        return {"passed": False, "reason": "final_answer_not_observed"}
    try:
        from validator_templates import validate_with_template
    except ModuleNotFoundError as exc:
        return {"passed": False, "reason": f"validator_templates_import_failed:{exc}"}
    try:
        passed, reason = validate_with_template(answer_to_validator_output(answer), template, args)
    except Exception as exc:
        return {"passed": False, "reason": f"{type(exc).__name__}: {exc}"}
    return {"passed": bool(passed), "reason": str(reason)}


def infer_answer_from_observation(observation: dict[str, Any]) -> Any:
    rows = observation.get("rows")
    if not isinstance(rows, list) or not rows:
        return None
    if len(rows) == 1 and isinstance(rows[0], dict):
        if len(rows[0]) == 1:
            return next(iter(rows[0].values()))
        return rows[0]
    return None


def tokenize_evidence_text(step: dict[str, Any]) -> set[str]:
    parts = [str(step.get("source", "")), str(step.get("action", ""))]
    fields = step.get("fields", [])
    if isinstance(fields, list):
        parts.extend(str(field) for field in fields)
    text = " ".join(parts).casefold()
    tokens = set()
    for raw in re.split(r"[^a-zA-Z0-9_]+", text):
        raw = raw.strip("_")
        if len(raw) >= 3 and raw not in {"the", "and", "for", "with", "from", "this", "that"}:
            tokens.add(raw)
    return tokens


def evidence_step_coverage(step: dict[str, Any], observations: list[dict[str, Any]], validator_seen: bool) -> dict[str, Any]:
    source = str(step.get("source", "")).casefold()
    action = str(step.get("action", "") or step.get("operation", "")).casefold()
    if (
        "validator" in source
        or "validator" in action
        or "validate" in action
        or "verify" in action
        or "final answer" in action
        or "return_answer" in action
    ):
        return {"covered": True, "matched_observation": None, "reason": "validator_or_final_answer_step"}
    tokens = tokenize_evidence_text(step)
    best_idx = None
    best_score = 0
    best_hits: list[str] = []
    for idx, obs in enumerate(observations):
        haystack_parts = [
            str(obs.get("db_name", "")),
            str(obs.get("db_type", "")),
            str(obs.get("query", "")),
            json.dumps(obs.get("summary", {}).get("columns", []), ensure_ascii=False),
        ]
        haystack = " ".join(haystack_parts).casefold()
        hits = sorted(token for token in tokens if token in haystack)
        if len(hits) > best_score:
            best_idx = idx
            best_score = len(hits)
            best_hits = hits[:12]
    return {
        "covered": best_score > 0,
        "matched_observation": best_idx,
        "matched_tokens": best_hits,
        "reason": "matched_query_or_columns" if best_score > 0 else "no_matching_observation",
    }


def verify_candidate_evidence(candidate: dict[str, Any], args: argparse.Namespace) -> dict[str, Any]:
    bench_root = Path(args.bench_root)
    dataset_dir = dataset_dir_for_candidate(candidate, bench_root)
    failures: list[str] = []
    if not dataset_dir.exists():
        failures.append(f"dataset_dir_missing:{dataset_dir}")
        return {"verified": False, "failures": failures, "observations": [], "evidence_steps": []}
    try:
        clients = load_db_clients(dataset_dir)
    except Exception as exc:
        failures.append(f"db_config_load_failed:{type(exc).__name__}: {exc}")
        clients = {}
    if not clients:
        failures.append("db_clients_missing")

    solution = candidate.get("candidate_solution", {})
    if not isinstance(solution, dict):
        solution = {}
    observations: list[dict[str, Any]] = []
    final_answer = None
    final_answer_source = ""

    python_code = str(solution.get("python") or "")
    query_calls = extract_query_db_calls(python_code)
    executed_from_python = False
    python_answer_without_query_observation = False
    if python_code and args.allow_python_exec:
        answer, py_observations, py_failures = execute_candidate_python(
            python_code,
            clients,
            dataset_dir,
            args.query_row_limit,
            args.python_timeout,
            args.mongo_uri,
        )
        observations.extend(py_observations)
        failures.extend(py_failures)
        executed_from_python = True
        if answer is not None:
            final_answer = answer
            final_answer_source = "candidate_solution.python"
            if not any(obs.get("success") for obs in py_observations):
                python_answer_without_query_observation = True

    if not executed_from_python:
        for call in query_calls:
            cfg = clients.get(call["db_name"])
            if not isinstance(cfg, dict):
                observations.append({"tool": "query_db", "db_name": call["db_name"], "query": call["query"], "success": False, "error": "unknown_db_name"})
                continue
            try:
                db_type = str(cfg.get("db_type") or "").casefold()
                if db_type in {"sqlite", "duckdb"}:
                    rows, observation = execute_sql_client(call["db_name"], cfg, dataset_dir, call["query"], args.query_row_limit)
                    observation["success"] = True
                    observation["rows"] = rows
                elif db_type == "mongo":
                    observation = execute_mongo_query(call["db_name"], cfg, call["query"], args.query_row_limit, args.mongo_uri)
                else:
                    observation = {"tool": "query_db", "db_name": call["db_name"], "query": call["query"], "success": False, "error": f"unsupported_db_type:{db_type}"}
            except Exception as exc:
                observation = {"tool": "query_db", "db_name": call["db_name"], "query": call["query"], "success": False, "error": f"{type(exc).__name__}: {exc}"}
            observations.append(observation)

    sql_items = solution.get("sql") or []
    if isinstance(sql_items, str):
        sql_items = [sql_items]
    has_successful_observation = any(obs.get("success") for obs in observations)
    if not query_calls and not has_successful_observation:
        for sql_item in sql_items:
            if isinstance(sql_item, dict):
                db_name = str(sql_item.get("db_name") or "")
                sql = str(sql_item.get("query") or "")
                cfg = clients.get(db_name)
                if not db_name or not isinstance(cfg, dict):
                    observations.append({"tool": "query_db", "db_name": db_name, "query": sql, "success": False, "error": "unknown_or_missing_db_name"})
                    continue
                try:
                    rows, observation = execute_sql_client(db_name, cfg, dataset_dir, sql, args.query_row_limit)
                    observation["success"] = True
                    observation["rows"] = rows
                except Exception as exc:
                    observation = {"tool": "query_db", "db_name": db_name, "query": sql, "success": False, "error": f"{type(exc).__name__}: {exc}"}
                observations.append(observation)
            else:
                observations.append(execute_sql_any_client(clients, dataset_dir, str(sql_item), args.query_row_limit))

    mongo_items = solution.get("mongo") or []
    if isinstance(mongo_items, (str, dict)):
        mongo_items = [mongo_items]
    for mongo_query in mongo_items:
        matched_mongo = False
        for logical_name, cfg in clients.items():
            if str(cfg.get("db_type") or "").casefold() != "mongo":
                continue
            matched_mongo = True
            try:
                observations.append(execute_mongo_query(logical_name, cfg, mongo_query, args.query_row_limit, args.mongo_uri))
            except Exception as exc:
                observations.append({"tool": "query_db", "db_name": logical_name, "db_type": "mongo", "query": mongo_query, "success": False, "error": f"{type(exc).__name__}: {exc}"})
        if not matched_mongo:
            observations.append({"tool": "query_db", "db_type": "mongo", "query": mongo_query, "success": False, "error": "no_mongo_source"})

    successful_observations = [obs for obs in observations if obs.get("success")]
    if final_answer is None and successful_observations:
        final_answer = infer_answer_from_observation(successful_observations[-1])
        if final_answer is not None:
            final_answer_source = "last_successful_query_result"

    for obs in observations:
        obs.pop("rows", None)
    template = str(candidate.get("validator_template") or "")
    validator_args = candidate.get("validator_args") if isinstance(candidate.get("validator_args"), dict) else {}
    validator_result = validate_observed_answer(final_answer, template, validator_args)

    evidence_steps = []
    for step in candidate.get("evidence_chain", []) or []:
        if not isinstance(step, dict):
            evidence_steps.append({"covered": False, "reason": "step_not_object"})
            continue
        coverage = evidence_step_coverage(step, observations, validator_result["passed"])
        evidence_steps.append({**step, **coverage})
        if not coverage.get("covered"):
            failures.append(f"evidence_step_uncovered:{step.get('step', len(evidence_steps))}")

    if not successful_observations:
        failures.append("no_successful_observations")
    if final_answer is None:
        failures.append("final_answer_not_observed")
    if python_answer_without_query_observation:
        failures.append("python_answer_without_query_observation")
    if not validator_result["passed"]:
        failures.append(f"validator_failed:{validator_result['reason']}")

    failures = sorted(set(str(failure) for failure in failures if failure))
    return {
        "verified": not failures,
        "dataset_dir": str(dataset_dir),
        "final_answer": jsonable(final_answer),
        "final_answer_source": final_answer_source,
        "validator_result": validator_result,
        "observations": observations,
        "evidence_steps": evidence_steps,
        "failures": failures,
        "policy": {
            "allow_python_exec": bool(args.allow_python_exec),
            "query_row_limit": int(args.query_row_limit),
            "python_timeout": int(args.python_timeout),
        },
    }


def write_evidence_dashboard(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    status = Counter("verified" if row.get("evidence_verification", {}).get("verified") else "failed" for row in rows)
    failures = Counter()
    for row in rows:
        for failure in row.get("evidence_verification", {}).get("failures", []) or []:
            failures[str(failure).split(":", 1)[0]] += 1
    lines = [
        "# DABench Evidence Verification",
        "",
        "## Summary",
        "",
        f"- total candidates: {len(rows)}",
        f"- verified: {status.get('verified', 0)}",
        f"- failed: {status.get('failed', 0)}",
        "",
        "## Failure Counts",
        "",
    ]
    if failures:
        for failure, count in failures.most_common():
            lines.append(f"- `{failure}`: {count}")
    else:
        lines.append("- no failures")
    lines.extend(["", "## Candidates", ""])
    for row in rows:
        ev = row.get("evidence_verification", {})
        mark = "verified" if ev.get("verified") else "failed"
        lines.append(f"- `{mark}` `{row.get('pipeline_id', '')}` `{row.get('dataset', '')}`: {row.get('query', '')}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def verify_evidence_chain(args: argparse.Namespace) -> None:
    rows = read_jsonl(Path(args.candidate_jsonl))
    verified_rows = []
    for idx, row in enumerate(rows):
        out = dict(row)
        out["pipeline_id"] = row.get("pipeline_id") or f"candidate_{idx:05d}_{stable_hash([row.get('dataset'), row.get('query')])}"
        out["evidence_verification"] = verify_candidate_evidence(out, args)
        verified_rows.append(out)
    write_jsonl(Path(args.output_jsonl), verified_rows)
    if args.dashboard:
        write_evidence_dashboard(Path(args.dashboard), verified_rows)
    counts = Counter("verified" if row["evidence_verification"]["verified"] else "failed" for row in verified_rows)
    print(json.dumps({"total": len(verified_rows), **counts, "output_jsonl": args.output_jsonl}, ensure_ascii=False, indent=2))


def flatten_answer_leaves(value: Any, max_items: int = 50) -> list[Any]:
    leaves: list[Any] = []

    def visit(item: Any) -> None:
        if len(leaves) >= max_items:
            return
        if item is None or isinstance(item, bool):
            return
        if isinstance(item, (str, int, float)):
            text = str(item).strip()
            if text:
                leaves.append(item)
            return
        if isinstance(item, dict):
            for sub in item.values():
                visit(sub)
            return
        if isinstance(item, (list, tuple, set)):
            for sub in item:
                visit(sub)

    visit(value)
    return leaves


def name_value_pairs_from_answer(answer: Any) -> list[dict[str, Any]]:
    if not isinstance(answer, list):
        return []
    name_keys = ("name", "title", "repo", "repository", "project", "package", "symbol", "id")
    value_keys = ("value", "count", "score", "total", "average", "mean", "ratio", "percentage", "stars", "forks")
    pairs: list[dict[str, Any]] = []
    for item in answer:
        if not isinstance(item, dict):
            return []
        name = None
        value = None
        for key in name_keys:
            if key in item and item[key] not in (None, ""):
                name = str(item[key])
                break
        for key in value_keys:
            if key in item and isinstance(item[key], (int, float)) and not isinstance(item[key], bool):
                value = item[key]
                break
        if name is None or value is None:
            return []
        pairs.append({"name": name, "value": value, "tolerance": 1e-6})
    return pairs


def validator_spec_from_observed_answer(answer: Any, template_hint: str = "") -> tuple[dict[str, Any], str, dict[str, Any], list[str]]:
    risks: list[str] = []
    if answer is None or answer == "" or answer == [] or answer == {}:
        return {"type": "unknown", "value": answer, "normalization": "none"}, "ordered_contains", {"items": []}, ["materialization_final_answer_empty"]
    if isinstance(answer, bool):
        return {"type": "string", "value": str(answer), "normalization": "normalized_contains_all"}, "normalized_contains_all", {"items": [str(answer)]}, risks
    if isinstance(answer, (int, float)):
        tolerance = 0.005 if abs(float(answer)) <= 1 else max(1e-6, abs(float(answer)) * 1e-6)
        return {"type": "number", "value": answer, "normalization": "numeric_tolerance"}, "numeric_tolerance", {"expected": answer, "tolerance": tolerance}, risks
    if isinstance(answer, str):
        return {"type": "string", "value": answer, "normalization": "normalized_contains_all"}, "normalized_contains_all", {"items": [answer]}, risks
    if isinstance(answer, dict):
        scalar_fields = {
            str(key): value
            for key, value in answer.items()
            if value is not None and isinstance(value, (str, int, float)) and not isinstance(value, bool)
        }
        if scalar_fields and len(scalar_fields) <= 20:
            return {"type": "json", "value": answer, "normalization": "json_fields"}, "json_exact_fields", {"expected": scalar_fields, "numeric_tolerance": 1e-6}, risks
        leaves = [str(item) for item in flatten_answer_leaves(answer) if len(str(item).strip()) >= 2]
        return {"type": "json", "value": answer, "normalization": "ordered_contains"}, "ordered_contains", {"items": leaves}, risks
    if isinstance(answer, list):
        if not answer:
            return {"type": "list", "value": answer, "normalization": "none"}, "ordered_contains", {"items": []}, ["materialization_final_answer_empty"]
        pairs = name_value_pairs_from_answer(answer)
        if pairs:
            return {"type": "list", "value": answer, "normalization": "name_value_proximity"}, "name_value_proximity", {"pairs": pairs, "window": 120}, risks
        leaves = flatten_answer_leaves(answer)
        if leaves and all(isinstance(item, (int, float)) and not isinstance(item, bool) for item in leaves):
            return {"type": "list", "value": answer, "normalization": "numeric_list_tolerance"}, "numeric_list_tolerance", {"expected": leaves, "tolerance": 1e-6}, risks
        items = [str(item) for item in leaves if len(str(item).strip()) >= 2]
        if not items:
            items = [json.dumps(jsonable(item), ensure_ascii=False, sort_keys=True) for item in answer[:20]]
        return {"type": "list", "value": answer, "normalization": "ordered_contains"}, "ordered_contains", {"items": items}, risks
    text = str(answer)
    return {"type": "string", "value": text, "normalization": "normalized_contains_all"}, "normalized_contains_all", {"items": [text]}, risks


def compact_materialization_observations(ev: dict[str, Any], limit: int = 8) -> list[dict[str, Any]]:
    observations = ev.get("observations", []) if isinstance(ev, dict) else []
    compact: list[dict[str, Any]] = []
    if not isinstance(observations, list):
        return compact
    for obs in observations[:limit]:
        if not isinstance(obs, dict):
            continue
        compact.append(
            {
                "tool": obs.get("tool", "query_db"),
                "db_name": obs.get("db_name"),
                "db_type": obs.get("db_type"),
                "query": truncate(str(obs.get("query", "")), 800),
                "success": bool(obs.get("success")),
                "error": truncate(str(obs.get("error", "")), 400) if obs.get("error") else "",
                "summary": obs.get("summary", {}),
            }
        )
    return compact


def materialized_evidence_card(row: dict[str, Any], ev: dict[str, Any], final_answer: Any, risks: list[str]) -> dict[str, Any]:
    card = dict(row.get("evidence_card")) if isinstance(row.get("evidence_card"), dict) else {}
    card.setdefault("probes", [])
    card["observed_answer"] = jsonable(final_answer)
    card["observed_answer_source"] = ev.get("final_answer_source", "")
    card["materialized_by"] = "materialize-observed-ground-truth"
    card["materialization_risks"] = list(risks)
    card["execution_failures"] = [
        str(failure)
        for failure in (ev.get("failures", []) if isinstance(ev, dict) else [])
        if not str(failure).startswith("validator_failed:")
    ]
    card["executed_observations"] = compact_materialization_observations(ev)
    if final_answer not in (None, "", [], {}):
        card.setdefault("nonempty_evidence_reason", "candidate_solution produced a non-empty final answer during local materialization.")
    return card


def materialize_observed_ground_truth(args: argparse.Namespace) -> None:
    rows = read_jsonl(Path(args.candidate_jsonl))
    output_rows = []
    materialized = 0
    failed = 0
    for idx, row in enumerate(rows):
        out = dict(row)
        out["pipeline_id"] = row.get("pipeline_id") or f"candidate_{idx:05d}_{stable_hash([row.get('dataset'), row.get('query')])}"
        ev = verify_candidate_evidence(out, args)
        final_answer = ev.get("final_answer")
        expected_answer, template, validator_args, risks = validator_spec_from_observed_answer(final_answer, str(row.get("validator_template", "")))
        out["ground_truth_materialization"] = {
            "strategy": "execute_candidate_solution_then_materialize_validator",
            "original_expected_answer": row.get("expected_answer"),
            "original_validator_template": row.get("validator_template"),
            "original_validator_args": row.get("validator_args"),
            "observed_final_answer": final_answer,
            "observed_final_answer_source": ev.get("final_answer_source", ""),
            "execution_failures": [
                str(failure)
                for failure in (ev.get("failures", []) if isinstance(ev, dict) else [])
                if not str(failure).startswith("validator_failed:")
            ],
            "pre_materialization_validator_failures": [
                str(failure)
                for failure in (ev.get("failures", []) if isinstance(ev, dict) else [])
                if str(failure).startswith("validator_failed:")
            ],
            "risks": risks,
        }
        out["evidence_card"] = materialized_evidence_card(row, ev, final_answer, risks)
        if risks or final_answer is None:
            failed += 1
            out["judge"] = merge_judge(
                row.get("judge"),
                {
                    "accepted": False,
                    "score": 0.0,
                    "risks": risks or ["materialization_final_answer_missing"],
                    "dimension_scores": {},
                },
            )
        else:
            materialized += 1
            out["expected_answer"] = expected_answer
            out["validator_template"] = template
            out["validator_args"] = validator_args
            out["reward_spec"] = {
                **(row.get("reward_spec", {}) if isinstance(row.get("reward_spec"), dict) else {}),
                "primary": "programmatic_validator",
                "format_reward": True,
                "ground_truth_materialized_from_candidate_solution": True,
            }
            out.pop("evidence_verification", None)
            out.pop("judge", None)
        output_rows.append(out)
    write_jsonl(Path(args.output_jsonl), output_rows)
    print(
        json.dumps(
            {
                "input": len(rows),
                "materialized": materialized,
                "failed": failed,
                "output_jsonl": args.output_jsonl,
                "policy": "Observed final_answer is only a candidate ground truth; rows still require judge, evidence verification, and ground-truth review.",
            },
            ensure_ascii=False,
            indent=2,
        )
    )


def judge_local(args: argparse.Namespace) -> None:
    candidates = read_jsonl(Path(args.candidate_jsonl))
    hint_catalog = load_hint_catalog(args.hint_catalog_json, args.bench_root)
    official_anchors = load_official_anchor_catalog(args.bench_root)
    seen_queries: set[str] = set()
    judged = []
    for idx, candidate in enumerate(candidates):
        result = judge_candidate(candidate, seen_queries, hint_catalog, official_anchors)
        row = dict(candidate)
        row["hints"] = result.pop("resolved_hints", [])
        row["source_signature_alignment"] = result.pop("source_signature_alignment", {})
        row["hint_policy"] = result.get("hint_policy", {})
        row["hint_refs"] = row["hint_policy"].get("selected_hint_refs", row.get("hint_refs", []))
        row["judge"] = merge_judge(candidate.get("judge"), result)
        row["pipeline_id"] = candidate.get("pipeline_id") or f"candidate_{idx:05d}_{stable_hash([candidate.get('dataset'), candidate.get('query')])}"
        judged.append(row)
    write_jsonl(Path(args.output_jsonl), judged)
    write_dashboard(Path(args.dashboard), judged)
    counts = Counter("accepted" if row["judge"]["accepted"] else "rejected" for row in judged)
    print(json.dumps({"total": len(judged), **counts, "output_jsonl": args.output_jsonl}, ensure_ascii=False, indent=2))


def make_judge_packets(args: argparse.Namespace) -> None:
    candidates = read_jsonl(Path(args.candidate_jsonl))
    global_inventory = json.loads(Path(args.global_inventory_json).read_text(encoding="utf-8"))
    judge_prompt = read_text(Path(args.judge_prompt))
    batch_summary = candidate_batch_summary(candidates)
    packets = []
    for idx, candidate in enumerate(candidates):
        packets.append(
            {
                "packet_id": candidate.get("pipeline_id") or f"judge_{idx:05d}_{stable_hash([candidate.get('dataset'), candidate.get('query')])}",
                "system_prompt": judge_prompt,
                "input": {
                    "global_inventory": global_inventory,
                    "candidate_batch_summary": batch_summary,
                    "candidate": candidate,
                },
                "expected_output": "One JSON object with accepted, score, dimension_scores, risks, required_fixes, and reward_recommendation.",
            }
        )
    write_jsonl(Path(args.output_jsonl), packets)
    print(json.dumps({"judge_packets": len(packets), "output_jsonl": args.output_jsonl}, ensure_ascii=False, indent=2))


def make_ground_truth_review_packets(args: argparse.Namespace) -> None:
    candidates = read_jsonl(Path(args.candidate_jsonl))
    review_prompt = read_text(Path(args.review_prompt))
    packets = []
    for idx, candidate in enumerate(candidates):
        pipeline_id = candidate.get("pipeline_id") or f"candidate_{idx:05d}_{stable_hash([candidate.get('dataset'), candidate.get('query')])}"
        ev = candidate.get("evidence_verification", {})
        packets.append(
            {
                "packet_id": f"gt_review_{pipeline_id}",
                "system_prompt": review_prompt,
                "input": {
                    "candidate": candidate,
                    "evidence_verification": ev,
                    "review_focus": [
                        "Does the executed final_answer match expected_answer and validator_args?",
                        "Is the final answer derived from non-empty data evidence rather than hard-coded text?",
                        "Is this a meaningful data-analysis task rather than sentinel/nonexistent/empty-result/identity-zero?",
                        "Are all evidence_chain data steps covered by actual observations?",
                        "Should this row be eligible for VERL training?",
                    ],
                },
                "expected_output": "One JSON object with accepted, score, ground_truth_correct, evidence_nonempty, training_value, risks, required_fixes, and final_decision_reason.",
            }
        )
    write_jsonl(Path(args.output_jsonl), packets)
    print(json.dumps({"ground_truth_review_packets": len(packets), "output_jsonl": args.output_jsonl}, ensure_ascii=False, indent=2))


def make_task_fit_review_packets(args: argparse.Namespace) -> None:
    candidates = read_jsonl(Path(args.candidate_jsonl))
    review_prompt = read_text(Path(args.review_prompt))
    packets = []
    for idx, candidate in enumerate(candidates):
        pipeline_id = candidate.get("pipeline_id") or f"candidate_{idx:05d}_{stable_hash([candidate.get('dataset'), candidate.get('query')])}"
        packets.append(
            {
                "packet_id": f"task_fit_review_{pipeline_id}",
                "system_prompt": review_prompt,
                "input": {
                    "candidate": {
                        "pipeline_id": pipeline_id,
                        "dataset": candidate.get("dataset"),
                        "query_id": candidate.get("query_id"),
                        "query": candidate.get("query"),
                        "difficulty": candidate.get("difficulty"),
                        "task_type": candidate.get("task_type"),
                        "provenance": candidate.get("provenance", {}),
                        "source_task_signature": candidate.get("source_task_signature", {}),
                        "signature_alignment": candidate.get("signature_alignment", {}),
                        "source_signature_alignment": candidate.get("source_signature_alignment", {}),
                        "evidence_card": candidate.get("evidence_card", {}),
                        "data_requirements": candidate.get("data_requirements", {}),
                        "evidence_chain": candidate.get("evidence_chain", []),
                        "solution_plan": candidate.get("solution_plan", []),
                        "validator_template": candidate.get("validator_template", ""),
                        "validator_args": candidate.get("validator_args", {}),
                        "ground_truth_materialization": candidate.get("ground_truth_materialization", {}),
                        "evidence_verification": candidate.get("evidence_verification", {}),
                    },
                    "review_focus": [
                        "Does the generated query match the source DABench task type and operation family?",
                        "Is the generated difficulty close to the source signature rather than downgraded to a trivial task?",
                        "Does the answer shape and validator style remain compatible or stricter in the same family?",
                        "Does the query require the same central reasoning pattern, such as join, ID normalization, ranking, temporal filtering, aggregation, JSON/Mongo access, or set/list output?",
                        "Is the task useful for VERL training and not only an easy lookup/schema question?",
                    ],
                },
                "expected_output": (
                    "One JSON object with accepted, score, task_type_match, difficulty_reasonable, "
                    "operation_family_match, answer_shape_match, not_downgraded, training_value, risks, "
                    "required_fixes, and final_decision_reason."
                ),
            }
        )
    write_jsonl(Path(args.output_jsonl), packets)
    print(json.dumps({"task_fit_review_packets": len(packets), "output_jsonl": args.output_jsonl}, ensure_ascii=False, indent=2))


def collect_observations_from_row(row: dict[str, Any], repeat_results: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    observations: list[dict[str, Any]] = []
    for source in (
        row.get("evidence_verification", {}),
        row.get("evidence_card", {}),
    ):
        if not isinstance(source, dict):
            continue
        raw = source.get("observations") or source.get("executed_observations") or []
        if isinstance(raw, list):
            observations.extend(obs for obs in raw if isinstance(obs, dict))
    for result in repeat_results or []:
        raw = result.get("observations", []) if isinstance(result, dict) else []
        if isinstance(raw, list):
            observations.extend(obs for obs in raw if isinstance(obs, dict))
    return observations


def iter_preview_rows(observation: dict[str, Any]) -> Iterable[dict[str, Any]]:
    summary = observation.get("summary") if isinstance(observation.get("summary"), dict) else {}
    preview = summary.get("preview") if isinstance(summary.get("preview"), list) else []
    for row in preview:
        if isinstance(row, dict):
            yield row


def count_like_metrics_from_observations(observations: list[dict[str, Any]]) -> list[dict[str, Any]]:
    metrics: list[dict[str, Any]] = []
    for obs in observations:
        for row in iter_preview_rows(obs):
            for key, value in row.items():
                if not COUNT_LIKE_COLUMN_RE.search(str(key)):
                    continue
                if isinstance(value, (int, float)) and not isinstance(value, bool):
                    metrics.append(
                        {
                            "name": str(key),
                            "value": float(value),
                            "db_name": obs.get("db_name"),
                            "query": truncate(str(obs.get("query", "")), 500),
                        }
                    )
    return metrics


def row_count_metrics_from_observations(observations: list[dict[str, Any]]) -> list[dict[str, Any]]:
    metrics: list[dict[str, Any]] = []
    for obs in observations:
        summary = obs.get("summary") if isinstance(obs.get("summary"), dict) else {}
        row_count = summary.get("row_count_observed")
        if isinstance(row_count, (int, float)) and not isinstance(row_count, bool):
            metrics.append(
                {
                    "name": "row_count_observed",
                    "value": float(row_count),
                    "columns": summary.get("columns", []),
                    "db_name": obs.get("db_name"),
                    "query": truncate(str(obs.get("query", "")), 500),
                }
            )
    return metrics


def review_risk_text(row: dict[str, Any]) -> str:
    payload = {
        "ground_truth_review_risks": (row.get("ground_truth_review", {}) if isinstance(row.get("ground_truth_review"), dict) else {}).get("risks", []),
        "task_fit_review_risks": (row.get("task_fit_review", {}) if isinstance(row.get("task_fit_review"), dict) else {}).get("risks", []),
        "judge_risks": (row.get("judge", {}) if isinstance(row.get("judge"), dict) else {}).get("risks", []),
    }
    return json.dumps(jsonable(payload), ensure_ascii=False).casefold()


def non_degeneracy_metrics(row: dict[str, Any], repeat_results: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    observations = collect_observations_from_row(row, repeat_results)
    count_metrics = count_like_metrics_from_observations(observations)
    row_count_metrics = row_count_metrics_from_observations(observations)
    review_text = review_risk_text(row)
    return {
        "count_like_metrics": count_metrics,
        "row_count_metrics": row_count_metrics,
        "review_risk_hits": [pattern for pattern in DEGENERATE_REVIEW_RISK_PATTERNS if pattern in review_text],
        "observation_count": len(observations),
    }


def anti_degenerate_audit_check(row: dict[str, Any], repeat_results: list[dict[str, Any]], args: argparse.Namespace) -> tuple[bool, str]:
    metrics = non_degeneracy_metrics(row, repeat_results)
    query_text = str(row.get("query", "")).casefold()
    source_ops = row.get("source_task_signature", {}).get("operation_families", []) if isinstance(row.get("source_task_signature"), dict) else []
    op_text = " ".join(str(op).casefold() for op in source_ops)
    ranking_like = any(marker in query_text or marker in op_text for marker in ("rank", "top", "highest", "most", "largest", "copied", "copy", "order by"))
    normalization_like = any(marker in query_text or marker in op_text for marker in ("normaliz", "same track", "same title", "case", "trim", "matching track_id", "id_normalization"))

    min_winner_count = float(getattr(args, "min_ranking_winner_count", 2))
    if ranking_like:
        weak_counts = [
            metric for metric in metrics["count_like_metrics"]
            if float(metric.get("value", 0.0)) < min_winner_count
        ]
        if weak_counts:
            return False, f"ranking_winner_count_below_{int(min_winner_count)}:{weak_counts[0]['name']}={weak_counts[0]['value']}"

    min_group_size = float(getattr(args, "min_normalized_group_size", 2))
    if normalization_like:
        weak_group_rows = [
            metric for metric in metrics["row_count_metrics"]
            if float(metric.get("value", 0.0)) < min_group_size
            and any("id" in str(col).casefold() for col in metric.get("columns", []))
            and any(marker in str(metric.get("query", "")).casefold() for marker in ("lower(", "trim(", " in (", "where"))
        ]
        if weak_group_rows:
            return False, f"normalized_group_size_below_{int(min_group_size)}:{weak_group_rows[0]['value']}"

    if metrics["review_risk_hits"]:
        return False, "review_flagged_degenerate_training_value:" + ",".join(metrics["review_risk_hits"][:3])
    return True, "non_degeneracy_checks_passed"


def training_value_review_passes_gate(review: dict[str, Any], min_score: float) -> tuple[bool, list[str]]:
    failures: list[str] = []
    if not review:
        return False, ["missing_training_value_review"]
    if not bool(review.get("accepted_for_training", review.get("accepted", False))):
        failures.append("training_value_review_rejected")
    score = review.get("training_value_score", review.get("score"))
    if isinstance(score, (int, float)) and float(score) < min_score:
        failures.append("training_value_review_score_too_low")
    required_true_fields = {
        "non_degenerate": "training_value_non_degenerate_not_confirmed",
        "credit_assignment_clear": "credit_assignment_not_confirmed",
    }
    for field, failure in required_true_fields.items():
        if field in review and review.get(field) is not True:
            failures.append(failure)
    shortcut_risk = str(review.get("shortcut_risk", "")).casefold()
    if shortcut_risk == "high":
        failures.append("training_value_shortcut_risk_high")
    return not failures, failures


def make_training_value_review_packets(args: argparse.Namespace) -> None:
    candidates = read_jsonl(Path(args.candidate_jsonl))
    review_prompt = read_text(Path(args.review_prompt))
    packets = []
    for idx, candidate in enumerate(candidates):
        pipeline_id = candidate.get("pipeline_id") or f"candidate_{idx:05d}_{stable_hash([candidate.get('dataset'), candidate.get('query')])}"
        metrics = non_degeneracy_metrics(candidate, (candidate.get("final_audit", {}) if isinstance(candidate.get("final_audit"), dict) else {}).get("repeat_results", []))
        packets.append(
            {
                "packet_id": f"training_value_review_{pipeline_id}",
                "system_prompt": review_prompt,
                "input": {
                    "candidate": {
                        "pipeline_id": pipeline_id,
                        "dataset": candidate.get("dataset"),
                        "query": candidate.get("query"),
                        "difficulty": candidate.get("difficulty"),
                        "task_type": candidate.get("task_type"),
                        "source_task_signature": candidate.get("source_task_signature", {}),
                        "signature_alignment": candidate.get("signature_alignment", {}),
                        "expected_answer": candidate.get("expected_answer", {}),
                        "validator_template": candidate.get("validator_template", ""),
                        "validator_args": candidate.get("validator_args", {}),
                        "evidence_card": candidate.get("evidence_card", {}),
                        "evidence_verification": candidate.get("evidence_verification", {}),
                        "ground_truth_materialization": candidate.get("ground_truth_materialization", {}),
                        "ground_truth_review": candidate.get("ground_truth_review", {}),
                        "task_fit_review": candidate.get("task_fit_review", {}),
                        "final_audit": candidate.get("final_audit", {}),
                    },
                    "deterministic_non_degeneracy_metrics": metrics,
                    "review_focus": [
                        "Is this useful for RL training rather than merely correct?",
                        "Does the task force a multi-step data operation with clear credit assignment?",
                        "Can a shortcut, singleton group, weak tie-breaker, answer leakage, or truncated query solve it accidentally?",
                        "Should this row be accepted, rejected, or sent back for stronger DB-mined evidence?",
                    ],
                },
                "expected_output": (
                    "One JSON object with accepted_for_training, training_value_score, difficulty, "
                    "shortcut_risk, credit_assignment_quality, credit_assignment_clear, non_degenerate, "
                    "reason, risks, and required_fixes."
                ),
            }
        )
    write_jsonl(Path(args.output_jsonl), packets)
    print(json.dumps({"training_value_review_packets": len(packets), "output_jsonl": args.output_jsonl}, ensure_ascii=False, indent=2))


def merge_llm_judges(args: argparse.Namespace) -> None:
    candidates = read_jsonl(Path(args.candidate_jsonl))
    judge_rows = read_jsonl(Path(args.judge_jsonl))
    judges_by_id: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in judge_rows:
        packet_id = str(row.get("packet_id") or row.get("pipeline_id") or "")
        payload = extract_judge_payload(row)
        if packet_id and payload:
            judges_by_id[packet_id].append(payload)
            if packet_id.startswith("judge_"):
                judges_by_id[packet_id.removeprefix("judge_")].append(payload)

    merged = []
    for idx, candidate in enumerate(candidates):
        pipeline_id = candidate.get("pipeline_id") or f"candidate_{idx:05d}_{stable_hash([candidate.get('dataset'), candidate.get('query')])}"
        packet_ids = {str(pipeline_id), f"judge_{pipeline_id}"}
        llm_judges = []
        for packet_id in packet_ids:
            llm_judges.extend(judges_by_id.get(packet_id, []))
        row = dict(candidate)
        row["pipeline_id"] = pipeline_id
        row["judge"] = combine_judges(candidate.get("judge"), llm_judges, args.min_score)
        merged.append(row)
    write_jsonl(Path(args.output_jsonl), merged)
    if args.dashboard:
        write_dashboard(Path(args.dashboard), merged)
    counts = Counter("accepted" if row.get("judge", {}).get("accepted") else "rejected" for row in merged)
    print(json.dumps({"total": len(merged), **counts, "output_jsonl": args.output_jsonl}, ensure_ascii=False, indent=2))


def extract_judge_payload(row: dict[str, Any]) -> dict[str, Any]:
    for key in ("judge", "output", "response", "content"):
        value = row.get(key)
        if isinstance(value, dict):
            return value
        if isinstance(value, str) and value.strip():
            try:
                parsed = json.loads(value)
            except json.JSONDecodeError:
                continue
            if isinstance(parsed, dict):
                return parsed
    if {"accepted", "score"} & row.keys():
        return row
    if {"accepted_for_training", "training_value_score"} & row.keys():
        return row
    return {}


def combine_judges(existing: Any, llm_judges: list[dict[str, Any]], min_score: float) -> dict[str, Any]:
    base = dict(existing) if isinstance(existing, dict) else {}
    local = base.get("local_static_judge", base)
    local_accepted = bool(local.get("accepted", base.get("accepted", False)))
    llm_scores = [float(j.get("score", 0.0)) for j in llm_judges if isinstance(j.get("score"), (int, float))]
    llm_accepts = [bool(j.get("accepted", False)) for j in llm_judges]
    llm_majority = not llm_accepts or sum(llm_accepts) >= (len(llm_accepts) / 2)
    score_values = [float(base.get("score", 0.0))] if isinstance(base.get("score"), (int, float)) else []
    score_values.extend(llm_scores)
    score = round(sum(score_values) / len(score_values), 4) if score_values else 0.0
    risks = list(base.get("risks", []) or [])
    for judge in llm_judges:
        risks.extend(str(risk) for risk in (judge.get("risks", []) or []))
    base.update(
        {
            "accepted": local_accepted and llm_majority and score >= min_score,
            "score": score,
            "risks": sorted(set(risks)),
            "llm_judges": llm_judges,
            "judge_policy": {
                "min_score": min_score,
                "require_local_static_pass": True,
                "llm_majority_passed": llm_majority,
            },
        }
    )
    return base


def candidate_batch_summary(candidates: list[dict[str, Any]]) -> dict[str, Any]:
    by_dataset = Counter(str(row.get("dataset", "unknown")) for row in candidates)
    by_task_type = Counter(str(row.get("task_type", "unknown")) for row in candidates)
    by_difficulty = Counter(str(row.get("difficulty", "unknown")) for row in candidates)
    accepted = Counter("accepted" if row.get("judge", {}).get("accepted") else "not_accepted_or_unjudged" for row in candidates)
    return {
        "total_candidates": len(candidates),
        "datasets": dict(sorted(by_dataset.items())),
        "task_types": dict(sorted(by_task_type.items())),
        "difficulty": dict(sorted(by_difficulty.items())),
        "local_acceptance": dict(sorted(accepted.items())),
    }


def select_curriculum(args: argparse.Namespace) -> None:
    rows = read_jsonl(Path(args.candidate_jsonl))
    accepted = [
        row for row in rows
        if row.get("judge", {}).get("accepted", False)
        and float(row.get("judge", {}).get("score", 0.0)) >= args.min_score
    ]
    accepted.sort(key=curriculum_sort_key)
    selected = []
    per_dataset: Counter[str] = Counter()
    for row in accepted:
        dataset = str(row.get("dataset", "unknown"))
        if per_dataset[dataset] >= args.per_dataset_cap:
            continue
        selected.append(row)
        per_dataset[dataset] += 1
        if args.max_rows and len(selected) >= args.max_rows:
            break
    write_jsonl(Path(args.output_jsonl), selected)
    if args.dashboard:
        write_dashboard(Path(args.dashboard), selected)
    print(
        json.dumps(
            {
                "input_accepted": len(accepted),
                "selected": len(selected),
                "datasets": dict(sorted(per_dataset.items())),
                "output_jsonl": args.output_jsonl,
            },
            ensure_ascii=False,
            indent=2,
        )
    )


def flatten_risks(value: Any) -> list[str]:
    risks: list[str] = []
    if isinstance(value, dict):
        for key, item in value.items():
            if key == "risks" and isinstance(item, list):
                risks.extend(str(risk) for risk in item)
            else:
                risks.extend(flatten_risks(item))
    elif isinstance(value, list):
        for item in value:
            risks.extend(flatten_risks(item))
    return risks


def risky_zero_or_empty_labels(row: dict[str, Any], review: dict[str, Any] | None = None) -> list[str]:
    labels = flatten_risks(row.get("judge", {}))
    labels.extend(flatten_risks(review or {}))
    exact_reject_labels = {
        "empty_result",
        "empty_result_task",
        "sentinel_or_deliberately_empty_task",
        "nonexistent_entity_task",
        "deliberately_empty_task",
        "zero_expected_answer_requires_manual_review",
        "zero_valued_identity_or_empty_stat",
        "zero_count_filter_or_difference",
    }
    prefix_reject_labels = (
        "zero_",
        "empty_result:",
        "sentinel_or_deliberately_empty:",
        "nonexistent_entity:",
    )
    rejected: set[str] = set()
    for label in labels:
        normalized = str(label).strip().casefold()
        if normalized in exact_reject_labels or any(normalized.startswith(prefix) for prefix in prefix_reject_labels):
            rejected.add(str(label))
    return sorted(rejected)


def verified_pool_failures(row: dict[str, Any], review: dict[str, Any] | None = None, allow_zero_or_empty: bool = False) -> list[str]:
    failures: list[str] = []
    evidence = row.get("evidence_verification", {}) if isinstance(row.get("evidence_verification"), dict) else {}
    if evidence.get("verified") is not True:
        failures.append("evidence_chain_not_verified")
    judge = row.get("judge", {}) if isinstance(row.get("judge"), dict) else {}
    hard_verified_risks = {
        "empty_query",
        "answer_leakage_in_hint",
        "answer_leakage_in_query",
        "answer_leakage_in_db_description",
        "possible_external_knowledge",
        "sentinel_or_deliberately_empty_task",
        "zero_valued_identity_or_empty_stat",
        "zero_count_filter_or_difference",
        "mutating_sql",
        "no_executable_candidate_solution",
        "python_no_query_db_or_sql",
        "python_missing_final_answer",
        "expected_answer_placeholder",
        "validator_args_placeholder",
    }
    hit_risks = sorted({risk for risk in flatten_risks(judge) if risk in hard_verified_risks})
    if hit_risks:
        failures.extend(f"verified_pool_hard_risk:{risk}" for risk in hit_risks)
    if review:
        if review.get("ground_truth_correct") is False:
            failures.append("ground_truth_not_confirmed")
        if review.get("evidence_nonempty") is False:
            failures.append("evidence_nonempty_not_confirmed")
    if not allow_zero_or_empty:
        failures.extend(risky_zero_or_empty_labels(row, review))
    return sorted(set(failures))


def task_fit_pool_passes_gate(review: dict[str, Any], min_score: float) -> tuple[bool, list[str]]:
    failures: list[str] = []
    if not review:
        return False, ["missing_task_fit_review"]
    score = review.get("score")
    if isinstance(score, (int, float)) and float(score) < min_score:
        failures.append("task_fit_pool_score_too_low")
    if review.get("operation_family_match") is not True and review.get("task_type_match") is not True:
        failures.append("task_fit_pool_operation_or_type_not_confirmed")
    if review.get("not_downgraded") is False:
        failures.append("task_fit_pool_downgraded")
    return not failures, failures


def warmup_pool_passes_gate(
    review: dict[str, Any],
    training_value_review: dict[str, Any],
    min_training_value_score: float,
) -> tuple[bool, list[str]]:
    failures: list[str] = []
    if review and review.get("ground_truth_correct") is not True:
        failures.append("ground_truth_not_confirmed")
    if training_value_review:
        score = training_value_review.get("training_value_score", training_value_review.get("score"))
        if isinstance(score, (int, float)) and float(score) < min_training_value_score:
            failures.append("warmup_training_value_score_too_low")
        if str(training_value_review.get("shortcut_risk", "")).casefold() == "high":
            failures.append("warmup_shortcut_risk_high")
        if training_value_review.get("non_degenerate") is False:
            failures.append("warmup_non_degenerate_not_confirmed")
    return not failures, failures


def build_review_map(review_jsonl: str) -> dict[str, dict[str, Any]]:
    if not review_jsonl:
        return {}
    reviews = read_jsonl(Path(review_jsonl))
    by_id: dict[str, dict[str, Any]] = {}
    for row in reviews:
        payload = extract_judge_payload(row)
        packet_id = str(row.get("packet_id") or "")
        candidate_pipeline_id = str(row.get("candidate_pipeline_id") or "")
        if not candidate_pipeline_id and isinstance(payload, dict):
            candidate_pipeline_id = str(payload.get("candidate_pipeline_id") or "")
        if not candidate_pipeline_id:
            candidate_pipeline_id = candidate_id_from_review_packet_id(packet_id) or ""
        ids = {
            packet_id,
            candidate_pipeline_id,
            str(row.get("pipeline_id") or ""),
            str(payload.get("pipeline_id") or "") if isinstance(payload, dict) else "",
            str(payload.get("candidate_pipeline_id") or "") if isinstance(payload, dict) else "",
        }
        for item in list(ids):
            if item.startswith("gt_review_"):
                ids.add(item.removeprefix("gt_review_"))
            if item.startswith("task_fit_review_"):
                ids.add(item.removeprefix("task_fit_review_"))
            if item.startswith("training_value_review_"):
                ids.add(item.removeprefix("training_value_review_"))
        for item in ids:
            if item:
                by_id[item] = payload
    return by_id


def review_passes_training_gate(review: dict[str, Any], min_score: float) -> tuple[bool, list[str]]:
    failures: list[str] = []
    if not review:
        return False, ["missing_ground_truth_review"]
    if not bool(review.get("accepted", False)):
        failures.append("ground_truth_review_rejected")
    if isinstance(review.get("score"), (int, float)) and float(review["score"]) < min_score:
        failures.append("ground_truth_review_score_too_low")
    if review.get("ground_truth_correct") is not True:
        failures.append("ground_truth_not_confirmed")
    if review.get("evidence_nonempty") is not True:
        failures.append("evidence_nonempty_not_confirmed")
    if review.get("training_value") is False:
        failures.append("training_value_rejected")
    return not failures, failures


def task_fit_review_passes_training_gate(review: dict[str, Any], min_score: float) -> tuple[bool, list[str]]:
    failures: list[str] = []
    if not review:
        return False, ["missing_task_fit_review"]
    if not bool(review.get("accepted", False)):
        failures.append("task_fit_review_rejected")
    if isinstance(review.get("score"), (int, float)) and float(review["score"]) < min_score:
        failures.append("task_fit_review_score_too_low")
    required_true_fields = {
        "task_type_match": "task_type_not_confirmed",
        "difficulty_reasonable": "difficulty_not_confirmed",
        "operation_family_match": "operation_family_not_confirmed",
        "answer_shape_match": "answer_shape_not_confirmed",
        "not_downgraded": "difficulty_or_logic_downgraded",
    }
    for field, failure in required_true_fields.items():
        if review.get(field) is not True:
            failures.append(failure)
    if review.get("training_value") is False:
        failures.append("task_fit_training_value_rejected")
    return not failures, failures


def write_training_ready_dashboard(path: Path, selected: list[dict[str, Any]], rejected: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    failures = Counter()
    for row in rejected:
        for failure in row.get("training_selection", {}).get("failures", []) or []:
            failures[str(failure)] += 1
    by_dataset = Counter(str(row.get("dataset", "unknown")) for row in selected)
    lines = [
        "# DABench Training-Ready Selection",
        "",
        "## Summary",
        "",
        f"- selected: {len(selected)}",
        f"- rejected: {len(rejected)}",
        "",
        "## Selected Dataset Coverage",
        "",
    ]
    if by_dataset:
        for dataset, count in sorted(by_dataset.items()):
            lines.append(f"- `{dataset}`: {count}")
    else:
        lines.append("- no selected rows")
    lines.extend(["", "## Rejection Counts", ""])
    if failures:
        for failure, count in failures.most_common():
            lines.append(f"- `{failure}`: {count}")
    else:
        lines.append("- no rejection reasons")
    lines.extend(["", "## Selected Rows", ""])
    for row in selected:
        lines.append(f"- `{row.get('pipeline_id', '')}` `{row.get('dataset', '')}`: {row.get('query', '')}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def select_training_ready(args: argparse.Namespace) -> None:
    rows = read_jsonl(Path(args.candidate_jsonl))
    review_map = build_review_map(args.review_jsonl)
    task_fit_review_map = build_review_map(args.task_fit_review_jsonl)
    training_value_review_map = build_review_map(args.training_value_review_jsonl)
    selected: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    verified_pool: list[dict[str, Any]] = []
    warmup_pool: list[dict[str, Any]] = []
    task_fit_pool: list[dict[str, Any]] = []
    for idx, row in enumerate(rows):
        pipeline_id = row.get("pipeline_id") or f"candidate_{idx:05d}_{stable_hash([row.get('dataset'), row.get('query')])}"
        review = review_map.get(str(pipeline_id), {})
        task_fit_review = task_fit_review_map.get(str(pipeline_id), {})
        training_value_review = training_value_review_map.get(str(pipeline_id), {})
        failures: list[str] = []
        judge = row.get("judge", {}) if isinstance(row.get("judge"), dict) else {}
        if not bool(judge.get("accepted", False)):
            failures.append("local_or_merged_judge_not_accepted")
        if float(judge.get("score", 0.0) or 0.0) < args.min_local_score:
            failures.append("local_or_merged_judge_score_too_low")
        evidence = row.get("evidence_verification", {}) if isinstance(row.get("evidence_verification"), dict) else {}
        if evidence.get("verified") is not True:
            failures.append("evidence_chain_not_verified")
        if args.review_jsonl or args.require_review:
            _, review_failures = review_passes_training_gate(review, args.min_review_score)
            failures.extend(review_failures)
        if args.task_fit_review_jsonl or args.require_task_fit_review:
            _, task_fit_failures = task_fit_review_passes_training_gate(task_fit_review, args.min_task_fit_score)
            failures.extend(task_fit_failures)
        if args.training_value_review_jsonl or args.require_training_value_review:
            _, training_value_failures = training_value_review_passes_gate(training_value_review, args.min_training_value_score)
            failures.extend(training_value_failures)
        if not args.allow_zero_or_empty:
            failures.extend(risky_zero_or_empty_labels(row, review))

        out = dict(row)
        out["pipeline_id"] = pipeline_id
        if review:
            out["ground_truth_review"] = review
        if task_fit_review:
            out["task_fit_review"] = task_fit_review
        if training_value_review:
            out["training_value_review"] = training_value_review
        verified_failures = verified_pool_failures(out, review, bool(args.allow_zero_or_empty))
        task_fit_pool_ok, task_fit_pool_failures = task_fit_pool_passes_gate(task_fit_review, args.min_task_fit_pool_score)
        warmup_pool_ok, warmup_pool_failures = warmup_pool_passes_gate(
            review,
            training_value_review,
            args.min_warmup_training_value_score,
        )
        pool_labels: list[str] = []
        if not verified_failures:
            pool_labels.append("verified_pool")
            verified_pool.append(out)
            if not warmup_pool_failures and warmup_pool_ok:
                pool_labels.append("warmup_pool")
                warmup_pool.append(out)
            if not task_fit_pool_failures and task_fit_pool_ok:
                pool_labels.append("task_fit_pool")
                task_fit_pool.append(out)
        out["training_selection"] = {
            "selected": not failures,
            "failures": sorted(set(failures)),
            "pool_labels": pool_labels,
            "pool_failures": {
                "verified_pool": verified_failures,
                "warmup_pool": warmup_pool_failures,
                "task_fit_pool": task_fit_pool_failures,
            },
            "policy": {
                "require_local_judge": True,
                "require_evidence_verification": True,
                "require_ground_truth_review": bool(args.review_jsonl or args.require_review),
                "require_task_fit_review": bool(args.task_fit_review_jsonl or args.require_task_fit_review),
                "require_training_value_review": bool(args.training_value_review_jsonl or args.require_training_value_review),
                "allow_zero_or_empty": bool(args.allow_zero_or_empty),
                "min_local_score": args.min_local_score,
                "min_review_score": args.min_review_score,
                "min_task_fit_score": args.min_task_fit_score,
                "min_training_value_score": args.min_training_value_score,
                "min_task_fit_pool_score": args.min_task_fit_pool_score,
                "min_warmup_training_value_score": args.min_warmup_training_value_score,
            },
        }
        if failures:
            rejected.append(out)
        else:
            out["training_selection"]["pool_labels"] = sorted(set(pool_labels + ["training_ready"]))
            selected.append(out)

    write_jsonl(Path(args.output_jsonl), selected)
    if args.rejected_jsonl:
        write_jsonl(Path(args.rejected_jsonl), rejected)
    if args.verified_pool_jsonl:
        write_jsonl(Path(args.verified_pool_jsonl), verified_pool)
    if args.warmup_pool_jsonl:
        write_jsonl(Path(args.warmup_pool_jsonl), warmup_pool)
    if args.task_fit_pool_jsonl:
        write_jsonl(Path(args.task_fit_pool_jsonl), task_fit_pool)
    if args.dashboard:
        write_training_ready_dashboard(Path(args.dashboard), selected, rejected)
    print(
        json.dumps(
            {
                "input": len(rows),
                "selected": len(selected),
                "rejected": len(rejected),
                "verified_pool": len(verified_pool),
                "warmup_pool": len(warmup_pool),
                "task_fit_pool": len(task_fit_pool),
                "output_jsonl": args.output_jsonl,
                "rejected_jsonl": args.rejected_jsonl,
                "verified_pool_jsonl": args.verified_pool_jsonl,
                "warmup_pool_jsonl": args.warmup_pool_jsonl,
                "task_fit_pool_jsonl": args.task_fit_pool_jsonl,
                "dashboard": args.dashboard,
            },
            ensure_ascii=False,
            indent=2,
        )
    )


def register_audit_check(name: str, check: AuditCheck) -> None:
    AUDIT_EXTENSION_CHECKS.append((name, check))


def value_is_empty(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, (list, tuple, set, dict)):
        return not value
    return False


def expected_answer_value(row: dict[str, Any]) -> Any:
    expected = row.get("expected_answer")
    if isinstance(expected, dict):
        return expected.get("value")
    return None


def contains_materialization_marker(value: Any) -> bool:
    text = json.dumps(jsonable(value), ensure_ascii=False, sort_keys=True).casefold()
    return any(marker in text for marker in MATERIALIZATION_MARKERS)


def answer_normalization_key(value: Any) -> Any:
    value = jsonable(value)
    if isinstance(value, str):
        text = re.sub(r"\s+", " ", value).strip().casefold()
        comma_parts = [part.strip().casefold() for part in text.split(",") if part.strip()]
        if len(comma_parts) > 1:
            return ("comma_set", tuple(sorted(comma_parts)))
        return ("string", text)
    if isinstance(value, bool) or value is None:
        return ("scalar", value)
    if isinstance(value, (int, float)):
        return ("number", round(float(value), 12))
    if isinstance(value, list):
        return ("json", json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
    if isinstance(value, dict):
        return ("json", json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
    return ("string", str(value).strip().casefold())


def answers_consistent(left: Any, right: Any, template: str = "", validator_args: dict[str, Any] | None = None) -> bool:
    if answer_normalization_key(left) == answer_normalization_key(right):
        return True
    if isinstance(left, (int, float)) and isinstance(right, (int, float)):
        tolerance = 1e-9
        if isinstance(validator_args, dict) and isinstance(validator_args.get("tolerance"), (int, float)):
            tolerance = max(tolerance, float(validator_args["tolerance"]))
        return abs(float(left) - float(right)) <= tolerance
    if template == "numeric_tolerance":
        try:
            tolerance = 1e-9
            if isinstance(validator_args, dict) and isinstance(validator_args.get("tolerance"), (int, float)):
                tolerance = max(tolerance, float(validator_args["tolerance"]))
            return abs(float(left) - float(right)) <= tolerance
        except (TypeError, ValueError):
            return False
    return False


def candidate_audit_hash(row: dict[str, Any]) -> str:
    return stable_hash(
        [
            row.get("pipeline_id"),
            row.get("dataset"),
            row.get("query_id"),
            row.get("query"),
            row.get("source_task_signature"),
            row.get("candidate_solution"),
            row.get("expected_answer"),
            row.get("validator_template"),
            row.get("validator_args"),
        ]
    )


def compact_verification_for_audit(ev: dict[str, Any]) -> dict[str, Any]:
    observations = []
    for obs in ev.get("observations", []) or []:
        if not isinstance(obs, dict):
            continue
        observations.append(
            {
                "tool": obs.get("tool", "query_db"),
                "db_name": obs.get("db_name"),
                "db_type": obs.get("db_type"),
                "query": truncate(str(obs.get("query", "")), 800),
                "success": bool(obs.get("success")),
                "error": truncate(str(obs.get("error", "")), 400) if obs.get("error") else "",
                "summary": obs.get("summary", {}),
            }
        )
    return {
        "verified": bool(ev.get("verified")),
        "final_answer": ev.get("final_answer"),
        "final_answer_source": ev.get("final_answer_source", ""),
        "validator_result": ev.get("validator_result", {}),
        "failures": ev.get("failures", []) or [],
        "observations": observations,
    }


def review_gate_check(name: str, review: dict[str, Any], min_score: float, required: bool) -> tuple[bool, list[str]]:
    if name == "ground_truth_review":
        passed, failures = review_passes_training_gate(review, min_score)
    else:
        passed, failures = task_fit_review_passes_training_gate(review, min_score)
    if required:
        return passed, failures
    if review:
        return passed, failures
    return True, []


def run_audit_extension_checks(
    row: dict[str, Any],
    repeat_results: list[dict[str, Any]],
    args: argparse.Namespace,
) -> tuple[dict[str, bool], list[str]]:
    checks: dict[str, bool] = {}
    failures: list[str] = []
    for name, check in AUDIT_EXTENSION_CHECKS:
        try:
            passed, reason = check(row, repeat_results, args)
        except Exception as exc:
            passed = False
            reason = f"{type(exc).__name__}: {exc}"
        checks[f"extension:{name}"] = bool(passed)
        if not passed:
            failures.append(f"extension:{name}:{reason}")
    return checks, failures


def audit_candidate(row: dict[str, Any], args: argparse.Namespace) -> dict[str, Any]:
    expected_value = expected_answer_value(row)
    template = str(row.get("validator_template") or "")
    validator_args = row.get("validator_args") if isinstance(row.get("validator_args"), dict) else {}
    evidence_card = row.get("evidence_card") if isinstance(row.get("evidence_card"), dict) else {}
    materialization = row.get("ground_truth_materialization") if isinstance(row.get("ground_truth_materialization"), dict) else {}
    training_selection = row.get("training_selection") if isinstance(row.get("training_selection"), dict) else {}
    judge = row.get("judge") if isinstance(row.get("judge"), dict) else {}
    query_transform = row.get("query_transform") if isinstance(row.get("query_transform"), dict) else {}
    candidate_solution = row.get("candidate_solution") if isinstance(row.get("candidate_solution"), dict) else {}

    checks: dict[str, bool] = {}
    failures: list[str] = []

    checks["training_selection_selected"] = training_selection.get("selected") is True
    checks["local_judge_accepted"] = bool(judge.get("accepted")) and float(judge.get("score", 0.0) or 0.0) >= args.min_local_score
    gt_ok, gt_failures = review_gate_check("ground_truth_review", row.get("ground_truth_review", {}) if isinstance(row.get("ground_truth_review"), dict) else {}, args.min_review_score, args.require_review)
    task_fit_ok, task_fit_failures = review_gate_check("task_fit_review", row.get("task_fit_review", {}) if isinstance(row.get("task_fit_review"), dict) else {}, args.min_task_fit_score, args.require_task_fit_review)
    training_value_ok, training_value_failures = training_value_review_passes_gate(
        row.get("training_value_review", {}) if isinstance(row.get("training_value_review"), dict) else {},
        args.min_training_value_score,
    ) if args.require_training_value_review else (True, [])
    checks["ground_truth_review_accepted"] = gt_ok
    checks["task_fit_review_accepted"] = task_fit_ok
    checks["training_value_review_accepted"] = training_value_ok
    failures.extend(gt_failures)
    failures.extend(task_fit_failures)
    failures.extend(training_value_failures)

    checks["expected_answer_nonempty"] = not value_is_empty(expected_value)
    checks["expected_answer_concrete"] = not contains_materialization_marker(row.get("expected_answer", {}))
    checks["validator_template_known"] = template in VALIDATOR_TEMPLATE_NAMES
    checks["validator_args_nonempty"] = isinstance(validator_args, dict) and bool(validator_args)
    checks["validator_args_concrete"] = not contains_materialization_marker(validator_args)
    checks["reward_model_rule_ready"] = checks["validator_template_known"] and checks["validator_args_nonempty"]
    checks["source_signature_present"] = bool(row.get("source_task_signature"))
    checks["candidate_solution_present"] = bool(candidate_solution) and any(candidate_solution.get(key) for key in ("python", "sql", "mongo"))
    checks["query_transform_safe"] = str(query_transform.get("type", "none")) in {"none", "injection", "fuzzing", "obfuscation"}
    checks["query_transform_documented"] = not query_transform or bool(query_transform.get("safety_check") or query_transform.get("rationale") or query_transform.get("description"))
    checks["evidence_card_materialized"] = not value_is_empty(evidence_card.get("observed_answer"))
    checks["evidence_card_concrete"] = not contains_materialization_marker(
        {
            "observed_answer": evidence_card.get("observed_answer"),
            "expected_answer": row.get("expected_answer", {}),
            "validator_args": validator_args,
        }
    )
    checks["materialization_risk_free"] = not materialization.get("risks") and not materialization.get("execution_failures")

    dataset_dir = dataset_dir_for_candidate(row, Path(args.bench_root))
    try:
        db_clients = load_db_clients(dataset_dir) if dataset_dir.exists() else {}
    except Exception as exc:
        db_clients = {}
        failures.append(f"db_runtime_load_failed:{type(exc).__name__}: {exc}")
    checks["db_runtime_available"] = dataset_dir.exists() and bool(db_clients)

    repeat_results: list[dict[str, Any]] = []
    baseline_answer: Any = None
    baseline_set = False
    for _ in range(max(1, args.repeat_runs)):
        ev = verify_candidate_evidence(row, args)
        compact = compact_verification_for_audit(ev)
        repeat_results.append(compact)
        if not ev.get("verified"):
            failures.extend(str(failure) for failure in ev.get("failures", []) or ["evidence_verification_failed"])
        final_answer = ev.get("final_answer")
        if value_is_empty(final_answer):
            failures.append("audit_final_answer_empty")
        elif not baseline_set:
            baseline_answer = final_answer
            baseline_set = True
        elif not answers_consistent(baseline_answer, final_answer, template, validator_args):
            failures.append("audit_repeat_final_answer_changed")
        validator_result = ev.get("validator_result", {}) if isinstance(ev.get("validator_result"), dict) else {}
        if validator_result.get("passed") is not True:
            failures.append(f"audit_validator_failed:{validator_result.get('reason', 'unknown')}")

    checks["evidence_verified_repeated"] = all(result.get("verified") for result in repeat_results)
    checks["final_answer_stable_across_repeats"] = not any(failure == "audit_repeat_final_answer_changed" for failure in failures)
    checks["final_answer_matches_expected"] = baseline_set and answers_consistent(baseline_answer, expected_value, template, validator_args)
    checks["evidence_card_matches_final_answer"] = (
        baseline_set
        and not value_is_empty(evidence_card.get("observed_answer"))
        and answers_consistent(baseline_answer, evidence_card.get("observed_answer"), template, validator_args)
    )
    anti_degenerate_metrics = non_degeneracy_metrics(row, repeat_results)
    if not args.disable_anti_degenerate_checks:
        anti_ok, anti_reason = anti_degenerate_audit_check(row, repeat_results, args)
        checks["anti_degenerate_training_value"] = anti_ok
        if not anti_ok:
            failures.append(f"anti_degenerate_training_value:{anti_reason}")
    else:
        anti_degenerate_metrics["disabled"] = True

    extension_checks, extension_failures = run_audit_extension_checks(row, repeat_results, args)
    checks.update(extension_checks)
    failures.extend(extension_failures)

    for check_name, passed in checks.items():
        if not passed:
            failures.append(f"check_failed:{check_name}")

    failures = sorted(set(str(failure) for failure in failures if failure))
    return {
        "audit_version": FINAL_AUDIT_VERSION,
        "audited_at_unix": int(time.time()),
        "candidate_hash": candidate_audit_hash(row),
        "passed": not failures,
        "checks": checks,
        "failures": failures,
        "repeat_runs": max(1, args.repeat_runs),
        "repeat_results": repeat_results,
        "verified_final_answer": jsonable(baseline_answer) if baseline_set else None,
        "verified_final_answer_source": repeat_results[0].get("final_answer_source", "") if repeat_results else "",
        "anti_degenerate_metrics": jsonable(anti_degenerate_metrics),
        "policy": {
            "require_review": bool(args.require_review),
            "require_task_fit_review": bool(args.require_task_fit_review),
            "min_local_score": args.min_local_score,
            "min_review_score": args.min_review_score,
            "min_task_fit_score": args.min_task_fit_score,
            "min_training_value_score": args.min_training_value_score,
            "disable_anti_degenerate_checks": bool(args.disable_anti_degenerate_checks),
            "min_ranking_winner_count": int(args.min_ranking_winner_count),
            "min_normalized_group_size": int(args.min_normalized_group_size),
            "query_row_limit": args.query_row_limit,
            "python_timeout": args.python_timeout,
            "allow_python_exec": bool(args.allow_python_exec),
            "extension_check_count": len(AUDIT_EXTENSION_CHECKS),
        },
    }


def write_audit_dashboard(path: Path, selected: list[dict[str, Any]], rejected: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    failure_counts = Counter()
    check_counts = Counter()
    for row in selected + rejected:
        audit = row.get("final_audit", {}) if isinstance(row.get("final_audit"), dict) else {}
        for failure in audit.get("failures", []) or []:
            failure_counts[str(failure)] += 1
        for name, passed in (audit.get("checks", {}) or {}).items():
            if not passed:
                check_counts[str(name)] += 1
    lines = [
        "# DABench Final Training Data Audit",
        "",
        "## Summary",
        "",
        f"- audit_version: `{FINAL_AUDIT_VERSION}`",
        f"- passed: {len(selected)}",
        f"- rejected: {len(rejected)}",
        f"- extension_checks_registered: {len(AUDIT_EXTENSION_CHECKS)}",
        "",
        "## Extension Points",
        "",
        "- Add deterministic domain checks by calling `register_audit_check(name, check)` in this module.",
        "- Each check receives `(row, repeat_results, args)` and must return `(passed, reason)`.",
        "- Final VERL export can require `final_audit.passed` with `build-verl --require-final-audit`.",
        "",
        "## Failed Checks",
        "",
    ]
    if check_counts:
        for name, count in check_counts.most_common():
            lines.append(f"- `{name}`: {count}")
    else:
        lines.append("- no failed checks")
    lines.extend(["", "## Failure Reasons", ""])
    if failure_counts:
        for failure, count in failure_counts.most_common():
            lines.append(f"- `{failure}`: {count}")
    else:
        lines.append("- no failure reasons")
    lines.extend(["", "## Passed Rows", ""])
    if selected:
        for row in selected:
            audit = row.get("final_audit", {}) if isinstance(row.get("final_audit"), dict) else {}
            lines.append(
                f"- `{row.get('pipeline_id', '')}` `{row.get('dataset', '')}` "
                f"answer=`{truncate(answer_to_validator_output(audit.get('verified_final_answer')), 160)}`"
            )
    else:
        lines.append("- no passed rows")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def audit_training_ready(args: argparse.Namespace) -> None:
    rows = read_jsonl(Path(args.candidate_jsonl))
    selected: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    for idx, row in enumerate(rows):
        out = dict(row)
        out["pipeline_id"] = row.get("pipeline_id") or f"candidate_{idx:05d}_{stable_hash([row.get('dataset'), row.get('query')])}"
        out["final_audit"] = audit_candidate(out, args)
        if out["final_audit"].get("passed"):
            selected.append(out)
        else:
            rejected.append(out)
    write_jsonl(Path(args.output_jsonl), selected)
    if args.rejected_jsonl:
        write_jsonl(Path(args.rejected_jsonl), rejected)
    if args.dashboard:
        write_audit_dashboard(Path(args.dashboard), selected, rejected)
    print(
        json.dumps(
            {
                "input": len(rows),
                "audit_passed": len(selected),
                "audit_rejected": len(rejected),
                "output_jsonl": args.output_jsonl,
                "rejected_jsonl": args.rejected_jsonl,
                "dashboard": args.dashboard,
                "audit_version": FINAL_AUDIT_VERSION,
            },
            ensure_ascii=False,
            indent=2,
        )
    )


def curriculum_sort_key(row: dict[str, Any]) -> tuple[int, float, int, str]:
    task_type = str(row.get("task_type", ""))
    preferred = 0 if task_type in {"aggregation_heavy", "multi_hop_join", "json_heavy", "mixed_sql_mongo"} else 1
    difficulty = DIFFICULTY_RANK.get(str(row.get("difficulty", "medium")).casefold(), 1)
    score = float(row.get("judge", {}).get("score", 0.0))
    return (preferred, -score, -difficulty, str(row.get("dataset", "")))


def make_sandbox_manifest(args: argparse.Namespace) -> None:
    if not getattr(args, "allow_sandbox_output", False):
        raise SystemExit(
            "Sandbox manifest output is disabled for the current data-pipeline stage. "
            "Pass --allow-sandbox-output only when the sandbox registration stage is explicitly needed."
        )
    rows = read_jsonl(Path(args.candidate_jsonl))
    tasks = []
    for idx, row in enumerate(rows):
        pipeline_id = row.get("pipeline_id") or f"synthetic_{idx:05d}_{stable_hash([row.get('dataset'), row.get('query')])}"
        tasks.append(
            {
                "pipeline_id": pipeline_id,
                "dataset": row.get("dataset"),
                "query_id": row.get("query_id", idx + 1),
                "query": row.get("query"),
                "db_description": row.get("db_description", ""),
                "db_config": row.get("db_config", {}),
                "expected_answer": row.get("expected_answer", {}),
                "validator_template": row.get("validator_template", ""),
                "validator_args": row.get("validator_args", {}),
                "reward_spec": row.get("reward_spec", {}),
                "candidate_solution": row.get("candidate_solution", {}),
                "registration_notes": row.get("registration_notes", {}),
                "artifact_refs": row.get("artifact_refs", []),
                "synthetic": True,
                "status": "needs_sandbox_registration",
            }
        )
    manifest = {
        "version": 1,
        "total": len(tasks),
        "source": args.candidate_jsonl,
        "sandbox_url": args.sandbox_url,
        "tasks": tasks,
    }
    path = Path(args.output_json)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"total": len(tasks), "output_json": args.output_json}, ensure_ascii=False, indent=2))


def slugify_name(value: Any, default: str = "synthetic") -> str:
    text = str(value or "").strip()
    text = re.sub(r"[^A-Za-z0-9_.-]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("._-")
    return text or default


def python_json_loads_expr(value: Any) -> str:
    encoded = json.dumps(jsonable(value), ensure_ascii=False, sort_keys=True)
    return f"json.loads({json.dumps(encoded, ensure_ascii=False)})"


def standalone_validate_py(template: str, validator_args: dict[str, Any], expected_answer: Any, metadata: dict[str, Any]) -> str:
    return (
        "#!/usr/bin/env python3\n"
        "\"\"\"Auto-generated validator for a synthetic DABench task.\n\n"
        "This file is intentionally standalone so sandbox reward can execute it\n"
        "without importing the data pipeline package.\n"
        "\"\"\"\n"
        "from __future__ import annotations\n\n"
        "import json\n"
        "import re\n"
        "from typing import Any\n\n"
        f"VALIDATOR_TEMPLATE = {json.dumps(template, ensure_ascii=False)}\n"
        f"VALIDATOR_ARGS = {python_json_loads_expr(validator_args)}\n"
        f"EXPECTED_ANSWER = {python_json_loads_expr(expected_answer)}\n"
        f"SYNTHETIC_METADATA = {python_json_loads_expr(metadata)}\n\n"
        "def normalize_text(text: str) -> str:\n"
        "    text = re.sub(r\"(?<=\\d),(?=\\d{3}\\b)\", \"\", str(text))\n"
        "    text = text.lower().replace(\"&\", \" and \").replace(\"@\", \" at \")\n"
        "    text = re.sub(r\"[^a-z0-9\\s]\", \" \", text)\n"
        "    return re.sub(r\"\\s+\", \" \", text).strip()\n\n"
        "def extract_numbers(text: str) -> list[float]:\n"
        "    values: list[float] = []\n"
        "    clean = re.sub(r\"(?<=\\d),(?=\\d{3}\\b)\", \"\", str(text))\n"
        "    for raw in re.findall(r\"(?<![\\w.-])-?\\d+(?:\\.\\d+)?(?![\\w.-])\", clean):\n"
        "        try:\n"
        "            values.append(float(raw))\n"
        "        except ValueError:\n"
        "            pass\n"
        "    return values\n\n"
        "def validate_with_template(output: str, template: str, args: dict[str, Any]) -> tuple[bool, str]:\n"
        "    if template == \"contains_all\":\n"
        "        items = [str(x) for x in args.get(\"items\", [])]\n"
        "        case_sensitive = bool(args.get(\"case_sensitive\", False))\n"
        "        haystack = output if case_sensitive else output.lower()\n"
        "        for item in items:\n"
        "            needle = item if case_sensitive else item.lower()\n"
        "            if needle not in haystack:\n"
        "                return False, f\"missing item: {item}\"\n"
        "        return True, \"all items present\"\n\n"
        "    if template == \"normalized_contains_all\":\n"
        "        items = [str(x) for x in args.get(\"items\", [])]\n"
        "        haystack = normalize_text(output)\n"
        "        for item in items:\n"
        "            if normalize_text(item) not in haystack:\n"
        "                return False, f\"missing normalized item: {item}\"\n"
        "        return True, \"all normalized items present\"\n\n"
        "    if template == \"numeric_tolerance\":\n"
        "        expected = float(args[\"expected\"])\n"
        "        tolerance = float(args.get(\"tolerance\", 1e-6))\n"
        "        for value in extract_numbers(output):\n"
        "            if abs(value - expected) <= tolerance:\n"
        "                return True, f\"matched {value}\"\n"
        "        return False, f\"expected numeric value {expected} within tolerance {tolerance}\"\n\n"
        "    if template == \"numeric_list_tolerance\":\n"
        "        expected_values = [float(x) for x in args.get(\"expected\", [])]\n"
        "        tolerance = float(args.get(\"tolerance\", 1e-6))\n"
        "        found = extract_numbers(output)\n"
        "        for expected in expected_values:\n"
        "            if not any(abs(value - expected) <= tolerance for value in found):\n"
        "                return False, f\"missing numeric value {expected} within tolerance {tolerance}\"\n"
        "        return True, \"all numeric values matched\"\n\n"
        "    if template == \"ordered_contains\":\n"
        "        items = [str(x) for x in args.get(\"items\", [])]\n"
        "        haystack = normalize_text(output)\n"
        "        pos = -1\n"
        "        for item in items:\n"
        "            idx = haystack.find(normalize_text(item), pos + 1)\n"
        "            if idx < 0:\n"
        "                return False, f\"missing or out-of-order item: {item}\"\n"
        "            pos = idx\n"
        "        return True, \"ordered items present\"\n\n"
        "    if template == \"unordered_set_contains\":\n"
        "        items = [str(x) for x in args.get(\"items\", [])]\n"
        "        haystack = normalize_text(output)\n"
        "        missing = [item for item in items if normalize_text(item) not in haystack]\n"
        "        if missing:\n"
        "            return False, f\"missing set items: {missing}\"\n"
        "        return True, \"set items present\"\n\n"
        "    if template == \"json_exact_fields\":\n"
        "        expected = args.get(\"expected\", {})\n"
        "        if not isinstance(expected, dict) or not expected:\n"
        "            return False, \"expected must be nonempty object\"\n"
        "        try:\n"
        "            obj = json.loads(output.strip())\n"
        "        except Exception as exc:\n"
        "            return False, f\"invalid json output: {exc}\"\n"
        "        if not isinstance(obj, dict):\n"
        "            return False, \"output is not a json object\"\n"
        "        tolerance = float(args.get(\"numeric_tolerance\", 1e-6))\n"
        "        for key, expected_value in expected.items():\n"
        "            if key not in obj:\n"
        "                return False, f\"missing json key: {key}\"\n"
        "            actual = obj[key]\n"
        "            if isinstance(expected_value, (int, float)):\n"
        "                try:\n"
        "                    if abs(float(actual) - float(expected_value)) > tolerance:\n"
        "                        return False, f\"json key {key} numeric mismatch\"\n"
        "                except Exception:\n"
        "                    return False, f\"json key {key} is not numeric\"\n"
        "            elif normalize_text(str(actual)) != normalize_text(str(expected_value)):\n"
        "                return False, f\"json key {key} mismatch\"\n"
        "        return True, \"json fields matched\"\n\n"
        "    if template == \"name_value_proximity\":\n"
        "        pairs = args.get(\"pairs\", [])\n"
        "        window = int(args.get(\"window\", 150))\n"
        "        norm_output = normalize_text(output)\n"
        "        for pair in pairs:\n"
        "            name = str(pair.get(\"name\", \"\"))\n"
        "            value = pair.get(\"value\")\n"
        "            norm_name = normalize_text(name)\n"
        "            idx = norm_output.find(norm_name)\n"
        "            if idx < 0:\n"
        "                return False, f\"missing name: {name}\"\n"
        "            raw_idx = max(0, output.lower().find(name.lower()))\n"
        "            raw_window = output[max(0, raw_idx - window): raw_idx + len(name) + window]\n"
        "            if isinstance(value, (int, float)):\n"
        "                tol = float(pair.get(\"tolerance\", args.get(\"tolerance\", 1e-6)))\n"
        "                if not any(abs(num - float(value)) <= tol for num in extract_numbers(raw_window)):\n"
        "                    return False, f\"numeric value {value} not near {name}\"\n"
        "            elif normalize_text(str(value)) not in normalize_text(raw_window):\n"
        "                return False, f\"value {value} not near {name}\"\n"
        "        return True, \"all name/value pairs matched\"\n\n"
        "    return False, f\"unknown validator template: {template}\"\n\n"
        "def validate(llm_output: str):\n"
        "    return validate_with_template(str(llm_output), VALIDATOR_TEMPLATE, VALIDATOR_ARGS)\n"
    )


def write_ground_truth_csv(path: Path, expected_answer: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    value = expected_answer.get("value") if isinstance(expected_answer, dict) else expected_answer
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["answer"])
        writer.writerow([answer_to_validator_output(value)])


def import_validate_py(path: Path) -> Any:
    import importlib.util

    module_name = f"synthetic_validate_{stable_hash([str(path), time.time()])}"
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot import validate.py from {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def self_test_exported_validate(validate_path: Path, row: dict[str, Any]) -> dict[str, Any]:
    expected = row.get("expected_answer", {})
    value = expected.get("value") if isinstance(expected, dict) else expected
    output = answer_to_validator_output(value)
    try:
        module = import_validate_py(validate_path)
        result = module.validate(output)
        if isinstance(result, tuple):
            passed, reason = bool(result[0]), str(result[1] if len(result) > 1 else "")
        else:
            passed, reason = bool(result), ""
        return {"passed": passed, "reason": reason, "test_output": output}
    except Exception as exc:
        return {"passed": False, "reason": f"{type(exc).__name__}: {exc}", "test_output": output}


def prepare_synthetic_dataset_dir(dataset_dir: Path, source_dir: Path, args: argparse.Namespace) -> dict[str, Any]:
    dataset_dir.mkdir(parents=True, exist_ok=True)
    copied: list[str] = []
    for name in ("db_description.txt", "db_description_withhint.txt", "db_config.yaml"):
        src = source_dir / name
        if src.exists():
            dst = dataset_dir / name
            if args.overwrite or not dst.exists():
                dst.write_text(src.read_text(encoding="utf-8"), encoding="utf-8")
            copied.append(name)
    source_query_dataset = source_dir / "query_dataset"
    target_query_dataset = dataset_dir / "query_dataset"
    query_dataset_mode = "missing"
    if source_query_dataset.exists():
        if target_query_dataset.exists() or target_query_dataset.is_symlink():
            query_dataset_mode = "existing"
        elif args.copy_query_dataset:
            shutil.copytree(source_query_dataset, target_query_dataset)
            query_dataset_mode = "copied"
        else:
            target_query_dataset.symlink_to(source_query_dataset, target_is_directory=True)
            query_dataset_mode = "symlink"
    return {"copied_files": copied, "query_dataset": query_dataset_mode, "source_dataset_dir": str(source_dir)}


def csv_file_as_answer_text(path: Path) -> str:
    """Render CSV answers as model-like text instead of raw comma-adjacent cells."""
    raw = read_text(path).lstrip("\ufeff")
    if not raw.strip():
        return ""
    rows = list(csv.reader(io.StringIO(raw)))
    return "\n".join(" | ".join(str(cell) for cell in row) for row in rows)


def load_db_clients_from_config(path: Path) -> dict[str, dict[str, Any]]:
    try:
        import yaml
    except ModuleNotFoundError as exc:
        raise RuntimeError("PyYAML is required to ingest DAB package db_config.yaml") from exc
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    clients = data.get("db_clients") if isinstance(data, dict) else {}
    if not isinstance(clients, dict):
        return {}
    return {str(name): cfg for name, cfg in clients.items() if isinstance(cfg, dict)}


def external_dab_dataset_dirs(input_root: Path, dataset_allowlist: set[str]) -> list[Path]:
    roots: list[Path] = []
    for path in sorted(input_root.iterdir()):
        if not path.is_dir() or not path.name.startswith("query_"):
            continue
        dataset = path.name.removeprefix("query_")
        if dataset_allowlist and dataset not in dataset_allowlist and path.name not in dataset_allowlist:
            continue
        roots.append(path)
    return roots


def db_config_artifact_status(dataset_dir: Path, clients: dict[str, dict[str, Any]]) -> tuple[list[str], list[str]]:
    missing: list[str] = []
    db_types: list[str] = []
    for logical_name, cfg in sorted(clients.items()):
        db_type = str(cfg.get("db_type") or "").casefold()
        if db_type:
            db_types.append(db_type)
        if db_type in {"sqlite", "duckdb"}:
            key = "db_path"
        elif db_type == "postgres":
            key = "sql_file"
        elif db_type == "mongo":
            key = "dump_folder"
        else:
            missing.append(f"{logical_name}:unsupported_db_type:{db_type or 'missing'}")
            continue
        rel_path = cfg.get(key)
        if not rel_path:
            missing.append(f"{logical_name}:missing_{key}")
            continue
        artifact_path = dataset_dir / str(rel_path)
        if not artifact_path.exists():
            missing.append(f"{logical_name}:missing_artifact:{rel_path}")
        elif db_type == "mongo" and artifact_path.is_dir() and not any(artifact_path.rglob("*.bson")):
            missing.append(f"{logical_name}:mongo_dump_has_no_bson:{rel_path}")
    return missing, sorted(set(db_types))


def external_task_type_from_db_types(db_types: list[str]) -> str:
    sql_types = {db_type for db_type in db_types if db_type in {"sqlite", "duckdb", "postgres"}}
    if "mongo" in db_types and sql_types:
        return "mixed_sql_mongo"
    if len(sql_types) >= 2:
        return "multi_sql_join"
    return "external_dab"


def smoke_validate_py(validate_path: Path, gold_answer: str) -> dict[str, Any]:
    try:
        module = import_validate_py(validate_path)
        gold_result = module.validate(gold_answer)
        bogus_result = module.validate("__definitely_wrong_external_dab_answer__")
        empty_result = module.validate("")
    except Exception as exc:
        return {"passed": False, "reason": f"{type(exc).__name__}: {exc}"}

    def normalize_result(result: Any) -> dict[str, Any]:
        if isinstance(result, tuple):
            return {"passed": bool(result[0]), "reason": str(result[1] if len(result) > 1 else "")}
        return {"passed": bool(result), "reason": ""}

    gold = normalize_result(gold_result)
    bogus = normalize_result(bogus_result)
    empty = normalize_result(empty_result)
    passed = bool(gold.get("passed")) and not bool(bogus.get("passed")) and not bool(empty.get("passed"))
    reason = "gold_passed_and_bogus_and_empty_failed" if passed else f"gold={gold}; bogus={bogus}; empty={empty}"
    return {"passed": passed, "reason": reason, "gold": gold, "bogus": bogus, "empty": empty}


def copy_external_dataset_for_ingest(source_dir: Path, output_root: Path, overwrite: bool) -> Path:
    target_dir = output_root / source_dir.name
    if target_dir.exists():
        if not overwrite:
            return target_dir
        shutil.rmtree(target_dir)
    ignore = shutil.ignore_patterns(".git", "__pycache__", "*.pyc", "clean", "logs", "manual_querycode")
    shutil.copytree(source_dir, target_dir, ignore=ignore)
    return target_dir


def ingest_external_dab_package(args: argparse.Namespace) -> None:
    input_root = Path(args.input_root).resolve()
    output_dir = Path(args.output_dir).resolve()
    tasks_root = output_dir / "dabench_tasks"
    output_dir.mkdir(parents=True, exist_ok=True)
    tasks_root.mkdir(parents=True, exist_ok=True)

    dataset_allowlist = {name.strip() for name in str(args.datasets or "").split(",") if name.strip()}
    source_dirs = external_dab_dataset_dirs(input_root, dataset_allowlist)
    rows: list[dict[str, Any]] = []
    tasks: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    dataset_summary: dict[str, dict[str, Any]] = {}

    for source_dir in source_dirs:
        dataset = source_dir.name.removeprefix("query_")
        copied_dataset_dir = copy_external_dataset_for_ingest(source_dir, tasks_root, bool(args.overwrite))
        db_config_path = copied_dataset_dir / "db_config.yaml"
        db_description_path = copied_dataset_dir / "db_description.txt"
        db_description_withhint_path = copied_dataset_dir / "db_description_withhint.txt"
        db_description_text = read_text(db_description_path) if db_description_path.exists() else ""
        db_description_withhint_text = read_text(db_description_withhint_path) if db_description_withhint_path.exists() else ""
        clients = load_db_clients_from_config(db_config_path) if db_config_path.exists() else {}
        missing_artifacts, db_types = db_config_artifact_status(copied_dataset_dir, clients)
        dataset_issues: list[str] = []
        if not db_config_path.exists():
            dataset_issues.append("missing_db_config")
        if not db_description_path.exists():
            dataset_issues.append("missing_db_description")
        if not db_description_withhint_path.exists():
            dataset_issues.append("missing_db_description_withhint")
        if len(clients) < 2:
            dataset_issues.append("fewer_than_two_logical_databases")
        if len(set(db_types)) < 2:
            dataset_issues.append("fewer_than_two_dbms_types")
        dataset_issues.extend(missing_artifacts)
        if args.skip_db_type:
            skipped_types = {value.casefold() for value in args.skip_db_type}
            if skipped_types.intersection(set(db_types)):
                skipped.append({"dataset": dataset, "reason": f"skipped_db_type:{sorted(skipped_types.intersection(set(db_types)))}"})
                continue

        query_dirs = [p for p in sorted(copied_dataset_dir.iterdir(), key=query_sort_key) if p.is_dir() and p.name.startswith("query") and p.name.removeprefix("query").isdigit()]
        for query_dir in query_dirs:
            query_id = int(query_dir.name.removeprefix("query"))
            query_json = query_dir / "query.json"
            ground_truth_csv = query_dir / "ground_truth.csv"
            validate_py = query_dir / "validate.py"
            if not query_json.exists() or not ground_truth_csv.exists() or not validate_py.exists():
                skipped.append({"dataset": dataset, "query_id": query_id, "reason": "missing_query_ground_truth_or_validate"})
                continue
            query = read_query(query_json)
            gold_answer = csv_file_as_answer_text(ground_truth_csv)
            query_issues: list[str] = []
            if not str(query).strip():
                query_issues.append("empty_query")
            if args.require_nonempty_answer and not gold_answer.strip():
                query_issues.append("empty_ground_truth_answer")
            if args.leakage_check and gold_answer.strip():
                leak_sources = {
                    "query": query,
                    "db_description": db_description_text,
                    "db_description_withhint": db_description_withhint_text,
                }
                for source_name, source_text in leak_sources.items():
                    if leaks_answer(gold_answer, str(source_text)):
                        query_issues.append(f"answer_leakage_in_{source_name}")
            if query_issues:
                skipped.append({"dataset": dataset, "query_id": query_id, "reason": "package_hygiene_failed:" + ";".join(query_issues)})
                continue
            validate_smoke = smoke_validate_py(validate_py, gold_answer) if args.self_test else {"passed": None, "reason": "disabled"}
            if args.self_test and args.require_validate_pass and not validate_smoke.get("passed"):
                skipped.append({"dataset": dataset, "query_id": query_id, "reason": f"validate_self_test_failed:{validate_smoke.get('reason')}"})
                continue

            pipeline_id = f"external_dab_{dataset}_q{query_id}_{stable_hash([dataset, query_id, query])}"
            metadata = {
                "pipeline_id": pipeline_id,
                "source": "external_dab_package",
                "source_repo": args.source_repo,
                "source_input_root": str(input_root),
                "dataset": dataset,
                "query_id": query_id,
                "db_types": db_types,
                "dataset_issues": dataset_issues,
                "validate_self_test": validate_smoke,
            }
            metadata_path = query_dir / "metadata.json"
            metadata_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")

            expected_answer = {
                "type": "external_dab_ground_truth_csv",
                "value": gold_answer,
                "normalization": "dataset_validate_py",
            }
            row = {
                "pipeline_id": pipeline_id,
                "dataset": dataset,
                "query_id": query_id,
                "query": query,
                "task_type": external_task_type_from_db_types(db_types),
                "difficulty": "hard" if len(db_types) >= 3 else "medium",
                "source_task_signature": {
                    "source": "external_dab_package",
                    "source_repo": args.source_repo,
                    "dataset": dataset,
                    "query_id": query_id,
                    "db_types": db_types,
                    "query_ops": detect_query_ops(query),
                },
                "expected_answer": expected_answer,
                "validator_template": "external_validate_py",
                "validator_args": {"validate_py": str(validate_py.resolve())},
                "candidate_solution": {"type": "external_curated_task", "note": "Ground truth and validator are provided by the imported DAB package."},
                "evidence_card": {"source": "provided_ground_truth_csv", "ground_truth_csv": str(ground_truth_csv.resolve())},
                "evidence_chain": [],
                "judge": {
                    "accepted": not dataset_issues,
                    "score": 0.9 if not dataset_issues else 0.6,
                    "reason": "External DAB package passed structural and validator smoke checks." if not dataset_issues else "Imported with package-level issues: " + "; ".join(dataset_issues),
                },
                "final_audit": {
                    "audit_version": f"{FINAL_AUDIT_VERSION}.external_dab",
                    "passed": bool(validate_smoke.get("passed")) and not dataset_issues,
                    "checks": {
                        "db_config_exists": db_config_path.exists(),
                        "at_least_two_logical_databases": len(clients) >= 2,
                        "at_least_two_dbms_types": len(set(db_types)) >= 2,
                        "db_artifacts_exist": not missing_artifacts,
                        "validate_py_gold_passes_bogus_fails": bool(validate_smoke.get("passed")),
                    },
                    "failures": dataset_issues + ([] if validate_smoke.get("passed") else ["validate_py_smoke_failed"]),
                    "verified_final_answer": gold_answer,
                    "verified_final_answer_source": str(ground_truth_csv.resolve()),
                },
                "external_dab_package": metadata,
            }
            rows.append(row)
            tasks.append(
                {
                    "pipeline_id": pipeline_id,
                    "dataset": dataset,
                    "source_dataset": dataset,
                    "query_id": query_id,
                    "query": query,
                    "query_dir": str(query_dir.resolve()),
                    "dataset_dir": str(copied_dataset_dir.resolve()),
                    "query_json": str(query_json.resolve()),
                    "ground_truth_csv": str(ground_truth_csv.resolve()),
                    "validate_py": str(validate_py.resolve()),
                    "metadata_json": str(metadata_path.resolve()),
                    "validator_template": "external_validate_py",
                    "validator_args": {"validate_py": str(validate_py.resolve())},
                    "expected_answer": expected_answer,
                    "final_audit_passed": row["final_audit"]["passed"],
                    "validate_self_test": validate_smoke,
                    "dataset_setup": {"query_dataset": "copied", "source_dataset_dir": str(source_dir.resolve())},
                }
            )
            summary = dataset_summary.setdefault(
                dataset,
                {"dataset_dir": str(copied_dataset_dir.resolve()), "num_tasks": 0, "query_ids": [], "db_types": db_types, "issues": dataset_issues},
            )
            summary["num_tasks"] += 1
            summary["query_ids"].append(query_id)

    candidate_jsonl = Path(args.candidate_jsonl) if args.candidate_jsonl else output_dir / "external_dab_candidates.jsonl"
    manifest_json = Path(args.manifest_json) if args.manifest_json else output_dir / "sandbox_task_manifest.json"
    report_json = Path(args.report_json) if args.report_json else output_dir / "ingest_report.json"
    write_jsonl(candidate_jsonl, rows)
    manifest = {
        "version": 1,
        "source": "external_dab_package",
        "source_repo": args.source_repo,
        "input_root": str(input_root),
        "output_dir": str(output_dir),
        "total": len(tasks),
        "datasets": dataset_summary,
        "tasks": tasks,
        "skipped": skipped,
    }
    manifest_json.parent.mkdir(parents=True, exist_ok=True)
    manifest_json.write_text(json.dumps(jsonable(manifest), ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    report = {
        "input_root": str(input_root),
        "output_dir": str(output_dir),
        "candidate_jsonl": str(candidate_jsonl),
        "manifest_json": str(manifest_json),
        "datasets": dataset_summary,
        "rows": len(rows),
        "tasks": len(tasks),
        "skipped": skipped,
        "accepted_rows": sum(1 for row in rows if row.get("judge", {}).get("accepted") and row.get("final_audit", {}).get("passed")),
        "db_type_counts": Counter(db_type for row in rows for db_type in row.get("source_task_signature", {}).get("db_types", [])),
    }
    report_json.parent.mkdir(parents=True, exist_ok=True)
    report_json.write_text(json.dumps(jsonable(report), ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(jsonable(report), ensure_ascii=False, indent=2, sort_keys=True))


def export_sandbox_tasks(args: argparse.Namespace) -> None:
    rows = read_jsonl(Path(args.candidate_jsonl))
    output_dir = Path(args.output_dir)
    bench_root = Path(args.bench_root)
    output_dir.mkdir(parents=True, exist_ok=True)
    counters: Counter[str] = Counter()
    tasks: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []

    for idx, row in enumerate(rows):
        pipeline_id = str(row.get("pipeline_id") or f"candidate_{idx:05d}_{stable_hash([row.get('dataset'), row.get('query')])}")
        final_audit = row.get("final_audit") if isinstance(row.get("final_audit"), dict) else {}
        if args.require_final_audit and final_audit.get("passed") is not True:
            skipped.append({"pipeline_id": pipeline_id, "reason": "final_audit_not_passed"})
            continue
        template = str(row.get("validator_template") or "")
        validator_args = row.get("validator_args") if isinstance(row.get("validator_args"), dict) else {}
        if template not in VALIDATOR_TEMPLATE_NAMES or not validator_args:
            skipped.append({"pipeline_id": pipeline_id, "reason": "validator_spec_missing_or_unknown"})
            continue

        source_name = source_dataset_for_candidate(row) or str(row.get("dataset") or "").removeprefix("synthetic_")
        source_dir = dataset_dir_for_candidate(row, bench_root)
        if not source_dir.exists():
            skipped.append({"pipeline_id": pipeline_id, "reason": f"source_dataset_missing:{source_dir}"})
            continue
        synthetic_dataset = slugify_name(args.dataset_prefix + source_name)
        dataset_dir = output_dir / f"query_{synthetic_dataset}"
        dataset_setup = prepare_synthetic_dataset_dir(dataset_dir, source_dir, args)

        counters[synthetic_dataset] += 1
        numeric_query_id = counters[synthetic_dataset]
        query_dir = dataset_dir / f"query{numeric_query_id}"
        if query_dir.exists() and not args.overwrite:
            skipped.append({"pipeline_id": pipeline_id, "reason": f"query_dir_exists:{query_dir}"})
            continue
        query_dir.mkdir(parents=True, exist_ok=True)

        query = str(row.get("query") or "")
        expected_answer = row.get("expected_answer", {})
        metadata = {
            "pipeline_id": pipeline_id,
            "packet_id": row.get("packet_id"),
            "synthetic_dataset": synthetic_dataset,
            "source_dataset": source_name,
            "source_query_id": row.get("provenance", {}).get("source_query_id") if isinstance(row.get("provenance"), dict) else row.get("query_id"),
            "original_query_id": row.get("query_id"),
            "validator_template": template,
            "validator_args": validator_args,
            "expected_answer": expected_answer,
            "query_transform": row.get("query_transform", {}),
            "evidence_chain": row.get("evidence_chain", []),
            "evidence_card": row.get("evidence_card", {}),
            "ground_truth_materialization": row.get("ground_truth_materialization", {}),
            "final_audit": compact_final_audit(final_audit),
            "candidate_hash": candidate_audit_hash(row),
        }
        (query_dir / "query.json").write_text(json.dumps({"query": query}, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        write_ground_truth_csv(query_dir / "ground_truth.csv", expected_answer)
        validate_path = query_dir / "validate.py"
        validate_path.write_text(standalone_validate_py(template, validator_args, expected_answer, metadata), encoding="utf-8")
        metadata_path = query_dir / "metadata.json"
        metadata_path.write_text(json.dumps(jsonable(metadata), ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")

        self_test = self_test_exported_validate(validate_path, row) if args.self_test else {"passed": None, "reason": "disabled"}
        if args.self_test and not self_test.get("passed"):
            skipped.append({"pipeline_id": pipeline_id, "reason": f"validate_self_test_failed:{self_test.get('reason')}", "query_dir": str(query_dir)})
            continue

        tasks.append(
            {
                "pipeline_id": pipeline_id,
                "dataset": synthetic_dataset,
                "source_dataset": source_name,
                "query_id": numeric_query_id,
                "query_dir": str(query_dir.resolve()),
                "dataset_dir": str(dataset_dir.resolve()),
                "query_json": str((query_dir / "query.json").resolve()),
                "ground_truth_csv": str((query_dir / "ground_truth.csv").resolve()),
                "validate_py": str(validate_path.resolve()),
                "metadata_json": str(metadata_path.resolve()),
                "validator_template": template,
                "validator_args": validator_args,
                "expected_answer": expected_answer,
                "final_audit_passed": final_audit.get("passed"),
                "validate_self_test": self_test,
                "dataset_setup": dataset_setup,
            }
        )

    dataset_summary: dict[str, dict[str, Any]] = {}
    for task in tasks:
        dataset_name = str(task["dataset"])
        item = dataset_summary.setdefault(
            dataset_name,
            {
                "dataset_dir": task["dataset_dir"],
                "source_dataset": task["source_dataset"],
                "num_tasks": 0,
                "query_ids": [],
            },
        )
        item["num_tasks"] += 1
        item["query_ids"].append(task["query_id"])

    manifest = {
        "version": 1,
        "manifest_type": "sandbox_dabench_tasks",
        "created_at_unix": int(time.time()),
        "source_candidate_jsonl": str(Path(args.candidate_jsonl).resolve()),
        "output_dir": str(output_dir.resolve()),
        "bench_root": str(bench_root),
        "layout": "dabench_dataset_query_dirs",
        "total_tasks": len(tasks),
        "datasets": dataset_summary,
        "skipped": skipped,
        "tasks": tasks,
    }
    manifest_path = Path(args.manifest_json) if args.manifest_json else output_dir / "sandbox_task_manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(jsonable(manifest), ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(
        json.dumps(
            {
                "exported": len(tasks),
                "skipped": len(skipped),
                "output_dir": str(output_dir),
                "manifest_json": str(manifest_path),
                "datasets": dict(sorted(counters.items())),
            },
            ensure_ascii=False,
            indent=2,
        )
    )


def merge_judge(existing: Any, local: dict[str, Any]) -> dict[str, Any]:
    if isinstance(existing, dict):
        merged = dict(local)
        merged["local_static_judge"] = dict(local)
        merged["previous_judge"] = existing
        if "external_llm_judge" in existing:
            merged["external_llm_judge"] = existing["external_llm_judge"]
        if "ground_truth_review" in existing:
            merged["ground_truth_review"] = existing["ground_truth_review"]
        return merged
    return local


def write_dashboard(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    by_status = Counter("accepted" if row.get("judge", {}).get("accepted") else "rejected" for row in rows)
    by_dataset = Counter(str(row.get("dataset", "unknown")) for row in rows)
    risks = Counter()
    scores = []
    for row in rows:
        judge = row.get("judge", {})
        if isinstance(judge.get("score"), (int, float)):
            scores.append(float(judge["score"]))
        for risk in judge.get("risks", []) or []:
            risks[str(risk)] += 1
        local = judge.get("local_static_judge", {})
        for risk in local.get("risks", []) or []:
            risks[str(risk)] += 1
    lines = [
        "# DABench Synthetic Data Dashboard",
        "",
        "## Summary",
        "",
        f"- total candidates: {len(rows)}",
        f"- accepted: {by_status.get('accepted', 0)}",
        f"- rejected: {by_status.get('rejected', 0)}",
        f"- score mean: {summarize_numbers([int(s * 1000) for s in scores])['mean'] / 1000 if scores else 0}",
        "",
        "## Dataset Coverage",
        "",
    ]
    for dataset, count in sorted(by_dataset.items()):
        lines.append(f"- `{dataset}`: {count}")
    lines.extend(["", "## Risk Counts", ""])
    if risks:
        for risk, count in risks.most_common():
            lines.append(f"- `{risk}`: {count}")
    else:
        lines.append("- no risks recorded")
    lines.extend(["", "## Accepted Candidates", ""])
    for row in rows:
        if row.get("judge", {}).get("accepted"):
            lines.append(f"- `{row.get('pipeline_id', '')}` `{row.get('dataset', '')}`: {row.get('query', '')}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _short_config_path_for_verl(value: Any, dataset_dir: Path) -> str | None:
    if not isinstance(value, str) or not value.strip():
        return None
    path = Path(value)
    if not path.is_absolute():
        return value
    try:
        return str(path.resolve().relative_to(dataset_dir.resolve()))
    except ValueError:
        return path.name or str(path)


def format_db_source_summary_for_verl(db_config_path: Path, dataset_dir: Path) -> tuple[str, list[str]]:
    try:
        import yaml

        data = yaml.safe_load(db_config_path.read_text(encoding="utf-8")) or {}
        clients = data.get("db_clients")
        clients = clients if isinstance(clients, dict) else {}
    except Exception as exc:
        return (
            "Legal database sources could not be parsed from db_config.yaml. "
            f"Error: {type(exc).__name__}: {exc}. Use list_db to recover before query_db.",
            [],
        )
    if not clients:
        return (
            "Legal database sources are not available in db_config.yaml. "
            "Use list_db to discover sources before query_db.",
            [],
        )

    valid_db_names: list[str] = []
    lines: list[str] = []
    sql_sources: list[str] = []
    mongo_sources: list[str] = []
    for logical_name, raw_cfg in clients.items():
        if not isinstance(raw_cfg, dict):
            continue
        valid_db_names.append(str(logical_name))
        db_type = str(raw_cfg.get("db_type") or "unknown")
        details: list[str] = []
        physical_name = raw_cfg.get("db_name")
        if physical_name:
            details.append(f"physical_db={physical_name}")
        for key in ("db_path", "sql_file", "dump_folder"):
            short_path = _short_config_path_for_verl(raw_cfg.get(key), dataset_dir)
            if short_path:
                details.append(f"{key}={short_path}")
        detail_text = f"; {', '.join(details)}" if details else ""
        lines.append(f"- {logical_name} ({db_type}){detail_text}")
        if db_type in {"sqlite", "duckdb", "postgres"}:
            sql_sources.append(str(logical_name))
        elif db_type == "mongo":
            mongo_sources.append(str(logical_name))

    syntax_lines = [
        "Legal database sources for this task. The `db_name` argument must be exactly one logical name from this list:",
        *lines,
        "Do not use physical database names, file names, table names, or paths as `db_name` unless they are listed as logical names above.",
        "Use list_db with a listed logical db_name to discover tables or collections before writing query_db calls.",
    ]
    if sql_sources:
        syntax_lines.append(
            "SQL sources (sqlite/duckdb/postgres) use a plain SQL string in query_db `query`: "
            + ", ".join(sql_sources)
            + "."
        )
    if mongo_sources:
        syntax_lines.append(
            "Mongo sources use a JSON string in query_db `query`, for example "
            '{"collection":"collection_name","filter":{},"projection":{"field":1},"limit":5}: '
            + ", ".join(mongo_sources)
            + "."
        )
    syntax_lines.append(
        "Planning rule: first decide which listed source(s) contain entities, metadata, events, or measures; "
        "then inspect only those schemas; then run focused evidence queries."
    )
    return "\n".join(syntax_lines), valid_db_names


def read_db_description_for_verl(dataset_dir: Path, use_hints: bool) -> str:
    hint_path = dataset_dir / "db_description_withhint.txt"
    plain_path = dataset_dir / "db_description.txt"
    path = hint_path if use_hints and hint_path.exists() else plain_path
    try:
        return path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return ""


def load_exported_task_manifest_tasks(path: str) -> list[dict[str, Any]]:
    if not path:
        return []
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    tasks = data.get("tasks") if isinstance(data, dict) else []
    return [task for task in tasks or [] if isinstance(task, dict)]


def load_exported_task_manifest(path: str) -> dict[str, dict[str, Any]]:
    index: dict[str, dict[str, Any]] = {}
    for task in load_exported_task_manifest_tasks(path):
        pipeline_id = str(task.get("pipeline_id") or "")
        if pipeline_id:
            index[pipeline_id] = task
    return index


def runtime_path_for_verl(host_path: Path, dataset_dir: Path, runtime_bench_root: Path) -> str:
    try:
        rel = host_path.resolve().relative_to(dataset_dir.resolve())
        return str(runtime_bench_root / dataset_dir.name / rel)
    except ValueError:
        return str(host_path)


def dab_sandbox_task_identity(row: dict[str, Any], idx: int, args: argparse.Namespace, manifest_index: dict[str, dict[str, Any]]) -> tuple[str, int]:
    pipeline_id = str(row.get("pipeline_id") or "")
    manifest_task = manifest_index.get(pipeline_id, {})
    if manifest_task:
        dataset = str(manifest_task.get("dataset") or row.get("dataset") or "synthetic")
        query_id = int(manifest_task.get("query_id") or idx + 1)
        return dataset, query_id
    source_name = source_dataset_for_candidate(row) or str(row.get("dataset") or "synthetic").removeprefix("synthetic_")
    dataset = str(row.get("dataset") or source_name).removeprefix("synthetic_")
    query_id = int(row.get("query_id") or idx + 1)
    return dataset, query_id


def eval_summary_paths(args: argparse.Namespace) -> list[Path]:
    paths: list[Path] = []
    seen: set[Path] = set()
    for raw_path in args.eval_summary_csv or []:
        path = Path(raw_path)
        if path not in seen:
            paths.append(path)
            seen.add(path)
    for raw_dir in args.eval_summary_dir or []:
        root = Path(raw_dir)
        if not root.exists():
            print(f"WARNING: eval summary dir does not exist: {root}", file=sys.stderr)
            continue
        for path in sorted(root.rglob("run_summary.csv")):
            if path not in seen:
                paths.append(path)
                seen.add(path)
    return paths


def int_or_none(value: Any) -> int | None:
    try:
        if value is None or value == "":
            return None
        return int(float(str(value)))
    except (TypeError, ValueError):
        return None


def float_or_none(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(str(value))
    except (TypeError, ValueError):
        return None


def infer_llm_calls_from_log_dir(log_dir: Any) -> int | None:
    if not log_dir:
        return None
    final_agent_path = Path(str(log_dir)) / "final_agent.json"
    if not final_agent_path.exists():
        return None
    try:
        payload = json.loads(final_agent_path.read_text(encoding="utf-8"))
    except Exception:
        return None
    messages = payload.get("messages") if isinstance(payload, dict) else None
    if isinstance(messages, list):
        assistant_count = sum(1 for message in messages if isinstance(message, dict) and message.get("role") == "assistant")
        if assistant_count:
            return assistant_count
    timing_log = payload.get("timing_log") if isinstance(payload, dict) else None
    if isinstance(timing_log, list):
        llm_count = sum(1 for item in timing_log if isinstance(item, dict) and "llm" in json.dumps(item, ensure_ascii=False).casefold())
        if llm_count:
            return llm_count
    return None


def normalized_eval_summary_row(raw: dict[str, Any], path: Path) -> tuple[tuple[str, int] | None, dict[str, Any] | None]:
    dataset = str(raw.get("dataset") or raw.get("type") or "").strip()
    query_id = int_or_none(raw.get("query_id"))
    if not dataset or query_id is None:
        return None, None
    valid = parse_bool(raw.get("valid"), default=False)
    llm_calls = int_or_none(raw.get("llm_calls"))
    if llm_calls is None or llm_calls <= 0:
        inferred_llm_calls = infer_llm_calls_from_log_dir(raw.get("log_dir"))
        if inferred_llm_calls is not None:
            llm_calls = inferred_llm_calls
    llm_calls = llm_calls or 0
    tool_calls = int_or_none(raw.get("tool_calls")) or 0
    turns = max(llm_calls, tool_calls)
    normalized = {
        "dataset": dataset,
        "query_id": query_id,
        "status": raw.get("status") or "",
        "valid": valid,
        "answer": raw.get("answer") or "",
        "reason": raw.get("reason") or "",
        "llm_calls": llm_calls,
        "tool_calls": tool_calls,
        "turns": turns,
        "parse_errors": int_or_none(raw.get("parse_errors")) or 0,
        "no_tool_corrections": int_or_none(raw.get("no_tool_corrections")) or 0,
        "duration_sec": float_or_none(raw.get("duration_sec")),
        "terminate_reason": raw.get("terminate_reason") or "",
        "log_dir": raw.get("log_dir") or "",
        "source_summary_csv": str(path),
    }
    return (dataset, query_id), normalized


def load_eval_summary_latest(paths: list[Path]) -> tuple[dict[tuple[str, int], dict[str, Any]], dict[str, Any]]:
    latest: dict[tuple[str, int], tuple[float, int, dict[str, Any]]] = {}
    scanned_rows = 0
    missing_key_rows = 0
    for order, path in enumerate(paths):
        if not path.exists():
            print(f"WARNING: eval summary csv does not exist: {path}", file=sys.stderr)
            continue
        mtime = path.stat().st_mtime
        with path.open(newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for raw in reader:
                scanned_rows += 1
                key, normalized = normalized_eval_summary_row(raw, path)
                if key is None or normalized is None:
                    missing_key_rows += 1
                    continue
                if key not in latest or (mtime, order) >= (latest[key][0], latest[key][1]):
                    latest[key] = (mtime, order, normalized)
    latest_rows = {key: row for key, (_, _, row) in latest.items()}
    valid_count = sum(1 for row in latest_rows.values() if row.get("valid"))
    summary = {
        "summary_csv_count": len(paths),
        "scanned_rows": scanned_rows,
        "missing_key_rows": missing_key_rows,
        "latest_unique_tasks": len(latest_rows),
        "valid_tasks": valid_count,
        "invalid_tasks": len(latest_rows) - valid_count,
    }
    return latest_rows, summary


def load_eval_summary_selection(paths: list[Path], min_turns: int) -> tuple[dict[tuple[str, int], dict[str, Any]], dict[str, Any]]:
    latest_rows, base_summary = load_eval_summary_latest(paths)
    selected: dict[tuple[str, int], dict[str, Any]] = {}
    valid_high_turns = 0
    invalid = 0
    for key, row in latest_rows.items():
        reasons = []
        if not row["valid"]:
            reasons.append("qwen_invalid")
        if row["valid"] and row["turns"] >= min_turns:
            reasons.append(f"qwen_valid_high_turns_ge_{min_turns}")
        if not reasons:
            continue
        if row["valid"]:
            valid_high_turns += 1
        else:
            invalid += 1
        selected_row = dict(row)
        selected_row["selection_reasons"] = reasons
        selected[key] = selected_row

    summary = dict(base_summary)
    summary.update(
        {
            "selected_tasks": len(selected),
            "selected_invalid": invalid,
            "selected_valid_high_turns": valid_high_turns,
            "min_turns": min_turns,
        }
    )
    return selected, summary


def qwen_bad_invalid_reasons(selection: dict[str, Any], empty_answer_bad_below_turns: int) -> list[str]:
    reasons: list[str] = []
    status = str(selection.get("status") or "").strip().casefold()
    terminate_reason = str(selection.get("terminate_reason") or "").strip().casefold()
    validator_reason = str(selection.get("reason") or "").strip().casefold()
    answer = str(selection.get("answer") or "").strip()
    turns = int_or_none(selection.get("turns")) or 0
    parse_errors = int_or_none(selection.get("parse_errors")) or 0

    if status and status not in {"ok", "success", "completed"}:
        reasons.append(f"status_{status}")
    infra_markers = (
        "fatal_error",
        "llm_response_failed",
        "api_connection",
        "apiconnection",
        "timeout",
        "clientresponseerror",
        "trial not found",
        "reward_error",
        "connectionerror",
    )
    if any(marker in terminate_reason for marker in infra_markers) or any(marker in validator_reason for marker in infra_markers):
        reasons.append("infra_or_runtime_failure")
    if parse_errors and not answer:
        reasons.append("parse_error_without_answer")
    if not answer or answer.casefold() in {"none", "null", "nan"}:
        if turns < empty_answer_bad_below_turns:
            reasons.append("empty_answer_short_run")
    if any(marker in validator_reason for marker in ("traceback", "syntaxerror", "validator_error", "exception")):
        reasons.append("validator_or_artifact_error")
    return sorted(set(reasons))


def qwen_eval_bucket(selection: dict[str, Any] | None, min_turns: int, empty_answer_bad_below_turns: int) -> tuple[str, list[str]]:
    if not selection:
        return "qwen_missing_eval", ["missing_eval_result"]
    valid = bool(selection.get("valid"))
    turns = int_or_none(selection.get("turns")) or 0
    if valid and turns >= min_turns:
        return "qwen_valid_high_turn", [f"qwen_valid_high_turns_ge_{min_turns}"]
    if valid:
        return "qwen_valid_easy", ["qwen_valid_below_turn_threshold"]
    bad_reasons = qwen_bad_invalid_reasons(selection, empty_answer_bad_below_turns)
    if bad_reasons:
        return "qwen_invalid_bad_or_ambiguous", bad_reasons
    return "qwen_invalid_good_hard", ["qwen_invalid"]


def qwen_filter_enrich_row(row: dict[str, Any], dataset: str, query_id: int, bucket: str, reasons: list[str], selection: dict[str, Any] | None) -> dict[str, Any]:
    out = dict(row)
    if selection:
        out["_eval_selection"] = selection
        out["qwen_eval_selection"] = selection
    out["qwen_eval_filter"] = {
        "dataset": dataset,
        "query_id": query_id,
        "bucket": bucket,
        "reasons": reasons,
        "selected_wrong_or_high_turn": bucket in {"qwen_invalid_good_hard", "qwen_invalid_bad_or_ambiguous", "qwen_valid_high_turn"},
        "selected_training_hard": bucket in {"qwen_invalid_good_hard", "qwen_valid_high_turn"},
    }
    return out


def qwen_eval_filter(args: argparse.Namespace) -> None:
    rows = read_jsonl(Path(args.candidate_jsonl))
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    eval_paths = eval_summary_paths(args)
    if not eval_paths:
        raise ValueError("qwen-eval-filter requires --eval-summary-csv or --eval-summary-dir")
    latest_eval, eval_summary = load_eval_summary_latest(eval_paths)
    manifest_index = load_exported_task_manifest(args.task_manifest_json)

    buckets: dict[str, list[dict[str, Any]]] = {
        "qwen_valid_easy": [],
        "qwen_valid_high_turn": [],
        "qwen_invalid_good_hard": [],
        "qwen_invalid_bad_or_ambiguous": [],
        "qwen_missing_eval": [],
        "generation_rejected": [],
        "final_audit_rejected": [],
    }
    selected_wrong_or_high_turn: list[dict[str, Any]] = []
    selected_training_hard: list[dict[str, Any]] = []

    for idx, row in enumerate(rows):
        judge = row.get("judge") if isinstance(row.get("judge"), dict) else {}
        if judge and not judge.get("accepted", False) and not args.include_judge_rejected:
            buckets["generation_rejected"].append(row)
            continue
        final_audit = row.get("final_audit") if isinstance(row.get("final_audit"), dict) else {}
        if args.require_final_audit and final_audit.get("passed") is not True:
            buckets["final_audit_rejected"].append(row)
            continue

        dataset, query_id = dab_sandbox_task_identity(row, idx, args, manifest_index)
        selection = latest_eval.get((dataset, query_id))
        bucket, reasons = qwen_eval_bucket(selection, args.eval_min_turns, args.bad_empty_answer_turns)
        enriched = qwen_filter_enrich_row(row, dataset, query_id, bucket, reasons, selection)
        buckets[bucket].append(enriched)
        if enriched["qwen_eval_filter"]["selected_wrong_or_high_turn"]:
            selected_wrong_or_high_turn.append(enriched)
        if enriched["qwen_eval_filter"]["selected_training_hard"] or (args.include_bad_invalid_in_training_hard and bucket == "qwen_invalid_bad_or_ambiguous"):
            selected_training_hard.append(enriched)

    output_files = {
        "selected_wrong_or_high_turn": output_dir / "qwen_selected_wrong_or_high_turn.jsonl",
        "selected_training_hard": output_dir / "qwen_selected_training_hard.jsonl",
        "valid_easy": output_dir / "qwen_valid_easy.jsonl",
        "valid_high_turn": output_dir / "qwen_valid_high_turn.jsonl",
        "invalid_good_hard": output_dir / "qwen_invalid_good_hard.jsonl",
        "invalid_bad_or_ambiguous": output_dir / "qwen_invalid_bad_or_ambiguous.jsonl",
        "missing_eval": output_dir / "qwen_missing_eval.jsonl",
        "generation_rejected": output_dir / "qwen_generation_rejected.jsonl",
        "final_audit_rejected": output_dir / "qwen_final_audit_rejected.jsonl",
    }
    write_jsonl_preserve_order(output_files["selected_wrong_or_high_turn"], selected_wrong_or_high_turn)
    write_jsonl_preserve_order(output_files["selected_training_hard"], selected_training_hard)
    for bucket, rows_for_bucket in buckets.items():
        file_key = bucket.removeprefix("qwen_") if bucket.startswith("qwen_") else bucket
        path = output_files.get(file_key) or output_dir / f"{bucket}.jsonl"
        write_jsonl_preserve_order(path, rows_for_bucket)

    selected_tasks = []
    for row in selected_wrong_or_high_turn:
        meta = row.get("qwen_eval_filter", {})
        selection = row.get("qwen_eval_selection", {}) if isinstance(row.get("qwen_eval_selection"), dict) else {}
        selected_tasks.append(
            {
                "dataset": meta.get("dataset"),
                "query_id": meta.get("query_id"),
                "bucket": meta.get("bucket"),
                "reasons": meta.get("reasons", []),
                "valid": selection.get("valid"),
                "turns": selection.get("turns"),
                "llm_calls": selection.get("llm_calls"),
                "tool_calls": selection.get("tool_calls"),
                "duration_sec": selection.get("duration_sec"),
                "terminate_reason": selection.get("terminate_reason"),
            }
        )

    bucket_counts = {bucket: len(rows_for_bucket) for bucket, rows_for_bucket in buckets.items()}
    report = {
        "pipeline": "qwen_eval_filter_v1",
        "candidate_jsonl": args.candidate_jsonl,
        "task_manifest_json": args.task_manifest_json,
        "eval_summary": eval_summary,
        "policy": {
            "selected_wrong_or_high_turn": "qwen invalid OR qwen valid with max(llm_calls, tool_calls) >= eval_min_turns",
            "selected_training_hard": "qwen_invalid_good_hard OR qwen_valid_high_turn; infra/ambiguous invalid rows are kept separately",
            "eval_min_turns": args.eval_min_turns,
            "bad_empty_answer_turns": args.bad_empty_answer_turns,
            "require_final_audit": bool(args.require_final_audit),
            "include_judge_rejected": bool(args.include_judge_rejected),
            "include_bad_invalid_in_training_hard": bool(args.include_bad_invalid_in_training_hard),
        },
        "counts": {
            "input_rows": len(rows),
            "candidate_rows_after_generation_audit_gates": sum(len(buckets[name]) for name in ("qwen_valid_easy", "qwen_valid_high_turn", "qwen_invalid_good_hard", "qwen_invalid_bad_or_ambiguous", "qwen_missing_eval")),
            "selected_wrong_or_high_turn": len(selected_wrong_or_high_turn),
            "selected_training_hard": len(selected_training_hard),
            "buckets": bucket_counts,
        },
        "selected_by_dataset": dict(Counter(str(item.get("dataset")) for item in selected_tasks)),
        "selected_tasks": selected_tasks,
        "outputs": {key: str(path) for key, path in output_files.items()},
    }
    (output_dir / "qwen_filter_report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))


def copy_task_artifact_file(src: Any, dst: Path, copied: list[dict[str, str]], missing: list[str]) -> bool:
    if not src:
        return False
    src_path = Path(str(src))
    if not src_path.exists() or not src_path.is_file():
        missing.append(str(src_path))
        return False
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src_path, dst)
    copied.append({"source": str(src_path), "artifact": str(dst)})
    return True


def write_dab_sandbox_task_artifacts(output_dir: Path, train_rows: list[dict[str, Any]], args: argparse.Namespace) -> dict[str, Any]:
    manifest_tasks = load_exported_task_manifest_tasks(args.task_manifest_json)
    by_task: dict[tuple[str, int], dict[str, Any]] = {}
    for task in manifest_tasks:
        dataset = str(task.get("dataset") or "")
        query_id = int_or_none(task.get("query_id"))
        if dataset and query_id is not None:
            by_task[(dataset, query_id)] = task

    artifacts_root = output_dir / args.task_artifacts_dir
    artifacts_root.mkdir(parents=True, exist_ok=True)
    copied: list[dict[str, str]] = []
    missing: list[str] = []
    artifact_tasks: list[dict[str, Any]] = []
    copied_dataset_dirs: set[str] = set()

    for row in train_rows:
        extra = row.get("extra_info", {}) if isinstance(row.get("extra_info"), dict) else {}
        dataset = str(extra.get("dataset") or "")
        query_id = int_or_none(extra.get("query_id"))
        if not dataset or query_id is None:
            continue

        manifest_task = by_task.get((dataset, query_id), {})
        dataset_dir = Path(str(manifest_task.get("dataset_dir") or f"query_{dataset}"))
        query_dir = Path(str(manifest_task.get("query_dir") or dataset_dir / f"query{query_id}"))
        artifact_dataset_dir = artifacts_root / dataset_dir.name
        artifact_query_dir = artifact_dataset_dir / f"query{query_id}"

        if str(dataset_dir) not in copied_dataset_dirs:
            copied_dataset_dirs.add(str(dataset_dir))
            for name in ("db_config.yaml", "db_description.txt", "db_description_withhint.txt"):
                copy_task_artifact_file(dataset_dir / name, artifact_dataset_dir / name, copied, missing)

        standard_files = {
            "query.json": manifest_task.get("query_json") or query_dir / "query.json",
            "metadata.json": manifest_task.get("metadata_json") or query_dir / "metadata.json",
            "ground_truth.csv": manifest_task.get("ground_truth_csv") or query_dir / "ground_truth.csv",
            "validate.py": manifest_task.get("validate_py") or query_dir / "validate.py",
        }
        for name, src in standard_files.items():
            copy_task_artifact_file(src, artifact_query_dir / name, copied, missing)

        task_artifact = {
            "dataset": dataset,
            "query_id": query_id,
            "artifact_dataset_dir": str(artifact_dataset_dir),
            "artifact_query_dir": str(artifact_query_dir),
            "files": {
                "query_json": str(artifact_query_dir / "query.json"),
                "metadata_json": str(artifact_query_dir / "metadata.json"),
                "ground_truth_csv": str(artifact_query_dir / "ground_truth.csv"),
                "validate_py": str(artifact_query_dir / "validate.py"),
            },
            "source": {
                "dataset_dir": str(dataset_dir),
                "query_dir": str(query_dir),
                "query_json": str(manifest_task.get("query_json") or query_dir / "query.json"),
                "metadata_json": str(manifest_task.get("metadata_json") or query_dir / "metadata.json"),
                "ground_truth_csv": str(manifest_task.get("ground_truth_csv") or query_dir / "ground_truth.csv"),
                "validate_py": str(manifest_task.get("validate_py") or query_dir / "validate.py"),
            },
            "validator_template": manifest_task.get("validator_template"),
            "validator_args": manifest_task.get("validator_args"),
            "expected_answer": manifest_task.get("expected_answer"),
            "validate_self_test": manifest_task.get("validate_self_test"),
            "qwen_eval_selection": extra.get("qwen_eval_selection"),
        }
        (artifact_query_dir / "task_artifact.json").write_text(
            json.dumps(task_artifact, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        artifact_tasks.append(task_artifact)

    manifest = {
        "artifact_format": "dab_sandbox_task_artifacts_v1",
        "artifact_root": str(artifacts_root),
        "task_count": len(artifact_tasks),
        "copied_file_count": len(copied),
        "missing_file_count": len(missing),
        "copied_files": copied,
        "missing_files": sorted(set(missing)),
        "tasks": artifact_tasks,
    }
    manifest_path = output_dir / "task_artifacts_manifest.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return {
        "enabled": True,
        "artifact_root": str(artifacts_root),
        "manifest": str(manifest_path),
        "task_count": len(artifact_tasks),
        "copied_file_count": len(copied),
        "missing_file_count": len(missing),
    }


def dab_sandbox_task_context(row: dict[str, Any], idx: int, args: argparse.Namespace, manifest_index: dict[str, dict[str, Any]]) -> dict[str, Any]:
    pipeline_id = str(row.get("pipeline_id") or "")
    manifest_task = manifest_index.get(pipeline_id, {})
    if manifest_task:
        dataset = str(manifest_task.get("dataset") or row.get("dataset") or "synthetic")
        query_id = int(manifest_task.get("query_id") or idx + 1)
        dataset_dir = Path(str(manifest_task.get("dataset_dir")))
        query_dir = Path(str(manifest_task.get("query_dir")))
    else:
        source_name = source_dataset_for_candidate(row) or str(row.get("dataset") or "synthetic").removeprefix("synthetic_")
        dataset = str(row.get("dataset") or source_name).removeprefix("synthetic_")
        query_id = int(row.get("query_id") or idx + 1)
        dataset_dir = dataset_dir_for_candidate(row, Path(args.bench_root))
        query_dir = dataset_dir / f"query{query_id}"

    query = str(row.get("query") or "")
    db_config_path = dataset_dir / "db_config.yaml"
    db_source_summary, valid_db_names = format_db_source_summary_for_verl(db_config_path, dataset_dir)
    runtime_bench_root = Path(args.runtime_bench_root)
    return {
        "dataset": dataset,
        "query_id": query_id,
        "query": query,
        "dataset_dir": dataset_dir,
        "query_dir": query_dir,
        "db_config_path": db_config_path,
        "runtime_bench_root": runtime_bench_root,
        "runtime_dataset_dir": runtime_path_for_verl(dataset_dir, dataset_dir, runtime_bench_root),
        "runtime_query_dir": runtime_path_for_verl(query_dir, dataset_dir, runtime_bench_root),
        "runtime_db_config_path": runtime_path_for_verl(db_config_path, dataset_dir, runtime_bench_root),
        "db_description": read_db_description_for_verl(dataset_dir, bool(args.use_hints)),
        "db_source_summary": db_source_summary,
        "valid_db_names": valid_db_names,
        "manifest_task": manifest_task,
    }


def dab_sandbox_verl_row(row: dict[str, Any], idx: int, split: str, args: argparse.Namespace, manifest_index: dict[str, dict[str, Any]]) -> dict[str, Any]:
    ctx = dab_sandbox_task_context(row, idx, args, manifest_index)
    query = ctx["query"]
    extra_info = {
        "split": split,
        "index": idx,
        "dataset": ctx["dataset"],
        "query_id": ctx["query_id"],
        "query": query,
        "prompt": query,
        "bench_root": str(ctx["runtime_bench_root"]),
        "dataset_dir": ctx["runtime_dataset_dir"],
        "query_dir": ctx["runtime_query_dir"],
        "db_config_path": ctx["runtime_db_config_path"],
        "use_hints": bool(args.use_hints),
        "db_description": ctx["db_description"],
        "db_source_summary": ctx["db_source_summary"],
        "valid_db_names": ctx["valid_db_names"],
        "sandbox_url": args.sandbox_url,
        "max_iterations": args.iterations,
        "query_timeout": args.query_timeout,
        "query_row_limit": args.query_row_limit,
        "need_tools_kwargs": bool(args.need_tools_kwargs),
    }
    eval_selection = row.get("_eval_selection")
    if isinstance(eval_selection, dict):
        extra_info["qwen_eval_selection"] = eval_selection
    if args.run_root:
        extra_info["run_root"] = str(Path(args.run_root).resolve())
    return {
        "level": "DABench",
        "type": ctx["dataset"],
        "data_source": args.data_source,
        "prompt": [{"role": "user", "content": query}],
        "ability": "data_agent_bench",
        "reward_model": {"style": "rule", "ground_truth": ""},
        "extra_info": extra_info,
    }


def synthetic_verl_row(row: dict[str, Any], idx: int, split: str, args: argparse.Namespace) -> dict[str, Any]:
    extra_info = {
        "split": split,
        "index": idx,
        "synthetic": True,
        "pipeline_id": row.get("pipeline_id"),
        "dataset": row.get("dataset"),
        "query_id": row.get("query_id", idx + 1),
        "query": row.get("query"),
        "db_description": row.get("db_description", ""),
        "hint_refs": row.get("hint_refs", []),
        "hints": row.get("hints", []),
        "hint_policy": row.get("hint_policy", {}),
        "data_requirements": row.get("data_requirements", {}),
        "evidence_chain": row.get("evidence_chain", []),
        "generation_rationale": row.get("generation_rationale", {}),
        "expected_answer": row.get("expected_answer", {}),
        "validator_template": row.get("validator_template", ""),
        "validator_args": row.get("validator_args", {}),
        "reward_spec": row.get("reward_spec", {}),
        "judge_summary": compact_judge(row.get("judge", {})),
        "task_fit_review": row.get("task_fit_review", {}),
        "final_audit": compact_final_audit(row.get("final_audit", {})),
        "artifact_refs": row.get("artifact_refs", []),
        "validator_runtime": "programmatic",
        "sandbox_manifest_required": False,
    }
    return {
        "level": "DABenchSynthetic",
        "type": row.get("dataset", "synthetic"),
        "data_source": args.data_source,
        "prompt": [{"role": "user", "content": row.get("query", "")}],
        "ability": "data_agent_bench",
        "reward_model": {
            "style": "rule",
            "ground_truth": json.dumps(row.get("expected_answer", {}), ensure_ascii=False),
            "validator_template": row.get("validator_template", ""),
            "validator_args": json.dumps(row.get("validator_args", {}), ensure_ascii=False),
        },
        "extra_info": extra_info,
    }


def build_verl(args: argparse.Namespace) -> None:
    rows = read_jsonl(Path(args.candidate_jsonl))
    accepted = []
    audit_rejected = 0
    for row in rows:
        if not row.get("judge", {}).get("accepted", False):
            continue
        final_audit = row.get("final_audit") if isinstance(row.get("final_audit"), dict) else None
        if args.require_final_audit and not (final_audit and final_audit.get("passed") is True):
            audit_rejected += 1
            continue
        if final_audit and final_audit.get("passed") is not True:
            audit_rejected += 1
            continue
        accepted.append(row)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    manifest_index = load_exported_task_manifest(args.task_manifest_json)
    if args.output_format == "dab_sandbox" and not manifest_index:
        print(
            "WARNING: build-verl --output-format dab_sandbox is most reliable with --task-manifest-json from export-dabench-tasks. "
            "Falling back to source dataset paths.",
            file=sys.stderr,
        )

    eval_filter_summary: dict[str, Any] = {}
    eval_filter_paths = eval_summary_paths(args)
    if eval_filter_paths:
        eval_selection, eval_filter_summary = load_eval_summary_selection(eval_filter_paths, args.eval_min_turns)
        before_eval_filter = len(accepted)
        filtered = []
        missing_eval_result = 0
        for idx, row in enumerate(accepted):
            dataset, query_id = dab_sandbox_task_identity(row, idx, args, manifest_index)
            selection = eval_selection.get((dataset, query_id))
            if not selection:
                missing_eval_result += 1
                continue
            filtered_row = dict(row)
            filtered_row["_eval_selection"] = selection
            filtered.append(filtered_row)
        accepted = filtered
        eval_filter_summary.update(
            {
                "enabled": True,
                "accepted_before_eval_filter": before_eval_filter,
                "accepted_after_eval_filter": len(accepted),
                "missing_eval_result_or_not_selected": missing_eval_result,
                "selected_rule": "qwen invalid OR qwen valid with max(llm_calls, tool_calls) >= eval_min_turns",
            }
        )
    else:
        eval_filter_summary = {"enabled": False}

    if args.output_format == "synthetic":
        train_rows = [synthetic_verl_row(row, idx, "train", args) for idx, row in enumerate(accepted)]
        test_rows = train_rows[: max(1, min(len(train_rows), args.max_test_rows))]
    else:
        train_rows = [dab_sandbox_verl_row(row, idx, "train", args, manifest_index) for idx, row in enumerate(accepted)]
        test_rows = [dab_sandbox_verl_row(row, idx, "val", args, manifest_index) for idx, row in enumerate(accepted)]

    if args.output_format == "dab_sandbox":
        write_jsonl_preserve_order(output_dir / "train.jsonl", train_rows)
        write_jsonl_preserve_order(output_dir / "test.jsonl", test_rows)
    else:
        write_jsonl(output_dir / "train.jsonl", train_rows)
        write_jsonl(output_dir / "test.jsonl", test_rows)
    if args.output_format == "dab_sandbox":
        maybe_write_raw_parquet(output_dir / "train.parquet", train_rows)
        maybe_write_raw_parquet(output_dir / "test.parquet", test_rows)
    else:
        maybe_write_parquet(output_dir / "train.parquet", train_rows)
        maybe_write_parquet(output_dir / "test.parquet", test_rows)
    task_artifacts_summary = {"enabled": False}
    if args.output_format == "dab_sandbox" and args.write_task_artifacts:
        task_artifacts_summary = write_dab_sandbox_task_artifacts(output_dir, train_rows, args)
    if args.output_format == "dab_sandbox":
        summary = {
            "total": len(train_rows),
            "accepted": len(accepted),
            "audit_rejected": audit_rejected,
            "require_final_audit": bool(args.require_final_audit),
            "output_format": args.output_format,
            "train": str(output_dir / "train.parquet"),
            "test": str(output_dir / "test.parquet"),
            "train_jsonl": str(output_dir / "train.jsonl"),
            "test_jsonl": str(output_dir / "test.jsonl"),
            "data_source": args.data_source,
            "task_manifest_json": args.task_manifest_json,
            "eval_filter": eval_filter_summary,
            "task_artifacts": task_artifacts_summary,
            "tasks": [
                {
                    "dataset": row.get("extra_info", {}).get("dataset"),
                    "query_id": row.get("extra_info", {}).get("query_id"),
                    "qwen_eval_selection": row.get("extra_info", {}).get("qwen_eval_selection"),
                }
                for row in train_rows
            ],
        }
    else:
        summary = {
            "total": len(train_rows),
            "accepted": len(accepted),
            "audit_rejected": audit_rejected,
            "require_final_audit": bool(args.require_final_audit),
            "output_format": args.output_format,
            "train": str(output_dir / "train.parquet"),
            "test": str(output_dir / "test.parquet"),
            "train_jsonl": str(output_dir / "train.jsonl"),
            "test_jsonl": str(output_dir / "test.jsonl"),
            "data_source": args.data_source,
            "task_manifest_json": args.task_manifest_json,
            "tasks": [
                {
                    "dataset": row.get("extra_info", {}).get("dataset"),
                    "query_id": row.get("extra_info", {}).get("query_id"),
                }
                for row in train_rows
            ],
        }
    (output_dir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


def compact_final_audit(audit: Any) -> dict[str, Any]:
    if not isinstance(audit, dict):
        return {}
    return {
        "audit_version": audit.get("audit_version"),
        "passed": audit.get("passed"),
        "candidate_hash": audit.get("candidate_hash"),
        "failures": audit.get("failures", []),
        "checks": audit.get("checks", {}),
        "repeat_runs": audit.get("repeat_runs"),
        "verified_final_answer": audit.get("verified_final_answer"),
        "verified_final_answer_source": audit.get("verified_final_answer_source", ""),
        "policy": audit.get("policy", {}),
    }


def compact_judge(judge: Any) -> dict[str, Any]:
    if not isinstance(judge, dict):
        return {}
    out = {
        "accepted": judge.get("accepted"),
        "score": judge.get("score"),
        "risks": judge.get("risks", []),
        "dimension_scores": judge.get("dimension_scores", {}),
    }
    if judge.get("judge_policy"):
        out["judge_policy"] = judge.get("judge_policy")
    return out


def parquet_safe(value: Any) -> Any:
    value = jsonable(value)
    if isinstance(value, dict):
        cleaned = {}
        for key, item in value.items():
            safe_item = parquet_safe(item)
            if safe_item is not None:
                cleaned[str(key)] = safe_item
        return cleaned or None
    if isinstance(value, list):
        return [parquet_safe(item) for item in value]
    return value


def parquet_safe_extra_info(value: Any) -> Any:
    value = jsonable(value)
    if not isinstance(value, dict):
        return value
    cleaned = {}
    for key, item in value.items():
        safe_item = jsonable(item)
        if safe_item is None:
            continue
        if isinstance(safe_item, (dict, list)):
            cleaned[str(key)] = json.dumps(safe_item, ensure_ascii=False, sort_keys=True)
        else:
            cleaned[str(key)] = safe_item
    return cleaned or None


def maybe_write_parquet(path: Path, rows: list[dict[str, Any]]) -> None:
    try:
        import pandas as pd
        path.parent.mkdir(parents=True, exist_ok=True)
        safe_rows = []
        for row in rows:
            safe_row = parquet_safe(row)
            if isinstance(safe_row, dict) and "extra_info" in safe_row:
                safe_row["extra_info"] = parquet_safe_extra_info(safe_row.get("extra_info"))
            safe_rows.append(safe_row)
        pd.DataFrame(safe_rows).to_parquet(path, index=False)
    except Exception as exc:
        path.with_suffix(path.suffix + ".skipped.txt").write_text(
            f"Parquet was not written: {type(exc).__name__}: {exc}\n"
            "JSONL output is still available. Run inside the VERL image if pandas/pyarrow are missing on the host.\n",
            encoding="utf-8",
        )
        return


def maybe_write_raw_parquet(path: Path, rows: list[dict[str, Any]]) -> None:
    try:
        import pandas as pd
        path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(rows).to_parquet(path, index=False)
    except Exception as exc:
        path.with_suffix(path.suffix + ".skipped.txt").write_text(
            f"Parquet was not written: {type(exc).__name__}: {exc}\n"
            "JSONL output is still available. Run inside the VERL image if pandas/pyarrow are missing on the host.\n",
            encoding="utf-8",
        )
        return


def first_nonempty(*values: str | None) -> str:
    for value in values:
        if value:
            text = str(value).strip()
            if text:
                return text
    return ""


def env_first(*names: str, default: str = "") -> str:
    return first_nonempty(*(os.environ.get(name) for name in names), default)


def parse_bool(value: Any, default: bool = False) -> bool:
    if value is None or value == "":
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().casefold() in {"1", "true", "yes", "y", "on"}


def chat_completions_url(base_url: str) -> str:
    base = base_url.rstrip("/")
    if base.endswith("/chat/completions"):
        return base
    return f"{base}/chat/completions"


def anthropic_messages_url(base_url: str) -> str:
    base = base_url.rstrip("/")
    if base.endswith("/messages"):
        return base
    return f"{base}/messages"


def infer_llm_provider(explicit: str, model: str, base_url: str) -> str:
    provider = explicit.strip().casefold()
    if provider:
        return provider
    lowered_model = model.casefold()
    lowered_url = base_url.casefold()
    if lowered_model.startswith("claude") or "anthropic.com" in lowered_url:
        return "anthropic"
    return "openai"


def token_limit_param_name(model: str, explicit: str = "") -> str:
    if explicit:
        return explicit
    lowered = model.casefold()
    if lowered.startswith("gpt-5") or lowered.startswith("o1") or lowered.startswith("o3") or lowered.startswith("o4"):
        return "max_completion_tokens"
    return "max_tokens"


def omit_temperature_for_model(model: str) -> bool:
    lowered = model.casefold()
    return lowered.startswith("gpt-5") or lowered.startswith("o1") or lowered.startswith("o3") or lowered.startswith("o4")


def packet_to_messages(packet: dict[str, Any]) -> list[dict[str, str]]:
    user_payload = {
        "packet_id": packet.get("packet_id", ""),
        "input": packet.get("input", {}),
        "expected_output": packet.get("expected_output", "Return exactly one JSON object."),
        "output_rules": [
            "Return exactly one JSON object.",
            "Do not include markdown fences or explanatory prose.",
            "For DAB data generation, use hint_refs only from allowed_hints and do not write new hint text.",
        ],
    }
    return [
        {"role": "system", "content": str(packet.get("system_prompt", ""))},
        {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False, indent=2)},
    ]


def post_chat_completion(payload: dict[str, Any], api_key: str, base_url: str, timeout: float) -> dict[str, Any]:
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    request = urllib.request.Request(
        chat_completions_url(base_url),
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers=headers,
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        error_body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code}: {error_body[:2000]}") from exc
    return json.loads(body)


def post_anthropic_messages(payload: dict[str, Any], api_key: str, base_url: str, timeout: float) -> dict[str, Any]:
    headers = {
        "Content-Type": "application/json",
        "anthropic-version": env_first("ANTHROPIC_VERSION", "DAB_PIPELINE_ANTHROPIC_VERSION", default="2023-06-01"),
    }
    if api_key:
        headers["x-api-key"] = api_key
    request = urllib.request.Request(
        anthropic_messages_url(base_url),
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers=headers,
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        error_body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code}: {error_body[:2000]}") from exc
    return json.loads(body)


def extract_chat_content(response: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    choices = response.get("choices") or []
    if not choices:
        raise ValueError("LLM response has no choices")
    message = choices[0].get("message") or {}
    content = message.get("content")
    if isinstance(content, list):
        parts = []
        for item in content:
            if isinstance(item, dict):
                parts.append(str(item.get("text") or item.get("content") or ""))
            else:
                parts.append(str(item))
        content = "".join(parts)
    if not isinstance(content, str) or not content.strip():
        raise ValueError("LLM response content is empty")
    return content, response.get("usage") or {}


def extract_anthropic_content(response: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    content = response.get("content")
    parts: list[str] = []
    if isinstance(content, list):
        for item in content:
            if isinstance(item, dict):
                if item.get("type") == "text" or "text" in item:
                    parts.append(str(item.get("text", "")))
            else:
                parts.append(str(item))
    elif isinstance(content, str):
        parts.append(content)
    text = "".join(parts).strip()
    if not text:
        raise ValueError("Anthropic response content is empty")
    return text, response.get("usage") or {}


def parse_llm_json(content: str) -> Any:
    text = content.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.IGNORECASE).strip()
        text = re.sub(r"\s*```$", "", text).strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        start_candidates = [idx for idx in (text.find("{"), text.find("[")) if idx >= 0]
        if not start_candidates:
            raise
        decoder = json.JSONDecoder()
        for start in sorted(start_candidates):
            try:
                parsed, _ = decoder.raw_decode(text[start:])
                return parsed
            except json.JSONDecodeError:
                pass
        start = min(start_candidates)
        end_obj = text.rfind("}")
        end_arr = text.rfind("]")
        end = max(end_obj, end_arr)
        if end <= start:
            raise
        return json.loads(text[start : end + 1])


def llm_request_with_retries(
    packet: dict[str, Any],
    model: str,
    api_key: str,
    base_url: str,
    provider: str,
    temperature: float,
    max_tokens: int,
    timeout: float,
    retries: int,
    json_mode: bool,
) -> tuple[str, dict[str, Any]]:
    messages = packet_to_messages(packet)
    if provider == "anthropic":
        system_content = "\n\n".join(msg["content"] for msg in messages if msg.get("role") == "system")
        user_messages = [
            {"role": "user" if msg.get("role") == "system" else msg.get("role", "user"), "content": msg.get("content", "")}
            for msg in messages
            if msg.get("role") != "system"
        ]
        payload: dict[str, Any] = {
            "model": model,
            "max_tokens": max_tokens,
            "messages": user_messages or [{"role": "user", "content": json.dumps(packet.get("input", {}), ensure_ascii=False)}],
        }
        if system_content:
            payload["system"] = system_content
        if temperature is not None:
            payload["temperature"] = temperature
    else:
        token_param = token_limit_param_name(model, env_first("DAB_PIPELINE_TOKEN_PARAM", "OPENAI_TOKEN_PARAM"))
        payload = {
            "model": model,
            "messages": messages,
            token_param: max_tokens,
        }
        if not omit_temperature_for_model(model):
            payload["temperature"] = temperature
        if json_mode:
            payload["response_format"] = {"type": "json_object"}
    last_error: Exception | None = None
    for attempt in range(max(1, retries + 1)):
        try:
            if provider == "anthropic":
                response = post_anthropic_messages(payload, api_key, base_url, timeout)
                return extract_anthropic_content(response)
            response = post_chat_completion(payload, api_key, base_url, timeout)
            return extract_chat_content(response)
        except Exception as exc:  # Keep packet runner resilient across long batches.
            last_error = exc
            if attempt < retries:
                time.sleep(min(2 ** attempt, 20))
    assert last_error is not None
    raise last_error




def resolve_llm_runtime(args: argparse.Namespace) -> dict[str, Any]:
    model = first_nonempty(
        getattr(args, "model", ""),
        env_first("DAB_PIPELINE_MODEL", "LLM_MODEL", "OPENAI_MODEL", "ANTHROPIC_MODEL"),
    )
    provider = infer_llm_provider(
        first_nonempty(getattr(args, "provider", ""), env_first("DAB_PIPELINE_PROVIDER", "LLM_PROVIDER")),
        model,
        first_nonempty(getattr(args, "base_url", ""), env_first("DAB_PIPELINE_BASE_URL", "DAB_PIPELINE_BASE_UR", "LLM_BASE_URL", "OPENAI_BASE_URL", "ANTHROPIC_BASE_URL")),
    )
    default_base_url = "https://api.anthropic.com/v1" if provider == "anthropic" else "https://api.openai.com/v1"
    api_key = first_nonempty(
        getattr(args, "api_key", ""),
        env_first(
            "DAB_PIPELINE_API_KEY",
            "LLM_API_KEY",
            "ANTHROPIC_API_KEY" if provider == "anthropic" else "OPENAI_API_KEY",
            "OPENAI_API_KEY",
            "ANTHROPIC_API_KEY",
        ),
    )
    base_url = first_nonempty(
        getattr(args, "base_url", ""),
        env_first("DAB_PIPELINE_BASE_URL", "DAB_PIPELINE_BASE_UR", "LLM_BASE_URL", "ANTHROPIC_BASE_URL" if provider == "anthropic" else "OPENAI_BASE_URL", default=default_base_url),
    )
    if not api_key:
        raise SystemExit("Missing API key. Set DAB_PIPELINE_API_KEY, OPENAI_API_KEY, or ANTHROPIC_API_KEY, or pass --api-key.")
    if not model:
        raise SystemExit("Missing model. Set DAB_PIPELINE_MODEL, OPENAI_MODEL, or ANTHROPIC_MODEL, or pass --model.")
    return {
        "model": model,
        "provider": provider,
        "api_key": api_key,
        "base_url": base_url,
        "temperature": float(first_nonempty(str(getattr(args, "temperature", "")) if getattr(args, "temperature", None) is not None else "", env_first("DAB_PIPELINE_TEMPERATURE", default="0.2"))),
        "max_tokens": int(first_nonempty(str(getattr(args, "max_tokens", "")) if getattr(args, "max_tokens", None) is not None else "", env_first("DAB_PIPELINE_MAX_TOKENS", default="8192"))),
        "timeout": float(first_nonempty(str(getattr(args, "timeout", "")) if getattr(args, "timeout", None) is not None else "", env_first("DAB_PIPELINE_TIMEOUT", default="300"))),
        "json_mode": parse_bool(first_nonempty(str(getattr(args, "json_mode", "")) if getattr(args, "json_mode", None) is not None else "", env_first("DAB_PIPELINE_JSON_MODE", default="true")), True),
        "retries": int(getattr(args, "retries", 2)),
    }


def compact_explorer_observation(observation: dict[str, Any], max_chars: int = 6000) -> dict[str, Any]:
    compact = {
        "tool": observation.get("tool", "query_db"),
        "db_name": observation.get("db_name"),
        "db_type": observation.get("db_type"),
        "success": bool(observation.get("success")),
        "query": observation.get("query"),
        "summary": observation.get("summary", {}),
    }
    if observation.get("error"):
        compact["error"] = truncate(str(observation.get("error")), 1000)
    if observation.get("errors"):
        compact["errors"] = observation.get("errors")
    payload = json.dumps(jsonable(compact), ensure_ascii=False, sort_keys=True)
    if len(payload) <= max_chars:
        return jsonable(compact)
    summary = compact.get("summary") if isinstance(compact.get("summary"), dict) else {}
    compact["summary"] = {
        "truncated": True,
        "row_count_observed": summary.get("row_count_observed"),
        "columns": summary.get("columns", [])[:20],
    }
    compact["query"] = truncate(str(compact.get("query", "")), 1000)
    return jsonable(compact)


def mongo_schema_summary(logical_name: str, cfg: dict[str, Any], args: argparse.Namespace) -> dict[str, Any]:
    try:
        from pymongo import MongoClient
    except ModuleNotFoundError as exc:
        return {"db_name": logical_name, "db_type": "mongo", "error": f"pymongo_missing:{exc}"}
    db_name = str(cfg.get("db_name") or logical_name)
    client = MongoClient(args.mongo_uri, serverSelectionTimeoutMS=3000)
    collections: list[dict[str, Any]] = []
    try:
        for collection_name in client[db_name].list_collection_names()[: args.max_schema_tables]:
            rows = [jsonable(row) for row in client[db_name][collection_name].find({}, limit=max(1, args.schema_sample_rows))]
            columns = sorted({key for row in rows if isinstance(row, dict) for key in row.keys()})[:30]
            collections.append({"collection": collection_name, "sample_columns": columns, "sample_rows": rows[: args.schema_sample_rows]})
    except Exception as exc:
        return {"db_name": logical_name, "db_type": "mongo", "mongo_db": db_name, "error": f"{type(exc).__name__}: {exc}"}
    finally:
        client.close()
    return {"db_name": logical_name, "db_type": "mongo", "mongo_db": db_name, "collections": collections}


def explore_dataset_schema_summary(dataset_dir: Path, clients: dict[str, dict[str, Any]], args: argparse.Namespace) -> list[dict[str, Any]]:
    summaries: list[dict[str, Any]] = []
    for logical_name, cfg in sorted(clients.items()):
        db_type = str(cfg.get("db_type") or "").casefold()
        if db_type in {"sqlite", "duckdb"}:
            db_summary: dict[str, Any] = {"db_name": logical_name, "db_type": db_type, "tables": []}
            try:
                for table in list_sql_tables(logical_name, cfg, dataset_dir)[: args.max_schema_tables]:
                    columns = list_sql_columns(logical_name, cfg, dataset_dir, table)
                    table_summary = {"table": table, "columns": columns[:40], "sample": {}}
                    if args.schema_sample_rows > 0:
                        try:
                            rows, obs = execute_sql_client(logical_name, cfg, dataset_dir, f"SELECT * FROM {quote_identifier(table)} LIMIT {int(args.schema_sample_rows)}", args.schema_sample_rows)
                            table_summary["sample"] = summarize_rows(rows, row_limit=args.schema_sample_rows)
                            table_summary["sample"]["query"] = obs.get("query")
                        except Exception as exc:
                            table_summary["sample_error"] = f"{type(exc).__name__}: {exc}"
                    db_summary["tables"].append(table_summary)
            except Exception as exc:
                db_summary["error"] = f"{type(exc).__name__}: {exc}"
            summaries.append(db_summary)
        elif db_type == "mongo":
            summaries.append(mongo_schema_summary(logical_name, cfg, args))
        else:
            summaries.append({"db_name": logical_name, "db_type": db_type or "unknown", "error": "unsupported_db_type_for_explorer"})
    return summaries


def execute_explorer_query(action: dict[str, Any], clients: dict[str, dict[str, Any]], dataset_dir: Path, args: argparse.Namespace) -> dict[str, Any]:
    db_name = str(action.get("db_name") or action.get("database") or "").strip()
    query = action.get("query")
    if not db_name:
        return {"tool": "query_db", "success": False, "error": "query_db_action_missing_db_name", "query": query}
    cfg = clients.get(db_name)
    if not isinstance(cfg, dict):
        return {"tool": "query_db", "db_name": db_name, "success": False, "error": "unknown_db_name", "query": query}
    db_type = str(cfg.get("db_type") or "").casefold()
    try:
        if db_type in {"sqlite", "duckdb"}:
            rows, obs = execute_sql_client(db_name, cfg, dataset_dir, str(query), args.query_row_limit)
            obs["rows"] = rows
            obs["success"] = True
            return compact_explorer_observation(obs, args.max_observation_chars)
        if db_type == "mongo":
            obs = execute_mongo_query(db_name, cfg, query, args.query_row_limit, args.mongo_uri)
            return compact_explorer_observation(obs, args.max_observation_chars)
        return {"tool": "query_db", "db_name": db_name, "db_type": db_type, "success": False, "error": "unsupported_db_type", "query": query}
    except Exception as exc:
        return {"tool": "query_db", "db_name": db_name, "db_type": db_type, "success": False, "query": query, "error": f"{type(exc).__name__}: {exc}"}


def explorer_action_schema() -> dict[str, Any]:
    return {
        "query_db": {
            "description": "Inspect bounded evidence from a logical DABench database.",
            "required": ["action", "db_name", "query", "purpose"],
            "shape": {"action": "query_db", "db_name": "logical_db_name", "query": "SQL string or Mongo query object", "purpose": "why this evidence is needed"},
        },
        "final_candidate": {
            "description": "Return one fully grounded DABench candidate after enough evidence has been observed.",
            "required": ["action", "candidate"],
            "candidate_required_fields": ["dataset", "query", "candidate_solution", "evidence_card", "evidence_chain", "validator_template", "validator_args", "expected_answer", "data_requirements", "solution_plan", "query_transform", "contrast_set", "distractor_plan", "reward_spec"],
        },
    }


def source_context_rows_from_bench(bench_root: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for seed in discover_seed_tasks(bench_root):
        record = seed.to_record()
        signature = source_task_signature_from_seed(record)
        rows.append(
            {
                "dataset": record.get("dataset"),
                "query_id": record.get("query_id"),
                "query": record.get("query"),
                "task_signature": signature,
                "allowed_hints": format_allowed_hints(record),
                "hint_policy": hint_policy_for_seed(record),
                "db_source_types": record.get("db_source_types", []),
                "query_ops": record.get("query_ops", []),
                "source_validation_style": record.get("source_validation_style", "unknown"),
                "source_answer_shape": signature.get("answer_shape", "unknown"),
                "official_dabench_anchor": official_anchor_for_seed(record, 0, 0),
                "db_description": truncate(record.get("db_description", ""), 12000),
                "db_config_summary": truncate(record.get("db_config_text", ""), 6000),
            }
        )
    return rows


def seed_context_for_explorer(context: dict[str, Any]) -> dict[str, Any]:
    return {
        "source_query_id": context.get("source_query_id"),
        "source_query": context.get("source_query"),
        "source_task_signature": context.get("source_task_signature", {}),
        "allowed_hints": context.get("allowed_hints", []),
        "hint_policy": context.get("hint_policy", {}),
        "official_dabench_anchor": context.get("official_dabench_anchor", {}),
    }


def fill_explorer_candidate_defaults(candidate: dict[str, Any], source_dataset: str, source_context: dict[str, Any], trace: list[dict[str, Any]]) -> dict[str, Any]:
    out = dict(candidate)
    out.setdefault("generation_strategy", "explore_and_validate_dab")
    out.setdefault("dataset", f"synthetic_{source_dataset}")
    provenance = out.get("provenance") if isinstance(out.get("provenance"), dict) else {}
    provenance.setdefault("source_dataset", source_dataset)
    provenance.setdefault("source_query_id", source_context.get("source_query_id"))
    provenance.setdefault("generation_method", "explore_and_validate_dab")
    out["provenance"] = provenance
    out.setdefault("source_task_signature", source_context.get("source_task_signature", {}))
    out.setdefault(
        "official_anchor_usage",
        {
            "source_query_id": source_context.get("source_query_id"),
            "usage": "wording_style_and_task_signature_only",
            "answer_values_used": False,
            "note": "Explore-and-validate candidates derive answers from replayed database evidence, not official ground truth.",
        },
    )
    out.setdefault("hint_refs", [])
    out.setdefault("hint_selection_rationale", {"selected": [], "reason": "No existing dataset hint was necessary or relevant."})
    out.setdefault("reward_spec", {"primary": "programmatic_validator", "format_reward": True})
    out.setdefault("query_transform", {"type": "none", "answer_materialized_after_transform": True, "added_constraints": [], "required_extra_operations": [], "safety_check": "Replay verification must validate the transformed query."})
    out.setdefault("contrast_set", [])
    out.setdefault("distractor_plan", {"distractors_checked": [], "disambiguation_rule": "candidate_solution and validator define the unique answer."})
    out.setdefault("exploration_summary", {"steps": len(trace), "successful_queries": sum(1 for item in trace if item.get("observation", {}).get("success"))})
    out.setdefault("pipeline_id", f"explore_{source_dataset}_{stable_hash([out.get('query'), out.get('candidate_solution'), len(trace)])}")
    return out


def apply_explorer_target_defaults(candidate: dict[str, Any], args: argparse.Namespace, dataset: str, clients: dict[str, dict[str, Any]]) -> dict[str, Any]:
    target = normalized_target_task_type(args)
    if not target:
        return candidate
    out = dict(candidate)
    provenance = out.get("provenance") if isinstance(out.get("provenance"), dict) else {}
    provenance["target_task_type"] = target
    out["provenance"] = provenance
    out["target_task_type"] = target
    out["target_source_mix"] = target_source_mix_for_dataset(target, dataset, clients)
    if target == "mixed_sql_mongo":
        out["task_type"] = "mixed_sql_mongo"
    return out


def candidate_hardness_profile(candidate: dict[str, Any]) -> dict[str, Any]:
    req = candidate.get("data_requirements", {}) if isinstance(candidate.get("data_requirements"), dict) else {}
    solution = candidate.get("candidate_solution", {}) if isinstance(candidate.get("candidate_solution"), dict) else {}
    tables = req.get("tables") or []
    collections = req.get("collections") or []
    operations = [str(item).casefold() for item in (req.get("operations") or [])]
    evidence_chain = candidate.get("evidence_chain") if isinstance(candidate.get("evidence_chain"), list) else []
    query_transform = candidate.get("query_transform", {}) if isinstance(candidate.get("query_transform"), dict) else {}
    solution_text = json.dumps(solution, ensure_ascii=False).casefold()
    contrast_set = candidate.get("contrast_set") if isinstance(candidate.get("contrast_set"), list) else []
    score = 0
    score += min(3, len(tables) + len(collections))
    score += min(3, len(evidence_chain))
    score += 1 if any("join" in op or "lookup" in op for op in operations) or " join " in solution_text else 0
    score += 1 if any("temporal" in op or "date" in op or "year" in op for op in operations) else 0
    score += 1 if any("normal" in op or "id" in op for op in operations) else 0
    score += 1 if any("group" in op or "aggregate" in op or "rank" in op or "sum" in op for op in operations) else 0
    score += 1 if str(query_transform.get("type") or "none") != "none" else 0
    score += 1 if contrast_set else 0
    score += 1 if solution.get("mongo") or "collection" in solution_text else 0
    band = "easy" if score <= 3 else "medium" if score <= 7 else "hard"
    return {"method": "static_hardness_profile_v2", "score": score, "band": band, "num_tables": len(tables), "num_collections": len(collections), "evidence_steps": len(evidence_chain), "query_transform_type": query_transform.get("type", "none"), "has_contrast_set": bool(contrast_set), "operation_markers": sorted(set(operations))[:20]}


def solver_calibration_stub(candidate: dict[str, Any], hardness: dict[str, Any]) -> dict[str, Any]:
    score = int(hardness.get("score") or 0)
    if score <= 3:
        target = "likely_easy; keep only if source task is also easy or the sample adds missing coverage"
        expected_weak_solver_pass_rate = "high"
    elif score <= 7:
        target = "training_sweet_spot; weak solver may fail but strong solver should pass after tools"
        expected_weak_solver_pass_rate = "medium"
    else:
        target = "hard; require downstream solver pass-rate calibration before training selection"
        expected_weak_solver_pass_rate = "low"
    return {"method": "static_pre_solver_calibration", "target": target, "expected_weak_solver_pass_rate": expected_weak_solver_pass_rate, "recommended_gate": "run tool-using solver attempts and keep samples where at least one strong solver passes and at least one weaker solver or no-hint attempt fails", "requires_external_solver_pass_rate": score >= 6}


def materialize_explorer_candidate(candidate: dict[str, Any], pre_ev: dict[str, Any]) -> dict[str, Any]:
    out = dict(candidate)
    final_answer = pre_ev.get("final_answer")
    expected_answer, template, validator_args, risks = validator_spec_from_observed_answer(final_answer, str(candidate.get("validator_template", "")))
    out["ground_truth_materialization"] = {
        "strategy": "explore_and_validate_replay_then_materialize_validator",
        "original_expected_answer": candidate.get("expected_answer"),
        "original_validator_template": candidate.get("validator_template"),
        "original_validator_args": candidate.get("validator_args"),
        "observed_final_answer": jsonable(final_answer),
        "observed_final_answer_source": pre_ev.get("final_answer_source", ""),
        "execution_failures": [str(item) for item in pre_ev.get("failures", []) if not str(item).startswith("validator_failed:")],
        "pre_materialization_validator_failures": [str(item) for item in pre_ev.get("failures", []) if str(item).startswith("validator_failed:")],
        "risks": risks,
    }
    out["evidence_card"] = materialized_evidence_card(out, pre_ev, final_answer, risks)
    if not risks and final_answer is not None:
        out["expected_answer"] = expected_answer
        out["validator_template"] = template
        out["validator_args"] = validator_args
        out["reward_spec"] = {**(out.get("reward_spec", {}) if isinstance(out.get("reward_spec"), dict) else {}), "primary": "programmatic_validator", "format_reward": True, "ground_truth_materialized_from_candidate_solution": True}
    return out


def judge_and_verify_explorer_candidate(candidate: dict[str, Any], args: argparse.Namespace, hint_catalog: dict[str, Any], official_anchors: dict[tuple[str, int], dict[str, Any]], seen_queries: set[str], trace: list[dict[str, Any]]) -> dict[str, Any]:
    candidate = dict(candidate)
    candidate["explore_trace"] = trace
    if getattr(args, "materialize_observed_ground_truth", True):
        pre_ev = verify_candidate_evidence(candidate, args)
        candidate = materialize_explorer_candidate(candidate, pre_ev)
        candidate["explore_trace"] = trace
    hardness = candidate_hardness_profile(candidate)
    candidate["hardness_profile"] = hardness
    candidate["solver_calibration"] = solver_calibration_stub(candidate, hardness)
    candidate["evidence_verification"] = verify_candidate_evidence(candidate, args)
    local = judge_candidate(candidate, seen_queries, hint_catalog, official_anchors)
    if not candidate.get("evidence_verification", {}).get("verified"):
        local["accepted"] = False
        local["risks"] = sorted(set((local.get("risks") or []) + ["explore_evidence_verification_failed"]))
    row = dict(candidate)
    row["hints"] = local.pop("resolved_hints", [])
    row["source_signature_alignment"] = local.pop("source_signature_alignment", {})
    row["hint_policy"] = local.get("hint_policy", {})
    row["hint_refs"] = row["hint_policy"].get("selected_hint_refs", row.get("hint_refs", []))
    row["judge"] = merge_judge(candidate.get("judge"), local)
    return row


def explore_one_candidate(dataset: str, candidate_index: int, contexts: list[dict[str, Any]], args: argparse.Namespace, runtime: dict[str, Any], prompt: str, hint_catalog: dict[str, Any], official_anchors: dict[tuple[str, int], dict[str, Any]], seen_queries: set[str]) -> dict[str, Any]:
    bench_root = Path(args.bench_root)
    dataset_dir = bench_root / f"query_{dataset}"
    if not dataset_dir.exists():
        return {"dataset": f"synthetic_{dataset}", "pipeline_id": f"explore_error_{dataset}_{candidate_index}", "judge": {"accepted": False, "score": 0.0, "risks": [f"dataset_dir_missing:{dataset_dir}"]}}
    clients = load_db_clients(dataset_dir)
    target_task_type = normalized_target_task_type(args)
    if target_task_type and not dataset_supports_target_task_type(dataset_dir, target_task_type):
        return {"dataset": f"synthetic_{dataset}", "pipeline_id": f"explore_error_{dataset}_{candidate_index}", "generation_strategy": "explore_and_validate_dab", "judge": {"accepted": False, "score": 0.0, "risks": [f"dataset_does_not_support_target_task_type:{target_task_type}"]}, "source_mix": db_source_mix(clients)}
    source_context = contexts[candidate_index % len(contexts)] if contexts else {}
    schema_summary = explore_dataset_schema_summary(dataset_dir, clients, args)
    ambiguity_strategy = ambiguity_strategy_from_contexts(contexts, limit=args.max_source_contexts)
    transcript: list[dict[str, Any]] = []
    trace: list[dict[str, Any]] = []
    final_candidate: dict[str, Any] | None = None
    final_error = ""

    for step in range(1, args.max_agent_steps + 1):
        packet = {
            "packet_id": f"explore_{dataset}_{candidate_index:03d}_step{step:02d}",
            "system_prompt": prompt,
            "input": {
                "dataset": dataset,
                "current_step": step,
                "max_agent_steps": args.max_agent_steps,
                "remaining_steps": args.max_agent_steps - step,
                "final_candidate_deadline": "If remaining_steps <= 1, return final_candidate now when any non-empty evidence has been observed.",
                "source_context": seed_context_for_explorer(source_context),
                "source_context_pool": [seed_context_for_explorer(item) for item in contexts[: args.max_source_contexts]],
                "strict_generation_policy": strict_generation_policy(),
                "hardness_targets": {
                    "include": [
                        "multi-step DB exploration before final candidate",
                        "controlled injection/fuzzing/obfuscation when supported by observed rows",
                        "multi-hop or multi-source evidence when available",
                        "ambiguous-but-unique DABench-style wording",
                        "distractor/contrast set with explicit disambiguation",
                        "solver calibration metadata for later pass-rate filtering",
                    ],
                    "do_not": [
                        "make the task subjective or externally dependent",
                        "hide an undefined criterion in vague wording",
                        "write a task whose answer cannot be replayed by candidate_solution",
                    ],
                },
                "dabench_ambiguity_strategy": ambiguity_strategy,
                "target_task_type": target_task_type or "auto",
                "target_source_mix": target_source_mix_for_dataset(target_task_type, dataset, clients),
                "db_clients": {name: {k: v for k, v in cfg.items() if k != "password"} for name, cfg in clients.items()},
                "schema_summary": schema_summary,
                "action_schema": explorer_action_schema(),
                "transcript": transcript[-args.max_transcript_items :],
            },
            "expected_output": "Exactly one JSON object: either a query_db action or final_candidate with a complete DABench candidate.",
        }
        try:
            content, usage = llm_request_with_retries(packet=packet, **runtime)
            parsed = parse_llm_json(content)
        except Exception as exc:
            final_error = f"llm_step_failed:{type(exc).__name__}: {exc}"
            trace.append({"step": step, "error": final_error})
            break
        action = parsed if isinstance(parsed, dict) else {"action": "invalid", "raw": parsed}
        trace_item: dict[str, Any] = {"step": step, "action": action, "usage": usage}
        action_name = str(action.get("action") or "").casefold()
        if action_name == "query_db":
            observation = execute_explorer_query(action, clients, dataset_dir, args)
            trace_item["observation"] = observation
            transcript.append({"assistant_action": action, "observation": observation})
            trace.append(trace_item)
            continue
        if action_name == "final_candidate":
            candidate = action.get("candidate") if isinstance(action.get("candidate"), dict) else {}
            final_candidate = fill_explorer_candidate_defaults(candidate, dataset, source_context, trace)
            final_candidate = apply_explorer_target_defaults(final_candidate, args, dataset, clients)
            trace.append(trace_item)
            break
        if all(key in action for key in ("query", "candidate_solution", "validator_template")):
            final_candidate = fill_explorer_candidate_defaults(action, dataset, source_context, trace)
            final_candidate = apply_explorer_target_defaults(final_candidate, args, dataset, clients)
            trace_item["coerced_final_candidate"] = True
            trace.append(trace_item)
            break
        observation = {"success": False, "error": "invalid_action_schema", "received_keys": sorted(action.keys())}
        trace_item["observation"] = observation
        transcript.append({"assistant_action": action, "observation": observation})
        trace.append(trace_item)

    if final_candidate is None:
        return {"dataset": f"synthetic_{dataset}", "pipeline_id": f"explore_failed_{dataset}_{candidate_index}_{stable_hash([trace, final_error])}", "generation_strategy": "explore_and_validate_dab", "explore_trace": trace, "judge": {"accepted": False, "score": 0.0, "risks": [final_error or "explorer_no_final_candidate"]}, "evidence_verification": {"verified": False, "failures": [final_error or "explorer_no_final_candidate"], "observations": [], "evidence_steps": []}}
    return judge_and_verify_explorer_candidate(final_candidate, args, hint_catalog, official_anchors, seen_queries, trace)


def explore_and_validate_dab(args: argparse.Namespace) -> None:
    runtime = resolve_llm_runtime(args)
    bench_root = Path(args.bench_root)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    prompt = read_text(Path(args.explorer_prompt))
    if not prompt.strip():
        raise SystemExit(f"Explorer prompt is empty or missing: {args.explorer_prompt}")
    signature_rows = read_jsonl(Path(args.signature_jsonl)) if args.signature_jsonl else source_context_rows_from_bench(bench_root)
    datasets = sorted({str(row.get("dataset")) for row in signature_rows if row.get("dataset")})
    target_task_type = normalized_target_task_type(args)
    if args.datasets:
        allowed = {item.strip() for item in args.datasets.split(",") if item.strip()}
        datasets = [dataset for dataset in datasets if dataset in allowed]
    if target_task_type:
        datasets = [
            dataset
            for dataset in datasets
            if dataset_supports_target_task_type(bench_root / f"query_{dataset}", target_task_type)
        ]
        if not datasets:
            raise SystemExit(f"No datasets support target_task_type={target_task_type}")
    if args.max_datasets:
        datasets = datasets[: args.max_datasets]
    hint_catalog = load_hint_catalog(args.hint_catalog_json, args.bench_root)
    official_anchors = load_official_anchor_catalog(args.bench_root)
    seen_queries: set[str] = set()
    rows: list[dict[str, Any]] = []
    traces: list[dict[str, Any]] = []
    for dataset in datasets:
        contexts = source_contexts_for_dataset(signature_rows, dataset)
        for candidate_index in range(max(1, args.max_candidates_per_dataset)):
            row = explore_one_candidate(dataset, candidate_index, contexts, args, runtime, prompt, hint_catalog, official_anchors, seen_queries)
            rows.append(row)
            traces.append({"pipeline_id": row.get("pipeline_id"), "dataset": row.get("dataset"), "explore_trace": row.get("explore_trace", [])})
            write_jsonl(output_dir / "candidates.jsonl", rows)
            write_jsonl(output_dir / "explore_traces.jsonl", traces)
            if args.sleep_sec > 0:
                time.sleep(args.sleep_sec)
    write_jsonl(output_dir / "verified_evidence.jsonl", rows)
    write_dashboard(output_dir / "dashboard.md", rows)
    write_evidence_dashboard(output_dir / "evidence_dashboard.md", rows)
    summary = {"datasets": len(datasets), "target_task_type": target_task_type or "auto", "candidates": len(rows), "accepted": sum(1 for row in rows if row.get("judge", {}).get("accepted")), "verified": sum(1 for row in rows if row.get("evidence_verification", {}).get("verified")), "mixed_sql_mongo_accepted": sum(1 for row in rows if row.get("judge", {}).get("accepted") and is_mixed_sql_mongo_candidate(row)), "output_dir": str(output_dir), "model": runtime.get("model"), "provider": runtime.get("provider"), "base_url": runtime.get("base_url")}
    (output_dir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))

def candidate_id_from_review_packet_id(packet_id: str) -> str | None:
    for prefix in ("gt_review_", "task_fit_review_", "training_value_review_"):
        if packet_id.startswith(prefix):
            return packet_id.removeprefix(prefix)
    return None


def run_llm_packets(args: argparse.Namespace) -> None:
    model = first_nonempty(
        args.model,
        env_first("DAB_PIPELINE_MODEL", "LLM_MODEL", "OPENAI_MODEL", "ANTHROPIC_MODEL"),
    )
    provider = infer_llm_provider(
        first_nonempty(args.provider, env_first("DAB_PIPELINE_PROVIDER", "LLM_PROVIDER")),
        model,
        first_nonempty(args.base_url, env_first("DAB_PIPELINE_BASE_URL", "DAB_PIPELINE_BASE_UR", "LLM_BASE_URL", "OPENAI_BASE_URL", "ANTHROPIC_BASE_URL")),
    )
    default_base_url = "https://api.anthropic.com/v1" if provider == "anthropic" else "https://api.openai.com/v1"
    api_key = first_nonempty(
        args.api_key,
        env_first(
            "DAB_PIPELINE_API_KEY",
            "LLM_API_KEY",
            "ANTHROPIC_API_KEY" if provider == "anthropic" else "OPENAI_API_KEY",
            "OPENAI_API_KEY",
            "ANTHROPIC_API_KEY",
        ),
    )
    base_url = first_nonempty(
        args.base_url,
        env_first("DAB_PIPELINE_BASE_URL", "DAB_PIPELINE_BASE_UR", "LLM_BASE_URL", "ANTHROPIC_BASE_URL" if provider == "anthropic" else "OPENAI_BASE_URL", default=default_base_url),
    )
    if not api_key:
        raise SystemExit("Missing API key. Set DAB_PIPELINE_API_KEY, OPENAI_API_KEY, or ANTHROPIC_API_KEY, or pass --api-key.")
    if not model:
        raise SystemExit("Missing model. Set DAB_PIPELINE_MODEL, OPENAI_MODEL, or ANTHROPIC_MODEL, or pass --model.")

    temperature = float(first_nonempty(str(args.temperature) if args.temperature is not None else "", env_first("DAB_PIPELINE_TEMPERATURE", default="0.2")))
    max_tokens = int(first_nonempty(str(args.max_tokens) if args.max_tokens is not None else "", env_first("DAB_PIPELINE_MAX_TOKENS", default="8192")))
    timeout = float(first_nonempty(str(args.timeout) if args.timeout is not None else "", env_first("DAB_PIPELINE_TIMEOUT", default="300")))
    json_mode = parse_bool(first_nonempty(str(args.json_mode) if args.json_mode is not None else "", env_first("DAB_PIPELINE_JSON_MODE", default="true")), True)

    packets = read_jsonl(Path(args.packet_jsonl))
    if args.limit:
        packets = packets[: args.limit]
    output_path = Path(args.output_jsonl)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    seen_packet_ids: set[str] = set()
    if args.resume and output_path.exists():
        for row in read_jsonl(output_path):
            packet_id = str(row.get("packet_id") or row.get("_packet_id") or "")
            if packet_id:
                seen_packet_ids.add(packet_id)

    mode = "a" if args.resume and output_path.exists() else "w"
    written = 0
    errors = 0
    with output_path.open(mode, encoding="utf-8") as out:
        for index, packet in enumerate(packets):
            packet_id = str(packet.get("packet_id") or f"packet_{index:05d}")
            if packet_id in seen_packet_ids:
                continue
            try:
                content, usage = llm_request_with_retries(
                    packet=packet,
                    model=model,
                    api_key=api_key,
                    base_url=base_url,
                    provider=provider,
                    temperature=temperature,
                    max_tokens=max_tokens,
                    timeout=timeout,
                    retries=args.retries,
                    json_mode=json_mode,
                )
                parsed = parse_llm_json(content)
                row = parsed if isinstance(parsed, dict) else {"output": parsed}
                row["packet_id"] = packet_id
                candidate_pipeline_id = candidate_id_from_review_packet_id(packet_id)
                if candidate_pipeline_id:
                    reported_pipeline_id = row.get("pipeline_id")
                    if reported_pipeline_id and reported_pipeline_id != candidate_pipeline_id:
                        row["llm_reported_pipeline_id"] = reported_pipeline_id
                    row["candidate_pipeline_id"] = candidate_pipeline_id
                    row["pipeline_id"] = candidate_pipeline_id
                else:
                    row.setdefault("pipeline_id", f"llm_{stable_hash([packet_id, row.get('query', '')])}")
                row["_llm"] = {
                    "model": model,
                    "base_url": base_url,
                    "provider": provider,
                    "usage": usage,
                }
            except Exception as exc:
                errors += 1
                row = {
                    "packet_id": packet_id,
                    "pipeline_id": f"llm_error_{stable_hash([packet_id])}",
                    "llm_error": f"{type(exc).__name__}: {exc}",
                    "judge": {"accepted": False, "score": 0.0, "risks": ["llm_generation_failed"]},
                }
                if args.fail_fast:
                    out.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
                    raise
            out.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
            out.flush()
            written += 1
            if args.sleep_sec > 0:
                time.sleep(args.sleep_sec)

    print(
        json.dumps(
            {
                "packets": len(packets),
                "written": written,
                "errors": errors,
                "output_jsonl": str(output_path),
                "model": model,
                "base_url": base_url,
                "provider": provider,
                "json_mode": json_mode,
                "token_param": "max_tokens" if provider == "anthropic" else token_limit_param_name(model, env_first("DAB_PIPELINE_TOKEN_PARAM", "OPENAI_TOKEN_PARAM")),
                "temperature_sent": provider == "anthropic" or not omit_temperature_for_model(model),
            },
            ensure_ascii=False,
            indent=2,
        )
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("collect-seeds", help="Extract seed task inventory from an existing DABench checkout.")
    p.add_argument("--bench-root", default=str(DEFAULT_BENCH_ROOT))
    p.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT / "seeds"))
    p.set_defaults(func=collect_seeds)

    p = sub.add_parser("make-generation-packets", help="Create Claude/GPT generation prompt packets.")
    p.add_argument("--seed-jsonl", required=True)
    p.add_argument("--output-jsonl", required=True)
    p.add_argument("--generator-prompt", default=str(Path(__file__).resolve().parent / "prompts" / "dabench_task_generator.md"))
    p.add_argument("--max-packets", type=int, default=0)
    p.add_argument("--max-db-description-chars", type=int, default=16000)
    p.add_argument("--max-db-config-chars", type=int, default=6000)
    p.add_argument("--max-ground-truth-chars", type=int, default=4000)
    p.add_argument("--max-validate-chars", type=int, default=6000)
    p.set_defaults(func=make_generation_packets)

    p = sub.add_parser("make-evolution-packets", help="Create seed-evolution prompt packets for harder verifiable tasks.")
    p.add_argument("--seed-jsonl", required=True)
    p.add_argument("--output-jsonl", required=True)
    p.add_argument("--evolver-prompt", default=str(Path(__file__).resolve().parent / "prompts" / "dabench_task_evolver.md"))
    p.add_argument("--evolution-types", default=",".join(EVOLUTION_TYPES))
    p.add_argument("--max-packets", type=int, default=0)
    p.add_argument("--max-db-description-chars", type=int, default=16000)
    p.add_argument("--max-db-config-chars", type=int, default=6000)
    p.add_argument("--max-ground-truth-chars", type=int, default=4000)
    p.add_argument("--max-validate-chars", type=int, default=6000)
    p.set_defaults(func=make_evolution_packets)

    p = sub.add_parser("make-solver-packets", help="Create Claude/GPT packets for solver, validator, and verification repair.")
    p.add_argument("--candidate-jsonl", required=True)
    p.add_argument("--output-jsonl", required=True)
    p.add_argument("--solver-prompt", default=str(Path(__file__).resolve().parent / "prompts" / "dabench_solver_verifier.md"))
    p.set_defaults(func=make_solver_packets)

    p = sub.add_parser("build-task-signatures", help="Extract source DABench task signatures for seed-conditioned generation.")
    p.add_argument("--seed-jsonl", required=True)
    p.add_argument("--output-jsonl", required=True)
    p.add_argument("--dashboard", default="")
    p.add_argument("--max-rows", type=int, default=0)
    p.add_argument("--max-db-description-chars", type=int, default=16000)
    p.add_argument("--max-db-config-chars", type=int, default=6000)
    p.add_argument("--max-ground-truth-chars", type=int, default=4000)
    p.add_argument("--max-validate-chars", type=int, default=6000)
    p.set_defaults(func=build_task_signatures)

    p = sub.add_parser("make-evidence-mining-packets", help="Create seed-conditioned evidence-first task construction packets.")
    p.add_argument("--signature-jsonl", required=True)
    p.add_argument("--output-jsonl", required=True)
    p.add_argument("--evidence-prompt", default=str(Path(__file__).resolve().parent / "prompts" / "dabench_evidence_miner.md"))
    p.add_argument("--max-packets", type=int, default=0)
    p.add_argument("--max-db-description-chars", type=int, default=16000)
    p.add_argument("--max-db-config-chars", type=int, default=6000)
    p.set_defaults(func=make_evidence_mining_packets)

    p = sub.add_parser("mine-db-facts", help="Mine concrete non-degenerate SQL facts before asking an LLM to write tasks.")
    p.add_argument("--seed-jsonl", default="")
    p.add_argument("--signature-jsonl", default="")
    p.add_argument("--output-jsonl", required=True)
    p.add_argument("--rejected-jsonl", default="")
    p.add_argument("--bench-root", default=str(DEFAULT_BENCH_ROOT))
    p.add_argument("--datasets", default="", help="Comma-separated dataset allowlist.")
    p.add_argument("--max-datasets", type=int, default=0)
    p.add_argument("--max-tables-per-db", type=int, default=8)
    p.add_argument("--max-group-columns-per-table", type=int, default=4)
    p.add_argument("--max-numeric-columns-per-table", type=int, default=4)
    p.add_argument("--max-temporal-columns-per-table", type=int, default=3)
    p.add_argument("--max-join-pairs-per-db", type=int, default=12)
    p.add_argument("--max-candidate-facts-per-table", type=int, default=12)
    p.add_argument("--max-candidate-facts-per-dataset", type=int, default=50)
    p.add_argument("--max-facts-per-dataset", type=int, default=5)
    p.add_argument("--min-group-size", type=int, default=2)
    p.add_argument("--min-group-key-chars", type=int, default=2)
    p.add_argument("--min-group-key-alpha-chars", type=int, default=2)
    p.add_argument("--max-group-key-value-chars", type=int, default=120)
    p.add_argument("--query-row-limit", type=int, default=50)
    p.add_argument(
        "--single-table-first",
        action="store_true",
        help="Compatibility mode: mine single-table facts before cross-table join facts. Default mines join facts first.",
    )
    p.set_defaults(func=mine_db_facts)

    p = sub.add_parser("filter-db-facts", help="Filter mined DB facts into strict join-focused or source-fit subsets.")
    p.add_argument("--fact-jsonl", required=True)
    p.add_argument("--output-jsonl", required=True)
    p.add_argument("--rejected-jsonl", default="")
    p.add_argument("--dashboard", default="")
    p.add_argument("--max-facts", type=int, default=0)
    p.add_argument("--fact-type-prefix", default="join_")
    p.add_argument("--require-source-join", action="store_true", default=True)
    p.add_argument("--no-require-source-join", dest="require_source_join", action="store_false")
    p.add_argument("--require-source-aggregation", action="store_true", default=False)
    p.add_argument("--require-join-match", action="store_true", default=True)
    p.add_argument("--no-require-join-match", dest="require_join_match", action="store_false")
    p.add_argument("--require-aggregation-match", action="store_true", default=False)
    p.add_argument("--require-family-match", action="store_true", default=False)
    p.add_argument("--reject-rank-ties", action="store_true", default=True)
    p.add_argument("--allow-rank-ties", dest="reject_rank_ties", action="store_false")
    p.add_argument("--reject-reason-markers", default="join_missing")
    p.add_argument("--reject-boolean-answers", action="store_true", default=True)
    p.add_argument("--allow-boolean-answers", dest="reject_boolean_answers", action="store_false")
    p.add_argument("--reject-cpe-answers", action="store_true", default=True)
    p.add_argument("--allow-cpe-answers", dest="reject_cpe_answers", action="store_false")
    p.add_argument("--dedupe-answers", action="store_true", default=True)
    p.add_argument("--no-dedupe-answers", dest="dedupe_answers", action="store_false")
    p.set_defaults(func=filter_db_facts)

    p = sub.add_parser("make-db-mined-task-packets", help="Create LLM packets that turn DB-mined facts into DABench tasks.")
    p.add_argument("--fact-jsonl", required=True)
    p.add_argument("--output-jsonl", required=True)
    p.add_argument("--prompt", default=str(Path(__file__).resolve().parent / "prompts" / "dabench_db_mined_task_writer.md"))
    p.add_argument("--max-packets", type=int, default=0)
    p.add_argument("--sampling-strategy", choices=["balanced", "sequential"], default="balanced")
    p.set_defaults(func=make_db_mined_task_packets)

    p = sub.add_parser("explore-and-validate-dab", help="Let an LLM explorer query DABench DBs, propose harder ambiguous candidates, and replay-verify them.")
    p.add_argument("--bench-root", default=str(DEFAULT_BENCH_ROOT))
    p.add_argument("--output-dir", required=True)
    p.add_argument("--signature-jsonl", default="", help="Optional build-task-signatures output; defaults to scanning bench-root.")
    p.add_argument("--hint-catalog-json", default="")
    p.add_argument("--datasets", default="", help="Comma-separated dataset allowlist.")
    p.add_argument("--target-task-type", default="", choices=["", "mixed_sql_mongo"], help="Optional curriculum bucket target. mixed_sql_mongo filters to datasets with both SQL and Mongo sources and enforces mixed-source replay.")
    p.add_argument("--max-datasets", type=int, default=1)
    p.add_argument("--max-candidates-per-dataset", type=int, default=1)
    p.add_argument("--max-agent-steps", type=int, default=8)
    p.add_argument("--max-source-contexts", type=int, default=5)
    p.add_argument("--max-transcript-items", type=int, default=10)
    p.add_argument("--max-schema-tables", type=int, default=8)
    p.add_argument("--schema-sample-rows", type=int, default=2)
    p.add_argument("--query-row-limit", type=int, default=200)
    p.add_argument("--max-observation-chars", type=int, default=6000)
    p.add_argument("--python-timeout", type=int, default=120)
    p.add_argument("--allow-python-exec", dest="allow_python_exec", action="store_true", default=True)
    p.add_argument("--no-python-exec", dest="allow_python_exec", action="store_false")
    p.add_argument("--materialize-observed-ground-truth", dest="materialize_observed_ground_truth", action="store_true", default=True)
    p.add_argument("--no-materialize-observed-ground-truth", dest="materialize_observed_ground_truth", action="store_false")
    p.add_argument("--mongo-uri", default=os.environ.get("DAB_PIPELINE_MONGO_URI", "mongodb://127.0.0.1:27017/"))
    p.add_argument("--explorer-prompt", default=str(Path(__file__).resolve().parent / "prompts" / "dabench_explore_validate_agent.md"))
    p.add_argument("--model", default="")
    p.add_argument("--base-url", default="")
    p.add_argument("--api-key", default="")
    p.add_argument("--provider", default="", choices=["", "openai", "anthropic"], help="LLM provider; auto-detects claude*/anthropic.com as anthropic.")
    p.add_argument("--temperature", type=float, default=None)
    p.add_argument("--max-tokens", type=int, default=None)
    p.add_argument("--timeout", type=float, default=None)
    p.add_argument("--retries", type=int, default=2)
    p.add_argument("--sleep-sec", type=float, default=0.0)
    p.add_argument("--json-mode", type=str, default=None, help="true/false; default reads DAB_PIPELINE_JSON_MODE or true")
    p.set_defaults(func=explore_and_validate_dab)

    p = sub.add_parser("run-llm-packets", help="Call an OpenAI-compatible chat API for packet JSONL and write parsed JSONL outputs.")
    p.add_argument("--packet-jsonl", required=True)
    p.add_argument("--output-jsonl", required=True)
    p.add_argument("--model", default="")
    p.add_argument("--base-url", default="")
    p.add_argument("--api-key", default="")
    p.add_argument("--provider", default="", choices=["", "openai", "anthropic"], help="LLM provider; auto-detects claude*/anthropic.com as anthropic.")
    p.add_argument("--temperature", type=float, default=None)
    p.add_argument("--max-tokens", type=int, default=None)
    p.add_argument("--timeout", type=float, default=None)
    p.add_argument("--retries", type=int, default=2)
    p.add_argument("--limit", type=int, default=0)
    p.add_argument("--sleep-sec", type=float, default=0.0)
    p.add_argument("--resume", action="store_true")
    p.add_argument("--fail-fast", action="store_true")
    p.add_argument("--json-mode", type=str, default=None, help="true/false; default reads DAB_PIPELINE_JSON_MODE or true")
    p.set_defaults(func=run_llm_packets)

    p = sub.add_parser("judge-local", help="Run deterministic local checks and write a global dashboard.")
    p.add_argument("--candidate-jsonl", required=True)
    p.add_argument("--output-jsonl", required=True)
    p.add_argument("--dashboard", required=True)
    p.add_argument("--bench-root", default=str(DEFAULT_BENCH_ROOT))
    p.add_argument("--hint-catalog-json", default="")
    p.set_defaults(func=judge_local)

    p = sub.add_parser("make-judge-packets", help="Create global-context Claude/GPT judge prompt packets.")
    p.add_argument("--candidate-jsonl", required=True)
    p.add_argument("--global-inventory-json", required=True)
    p.add_argument("--output-jsonl", required=True)
    p.add_argument("--judge-prompt", default=str(Path(__file__).resolve().parent / "prompts" / "dabench_task_judge.md"))
    p.set_defaults(func=make_judge_packets)

    p = sub.add_parser("make-ground-truth-review-packets", help="Create Claude/GPT packets to review executed evidence and ground-truth correctness.")
    p.add_argument("--candidate-jsonl", required=True)
    p.add_argument("--output-jsonl", required=True)
    p.add_argument("--review-prompt", default=str(Path(__file__).resolve().parent / "prompts" / "dabench_ground_truth_reviewer.md"))
    p.set_defaults(func=make_ground_truth_review_packets)

    p = sub.add_parser("make-task-fit-review-packets", help="Create Claude/GPT packets to review task-type and difficulty fit against source signatures.")
    p.add_argument("--candidate-jsonl", required=True)
    p.add_argument("--output-jsonl", required=True)
    p.add_argument("--review-prompt", default=str(Path(__file__).resolve().parent / "prompts" / "dabench_task_fit_reviewer.md"))
    p.set_defaults(func=make_task_fit_review_packets)

    p = sub.add_parser("make-training-value-review-packets", help="Create external LLM review packets for training usefulness and shortcut risk.")
    p.add_argument("--candidate-jsonl", required=True)
    p.add_argument("--output-jsonl", required=True)
    p.add_argument("--review-prompt", default=str(Path(__file__).resolve().parent / "prompts" / "dabench_training_value_reviewer.md"))
    p.set_defaults(func=make_training_value_review_packets)

    p = sub.add_parser("merge-llm-judges", help="Merge external LLM judge JSONL output back into candidates.")
    p.add_argument("--candidate-jsonl", required=True)
    p.add_argument("--judge-jsonl", required=True)
    p.add_argument("--output-jsonl", required=True)
    p.add_argument("--dashboard", default="")
    p.add_argument("--min-score", type=float, default=0.72)
    p.set_defaults(func=merge_llm_judges)

    p = sub.add_parser("select-curriculum", help="Select a balanced accepted curriculum from judged candidates.")
    p.add_argument("--candidate-jsonl", required=True)
    p.add_argument("--output-jsonl", required=True)
    p.add_argument("--dashboard", default="")
    p.add_argument("--max-rows", type=int, default=0)
    p.add_argument("--per-dataset-cap", type=int, default=100)
    p.add_argument("--min-score", type=float, default=0.72)
    p.set_defaults(func=select_curriculum)

    p = sub.add_parser("select-training-ready", help="Select rows that pass judge, evidence execution, and optional ground-truth review gates.")
    p.add_argument("--candidate-jsonl", required=True)
    p.add_argument("--output-jsonl", required=True)
    p.add_argument("--review-jsonl", default="")
    p.add_argument("--task-fit-review-jsonl", default="")
    p.add_argument("--training-value-review-jsonl", default="")
    p.add_argument("--rejected-jsonl", default="")
    p.add_argument("--verified-pool-jsonl", default="", help="Optional output for executable/evidence-verified rows before strict task-fit gates.")
    p.add_argument("--warmup-pool-jsonl", default="", help="Optional output for verified rows with acceptable ground-truth/training-value signals, ignoring strict task-fit.")
    p.add_argument("--task-fit-pool-jsonl", default="", help="Optional output for verified rows that approximately pass relaxed task-fit checks.")
    p.add_argument("--dashboard", default="")
    p.add_argument("--min-local-score", type=float, default=0.72)
    p.add_argument("--min-review-score", type=float, default=0.72)
    p.add_argument("--min-task-fit-score", type=float, default=0.72)
    p.add_argument("--min-training-value-score", type=float, default=0.75)
    p.add_argument("--min-task-fit-pool-score", type=float, default=0.55)
    p.add_argument("--min-warmup-training-value-score", type=float, default=0.60)
    p.add_argument("--require-review", action="store_true")
    p.add_argument("--require-task-fit-review", action="store_true")
    p.add_argument("--require-training-value-review", action="store_true")
    p.add_argument("--allow-zero-or-empty", action="store_true")
    p.set_defaults(func=select_training_ready)

    p = sub.add_parser("audit-training-ready", help="Run final repeatable audit gates before VERL export.")
    p.add_argument("--candidate-jsonl", required=True)
    p.add_argument("--output-jsonl", required=True)
    p.add_argument("--rejected-jsonl", default="")
    p.add_argument("--dashboard", default="")
    p.add_argument("--bench-root", default=str(DEFAULT_BENCH_ROOT))
    p.add_argument("--query-row-limit", type=int, default=5000)
    p.add_argument("--python-timeout", type=int, default=120)
    p.add_argument("--repeat-runs", type=int, default=2)
    p.add_argument("--allow-python-exec", action="store_true", help="Opt in to executing candidate_solution.python after AST safety checks.")
    p.add_argument("--mongo-uri", default=os.environ.get("DAB_PIPELINE_MONGO_URI", "mongodb://127.0.0.1:27017/"))
    p.add_argument("--min-local-score", type=float, default=0.72)
    p.add_argument("--min-review-score", type=float, default=0.72)
    p.add_argument("--min-task-fit-score", type=float, default=0.72)
    p.add_argument("--min-training-value-score", type=float, default=0.75)
    p.add_argument("--min-ranking-winner-count", type=int, default=2)
    p.add_argument("--min-normalized-group-size", type=int, default=2)
    p.add_argument("--require-review", dest="require_review", action="store_true", default=True)
    p.add_argument("--no-require-review", dest="require_review", action="store_false")
    p.add_argument("--require-task-fit-review", dest="require_task_fit_review", action="store_true", default=True)
    p.add_argument("--no-require-task-fit-review", dest="require_task_fit_review", action="store_false")
    p.add_argument("--require-training-value-review", dest="require_training_value_review", action="store_true", default=False)
    p.add_argument("--no-require-training-value-review", dest="require_training_value_review", action="store_false")
    p.add_argument("--disable-anti-degenerate-checks", action="store_true")
    p.set_defaults(func=audit_training_ready)

    p = sub.add_parser("verify-evidence-chain", help="Execute candidate evidence queries and validate the observed final answer.")
    p.add_argument("--candidate-jsonl", required=True)
    p.add_argument("--output-jsonl", required=True)
    p.add_argument("--dashboard", default="")
    p.add_argument("--bench-root", default=str(DEFAULT_BENCH_ROOT))
    p.add_argument("--query-row-limit", type=int, default=5000)
    p.add_argument("--python-timeout", type=int, default=60)
    p.add_argument("--allow-python-exec", action="store_true", help="Opt in to executing candidate_solution.python after AST safety checks.")
    p.add_argument("--mongo-uri", default=os.environ.get("DAB_PIPELINE_MONGO_URI", "mongodb://127.0.0.1:27017/"))
    p.set_defaults(func=verify_evidence_chain)

    p = sub.add_parser("materialize-observed-ground-truth", help="Execute candidate solutions and rewrite expected_answer/validator_args from observed final answers.")
    p.add_argument("--candidate-jsonl", required=True)
    p.add_argument("--output-jsonl", required=True)
    p.add_argument("--bench-root", default=str(DEFAULT_BENCH_ROOT))
    p.add_argument("--query-row-limit", type=int, default=5000)
    p.add_argument("--python-timeout", type=int, default=60)
    p.add_argument("--allow-python-exec", action="store_true", help="Opt in to executing candidate_solution.python after AST safety checks.")
    p.add_argument("--mongo-uri", default=os.environ.get("DAB_PIPELINE_MONGO_URI", "mongodb://127.0.0.1:27017/"))
    p.set_defaults(func=materialize_observed_ground_truth)

    p = sub.add_parser("make-sandbox-manifest", help="Disabled by default; write sandbox registration manifest only when explicitly enabled.")
    p.add_argument("--candidate-jsonl", required=True)
    p.add_argument("--output-json", required=True)
    p.add_argument("--sandbox-url", default="http://127.0.0.1:8084")
    p.add_argument("--allow-sandbox-output", action="store_true")
    p.set_defaults(func=make_sandbox_manifest)

    p = sub.add_parser("ingest-dab-package", help="Import an existing DAB-style task package into the synthetic pipeline candidate/manifest shape.")
    p.add_argument("--input-root", required=True, help="Directory containing query_* DAB package folders.")
    p.add_argument("--output-dir", required=True)
    p.add_argument("--candidate-jsonl", default="")
    p.add_argument("--manifest-json", default="")
    p.add_argument("--report-json", default="")
    p.add_argument("--datasets", default="", help="Comma-separated dataset allowlist, with or without query_ prefix.")
    p.add_argument("--source-repo", default="")
    p.add_argument("--skip-db-type", action="append", default=[], help="Skip datasets containing this db_type, for example postgres.")
    p.add_argument("--self-test", dest="self_test", action="store_true", default=True)
    p.add_argument("--no-self-test", dest="self_test", action="store_false")
    p.add_argument("--require-validate-pass", dest="require_validate_pass", action="store_true", default=True)
    p.add_argument("--no-require-validate-pass", dest="require_validate_pass", action="store_false")
    p.add_argument("--leakage-check", dest="leakage_check", action="store_true", default=False)
    p.add_argument("--no-leakage-check", dest="leakage_check", action="store_false")
    p.add_argument("--require-nonempty-answer", dest="require_nonempty_answer", action="store_true", default=True)
    p.add_argument("--allow-empty-answer", dest="require_nonempty_answer", action="store_false")
    p.add_argument("--overwrite", action="store_true")
    p.set_defaults(func=ingest_external_dab_package)

    for command_name in ("export-sandbox-tasks", "export-dabench-tasks"):
        p = sub.add_parser(command_name, help="Export audited synthetic rows as DABench-style task directories with standalone validate.py files.")
        p.add_argument("--candidate-jsonl", required=True)
        p.add_argument("--output-dir", required=True)
        p.add_argument("--manifest-json", default="")
        p.add_argument("--bench-root", default=str(DEFAULT_BENCH_ROOT))
        p.add_argument("--dataset-prefix", default="synthetic_")
        p.add_argument("--require-final-audit", dest="require_final_audit", action="store_true", default=True)
        p.add_argument("--no-require-final-audit", dest="require_final_audit", action="store_false")
        p.add_argument("--self-test", dest="self_test", action="store_true", default=True)
        p.add_argument("--no-self-test", dest="self_test", action="store_false")
        p.add_argument("--copy-query-dataset", action="store_true", help="Copy source query_dataset instead of symlinking it into exported dataset dirs.")
        p.add_argument("--overwrite", action="store_true")
        p.set_defaults(func=export_sandbox_tasks)

    p = sub.add_parser("qwen-eval-filter", help="Split candidate tasks by Qwen eval outcome before VERL export.")
    p.add_argument("--candidate-jsonl", required=True)
    p.add_argument("--output-dir", required=True)
    p.add_argument("--task-manifest-json", default="", help="sandbox_task_manifest.json from export-dabench-tasks/export-sandbox-tasks. Recommended for synthetic rows.")
    p.add_argument("--bench-root", default=str(DEFAULT_BENCH_ROOT), help="Host DABench root used for fallback task identity.")
    p.add_argument("--eval-summary-csv", action="append", default=[], help="Qwen eval run_summary.csv. Can be repeated.")
    p.add_argument("--eval-summary-dir", action="append", default=[], help="Directory scanned recursively for Qwen eval run_summary.csv files.")
    p.add_argument("--eval-min-turns", type=int, default=50, help="Keep valid tasks whose max(llm_calls, tool_calls) is at least this value.")
    p.add_argument("--bad-empty-answer-turns", type=int, default=10, help="Classify invalid empty-answer runs shorter than this as bad/ambiguous instead of useful hard cases.")
    p.add_argument("--require-final-audit", action="store_true", help="Only keep rows with final_audit.passed=true before Qwen bucketing.")
    p.add_argument("--include-judge-rejected", action="store_true", help="Include rows whose local/LLM judge rejected them; default keeps only accepted rows when judge metadata exists.")
    p.add_argument("--include-bad-invalid-in-training-hard", action="store_true", help="Also include infra/ambiguous invalid rows in qwen_selected_training_hard.jsonl. Default keeps them separate.")
    p.set_defaults(func=qwen_eval_filter)

    p = sub.add_parser("build-verl", help="Build VERL-shaped JSONL/parquet from accepted candidates.")
    p.add_argument("--candidate-jsonl", required=True)
    p.add_argument("--output-dir", required=True)
    p.add_argument("--data-source", default="dab_sandbox")
    p.add_argument("--output-format", choices=["dab_sandbox", "synthetic"], default="dab_sandbox", help="dab_sandbox matches data/dab_parquet_all104; synthetic preserves the older validator-in-row format.")
    p.add_argument("--task-manifest-json", default="", help="sandbox_task_manifest.json from export-dabench-tasks/export-sandbox-tasks. Recommended for synthetic rows.")
    p.add_argument("--bench-root", default=str(DEFAULT_BENCH_ROOT), help="Host DABench root used for fallback runtime metadata.")
    p.add_argument("--runtime-bench-root", default="/workspace/DataAgentBench", help="Path seen inside the VERL/sandbox runtime.")
    p.add_argument("--sandbox-url", default="http://localhost:8080")
    p.add_argument("--run-root", default="")
    p.add_argument("--iterations", type=int, default=75)
    p.add_argument("--query-timeout", type=int, default=60)
    p.add_argument("--query-row-limit", type=int, default=5000)
    p.add_argument("--use-hints", dest="use_hints", action="store_true", default=True)
    p.add_argument("--no-hints", dest="use_hints", action="store_false")
    p.add_argument("--need-tools-kwargs", action="store_true", default=False)
    p.add_argument("--eval-summary-csv", action="append", default=[], help="Optional Qwen eval run_summary.csv. When provided, keep only invalid tasks or valid tasks with high turn counts.")
    p.add_argument("--eval-summary-dir", action="append", default=[], help="Optional directory scanned recursively for Qwen eval run_summary.csv files.")
    p.add_argument("--eval-min-turns", type=int, default=50, help="With eval summaries, keep valid tasks whose max(llm_calls, tool_calls) is at least this value.")
    p.add_argument("--task-artifacts-dir", default="task_artifacts", help="For --output-format dab_sandbox, copy per-task query/validator artifacts here under --output-dir.")
    p.add_argument("--no-task-artifacts", dest="write_task_artifacts", action="store_false", default=True, help="Disable the default dab_sandbox task_artifacts bundle.")
    p.add_argument("--max-test-rows", type=int, default=64, help="Only used with --output-format synthetic; dab_sandbox mirrors all rows into val/test like dab_parquet_all104.")
    p.add_argument("--require-final-audit", action="store_true")
    p.set_defaults(func=build_verl)

    return parser


def main() -> None:
    load_pipeline_env()
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
