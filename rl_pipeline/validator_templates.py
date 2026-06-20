"""Reusable validator templates for DABench synthetic task specs.

The data pipeline should ask LLMs to choose one template and fill arguments,
not to write arbitrary validator code. These templates mirror common patterns in
DataAgentBench validate.py files: normalized string contains, numeric tolerance,
ordered/list matching, JSON object checks, and value proximity near entity names.
"""
from __future__ import annotations

import json
import re
from typing import Any

TEMPLATE_NAMES = {
    "contains_all",
    "normalized_contains_all",
    "numeric_tolerance",
    "numeric_list_tolerance",
    "ordered_contains",
    "unordered_set_contains",
    "json_exact_fields",
    "name_value_proximity",
}


def normalize_text(text: str) -> str:
    text = re.sub(r"(?<=\d),(?=\d{3}\b)", "", str(text))
    text = text.lower().replace("&", " and ").replace("@", " at ")
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def extract_numbers(text: str) -> list[float]:
    values: list[float] = []
    clean = re.sub(r"(?<=\d),(?=\d{3}\b)", "", str(text))
    for raw in re.findall(r"(?<![\w.-])-?\d+(?:\.\d+)?(?![\w.-])", clean):
        try:
            values.append(float(raw))
        except ValueError:
            pass
    return values


def validate_with_template(output: str, template: str, args: dict[str, Any]) -> tuple[bool, str]:
    if template == "contains_all":
        items = [str(x) for x in args.get("items", [])]
        case_sensitive = bool(args.get("case_sensitive", False))
        haystack = output if case_sensitive else output.lower()
        for item in items:
            needle = item if case_sensitive else item.lower()
            if needle not in haystack:
                return False, f"missing item: {item}"
        return True, "all items present"

    if template == "normalized_contains_all":
        items = [str(x) for x in args.get("items", [])]
        haystack = normalize_text(output)
        for item in items:
            if normalize_text(item) not in haystack:
                return False, f"missing normalized item: {item}"
        return True, "all normalized items present"

    if template == "numeric_tolerance":
        expected = float(args["expected"])
        tolerance = float(args.get("tolerance", 1e-6))
        for value in extract_numbers(output):
            if abs(value - expected) <= tolerance:
                return True, f"matched {value}"
        return False, f"expected numeric value {expected} within tolerance {tolerance}"

    if template == "numeric_list_tolerance":
        expected_values = [float(x) for x in args.get("expected", [])]
        tolerance = float(args.get("tolerance", 1e-6))
        found = extract_numbers(output)
        for expected in expected_values:
            if not any(abs(value - expected) <= tolerance for value in found):
                return False, f"missing numeric value {expected} within tolerance {tolerance}"
        return True, "all numeric values matched"

    if template == "ordered_contains":
        items = [str(x) for x in args.get("items", [])]
        haystack = normalize_text(output)
        pos = -1
        for item in items:
            idx = haystack.find(normalize_text(item), pos + 1)
            if idx < 0:
                return False, f"missing or out-of-order item: {item}"
            pos = idx
        return True, "ordered items present"

    if template == "unordered_set_contains":
        items = [str(x) for x in args.get("items", [])]
        haystack = normalize_text(output)
        missing = [item for item in items if normalize_text(item) not in haystack]
        if missing:
            return False, f"missing set items: {missing}"
        return True, "set items present"

    if template == "json_exact_fields":
        expected = args.get("expected", {})
        if not isinstance(expected, dict) or not expected:
            return False, "expected must be nonempty object"
        try:
            obj = json.loads(output.strip())
        except Exception as exc:
            return False, f"invalid json output: {exc}"
        if not isinstance(obj, dict):
            return False, "output is not a json object"
        tolerance = float(args.get("numeric_tolerance", 1e-6))
        for key, expected_value in expected.items():
            if key not in obj:
                return False, f"missing json key: {key}"
            actual = obj[key]
            if isinstance(expected_value, (int, float)):
                try:
                    if abs(float(actual) - float(expected_value)) > tolerance:
                        return False, f"json key {key} numeric mismatch"
                except Exception:
                    return False, f"json key {key} is not numeric"
            elif normalize_text(str(actual)) != normalize_text(str(expected_value)):
                return False, f"json key {key} mismatch"
        return True, "json fields matched"

    if template == "name_value_proximity":
        pairs = args.get("pairs", [])
        window = int(args.get("window", 150))
        norm_output = normalize_text(output)
        for pair in pairs:
            name = str(pair.get("name", ""))
            value = pair.get("value")
            norm_name = normalize_text(name)
            idx = norm_output.find(norm_name)
            if idx < 0:
                return False, f"missing name: {name}"
            raw_idx = max(0, output.lower().find(name.lower()))
            raw_window = output[max(0, raw_idx - window): raw_idx + len(name) + window]
            if isinstance(value, (int, float)):
                tol = float(pair.get("tolerance", args.get("tolerance", 1e-6)))
                if not any(abs(num - float(value)) <= tol for num in extract_numbers(raw_window)):
                    return False, f"numeric value {value} not near {name}"
            elif normalize_text(str(value)) not in normalize_text(raw_window):
                return False, f"value {value} not near {name}"
        return True, "all name/value pairs matched"

    return False, f"unknown validator template: {template}"


def validate_template_spec(template: str, args: dict[str, Any]) -> list[str]:
    risks: list[str] = []
    if template not in TEMPLATE_NAMES:
        return ["unknown_validator_template"]
    if not isinstance(args, dict):
        return ["validator_args_not_object"]
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
