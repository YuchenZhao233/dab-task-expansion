import csv
import re
from pathlib import Path


NUMBER_RE = re.compile(r"(?<![A-Za-z])[-+]?\d[\d,]*(?:\.\d+)?")


def _normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", value.strip().lower())


def _contains_number(text: str, expected: float, tolerance: float = 0.01) -> bool:
    for raw in NUMBER_RE.findall(text):
        try:
            if abs(float(raw.replace(",", "")) - expected) <= tolerance:
                return True
        except ValueError:
            pass
    return False


def _is_number(value: str) -> bool:
    try:
        float(value)
        return True
    except ValueError:
        return False


def _segments(text: str) -> list[str]:
    pieces = []
    pieces.extend(line for line in text.splitlines() if line.strip())
    pieces.extend(part for part in re.split(r"(?<=[.!?])\s+|\n+", text) if part.strip())
    return pieces


def _row_matches(output: str, row: dict[str, str]) -> tuple[bool, str]:
    text_values = []
    number_values = []
    for value in row.values():
        value = str(value).strip()
        if not value:
            continue
        if _is_number(value):
            number_values.append(float(value))
        else:
            text_values.append(value)

    normalized_output = _normalize_text(output)
    missing_text = [value for value in text_values if _normalize_text(value) not in normalized_output]
    if missing_text:
        return False, "Missing expected text value(s): " + ", ".join(missing_text)

    if text_values and number_values:
        normalized_text_values = [_normalize_text(value) for value in text_values]
        for segment in _segments(output):
            normalized_segment = _normalize_text(segment)
            if all(value in normalized_segment for value in normalized_text_values) and all(
                _contains_number(segment, number)
                for number in number_values
            ):
                return True, "Found expected row values in the same answer segment."
        return (
            False,
            "Expected text value(s) were present, but the expected numeric value(s) "
            "were not associated with them in the same line or sentence.",
        )

    missing_numbers = [
        str(number).rstrip("0").rstrip(".") if number % 1 else str(int(number))
        for number in number_values
        if not _contains_number(output, number)
    ]
    if missing_numbers:
        return False, "Missing expected numeric value(s): " + ", ".join(missing_numbers)
    return True, "Found expected row values."


def validate(llm_output: str):
    expected_path = Path(__file__).with_name("ground_truth.csv")
    with expected_path.open(newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    if not rows:
        return False, "Ground truth is empty."
    failures = []
    for row in rows:
        ok, reason = _row_matches(llm_output, row)
        if not ok:
            failures.append(reason)
    if failures:
        return False, " ".join(failures)
    return True, "Found expected row value(s)."

