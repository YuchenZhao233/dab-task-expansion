import re

EXPECTED_STRINGS = ["luggage_accessories"]
EXPECTED_NUMBERS = [4.35]


def contains_number(text: str, expected: float, tolerance: float = 0.02) -> bool:
    for raw in re.findall(r"\b[\d,]+(?:\.\d+)?\b", text):
        try:
            if abs(float(raw.replace(",", "")) - expected) <= tolerance:
                return True
        except ValueError:
            pass
    return False


def validate(llm_output: str):
    text = llm_output.lower()
    missing = [s for s in EXPECTED_STRINGS if s.lower() not in text]
    missing.extend(str(n) for n in EXPECTED_NUMBERS if not contains_number(llm_output, n))
    if missing:
        return False, "Missing expected value(s): " + ", ".join(missing)
    return True, "Found expected category and average review score."
