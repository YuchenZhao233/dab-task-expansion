import re
EXPECTED_NUMBERS = [66]
def contains_number(text, expected, tolerance=0.5):
    for raw in re.findall(r"\b[\d,]+(?:\.\d+)?\b", text):
        try:
            if abs(float(raw.replace(",", "")) - expected) <= tolerance: return True
        except ValueError: pass
    return False
def validate(llm_output):
    missing = [str(n) for n in EXPECTED_NUMBERS if not contains_number(llm_output, n)]
    if missing: return False, "Missing expected value(s): " + ", ".join(missing)
    return True, "Found expected count."
