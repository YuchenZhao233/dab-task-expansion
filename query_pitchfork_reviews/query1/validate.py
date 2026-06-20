import re
EXPECTED_STRINGS = ["experimental"]
EXPECTED_NUMBERS = [7.34]
def contains_number(text, expected, tolerance=0.02):
    for raw in re.findall(r"\b[\d,]+(?:\.\d+)?\b", text):
        try:
            if abs(float(raw.replace(",", "")) - expected) <= tolerance: return True
        except ValueError: pass
    return False
def validate(llm_output):
    t = llm_output.lower()
    missing = [s for s in EXPECTED_STRINGS if s.lower() not in t]
    missing += [str(n) for n in EXPECTED_NUMBERS if not contains_number(llm_output, n)]
    if missing: return False, "Missing expected value(s): " + ", ".join(missing)
    return True, "Found expected genre and average score."
