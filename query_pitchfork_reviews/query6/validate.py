import re
EXPECTED_STRINGS = []
EXPECTED_NUMBERS = [1990, 7.77]
def contains_number(text, expected, tolerance=0.02):
    tol = 0.5 if float(expected).is_integer() else tolerance
    for raw in re.findall(r"\b[\d,]+(?:\.\d+)?\b", text):
        try:
            if abs(float(raw.replace(",", "")) - expected) <= tol: return True
        except ValueError: pass
    return False
def validate(llm_output):
    t = llm_output.lower()
    missing = [s for s in EXPECTED_STRINGS if s.lower() not in t]
    missing += [str(n) for n in EXPECTED_NUMBERS if not contains_number(llm_output, n)]
    if missing: return False, "Missing expected value(s): " + ", ".join(missing)
    return True, "Found expected decade and average score."
