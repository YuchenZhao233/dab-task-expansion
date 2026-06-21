import re

EXPECTED_STRINGS = ['west Bohemia']
EXPECTED_NUMBERS = [58902.88]


def contains_number(text: str, expected: float) -> bool:
    thousands_re = re.compile(r"(?<![A-Za-z0-9_.-])-?(?:\d{1,3}(?:,\d{3})+)(?:\.\d+)?(?![A-Za-z0-9_.-])")
    values = []
    for raw in thousands_re.findall(text):
        try:
            values.append(float(raw.replace(",", "")))
        except ValueError:
            pass
    for raw in re.findall(r"\d+(?:\.\d+)?", text.replace(",", " ")):
        try:
            values.append(float(raw))
        except ValueError:
            pass
    exp = float(expected)
    is_int = abs(exp - round(exp)) <= 1e-9
    if is_int:
        tol = 0.5
    else:
        # Forbid false negatives: large monetary sums can differ by a few units
        # across DB engines; runner-up answers are far away, so a wide tolerance
        # is safe. Scale with magnitude but keep a floor for small percentages.
        tol = max(0.05, abs(exp) * 1e-5)
    return any(abs(value - exp) <= tol for value in values)


def validate(llm_output: str):
    text = llm_output.lower()
    missing = [s for s in EXPECTED_STRINGS if s.lower() not in text]
    missing += [str(n) for n in EXPECTED_NUMBERS if not contains_number(llm_output, n)]
    if missing:
        return False, "Missing expected value(s): " + ", ".join(missing)
    return True, "Found expected value(s)."
