import re

EXPECTED_STRINGS = ['Sports']
EXPECTED_NUMBERS = [4993.36]


def contains_number(text: str, expected: float, tolerance: float = 0.01) -> bool:
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
    if abs(exp - round(exp)) <= 1e-9:
        tol = 0.5
    else:
        tol = max(0.02, abs(exp) * 1e-6)
    return any(abs(value - exp) <= tol for value in values)


def validate(llm_output: str):
    text = llm_output.lower()
    missing = [s for s in EXPECTED_STRINGS if s.lower() not in text]
    missing += [str(n) for n in EXPECTED_NUMBERS if not contains_number(llm_output, n)]
    if missing:
        return False, "Missing expected value(s): " + ", ".join(missing)
    return True, "Found expected category and revenue."
