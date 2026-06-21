import re

EXPECTED_STRINGS = ['Blue']
EXPECTED_NUMBERS = [64]


def contains_number(text: str, expected: float, tolerance: float = 0.01) -> bool:
    values = []
    for raw in re.findall(r"\d+(?:\.\d+)?", text.replace(",", " ")):
        try:
            values.append(float(raw))
        except ValueError:
            pass
    exp = float(expected)
    is_int = abs(exp - round(exp)) <= 1e-9
    tol = 0.5 if is_int else max(0.02, abs(exp) * 1e-4)
    return any(abs(v - exp) <= tol for v in values)


def validate(llm_output: str):
    text = llm_output.lower()
    missing = [s for s in EXPECTED_STRINGS if s.lower() not in text]
    missing += [str(n) for n in EXPECTED_NUMBERS if not contains_number(llm_output, n)]
    if missing:
        return False, "Missing expected value(s): " + ", ".join(missing)
    return True, 'Found expected eye colour and flying-hero count.'
