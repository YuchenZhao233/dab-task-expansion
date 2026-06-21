import re

# Gold: weekly frequency (code POPLATEK TYDNE) has the highest loan-uptake rate,
# 37.92%. The frequency must be identified as the weekly cohort; accept either the
# Czech code or the word "weekly". The naive raw-count answer (monthly /
# POPLATEK MESICNE) is excluded because that code/word will not appear.
EXPECTED_NUMBERS = [37.92]
# At least one of these tokens must be present to name the weekly cohort.
FREQUENCY_TOKENS = ['tydne', 'weekly']


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
        tol = max(0.05, abs(exp) * 1e-5)
    return any(abs(value - exp) <= tol for value in values)


def validate(llm_output: str):
    text = llm_output.lower()
    missing = []
    if not any(tok in text for tok in FREQUENCY_TOKENS):
        missing.append("weekly frequency (POPLATEK TYDNE)")
    missing += [str(n) for n in EXPECTED_NUMBERS if not contains_number(llm_output, n)]
    if missing:
        return False, "Missing expected value(s): " + ", ".join(missing)
    return True, "Found expected value(s)."
