import re

EXPECTED_STRINGS = ["Mercedes"]
# Mean-of-season-means gap to pole = 0.4650 s. The naive pooled mean (no
# per-season normalization) is ~0.4738 and falls outside the tolerance, so a
# solver that skips per-season averaging gets the value wrong.
EXPECTED_NUMBER = 0.465
TOLERANCE = 0.004


def numbers_in(text: str):
    values = []
    for variant in (text.replace(",", " "), text.replace(",", "")):
        for raw in re.findall(r"\d+(?:\.\d+)?", variant):
            try:
                values.append(float(raw))
            except ValueError:
                pass
    return values


def validate(llm_output: str):
    text = llm_output.lower()
    missing = [s for s in EXPECTED_STRINGS if s.lower() not in text]
    if not any(abs(v - EXPECTED_NUMBER) <= TOLERANCE for v in numbers_in(llm_output)):
        missing.append(str(EXPECTED_NUMBER))
    if missing:
        return False, "Missing expected value(s): " + ", ".join(missing)
    return True, "Found most-competitive-in-qualifying constructor and mean gap."
