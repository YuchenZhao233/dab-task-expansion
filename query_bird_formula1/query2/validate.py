import re

EXPECTED_STRINGS = ["Jordan"]
# Crash-retirement rate 87/500 = 0.174. Accept the rate as a fraction (0.174)
# or as a percentage (17.4). The naive Accident+Collision-only answer is a
# different constructor (Force India, ~0.105) and fails the string check.
EXPECTED_RATE = 0.174
RATE_TOL = 0.005
EXPECTED_PCT = 17.4
PCT_TOL = 0.5


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
    vals = numbers_in(llm_output)
    rate_ok = any(abs(v - EXPECTED_RATE) <= RATE_TOL for v in vals) or any(
        abs(v - EXPECTED_PCT) <= PCT_TOL for v in vals
    )
    if not rate_ok:
        missing.append(str(EXPECTED_RATE))
    if missing:
        return False, "Missing expected value(s): " + ", ".join(missing)
    return True, "Found highest crash-retirement-rate constructor and rate."
