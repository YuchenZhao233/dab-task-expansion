import re

EXPECTED_STRINGS = ["Mercedes"]
EXPECTED_NUMBER = 23.287
TOLERANCE = 0.05


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
    return True, "Found lowest-average green-flag pit-stop constructor and duration."
