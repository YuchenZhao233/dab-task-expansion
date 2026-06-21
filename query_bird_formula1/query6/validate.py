import re

EXPECTED_STRINGS = ["Sebastian Vettel"]
# Age in days at the final race of the 2010 season = 8535 days. Also accept the
# equivalent age in years (~23.4) to avoid false negatives on unit choice. The
# naive "youngest ever to lead the standings after any race" answer is a
# different driver (Lewis Hamilton, 8161 days) and fails the string check.
EXPECTED_DAYS = 8535


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
    age_ok = any(abs(v - EXPECTED_DAYS) <= 2 for v in vals) or any(
        abs(v - (EXPECTED_DAYS / 365.25)) <= 0.1 for v in vals
    )
    if not age_ok:
        missing.append(str(EXPECTED_DAYS))
    if missing:
        return False, "Missing expected value(s): " + ", ".join(missing)
    return True, "Found youngest driver to clinch the title and age in days."
