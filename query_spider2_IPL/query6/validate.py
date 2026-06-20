import re


EXPECTED_STRINGS = [
    "A Symonds", "Deccan Chargers", "YK Pathan", "Rajasthan Royals",
    "SR Tendulkar", "Mumbai Indians", "SR Watson", "WP Saha",
    "Kings XI Punjab", "V Kohli", "Royal Challengers Bangalore",
    "SPD Smith", "Rising Pune Supergiants",
]
EXPECTED_NUMBERS = [335995, 117, 419112, 100, 501215, 598031, 101, 734054, 115, 980942, 980954]


def contains_number(text: str, expected: float, tolerance: float = 0.01) -> bool:
    thousands_re = re.compile(r"(?<![A-Za-z0-9_.-])-?(?:\d{1,3}(?:,\d{3})+)(?:\.\d+)?(?![A-Za-z0-9_.-])")
    values = []
    for raw in thousands_re.findall(text):
        try:
            values.append(float(raw.replace(",", "")))
        except ValueError:
            pass
    # Also accept IDs embedded in strings such as M-501252 or player::000056.
    for raw in re.findall(r"\d+(?:\.\d+)?", text.replace(",", " ")):
        try:
            values.append(float(raw))
        except ValueError:
            pass
    tol = 0.02 if abs(float(expected) - round(float(expected))) > 1e-9 else tolerance
    return any(abs(value - float(expected)) <= tol for value in values)


def validate(llm_output: str):
    text = llm_output.lower()
    missing = [value for value in EXPECTED_STRINGS if value.lower() not in text]
    missing.extend(str(value) for value in EXPECTED_NUMBERS if not contains_number(llm_output, value))
    if missing:
        return False, "Missing expected value(s): " + ", ".join(missing)
    return True, "Found expected losing-team centuries."
