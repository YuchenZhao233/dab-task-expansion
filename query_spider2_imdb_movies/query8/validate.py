import re
import unicodedata

EXPECTED_ROWS = [['person-card:0838289', 'Peter Sullivan', '2', '2', '5.50', '4.35', '1.15'], ['person-card:1655252', 'Joel Paul Reisig', '2', '1', '4.85', '4.00', '0.85'], ['person-card:0784805', 'Giorgio Serafini', '2', '1', '3.95', '3.20', '0.75'], ['person-card:1729447', 'Onur Ünlü', '3', '1', '5.80', '5.20', '0.60'], ['person-card:1953143', 'Steven M. Smith', '2', '3', '2.90', '2.33', '0.57']]


def _norm(text):
    text = unicodedata.normalize("NFKD", str(text))
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return text.lower().replace("–", "-").replace("—", "-").replace("−", "-")


def _numbers(text):
    out = []
    for raw in re.findall(r"(?<![A-Za-z])[-+]?\d[\d,]*(?:\.\d+)?", text):
        try:
            out.append(float(raw.replace(",", "")))
        except ValueError:
            pass
    return out


def _contains_number(numbers, expected):
    try:
        target = float(str(expected).replace(",", ""))
    except ValueError:
        return False
    tol = 0.011 if abs(target - round(target)) > 1e-9 else 0.1
    return any(abs(value - target) <= tol for value in numbers)


def _contains_text_value(text, expected):
    expected_norm = _norm(expected)
    if "|" in expected:
        return all(_norm(part.strip()) in text for part in expected.split("|") if part.strip())
    return expected_norm in text


def validate(llm_output: str):
    text = _norm(llm_output)
    numbers = _numbers(llm_output)
    missing = []
    for row in EXPECTED_ROWS:
        for value in row:
            value_s = str(value)
            if re.fullmatch(r"[-+]?\d+(?:\.\d+)?", value_s):
                if not _contains_number(numbers, value_s):
                    missing.append(value_s)
            else:
                if not _contains_text_value(text, value_s):
                    missing.append(value_s)
    if missing:
        return False, "Missing expected value(s): " + ", ".join(missing[:10])
    return True, "Found all expected values."
