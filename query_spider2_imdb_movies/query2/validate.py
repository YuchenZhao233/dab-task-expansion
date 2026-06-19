import re
import unicodedata

EXPECTED_ROWS = [['Drama|Action|Comedy', 'person-card:0751577', 'Anthony Russo', '2'], ['Drama|Action|Comedy', 'person-card:0003506', 'James Mangold', '2'], ['Drama|Action|Comedy', 'person-card:0751648', 'Joe Russo', '2'], ['Drama|Action|Comedy', 'person-card:2765738', 'Marianne Elliott', '2']]


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
