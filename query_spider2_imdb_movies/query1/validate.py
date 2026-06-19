import re
import unicodedata

EXPECTED_ROWS = [['person-card:0744834', 'Eli Roth', '2', '6.20', '91852', '212'], ['person-card:0573732', 'Sean McNamara', '2', '6.25', '4001', '202'], ['person-card:5141259', 'Fabien Delage', '2', '6.90', '2407', '181'], ['person-card:3163561', 'Rene Perez', '2', '4.40', '1475', '164'], ['person-card:4335588', 'Jamie Patterson', '2', '4.95', '1177', '166']]


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
