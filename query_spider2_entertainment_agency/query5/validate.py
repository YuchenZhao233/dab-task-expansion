import re
import unicodedata

EXPECTED_ROWS = [['December 2017', '17', '17275', '875', '0.05'], ['February 2018', '20', '21685', '950', '0.04'], ['January 2018', '28', '43880', '1240', '0.03']]


def _norm(text):
    text = unicodedata.normalize("NFKD", str(text))
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.lower().replace("&", " and ").replace("@", " at ").replace("'", "")
    text = text.replace("–", "-").replace("—", "-").replace("−", "-")
    text = re.sub(r"[^a-z0-9\s:./-]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _is_number(value):
    return re.fullmatch(r"[-+]?\d+(?:\.\d+)?", str(value).replace(",", "")) is not None


def _numbers(text):
    out = []
    pattern = r"(?<![A-Za-z0-9_.-])-?(?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?(?![A-Za-z0-9_.-])"
    for raw in re.findall(pattern, str(text)):
        try:
            out.append(float(raw.replace(",", "")))
        except ValueError:
            pass
    for raw in re.findall(r"(?<![A-Za-z0-9_.-])-?\d+(?:\.\d+)?(?![A-Za-z0-9_.-])", str(text).replace(",", " ")):
        try:
            out.append(float(raw))
        except ValueError:
            pass
    return out


def _contains_number(text, expected):
    target = float(str(expected).replace(",", ""))
    tolerance = 0.03 if abs(target - round(target)) > 1e-9 else 0.001
    values = _numbers(text)
    if any(abs(value - target) <= tolerance for value in values):
        return True
    if abs(target) <= 1:
        percent_target = target * 100
        return any(abs(value - percent_target) <= max(0.5, tolerance * 100) for value in values)
    return False


def _row_window(norm_output, row):
    text_values = [str(value).strip() for value in row if str(value).strip() and not _is_number(value)]
    if not text_values:
        return norm_output
    first = _norm(text_values[0])
    pos = norm_output.find(first)
    if pos < 0:
        return ""
    return norm_output[max(0, pos - 80):pos + 220]


def validate(llm_output: str):
    norm_output = _norm(llm_output)
    missing = []
    for row in EXPECTED_ROWS:
        window = _row_window(norm_output, row)
        for value in row:
            value = str(value).strip()
            if not value:
                continue
            if _is_number(value):
                if not _contains_number(llm_output, value):
                    missing.append(value)
            elif _norm(value) not in norm_output:
                missing.append(value)
    if missing:
        return False, "Missing expected value(s): " + ", ".join(missing[:10])
    return True, "Found expected values."
