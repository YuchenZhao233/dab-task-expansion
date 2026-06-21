import re
import unicodedata

EXPECTED_ROWS = [['Rhythm and Blues', '7', '2', '5', '0'], ['Show Tunes', '5', '1', '4', '4345'], ['Jazz', '8', '4', '4', '5480'], ['Standards', '10', '6', '4', '22630'], ["40's Ballroom Music", '3', '0', '3', '0'], ['Chamber Music', '3', '0', '3', '0'], ['Modern Rock', '3', '0', '3', '0'], ['Classical', '3', '0', '3', '2670'], ['Contemporary', '7', '4', '3', '15070'], ['Classic Rock & Roll', '5', '2', '3', '17150']]


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
