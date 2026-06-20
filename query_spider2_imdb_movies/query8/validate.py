
EXPECTED_ROWS = [['person-card:0838289', 'Peter Sullivan', '2', '2', '5.50', '4.35', '1.15'], ['person-card:1655252', 'Joel Paul Reisig', '2', '1', '4.85', '4.00', '0.85'], ['person-card:0784805', 'Giorgio Serafini', '2', '1', '3.95', '3.20', '0.75'], ['person-card:1729447', 'Onur Ünlü', '3', '1', '5.80', '5.20', '0.60'], ['person-card:1953143', 'Steven M. Smith', '2', '3', '2.90', '2.33', '0.57']]
import re
import unicodedata

THOUSANDS_RE = re.compile(r"(?<![A-Za-z0-9_.-])-?(?:\d{1,3}(?:,\d{3})+)(?:\.\d+)?(?![A-Za-z0-9_.-])")
PLAIN_RE = re.compile(r"(?<![A-Za-z0-9_.-])-?\d+(?:\.\d+)?(?![A-Za-z0-9_.-])")


def _norm(text: str) -> str:
    text = unicodedata.normalize("NFKD", str(text))
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.lower().replace("&", " and ").replace("@", " at ")
    text = text.replace("–", "-").replace("—", "-").replace("−", "-")
    text = re.sub(r"[^a-z0-9\s:./-]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _is_number(value: str) -> bool:
    return re.fullmatch(r"[-+]?\d+(?:\.\d+)?", str(value).replace(",", "")) is not None


def _numbers(text: str) -> list[float]:
    values = []
    raw_text = str(text)
    for raw in THOUSANDS_RE.findall(raw_text):
        try:
            values.append(float(raw.replace(",", "")))
        except ValueError:
            pass
    comma_split = raw_text.replace(",", " ")
    for raw in PLAIN_RE.findall(comma_split):
        try:
            values.append(float(raw))
        except ValueError:
            pass
    return values


def _contains_number(text: str, expected: str) -> bool:
    target = float(str(expected).replace(",", ""))
    tolerance = 0.02 if abs(target - round(target)) > 1e-9 else 0.001
    return any(abs(value - target) <= tolerance for value in _numbers(text))


def _contains_date(text: str, value: str) -> bool:
    if _norm(value) in _norm(text):
        return True
    match = re.fullmatch(r"(\d{4})-(\d{2})(?:-(\d{2}))?", str(value))
    if not match:
        return False
    parts = [float(int(part)) for part in match.groups() if part is not None]
    date_text = str(text).replace("-", " ").replace("/", " ")
    nums = _numbers(date_text)
    return all(part in nums for part in parts)


def _contains_text(norm_output: str, value: str) -> bool:
    if "|" in value:
        return all(_norm(part.strip()) in norm_output for part in value.split("|") if part.strip())
    return _norm(value) in norm_output


def validate(llm_output: str):
    norm_output = _norm(llm_output)
    missing = []
    for row in EXPECTED_ROWS:
        for value in row:
            value = str(value).strip()
            if not value:
                continue
            if _is_number(value):
                if not _contains_number(llm_output, value):
                    missing.append(value)
            elif re.fullmatch(r"\d{4}-\d{2}(?:-\d{2})?", value):
                if not _contains_date(llm_output, value):
                    missing.append(value)
            elif not _contains_text(norm_output, value):
                missing.append(value)
    if missing:
        return False, "Missing expected value(s): " + ", ".join(missing[:10])
    return True, "Found expected value(s)."

