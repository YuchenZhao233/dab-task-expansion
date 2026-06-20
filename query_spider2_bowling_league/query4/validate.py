import re
import unicodedata

EXPECTED_ROWS = [['roster-team-004', 'Barracudas', 'captain-bowler-016', 'Richard Sheskey', '7', '21', '4', '188.75'], ['roster-team-001', 'Marlins', 'captain-bowler-002', 'David Fournier', '7', '21', '4', '181.00'], ['roster-team-005', 'Dolphins', 'captain-bowler-020', 'Suzanne Viescas', '7', '21', '3', '189.00'], ['roster-team-007', 'Manatees', 'captain-bowler-028', 'Michael Viescas', '7', '21', '3', '188.33'], ['roster-team-002', 'Sharks', 'captain-bowler-005', 'Ann Patterson', '7', '21', '2', '188.00']]


def _norm(text):
    text = unicodedata.normalize("NFKD", str(text))
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.lower().replace("&", " and ").replace("@", " at ")
    text = text.replace("–", "-").replace("—", "-").replace("−", "-")
    text = re.sub(r"[^a-z0-9\s:./-]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _is_number(value):
    return re.fullmatch(r"[-+]?\d+(?:\.\d+)?", str(value).replace(",", "")) is not None


def _numbers(text):
    out = []
    clean = str(text).replace(",", " ")
    for raw in re.findall(r"(?<![A-Za-z])[-+]?\d+(?:\.\d+)?", clean):
        try:
            out.append(float(raw))
        except ValueError:
            pass
    return out


def _contains_number(text, expected):
    target = float(str(expected).replace(",", ""))
    tolerance = 0.02 if abs(target - round(target)) > 1e-9 else 0.001
    return any(abs(value - target) <= tolerance for value in _numbers(text))


def _contains_date(norm_output, value):
    match = re.fullmatch(r"(\d{4})-(\d{2})-(\d{2})", str(value))
    if not match:
        return False
    if _norm(value) in norm_output:
        return True
    year, month, day = (int(part) for part in match.groups())
    numbers = _numbers(norm_output)
    return float(year) in numbers and float(month) in numbers and float(day) in numbers


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
            elif re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
                if not _contains_date(norm_output, value):
                    missing.append(value)
            elif _norm(value) not in norm_output:
                missing.append(value)
    if missing:
        return False, "Missing expected value(s): " + ", ".join(missing[:8])
    return True, "Found expected values."
