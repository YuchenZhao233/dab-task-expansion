import re
import unicodedata

EXPECTED_ROWS = [['strong', 'regression', '78'], ['soft', 'regression', '36']]
NUMBER_RE = re.compile(r"(?<![A-Za-z])[-+]?\d[\d,]*(?:\.\d+)?")


def _norm(text):
    text = unicodedata.normalize("NFKD", str(text))
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.lower().replace("–", "-").replace("—", "-").replace("−", "-")
    text = re.sub(r"\s*-\s*", "-", text)
    return re.sub(r"\s+", " ", text).strip()


def _is_number(value):
    return re.fullmatch(r"[-+]?\d+(?:\.\d+)?", str(value).replace(",", "")) is not None


def _numbers(text):
    out = []
    for raw in NUMBER_RE.findall(text):
        try:
            out.append(float(raw.replace(",", "")))
        except ValueError:
            pass
    return out


def _contains_number(text, expected):
    target = float(str(expected).replace(",", ""))
    tol = 0.011
    return any(abs(value - target) <= tol for value in _numbers(text))


def _find_positions(norm_text, value):
    value_norm = _norm(value)
    if not value_norm:
        return []
    return [match.start() for match in re.finditer(re.escape(value_norm), norm_text)]


def _anchor_info(row_values):
    if not row_values:
        return [], False
    first = str(row_values[0])
    if first.startswith("problem-card::"):
        anchors = [first]
        if len(row_values) > 1 and not _is_number(row_values[1]):
            anchors.append(str(row_values[1]))
        return anchors, True
    if first.startswith("run::") and "::stage-" in first:
        suffix = first.split("run::", 1)[1]
        return [first, "step-instance::" + suffix, "score::" + suffix], True
    if first.startswith("run::"):
        return [first], True
    if _is_number(first):
        return [first], False
    for value in row_values:
        if not _is_number(value):
            return [str(value)], False
    return [], False


def _row_anchor_values(row_values):
    anchors, _ = _anchor_info(row_values)
    return anchors


def _other_anchor_positions(norm_output, row_index):
    positions = []
    for index, row in enumerate(EXPECTED_ROWS):
        if index == row_index:
            continue
        for anchor in _row_anchor_values(row):
            positions.extend(_find_positions(norm_output, anchor))
    return sorted(set(positions))


def _window_for_anchor(norm_output, pos, other_positions):
    following = [other for other in other_positions if other > pos]
    end = following[0] if following else pos + 800
    start = pos
    return norm_output[start:min(len(norm_output), end)]


def _row_matches(output, row, row_index):
    norm_output = _norm(output)
    values = [str(value).strip() for value in row if str(value).strip()]
    anchors, row_keyed = _anchor_info(values)
    anchor_positions = []
    for anchor in anchors:
        anchor_positions.extend(_find_positions(norm_output, anchor))
    if not anchor_positions:
        return False, "Missing expected row anchor(s): " + ", ".join(anchors[:3])

    other_positions = _other_anchor_positions(norm_output, row_index)
    conflict_reasons = []
    for pos in sorted(set(anchor_positions)):
        window = _window_for_anchor(norm_output, pos, other_positions)
        text_values = [value for value in values if not _is_number(value)]
        if row_keyed:
            # The first value is a row/entity key. The second value is usually the
            # display name for that same entity and can be a typo-normalized alias
            # in model answers. The key/name anchors identity; bind remaining text
            # fields such as status or L1 family inside the row window.
            text_values = text_values[2:]
        missing_text = [value for value in text_values if _norm(value) not in window]
        if missing_text:
            conflict_reasons.append("missing text near anchor: " + ", ".join(missing_text))
            continue
        missing_numbers = [value for value in values if _is_number(value) and not _contains_number(window, value)]
        if missing_numbers:
            conflict_reasons.append("missing/conflicting numbers near anchor: " + ", ".join(missing_numbers))
            continue
        return True, "Found expected row values in a bounded window."
    return False, conflict_reasons[0] if conflict_reasons else "Expected row values were not associated closely enough."


def validate(llm_output: str):
    failures = []
    for index, row in enumerate(EXPECTED_ROWS):
        ok, reason = _row_matches(llm_output, row, index)
        if not ok:
            failures.append(reason)
    if failures:
        return False, " ".join(failures[:3])
    return True, "Found all expected rows with associated values."
