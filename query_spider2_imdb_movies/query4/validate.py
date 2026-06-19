import re
import unicodedata

EXPECTED_ROWS = [['person-card:0000616', 'Eric Roberts', '4', '3.80'], ['person-card:0001744', 'Tom Sizemore', '3', '4.23'], ['person-card:0000185', 'Dolph Lundgren', '3', '4.10'], ['person-card:0865302', 'Tony Todd', '3', '3.23'], ['person-card:0000448', 'Lance Henriksen', '3', '3.03']]
NUMBER_RE = re.compile(r"(?<![A-Za-z])[-+]?\d[\d,]*(?:\.\d+)?")


def _norm(text):
    text = unicodedata.normalize("NFKD", str(text))
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.lower().replace("–", "-").replace("—", "-").replace("−", "-")
    text = re.sub(r"\s*-\s*", "-", text)
    return re.sub(r"\s+", " ", text).strip()


def _numbers(text):
    out = []
    for raw in NUMBER_RE.findall(text):
        try:
            out.append(float(raw.replace(",", "")))
        except ValueError:
            pass
    return out


def _is_number(value):
    return re.fullmatch(r"[-+]?\d+(?:\.\d+)?", str(value).replace(",", "")) is not None


def _contains_number(text, expected):
    target = float(str(expected).replace(",", ""))
    tol = 0.011 if abs(target - round(target)) > 1e-9 else 0.1
    return any(abs(value - target) <= tol for value in _numbers(text))


def _contains_text_value(norm_text, expected):
    if "|" in expected:
        return all(_norm(part.strip()) in norm_text for part in expected.split("|") if part.strip())
    return _norm(expected) in norm_text


def _find_positions(norm_text, value):
    value_norm = _norm(value)
    if not value_norm:
        return []
    return [match.start() for match in re.finditer(re.escape(value_norm), norm_text)]


def _row_anchor_values(row_values):
    # Numeric first columns, such as years, are usually the most stable row key.
    # Prefer them over repeated labels like "USA".
    if row_values and _is_number(row_values[0]):
        return [str(row_values[0])]
    anchors = []
    for value in row_values:
        value = str(value)
        if "|" in value:
            continue
        if not _is_number(value):
            anchors.append(value)
    return anchors


def _other_anchor_positions(norm_output, row_index):
    positions = []
    for index, row in enumerate(EXPECTED_ROWS):
        if index == row_index:
            continue
        for anchor in _row_anchor_values(row):
            positions.extend(_find_positions(norm_output, anchor))
    return sorted(set(positions))


def _window_for_anchor(output, norm_output, pos, other_positions):
    # Use the next expected-row anchor as a boundary when possible so one row
    # cannot borrow numeric values from a neighboring row in a table or list.
    following = [other for other in other_positions if other > pos]
    end = following[0] if following else pos + 700
    start = max(0, pos - 120)
    return norm_output[start:min(len(norm_output), end + 80)]


def _row_matches(output, row, row_index):
    norm_output = _norm(output)
    values = [str(value).strip() for value in row if str(value).strip()]
    global_set_values = [value for value in values if "|" in value]
    row_values = [value for value in values if "|" not in value]

    for value in global_set_values:
        if not _contains_text_value(norm_output, value):
            return False, f"Missing expected set value(s): {value}"

    anchors = _row_anchor_values(row_values)
    anchor_positions = []
    for anchor in anchors:
        anchor_positions.extend(_find_positions(norm_output, anchor))
    if not anchor_positions:
        return False, "Missing expected row anchor(s): " + ", ".join(anchors[:3])

    other_positions = _other_anchor_positions(norm_output, row_index)
    conflict_reasons = []
    for pos in sorted(set(anchor_positions)):
        window = _window_for_anchor(output, norm_output, pos, other_positions)
        missing_text = [
            value for value in row_values
            if not _is_number(value) and _norm(value) not in window
        ]
        if missing_text:
            conflict_reasons.append("missing text near anchor: " + ", ".join(missing_text))
            continue
        missing_numbers = [
            value for value in row_values
            if _is_number(value) and not _contains_number(window, value)
        ]
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
