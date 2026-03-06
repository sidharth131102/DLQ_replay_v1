import json
import re
import hashlib
from decimal import Decimal, InvalidOperation
from datetime import datetime, timezone
from typing import Any

from google.cloud import bigquery

INT64_MIN = -(2**63)
INT64_MAX = 2**63 - 1
NUMERIC_MAX = Decimal("99999999999999999999999999999.999999999")
NUMERIC_MIN = -NUMERIC_MAX
BIGNUMERIC_MAX = Decimal("578960446186580977117854925043439539266.34992332820282019728792003956564819967")
BIGNUMERIC_MIN = -BIGNUMERIC_MAX


def _try_parse_timestamp(value: str) -> str | None:
    candidate = value.strip()
    if not candidate:
        return None
    try:
        if candidate.endswith("Z"):
            dt = datetime.fromisoformat(candidate.replace("Z", "+00:00"))
        else:
            dt = datetime.fromisoformat(candidate)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    except ValueError:
        return None


def _try_parse_date_like(value: str) -> str | None:
    candidate = value.strip()
    if not candidate:
        return None
    try:
        return datetime.strptime(candidate, "%Y-%m-%d").date().isoformat()
    except ValueError:
        pass

    ts = _try_parse_timestamp(candidate)
    if ts:
        try:
            return datetime.fromisoformat(ts.replace("Z", "+00:00")).date().isoformat()
        except ValueError:
            return None
    return None


def _try_parse_datetime_like(value: str) -> str | None:
    candidate = value.strip()
    if not candidate:
        return None
    try:
        if candidate.endswith("Z"):
            dt = datetime.fromisoformat(candidate.replace("Z", "+00:00"))
        else:
            dt = datetime.fromisoformat(candidate)
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt.isoformat(sep="T")
    except ValueError:
        return None


def _try_parse_time_like(value: str) -> str | None:
    candidate = value.strip()
    if not candidate:
        return None
    for fmt in ("%H:%M:%S", "%H:%M:%S.%f"):
        try:
            return datetime.strptime(candidate, fmt).time().isoformat()
        except ValueError:
            continue
    return None


def _is_ambiguous_temporal_text(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    candidate = value.strip().lower()
    if not candidate:
        return False
    tokens = re.findall(r"[a-z]+", candidate)
    if not tokens:
        return False
    allowed = {"t", "z", "utc"}
    return any(tok not in allowed for tok in tokens)


def _failed_timestamp_to_iso(failed_timestamp: Any) -> str | None:
    if failed_timestamp is None:
        return None
    if isinstance(failed_timestamp, datetime):
        dt = failed_timestamp
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(failed_timestamp, str):
        return _try_parse_timestamp(failed_timestamp)
    return None


def _build_replay_event_id(payload: dict[str, Any], failed_ts_iso: str | None) -> str:
    # Deterministic suffix preserves idempotency across replay attempts.
    base = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    seed = f"{base}|{failed_ts_iso or ''}"
    digest = hashlib.sha1(seed.encode("utf-8")).hexdigest()[:12]
    return f"replay_event_{digest}"


def _try_parse_json_object(value: str) -> dict[str, Any] | None:
    try:
        parsed = json.loads(value)
        if isinstance(parsed, dict):
            return parsed
        return None
    except json.JSONDecodeError:
        return None


def _try_parse_json_object_lenient(value: str) -> tuple[dict[str, Any] | None, str | None]:
    parsed = _try_parse_json_object(value)
    if parsed is not None:
        return parsed, None

    s = value.strip()
    candidates: list[tuple[str, str]] = []

    # 1) Unwrap one quoted layer and unescape common JSON escape sequences.
    if len(s) >= 2 and s[0] == '"' and s[-1] == '"':
        unwrapped = s[1:-1].replace('\\"', '"').replace("\\\\", "\\")
        candidates.append((unwrapped, "normalized_wrapped_json_string"))

    # 2) Remove trailing commas before object/array closers.
    no_trailing = re.sub(r",\s*([}\]])", r"\1", s)
    if no_trailing != s:
        candidates.append((no_trailing, "removed_trailing_commas"))

    # 3) If JSON-like but using only single quotes, convert to double quotes.
    if "{" in s and "}" in s and "'" in s and '"' not in s:
        candidates.append((s.replace("'", '"'), "replaced_single_quotes_json"))

    # 4) Combined common cleanup.
    if len(s) >= 2 and s[0] == '"' and s[-1] == '"':
        combined = s[1:-1].replace('\\"', '"').replace("\\\\", "\\")
        combined = re.sub(r",\s*([}\]])", r"\1", combined)
        candidates.append((combined, "normalized_wrapped_json_and_removed_trailing_commas"))

    seen: set[str] = set()
    for candidate, note in candidates:
        if candidate in seen:
            continue
        seen.add(candidate)
        parsed = _try_parse_json_object(candidate)
        if parsed is not None:
            return parsed, note

    return None, None


def _default_for_field(
    field_name: str,
    field: bigquery.SchemaField,
    payload: dict[str, Any],
    failed_ts_iso: str | None,
) -> tuple[Any, str] | tuple[None, None]:
    if field_name == "event_id":
        return _build_replay_event_id(payload, failed_ts_iso), "filled_missing_event_id_with_replay_event_id"

    if field.field_type == "TIMESTAMP" and failed_ts_iso:
        return failed_ts_iso, f"filled_missing_{field_name}_from_failed_timestamp"

    if field.field_type == "STRING" and field.mode != "REQUIRED":
        return "", f"filled_missing_{field_name}_with_default_empty_string"

    if field.mode != "REQUIRED":
        return None, f"filled_missing_{field_name}_with_null"

    return None, None


def _extract_missing_fields(error_details: Any) -> list[str]:
    if not isinstance(error_details, str) or not error_details.strip():
        return []
    parsed = _try_parse_json_object(error_details)
    if not parsed:
        return []
    fields = parsed.get("missing_fields")
    if not isinstance(fields, list):
        return []
    return [f for f in fields if isinstance(f, str) and f.strip()]


def _extract_partition_column(error_details: Any) -> str | None:
    if isinstance(error_details, dict):
        value = error_details.get("partition_column")
        return str(value).strip() if value else None
    if not isinstance(error_details, str) or not error_details.strip():
        return None
    parsed = _try_parse_json_object(error_details)
    if not parsed:
        return None
    value = parsed.get("partition_column")
    return str(value).strip() if value else None


def _coerce_repeated_scalar_element(
    value: Any,
    field_type: str,
    failed_ts_iso: str | None,
) -> tuple[Any, bool]:
    if value is None:
        return None, True

    if field_type in {"INT64", "INTEGER"}:
        if isinstance(value, bool):
            return None, False
        if isinstance(value, int):
            return (value, INT64_MIN <= value <= INT64_MAX)
        if isinstance(value, float):
            as_int = int(value)
            return (as_int, INT64_MIN <= as_int <= INT64_MAX)
        if isinstance(value, str):
            parsed = _try_parse_int_like(value)
            return (parsed, parsed is not None)
        return None, False

    if field_type in {"FLOAT64", "FLOAT"}:
        if isinstance(value, bool):
            return None, False
        if isinstance(value, (int, float)):
            parsed = float(value)
            if parsed in {float("inf"), float("-inf")} or parsed != parsed:
                return None, False
            return parsed, True
        if isinstance(value, str):
            parsed = _try_parse_float_like(value)
            return (parsed, parsed is not None)
        return None, False

    if field_type == "NUMERIC":
        if isinstance(value, (int, float, Decimal)):
            parsed = _try_parse_numeric_like(str(value))
            return (str(parsed), parsed is not None) if parsed is not None else (None, False)
        if isinstance(value, str):
            parsed = _try_parse_numeric_like(value)
            return (str(parsed), parsed is not None) if parsed is not None else (None, False)
        return None, False

    if field_type == "BIGNUMERIC":
        if isinstance(value, (int, float, Decimal)):
            parsed = _try_parse_bignumeric_like(str(value))
            return (str(parsed), parsed is not None) if parsed is not None else (None, False)
        if isinstance(value, str):
            parsed = _try_parse_bignumeric_like(value)
            return (str(parsed), parsed is not None) if parsed is not None else (None, False)
        return None, False

    if field_type in {"BOOL", "BOOLEAN"}:
        if isinstance(value, bool):
            return value, True
        if isinstance(value, str):
            parsed = _try_parse_bool_like(value)
            return (parsed, parsed is not None)
        return None, False

    if field_type == "TIMESTAMP":
        if isinstance(value, bool):
            return None, False
        if isinstance(value, str):
            parsed = _try_parse_timestamp(value)
            if parsed is not None:
                return parsed, True
            if failed_ts_iso:
                return failed_ts_iso, True
            return None, False
        if failed_ts_iso:
            return failed_ts_iso, True
        return None, False

    if field_type == "DATE":
        if isinstance(value, str):
            parsed = _try_parse_date_like(value)
            return (parsed, parsed is not None)
        return None, False

    if field_type == "DATETIME":
        if isinstance(value, str):
            parsed = _try_parse_datetime_like(value)
            return (parsed, parsed is not None)
        return None, False

    if field_type == "TIME":
        if isinstance(value, str):
            parsed = _try_parse_time_like(value)
            return (parsed, parsed is not None)
        return None, False

    if field_type in {"STRING", "JSON", "BYTES"}:
        if isinstance(value, (dict, list)):
            return json.dumps(value, separators=(",", ":"), ensure_ascii=False), True
        if isinstance(value, bool):
            return ("true" if value else "false"), True
        return str(value), True

    # Unknown scalar types: keep as-is and let BigQuery validation decide.
    return value, True


def _extract_bad_value_from_error_message(error_message: str) -> str | None:
    patterns = [
        r"bad value\):\s*(.+)$",
        r"Invalid (?:INT64|FLOAT64|NUMERIC|BIGNUMERIC|BOOL|BOOLEAN|TIMESTAMP|DATE|DATETIME) value:\s*(.+)$",
    ]
    for pattern in patterns:
        m = re.search(pattern, error_message, flags=re.IGNORECASE)
        if m:
            return m.group(1).strip().strip("'\"")
    return None


def _normalize_scalar(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value).strip()


def _extract_numeric_token(value: str) -> str | None:
    cleaned = value.strip().replace(",", "")
    cleaned = re.sub(r"^\$", "", cleaned)
    cleaned = re.sub(r"\b(?:usd|dollar|dollars|eur|inr|rupee|rupees)\b", " ", cleaned, flags=re.IGNORECASE)
    numeric_pattern = r"[-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?"
    if re.fullmatch(numeric_pattern, cleaned):
        return cleaned
    token_match = re.search(numeric_pattern, cleaned)
    if token_match:
        return token_match.group(0)
    parsed_words = _try_parse_number_words(cleaned)
    if parsed_words is not None:
        return str(parsed_words)
    return None


def _try_parse_decimal_token(value: str) -> Decimal | None:
    token = _extract_numeric_token(value)
    if token is None:
        return None
    try:
        parsed = Decimal(token)
    except InvalidOperation:
        return None
    if not parsed.is_finite():
        return None
    return parsed


def _decimal_scale(value: Decimal) -> int:
    exponent = value.as_tuple().exponent
    return -exponent if exponent < 0 else 0


def _try_parse_numeric_like(value: str) -> Decimal | None:
    parsed = _try_parse_decimal_token(value)
    if parsed is None:
        return None
    if parsed < NUMERIC_MIN or parsed > NUMERIC_MAX:
        return None
    if _decimal_scale(parsed) > 9:
        return None
    return parsed


def _try_parse_bignumeric_like(value: str) -> Decimal | None:
    parsed = _try_parse_decimal_token(value)
    if parsed is None:
        return None
    if parsed < BIGNUMERIC_MIN or parsed > BIGNUMERIC_MAX:
        return None
    if _decimal_scale(parsed) > 38:
        return None
    return parsed


def _try_parse_int_like(value: str) -> int | None:
    cleaned = value.strip().replace(",", "")
    cleaned = re.sub(r"\b(?:usd|dollar|dollars|eur|inr|rupee|rupees)\b", " ", cleaned, flags=re.IGNORECASE)
    if re.fullmatch(r"[-+]?\d+", cleaned):
        parsed = int(cleaned)
        return parsed if INT64_MIN <= parsed <= INT64_MAX else None
    if re.fullmatch(r"[-+]?\d+\.0+", cleaned):
        parsed = int(float(cleaned))
        return parsed if INT64_MIN <= parsed <= INT64_MAX else None
    if re.fullmatch(r"[-+]?\d+\.\d+", cleaned):
        parsed = int(float(cleaned))
        return parsed if INT64_MIN <= parsed <= INT64_MAX else None
    # If noisy text remains (e.g. "$123@%"), extract first integer-like token.
    token_match = re.search(r"[-+]?\d+(?:\.\d+)?", cleaned)
    if token_match:
        token = token_match.group(0)
        if re.fullmatch(r"[-+]?\d+", token):
            parsed = int(token)
            return parsed if INT64_MIN <= parsed <= INT64_MAX else None
        if re.fullmatch(r"[-+]?\d+\.0+", token):
            parsed = int(float(token))
            return parsed if INT64_MIN <= parsed <= INT64_MAX else None
        if re.fullmatch(r"[-+]?\d+\.\d+", token):
            parsed = int(float(token))
            return parsed if INT64_MIN <= parsed <= INT64_MAX else None
    parsed_words = _try_parse_number_words(cleaned)
    if parsed_words is not None:
        parsed = int(parsed_words)
        return parsed if INT64_MIN <= parsed <= INT64_MAX else None
    return None


def _try_parse_float_like(value: str) -> float | None:
    token = _extract_numeric_token(value)
    if token is None:
        return None
    try:
        parsed = float(token)
        if parsed in {float("inf"), float("-inf")}:
            return None
        if parsed != parsed:  # NaN guard
            return None
        return parsed
    except ValueError:
        return None


def _try_parse_bool_like(value: str) -> bool | None:
    lowered = value.strip().lower()
    if lowered in {"true", "1", "t", "yes", "y"}:
        return True
    if lowered in {"false", "0", "f", "no", "n"}:
        return False
    return None


def _try_parse_number_words(value: str) -> int | None:
    s = value.lower()
    # Strip simple currency/unit words while preserving numeric words.
    s = re.sub(r"\b(dollar|dollars|usd|eur|inr|rupees|rupee|only)\b", " ", s)
    s = s.replace("-", " ")
    s = re.sub(r"[^a-z\s]", " ", s)
    tokens = [t for t in s.split() if t and t != "and"]
    if not tokens:
        return None

    units = {
        "zero": 0,
        "one": 1,
        "two": 2,
        "three": 3,
        "four": 4,
        "five": 5,
        "six": 6,
        "seven": 7,
        "eight": 8,
        "nine": 9,
        "ten": 10,
        "eleven": 11,
        "twelve": 12,
        "thirteen": 13,
        "fourteen": 14,
        "fifteen": 15,
        "sixteen": 16,
        "seventeen": 17,
        "eighteen": 18,
        "nineteen": 19,
    }
    tens = {
        "twenty": 20,
        "thirty": 30,
        "forty": 40,
        "fifty": 50,
        "sixty": 60,
        "seventy": 70,
        "eighty": 80,
        "ninety": 90,
    }
    scales = {
        "hundred": 100,
        "thousand": 1000,
        "million": 1_000_000,
        "billion": 1_000_000_000,
    }

    total = 0
    current = 0
    seen = False
    for tok in tokens:
        if tok in units:
            current += units[tok]
            seen = True
        elif tok in tens:
            current += tens[tok]
            seen = True
        elif tok == "hundred":
            if current == 0:
                return None
            current *= 100
            seen = True
        elif tok in {"thousand", "million", "billion"}:
            if current == 0:
                return None
            total += current * scales[tok]
            current = 0
            seen = True
        else:
            return None

    if not seen:
        return None
    return total + current


def _sanitize_text(value: str) -> str:
    # Remove control chars except common whitespace and drop surrogate code points.
    return "".join(
        ch
        for ch in value
        if ((ord(ch) >= 32 or ch in "\t\n\r") and not (0xD800 <= ord(ch) <= 0xDFFF))
    )


def _sanitize_value_recursive(value: Any) -> Any:
    if isinstance(value, str):
        return _sanitize_text(value)
    if isinstance(value, list):
        return [_sanitize_value_recursive(v) for v in value]
    if isinstance(value, dict):
        return {k: _sanitize_value_recursive(v) for k, v in value.items()}
    return value


def _failed_timestamp_to_date(failed_ts_iso: str | None) -> str | None:
    if not failed_ts_iso:
        return None
    try:
        return datetime.fromisoformat(failed_ts_iso.replace("Z", "+00:00")).date().isoformat()
    except ValueError:
        return None


def _max_nesting_depth(value: Any, current_depth: int = 0) -> int:
    if isinstance(value, dict):
        if not value:
            return current_depth + 1
        return max(_max_nesting_depth(v, current_depth + 1) for v in value.values())
    if isinstance(value, list):
        if not value:
            return current_depth + 1
        return max(_max_nesting_depth(v, current_depth + 1) for v in value)
    return current_depth


def _collect_missing_required_nested_fields(
    value: Any,
    field: bigquery.SchemaField,
    path_prefix: str,
) -> list[str]:
    if field.field_type != "RECORD" or not isinstance(value, dict):
        return []

    missing: list[str] = []
    for child in field.fields:
        child_path = f"{path_prefix}.{child.name}" if path_prefix else child.name
        child_value = value.get(child.name)

        if child.mode == "REQUIRED" and (child.name not in value or child_value in (None, "")):
            missing.append(child_path)
            continue

        if child.field_type == "RECORD":
            if child.mode == "REPEATED":
                if isinstance(child_value, list):
                    for idx, item in enumerate(child_value):
                        if isinstance(item, dict):
                            missing.extend(
                                _collect_missing_required_nested_fields(
                                    item,
                                    child,
                                    f"{child_path}[{idx}]",
                                )
                            )
            elif isinstance(child_value, dict):
                missing.extend(_collect_missing_required_nested_fields(child_value, child, child_path))

    return missing


def apply_deterministic_fixes(
    payload: dict[str, Any],
    error_type: str | None,
    schema_by_name: dict[str, bigquery.SchemaField],
    failed_timestamp: Any = None,
    error_details: Any = None,
    error_message: str | None = None,
) -> tuple[dict[str, Any], list[str]]:
    original_payload = dict(payload)
    fixed = _sanitize_value_recursive(dict(payload))
    notes: list[str] = []
    err = (error_type or "").upper()
    err_msg = (error_message or "").strip()
    err_msg_lower = err_msg.lower()
    failed_ts_iso = _failed_timestamp_to_iso(failed_timestamp)
    failed_date_iso = _failed_timestamp_to_date(failed_ts_iso)
    notes.append("sanitized_text_fields")
    payload_event_type = str(fixed.get("event_type") or "").upper()
    is_type_coercion_error = (
        err in {"TYPE_CONVERSION_ERROR", "INVALID_SCALAR_VALUE", "BIGQUERY_INSERT_FAILED"}
        or payload_event_type in {"TYPE_CONVERSION_ERROR", "INVALID_SCALAR_VALUE"}
    )
    payload_depth = _max_nesting_depth(fixed)

    # For schema/insert failures, do not trust only error-message parsing.
    # If payload itself is over-deep, route to manual intervention.
    if err in {"BIGQUERY_SCHEMA_ERROR", "BIGQUERY_INSERT_FAILED"} and payload_depth > 14:
        notes.append(f"manual_intervention_required_overdeep_payload_depth:{payload_depth}")

    if fixed != original_payload:
        if err == "NULL_BYTE_IN_STRING":
            notes.append("removed_null_bytes_from_text_fields")
        if err == "CONTROL_CHARACTER_REJECTED":
            notes.append("removed_control_characters_from_text_fields")
        if err == "INVALID_UTF8":
            notes.append("normalized_invalid_utf8_text_fields")

    if "event_id" in fixed and isinstance(fixed["event_id"], str):
        original = fixed["event_id"]
        sanitized = re.sub(r"[^A-Za-z0-9_.:-]", "", original)
        if sanitized != original:
            fixed["event_id"] = sanitized
            notes.append("sanitized_event_id_removed_disallowed_chars")

    # Add event_id if missing.
    if "event_id" not in fixed:
        fixed["event_id"] = _build_replay_event_id(fixed, failed_ts_iso)
        notes.append("added_missing_event_id_with_replay_event_id")

    # Replace any invalid/empty event_id with deterministic replay id.
    if "event_id" in fixed:
        event_id = fixed.get("event_id")
        needs_replace = (
            event_id in (None, "")
            or not isinstance(event_id, str)
            or re.fullmatch(r"[A-Za-z0-9_.:-]+", event_id) is None
        )
        if needs_replace:
            fixed["event_id"] = _build_replay_event_id(fixed, failed_ts_iso)
            notes.append("filled_event_id_with_replay_event_id")

    # Fill missing fields from DLQ missing_fields when deterministic defaults exist.
    missing_from_error = _extract_missing_fields(error_details)
    for field_name in missing_from_error:
        if field_name in fixed:
            continue
        field = schema_by_name.get(field_name)
        if not field:
            continue
        default_value, note = _default_for_field(field_name, field, fixed, failed_ts_iso)
        if note is None:
            continue
        fixed[field_name] = default_value
        notes.append(note)

    # Strict unknown-field policy: if payload contains fields not present in
    # target schema, route to manual intervention instead of dropping them.
    if schema_by_name:
        unknown_fields = [
            field_name
            for field_name in fixed.keys()
            if field_name not in schema_by_name and not str(field_name).startswith("_")
        ]
        if unknown_fields:
            preview = "|".join(sorted(unknown_fields)[:10])
            notes.append(f"manual_intervention_required_unknown_fields:{preview}")

    # Additional required-field recovery when schema errors indicate missing required fields.
    if schema_by_name and err in {"REQUIRED_FIELD_MISSING", "SCHEMA_MISMATCH", "BIGQUERY_SCHEMA_ERROR"}:
        for field_name, field in schema_by_name.items():
            if field.mode != "REQUIRED":
                continue
            if field_name in fixed and fixed.get(field_name) not in (None, ""):
                continue
            default_value, note = _default_for_field(field_name, field, fixed, failed_ts_iso)
            if note is None:
                continue
            fixed[field_name] = default_value
            notes.append(note)

    # Explicit scalar coercion handling for TYPE_CONVERSION_ERROR / INVALID_SCALAR_VALUE.
    if schema_by_name and is_type_coercion_error:
        for field_name, field in schema_by_name.items():
            if field_name not in fixed:
                continue
            if field.mode == "REPEATED" or field.field_type == "RECORD":
                continue

            value = fixed.get(field_name)
            if value is None:
                continue

            applied = False
            fallback_to_null = False

            if field.field_type in {"INT64", "INTEGER"} and isinstance(value, str):
                parsed_int = _try_parse_int_like(value)
                if parsed_int is not None:
                    fixed[field_name] = parsed_int
                    notes.append(f"coerced_{field_name}_for_type_conversion_error")
                    applied = True
                else:
                    fallback_to_null = True

            elif field.field_type in {"FLOAT64", "FLOAT"} and isinstance(value, str):
                parsed_float = _try_parse_float_like(value)
                if parsed_float is not None:
                    fixed[field_name] = parsed_float
                    notes.append(f"coerced_{field_name}_for_type_conversion_error")
                    applied = True
                else:
                    fallback_to_null = True

            elif field.field_type == "NUMERIC" and isinstance(value, str):
                parsed_numeric = _try_parse_numeric_like(value)
                if parsed_numeric is not None:
                    fixed[field_name] = str(parsed_numeric)
                    notes.append(f"coerced_{field_name}_for_type_conversion_error")
                    applied = True
                else:
                    fallback_to_null = True

            elif field.field_type == "BIGNUMERIC" and isinstance(value, str):
                parsed_bignumeric = _try_parse_bignumeric_like(value)
                if parsed_bignumeric is not None:
                    fixed[field_name] = str(parsed_bignumeric)
                    notes.append(f"coerced_{field_name}_for_type_conversion_error")
                    applied = True
                else:
                    fallback_to_null = True

            elif field.field_type in {"BOOL", "BOOLEAN"} and isinstance(value, str):
                parsed_bool = _try_parse_bool_like(value)
                if parsed_bool is not None:
                    fixed[field_name] = parsed_bool
                    notes.append(f"coerced_{field_name}_for_type_conversion_error")
                    applied = True
                else:
                    fallback_to_null = True

            elif field.field_type == "TIMESTAMP":
                if isinstance(value, bool):
                    fallback_to_null = True
                elif isinstance(value, str):
                    parsed_ts = _try_parse_timestamp(value)
                    if parsed_ts is not None:
                        fixed[field_name] = parsed_ts
                        notes.append(f"coerced_{field_name}_for_type_conversion_error")
                        applied = True
                    else:
                        fallback_to_null = True
                elif isinstance(value, (int, float)):
                    fallback_to_null = True

            elif field.field_type == "DATE":
                if isinstance(value, str):
                    parsed_date = _try_parse_date_like(value)
                    if parsed_date is not None:
                        fixed[field_name] = parsed_date
                        notes.append(f"coerced_{field_name}_for_type_conversion_error")
                        applied = True
                    else:
                        fallback_to_null = True
                else:
                    fallback_to_null = True

            elif field.field_type == "DATETIME":
                if isinstance(value, str):
                    parsed_dt = _try_parse_datetime_like(value)
                    if parsed_dt is not None:
                        fixed[field_name] = parsed_dt
                        notes.append(f"coerced_{field_name}_for_type_conversion_error")
                        applied = True
                    else:
                        fallback_to_null = True
                else:
                    fallback_to_null = True

            elif field.field_type == "TIME":
                if isinstance(value, str):
                    parsed_time = _try_parse_time_like(value)
                    if parsed_time is not None:
                        fixed[field_name] = parsed_time
                        notes.append(f"coerced_{field_name}_for_type_conversion_error")
                        applied = True
                    else:
                        fallback_to_null = True
                else:
                    fallback_to_null = True

            if not applied and fallback_to_null:
                if field.field_type in {"TIMESTAMP", "DATE", "DATETIME", "TIME"} and _is_ambiguous_temporal_text(value):
                    notes.append(f"manual_intervention_required_ambiguous_temporal_{field_name}")
                    continue
                if field.mode != "REQUIRED":
                    fixed[field_name] = None
                    notes.append(f"set_{field_name}_null_for_type_conversion_error")
                else:
                    notes.append(f"manual_intervention_required_type_conversion_{field_name}")

    # Message-based deterministic fixes. If schema is available, prefer schema-aware behavior.
    if "Array specified for non-repeated field:" in err_msg:
        m = re.search(r"Array specified for non-repeated field:\s*([A-Za-z0-9_]+)\.?", err_msg)
        if m:
            field_name = m.group(1)
            value = fixed.get(field_name)
            if isinstance(value, list):
                field = schema_by_name.get(field_name)
                if field is None:
                    fixed[field_name] = json.dumps(value, separators=(",", ":"), ensure_ascii=False)
                    notes.append(f"serialized_array_{field_name}_from_error_message_no_schema")
                elif field.mode == "REPEATED":
                    notes.append(f"kept_array_{field_name}_for_repeated_field")
                elif field.field_type == "STRING":
                    fixed[field_name] = json.dumps(value, separators=(",", ":"), ensure_ascii=False)
                    notes.append(f"serialized_array_{field_name}_from_error_message")
                elif len(value) == 1:
                    fixed[field_name] = value[0]
                    notes.append(f"flattened_single_item_array_{field_name}_from_error_message")
                elif field.mode != "REQUIRED":
                    fixed[field_name] = None
                    notes.append(f"dropped_array_{field_name}_from_error_message")

    if "This field:" in err_msg and "is not a record" in err_msg:
        m = re.search(r"This field:\s*([A-Za-z0-9_]+)\s+is not a record\.?", err_msg)
        if m:
            field_name = m.group(1)
            value = fixed.get(field_name)
            field = schema_by_name.get(field_name)
            if field is None:
                if isinstance(value, (dict, list)):
                    fixed[field_name] = json.dumps(value, separators=(",", ":"), ensure_ascii=False)
                    notes.append(f"serialized_{field_name}_from_error_message_no_schema")
            elif field.field_type == "RECORD":
                if isinstance(value, str):
                    parsed_obj, parse_note = _try_parse_json_object_lenient(value)
                    if parsed_obj is not None:
                        fixed[field_name] = parsed_obj
                        notes.append(f"parsed_{field_name}_to_record_from_error_message")
                        if parse_note:
                            notes.append(f"{parse_note}_{field_name}")
                elif isinstance(value, dict):
                    notes.append(f"kept_{field_name}_as_record_from_error_message")
            elif field.field_type == "STRING":
                if isinstance(value, (dict, list)):
                    fixed[field_name] = json.dumps(value, separators=(",", ":"), ensure_ascii=False)
                    notes.append(f"serialized_{field_name}_from_error_message")
            elif field.mode != "REQUIRED":
                fixed[field_name] = None
                notes.append(f"dropped_{field_name}_from_error_message")

    # Generic BigQuery conversion-error handling (schema-aware).
    # BIGQUERY_SCHEMA_ERROR is treated the same as insert-failed conversion
    # errors when the message includes a deterministic bad value token.
    if schema_by_name and err in {"BIGQUERY_INSERT_FAILED", "BIGQUERY_SCHEMA_ERROR"}:
        bad_value = _extract_bad_value_from_error_message(err_msg)
        if bad_value:
            candidates = []
            normalized_bad = _normalize_scalar(bad_value)
            for field_name, value in fixed.items():
                if field_name not in schema_by_name:
                    continue
                if _normalize_scalar(value) == normalized_bad:
                    candidates.append(field_name)

            for field_name in candidates:
                field = schema_by_name[field_name]
                value = fixed.get(field_name)
                if value is None or field.mode == "REPEATED" or field.field_type == "RECORD":
                    continue

                applied = False
                if field.field_type in {"INT64", "INTEGER"} and isinstance(value, str):
                    parsed_int = _try_parse_int_like(value)
                    if parsed_int is not None:
                        fixed[field_name] = parsed_int
                        notes.append(f"coerced_{field_name}_from_bq_conversion_error")
                        applied = True
                elif field.field_type in {"FLOAT64", "FLOAT"} and isinstance(value, str):
                    parsed_float = _try_parse_float_like(value)
                    if parsed_float is not None:
                        fixed[field_name] = parsed_float
                        notes.append(f"coerced_{field_name}_from_bq_conversion_error")
                        applied = True
                elif field.field_type == "NUMERIC" and isinstance(value, str):
                    parsed_numeric = _try_parse_numeric_like(value)
                    if parsed_numeric is not None:
                        fixed[field_name] = str(parsed_numeric)
                        notes.append(f"coerced_{field_name}_from_bq_conversion_error")
                        applied = True
                elif field.field_type == "BIGNUMERIC" and isinstance(value, str):
                    parsed_bignumeric = _try_parse_bignumeric_like(value)
                    if parsed_bignumeric is not None:
                        fixed[field_name] = str(parsed_bignumeric)
                        notes.append(f"coerced_{field_name}_from_bq_conversion_error")
                        applied = True
                elif field.field_type in {"BOOL", "BOOLEAN"} and isinstance(value, str):
                    parsed_bool = _try_parse_bool_like(value)
                    if parsed_bool is not None:
                        fixed[field_name] = parsed_bool
                        notes.append(f"coerced_{field_name}_from_bq_conversion_error")
                        applied = True
                elif field.field_type == "TIMESTAMP" and isinstance(value, str):
                    parsed_ts = _try_parse_timestamp(value)
                    if parsed_ts:
                        fixed[field_name] = parsed_ts
                        notes.append(f"coerced_{field_name}_from_bq_conversion_error")
                        applied = True
                elif field.field_type == "DATE" and isinstance(value, str):
                    parsed_date = _try_parse_date_like(value)
                    if parsed_date:
                        fixed[field_name] = parsed_date
                        notes.append(f"coerced_{field_name}_from_bq_conversion_error")
                        applied = True
                elif field.field_type == "DATETIME" and isinstance(value, str):
                    parsed_dt = _try_parse_datetime_like(value)
                    if parsed_dt:
                        fixed[field_name] = parsed_dt
                        notes.append(f"coerced_{field_name}_from_bq_conversion_error")
                        applied = True
                elif field.field_type == "TIME" and isinstance(value, str):
                    parsed_time = _try_parse_time_like(value)
                    if parsed_time:
                        fixed[field_name] = parsed_time
                        notes.append(f"coerced_{field_name}_from_bq_conversion_error")
                        applied = True

                if not applied:
                    if field.field_type in {"TIMESTAMP", "DATE", "DATETIME", "TIME"} and _is_ambiguous_temporal_text(value):
                        notes.append(f"manual_intervention_required_ambiguous_temporal_{field_name}")
                    elif field.mode != "REQUIRED":
                        fixed[field_name] = None
                        notes.append(f"set_{field_name}_null_from_bq_conversion_error")

    # Large row/value mitigation: truncate large strings and drop nullable complex fields.
    if schema_by_name and (
        err == "ROW_SIZE_LIMIT_EXCEEDED"
        or err == "MESSAGE_TOO_LARGE"
        or "row size too large" in err_msg_lower
        or "row is too large" in err_msg_lower
        or "too large" in err_msg_lower
        or "maximum allowed row size" in err_msg_lower
        or "message too large" in err_msg_lower
    ):
        notes.append("applied_large_payload_slimming")
        max_len = 16000
        for field_name, value in list(fixed.items()):
            field = schema_by_name.get(field_name)
            if not field:
                continue
            if isinstance(value, str) and len(value) > max_len:
                if field.mode == "REQUIRED":
                    notes.append(f"manual_intervention_required_row_size_{field_name}")
                else:
                    fixed[field_name] = value[:max_len]
                    notes.append(f"truncated_nullable_{field_name}_for_row_size")
            elif isinstance(value, (dict, list)) and field.mode != "REQUIRED":
                fixed[field_name] = None
                notes.append(f"dropped_complex_{field_name}_for_row_size")

    # NESTING_DEPTH_EXCEEDED mitigation: remove over-deep nullable complex values.
    if schema_by_name and (
        err == "NESTING_DEPTH_EXCEEDED"
        or ("nesting" in err_msg_lower and "depth" in err_msg_lower)
        or "too many levels" in err_msg_lower
    ):
        max_depth = 14
        for field_name, value in list(fixed.items()):
            field = schema_by_name.get(field_name)
            if not field or not isinstance(value, (dict, list)):
                continue
            depth = _max_nesting_depth(value)
            if depth <= max_depth:
                continue
            if field.field_type == "STRING":
                fixed[field_name] = json.dumps(value, separators=(",", ":"), ensure_ascii=False)
                notes.append(f"serialized_overdeep_{field_name}_for_string_target")
                continue
            if field.field_type == "RECORD":
                notes.append(f"manual_intervention_required_overdeep_record_{field_name}")
                continue
            if field.mode == "REQUIRED":
                notes.append(f"manual_intervention_required_overdeep_non_string_{field_name}")
                continue
            if field.mode != "REQUIRED":
                fixed[field_name] = None
                notes.append(f"dropped_overdeep_{field_name}_for_nesting_depth")
            else:
                notes.append(f"detected_overdeep_required_{field_name}_not_dropped")

    # PARTITION_ERROR mitigation: repair partition-typed columns from failed timestamp.
    if schema_by_name and (err == "PARTITION_ERROR" or "partition" in err_msg_lower):
        forced_partition_column = _extract_partition_column(error_details)
        for field_name, field in schema_by_name.items():
            if field_name not in fixed:
                continue
            value = fixed.get(field_name)
            if field.field_type == "TIMESTAMP" and failed_ts_iso:
                if forced_partition_column and field_name == forced_partition_column:
                    fixed[field_name] = failed_ts_iso
                    notes.append(f"forced_partition_{field_name}_from_failed_timestamp")
                    continue
                if value in (None, ""):
                    fixed[field_name] = failed_ts_iso
                    notes.append(f"filled_partition_{field_name}_from_failed_timestamp")
                elif not isinstance(value, str) or _try_parse_timestamp(value) is None:
                    fixed[field_name] = failed_ts_iso
                    notes.append(f"repaired_partition_{field_name}_from_failed_timestamp")
            elif field.field_type == "DATE" and failed_date_iso:
                if forced_partition_column and field_name == forced_partition_column:
                    fixed[field_name] = failed_date_iso
                    notes.append(f"forced_partition_{field_name}_from_failed_date")
                    continue
                if value in (None, ""):
                    fixed[field_name] = failed_date_iso
                    notes.append(f"filled_partition_{field_name}_from_failed_date")
                elif not isinstance(value, str):
                    fixed[field_name] = failed_date_iso
                    notes.append(f"repaired_partition_{field_name}_from_failed_date")
                else:
                    try:
                        datetime.strptime(value.strip(), "%Y-%m-%d")
                    except ValueError:
                        fixed[field_name] = failed_date_iso
                        notes.append(f"repaired_partition_{field_name}_from_failed_date")

    # If event_time exists as a STRING column, fill/repair it from failed_timestamp.
    event_time_field = schema_by_name.get("event_time")
    if event_time_field and event_time_field.field_type == "STRING" and failed_ts_iso:
        current = fixed.get("event_time")
        if current in (None, ""):
            fixed["event_time"] = failed_ts_iso
            notes.append("filled_event_time_from_failed_timestamp")
        elif not isinstance(current, str):
            fixed["event_time"] = failed_ts_iso
            notes.append("replaced_non_string_event_time_with_failed_timestamp")
        elif _try_parse_timestamp(current) is None:
            fixed["event_time"] = failed_ts_iso
            notes.append("replaced_invalid_event_time_with_failed_timestamp")
    elif failed_ts_iso:
        # When no schema is available, still apply deterministic event_time fallback.
        current = fixed.get("event_time")
        if current in (None, ""):
            fixed["event_time"] = failed_ts_iso
            notes.append("filled_event_time_from_failed_timestamp_no_schema")
        elif not isinstance(current, str):
            fixed["event_time"] = failed_ts_iso
            notes.append("replaced_non_string_event_time_with_failed_timestamp_no_schema")
        elif _try_parse_timestamp(current) is None:
            fixed["event_time"] = failed_ts_iso
            notes.append("replaced_invalid_event_time_with_failed_timestamp_no_schema")

    # Conservative TIMESTAMP backfill: only for required timestamp fields and
    # only when the error context indicates missing/partition repair.
    if failed_ts_iso and err in {"REQUIRED_FIELD_MISSING", "SCHEMA_MISMATCH", "BIGQUERY_SCHEMA_ERROR", "PARTITION_ERROR"}:
        for field_name, field in schema_by_name.items():
            if field.field_type != "TIMESTAMP":
                continue
            if field.mode != "REQUIRED":
                continue
            if field_name in fixed:
                continue
            fixed[field_name] = failed_ts_iso
            notes.append(f"filled_required_{field_name}_from_failed_timestamp")

    for field_name, field in schema_by_name.items():
        if field_name not in fixed:
            continue

        value = fixed[field_name]
        if value is None:
            continue
        if f"manual_intervention_required_ambiguous_temporal_{field_name}" in notes:
            # Preserve value for manual routing; do not auto-rewrite with
            # failed_timestamp in downstream generic normalization.
            continue

        if field.mode == "REPEATED":
            if isinstance(value, list):
                normalized_list = value
            elif isinstance(value, str):
                stripped = value.strip()
                parsed_list: list[Any] | None = None
                if stripped.startswith("[") and stripped.endswith("]"):
                    try:
                        loaded = json.loads(stripped)
                        if isinstance(loaded, list):
                            parsed_list = loaded
                    except json.JSONDecodeError:
                        parsed_list = None
                if parsed_list is not None:
                    normalized_list = parsed_list
                    notes.append(f"parsed_{field_name}_string_to_array")
                else:
                    normalized_list = [value]
                    notes.append(f"wrapped_scalar_{field_name}_to_array")
            else:
                normalized_list = [value]
                notes.append(f"wrapped_scalar_{field_name}_to_array")

            if field.field_type != "RECORD":
                coerced_values: list[Any] = []
                invalid_indexes: list[int] = []
                for idx, element in enumerate(normalized_list):
                    coerced, ok = _coerce_repeated_scalar_element(
                        element,
                        field.field_type,
                        failed_ts_iso,
                    )
                    if ok:
                        coerced_values.append(coerced)
                    else:
                        invalid_indexes.append(idx)
                fixed[field_name] = coerced_values
                if invalid_indexes:
                    preview = ",".join(str(i) for i in invalid_indexes[:10])
                    notes.append(
                        f"manual_intervention_required_repeated_{field_name}_invalid_elements:{preview}"
                    )
                elif coerced_values != normalized_list:
                    notes.append(f"coerced_repeated_{field_name}_elements")
            else:
                fixed[field_name] = normalized_list
            continue

        if field.field_type == "RECORD" and isinstance(value, str):
            parsed_obj, parse_note = _try_parse_json_object_lenient(value)
            if parsed_obj is not None:
                fixed[field_name] = parsed_obj
                notes.append(f"parsed_{field_name}_json_string_to_record")
                if parse_note:
                    notes.append(f"{parse_note}_{field_name}")
                value = fixed[field_name]
            elif field.mode != "REQUIRED":
                fixed[field_name] = None
                notes.append(f"set_{field_name}_null_from_record_string_mismatch")
                continue
            else:
                notes.append(f"manual_intervention_required_record_string_mismatch_{field_name}")

        if field.field_type == "RECORD" and isinstance(value, dict):
            missing_nested = _collect_missing_required_nested_fields(value, field, field_name)
            if missing_nested:
                missing_preview = "|".join(missing_nested[:5])
                if field.mode != "REQUIRED":
                    fixed[field_name] = None
                    notes.append(f"set_{field_name}_null_missing_required_nested:{missing_preview}")
                    continue
                notes.append(f"missing_required_nested_in_{field_name}:{missing_preview}")

        if field.field_type == "STRING" and field.mode != "REPEATED" and isinstance(value, (dict, list)):
            fixed[field_name] = json.dumps(value, separators=(",", ":"), ensure_ascii=False)
            notes.append(f"serialized_{field_name}_to_string")
            continue

        if isinstance(value, dict) and field.field_type not in {"RECORD", "JSON", "STRING"}:
            if field.mode != "REQUIRED":
                fixed[field_name] = None
                notes.append(f"dropped_object_value_for_{field_name}")
            continue

        if isinstance(value, list) and field.mode != "REPEATED":
            if field.field_type == "STRING":
                fixed[field_name] = json.dumps(value, separators=(",", ":"), ensure_ascii=False)
                notes.append(f"serialized_array_{field_name}_to_string")
            elif len(value) == 1:
                fixed[field_name] = value[0]
                notes.append(f"flattened_single_item_array_{field_name}")
            elif field.mode != "REQUIRED":
                fixed[field_name] = None
                notes.append(f"dropped_array_value_for_{field_name}")
            continue

        if field.field_type in {"INT64", "INTEGER"} and isinstance(value, str):
            parsed_int = _try_parse_int_like(value)
            if parsed_int is not None:
                fixed[field_name] = parsed_int
                notes.append(f"coerced_{field_name}_to_int")
            continue

        if field.field_type in {"FLOAT64", "FLOAT"} and isinstance(value, str):
            parsed_float = _try_parse_float_like(value)
            if parsed_float is not None:
                fixed[field_name] = parsed_float
                notes.append(f"coerced_{field_name}_to_float")
            continue

        if field.field_type == "NUMERIC" and isinstance(value, str):
            parsed_numeric = _try_parse_numeric_like(value)
            if parsed_numeric is not None:
                fixed[field_name] = str(parsed_numeric)
                notes.append(f"coerced_{field_name}_to_numeric")
            continue

        if field.field_type == "BIGNUMERIC" and isinstance(value, str):
            parsed_bignumeric = _try_parse_bignumeric_like(value)
            if parsed_bignumeric is not None:
                fixed[field_name] = str(parsed_bignumeric)
                notes.append(f"coerced_{field_name}_to_bignumeric")
            continue

        if field.field_type in {"BOOL", "BOOLEAN"} and isinstance(value, str):
            lowered = value.strip().lower()
            if lowered in {"true", "1", "t", "yes", "y"}:
                fixed[field_name] = True
                notes.append(f"coerced_{field_name}_to_bool")
            elif lowered in {"false", "0", "f", "no", "n"}:
                fixed[field_name] = False
                notes.append(f"coerced_{field_name}_to_bool")
            continue

        if field.field_type == "TIMESTAMP":
            if isinstance(value, bool):
                if failed_ts_iso:
                    fixed[field_name] = failed_ts_iso
                    notes.append(f"replaced_bool_{field_name}_with_failed_timestamp")
                else:
                    fixed[field_name] = None
                    notes.append(f"dropped_bool_{field_name}_timestamp")
                continue
            if isinstance(value, str):
                parsed = _try_parse_timestamp(value)
                if parsed and parsed != value:
                    fixed[field_name] = parsed
                    notes.append(f"normalized_{field_name}_timestamp")
                elif parsed is None and failed_ts_iso:
                    if _is_ambiguous_temporal_text(value):
                        notes.append(f"manual_intervention_required_ambiguous_temporal_{field_name}")
                    else:
                        fixed[field_name] = failed_ts_iso
                        notes.append(f"replaced_invalid_{field_name}_with_failed_timestamp")
            elif failed_ts_iso:
                fixed[field_name] = failed_ts_iso
                notes.append(f"replaced_non_string_{field_name}_with_failed_timestamp")

    return fixed, notes
