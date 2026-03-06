import re
from decimal import Decimal, InvalidOperation
from datetime import datetime
from typing import Any

from google.cloud import bigquery

INT64_MIN = -(2**63)
INT64_MAX = 2**63 - 1
NUMERIC_MAX = Decimal("99999999999999999999999999999.999999999")
NUMERIC_MIN = -NUMERIC_MAX
BIGNUMERIC_MAX = Decimal("578960446186580977117854925043439539266.34992332820282019728792003956564819967")
BIGNUMERIC_MIN = -BIGNUMERIC_MAX


def _parse_decimal(value: Any) -> Decimal | None:
    try:
        parsed = Decimal(str(value).strip())
    except (InvalidOperation, ValueError):
        return None
    if not parsed.is_finite():
        return None
    return parsed


def _decimal_scale(value: Decimal) -> int:
    exponent = value.as_tuple().exponent
    return -exponent if exponent < 0 else 0


def _is_valid_by_type(value: Any, field_type: str) -> bool:
    if value is None:
        return True

    if field_type in {"STRING", "JSON"}:
        return True

    if field_type in {"INT64", "INTEGER"}:
        if isinstance(value, bool):
            return False
        try:
            parsed = int(value)
            return INT64_MIN <= parsed <= INT64_MAX
        except (TypeError, ValueError):
            return False

    if field_type in {"FLOAT64", "FLOAT"}:
        if isinstance(value, bool):
            return False
        try:
            parsed = float(value)
            return parsed not in {float("inf"), float("-inf")} and parsed == parsed
        except (TypeError, ValueError):
            return False

    if field_type == "NUMERIC":
        parsed = _parse_decimal(value)
        if parsed is None:
            return False
        if parsed < NUMERIC_MIN or parsed > NUMERIC_MAX:
            return False
        return _decimal_scale(parsed) <= 9

    if field_type == "BIGNUMERIC":
        parsed = _parse_decimal(value)
        if parsed is None:
            return False
        if parsed < BIGNUMERIC_MIN or parsed > BIGNUMERIC_MAX:
            return False
        return _decimal_scale(parsed) <= 38

    if field_type in {"BOOL", "BOOLEAN"}:
        if isinstance(value, bool):
            return True
        if isinstance(value, str):
            return value.strip().lower() in {"true", "false", "1", "0", "t", "f", "yes", "no", "y", "n"}
        return False

    if field_type == "TIMESTAMP":
        if not isinstance(value, str):
            return False
        s = value.strip()
        if s.endswith("Z"):
            s = s.replace("Z", "+00:00")
        try:
            datetime.fromisoformat(s)
            return True
        except ValueError:
            return False

    if field_type == "DATE":
        if not isinstance(value, str):
            return False
        try:
            datetime.strptime(value.strip(), "%Y-%m-%d")
            return True
        except ValueError:
            return False

    if field_type == "DATETIME":
        if not isinstance(value, str):
            return False
        try:
            datetime.fromisoformat(value.strip())
            return True
        except ValueError:
            return False

    if field_type == "TIME":
        if not isinstance(value, str):
            return False
        for fmt in ("%H:%M:%S", "%H:%M:%S.%f"):
            try:
                datetime.strptime(value.strip(), fmt)
                return True
            except ValueError:
                continue
        return False

    return True


def _missing_required_nested_fields(
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
                        if not isinstance(item, dict):
                            missing.append(f"{child_path}[{idx}]")
                            continue
                        missing.extend(_missing_required_nested_fields(item, child, f"{child_path}[{idx}]"))
            elif isinstance(child_value, dict):
                missing.extend(_missing_required_nested_fields(child_value, child, child_path))

    return missing


def validate_payload(
    payload: dict[str, Any],
    schema_by_name: dict[str, bigquery.SchemaField],
    required_fields: tuple[str, ...],
    regex_rules: dict[str, str],
    merge_key: str,
) -> list[str]:
    errors: list[str] = []

    if merge_key not in payload or payload.get(merge_key) in (None, ""):
        errors.append(f"merge_key_null:{merge_key}")

    required = set(required_fields)
    required.add(merge_key)
    for field_name, field in schema_by_name.items():
        if field.mode == "REQUIRED":
            required.add(field_name)

    for field in required:
        if field not in payload or payload[field] in (None, ""):
            errors.append(f"missing_required_field:{field}")

    for field_name, pattern in regex_rules.items():
        value = payload.get(field_name)
        if value is None:
            continue
        if not isinstance(value, str):
            errors.append(f"regex_not_string:{field_name}")
            continue
        if re.fullmatch(pattern, value) is None:
            errors.append(f"regex_mismatch:{field_name}")

    for field_name, value in payload.items():
        if field_name not in schema_by_name:
            continue
        field = schema_by_name[field_name]
        if field.mode == "REPEATED":
            if not isinstance(value, list):
                errors.append(f"type_mismatch:{field_name}:expected_list")
            elif field.field_type == "RECORD":
                for idx, item in enumerate(value):
                    if not isinstance(item, dict):
                        errors.append(f"type_mismatch:{field_name}[{idx}]:expected_record")
                        continue
                    missing_nested = _missing_required_nested_fields(item, field, f"{field_name}[{idx}]")
                    for path in missing_nested:
                        errors.append(f"missing_required_nested_field:{path}")
            continue
        if field.field_type == "RECORD":
            if value is None:
                # Nullable RECORD is valid when explicitly null.
                if field.mode != "REQUIRED":
                    continue
                errors.append(f"missing_required_field:{field_name}")
                continue
            if not isinstance(value, dict):
                errors.append(f"type_mismatch:{field_name}:expected_record")
            else:
                missing_nested = _missing_required_nested_fields(value, field, field_name)
                for path in missing_nested:
                    errors.append(f"missing_required_nested_field:{path}")
            continue
        if not _is_valid_by_type(value, field.field_type):
            errors.append(f"type_mismatch:{field_name}:expected_{field.field_type}")

    return errors
