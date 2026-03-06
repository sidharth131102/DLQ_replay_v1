import argparse
import base64
import binascii
import gzip
import json
import logging
import re
import zlib
from dataclasses import asdict
from typing import Any

from google.api_core.exceptions import NotFound

from app.bigquery_repo import BigQueryRepository
from app.config import AppConfig
from app.fixes import apply_deterministic_fixes
from app.models import ReplayOutcome, row_to_dict
from app.validators import validate_payload


_TRANSPORT_ERROR_TYPES = {
    "BASE64_DECODE_ERROR",
    "COMPRESSION_ERROR",
    "MESSAGE_TOO_LARGE",
    "INVALID_UTF8",
    "NULL_BYTE_IN_STRING",
    "CONTROL_CHARACTER_REJECTED",
}

_CDC_PENDING_ERROR_TYPES = {
    "DUPLICATE_MERGE_KEY",
    "BATCH_DUPLICATE",
    "VERSION_CONFLICT",
    "OUT_OF_ORDER_EVENT",
    "SEQUENCE_GAP_DETECTED",
    "CAUSALITY_VIOLATION",
    "MISSING_OPERATION_TYPE",
    "INVALID_CDC_EVENT",
    "MISSING_METADATA",
    "TOMBSTONE_WITHOUT_KEY",
    "LATE_ARRIVING_EVENT",
}


def _setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )


def _parse_message(raw: Any) -> tuple[dict[str, Any] | None, str | None]:
    if isinstance(raw, dict):
        return raw, None
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                return parsed, None
            return None, "original_message_not_json_object"
        except json.JSONDecodeError:
            return None, "invalid_json"
    return None, "unsupported_original_message_type"


def _is_transport_error_type(error_type: Any) -> bool:
    return str(error_type or "").strip().upper() in _TRANSPORT_ERROR_TYPES


def _is_cdc_pending_error_type(error_type: Any) -> bool:
    return str(error_type or "").strip().upper() in _CDC_PENDING_ERROR_TYPES


def _sanitize_transport_text(raw: str) -> tuple[str, bool]:
    sanitized = "".join(ch for ch in raw if ch in "\t\n\r" or ord(ch) >= 32)
    return sanitized, sanitized != raw


def _sanitize_json_control_chars(raw: str) -> tuple[str, bool]:
    # For malformed JSON strings, raw control chars inside quoted values make
    # json.loads fail. Drop all ASCII control chars and retry parsing.
    sanitized = "".join(ch for ch in raw if ord(ch) >= 32)
    return sanitized, sanitized != raw


def _decode_base64_variants(raw: str) -> list[tuple[bytes, str]]:
    variants: list[tuple[str, str]] = []
    stripped = raw.strip()
    if not stripped:
        return []

    variants.append((stripped, "decoded_base64_standard"))
    missing_padding = len(stripped) % 4
    if missing_padding:
        variants.append((stripped + ("=" * (4 - missing_padding)), "decoded_base64_with_padding"))
    variants.append((stripped.replace("-", "+").replace("_", "/"), "decoded_base64_urlsafe"))

    outputs: list[tuple[bytes, str]] = []
    seen: set[bytes] = set()
    for candidate, note in variants:
        try:
            decoded = base64.b64decode(candidate, validate=True)
        except (binascii.Error, ValueError):
            continue
        if decoded in seen:
            continue
        seen.add(decoded)
        outputs.append((decoded, note))
    return outputs


def _decompress_variants(raw: bytes) -> list[tuple[bytes, str]]:
    outputs: list[tuple[bytes, str]] = []
    decompressors: list[tuple[str, Any]] = [
        ("decompressed_gzip", gzip.decompress),
        ("decompressed_zlib", zlib.decompress),
    ]
    seen: set[bytes] = set()
    for note, fn in decompressors:
        try:
            value = fn(raw)
        except Exception:
            continue
        if value in seen:
            continue
        seen.add(value)
        outputs.append((value, note))
    return outputs


def _decode_text_variants(raw: bytes) -> list[tuple[str, str]]:
    candidates: list[tuple[str, str]] = []
    try:
        candidates.append((raw.decode("utf-8"), "decoded_utf8"))
    except UnicodeDecodeError:
        pass

    # Replacement decode can recover malformed UTF-8 enough for deterministic fixes.
    candidates.append((raw.decode("utf-8", errors="replace"), "decoded_utf8_with_replacement"))
    candidates.append((raw.decode("latin-1"), "decoded_latin1_fallback"))

    outputs: list[tuple[str, str]] = []
    seen: set[str] = set()
    for text, note in candidates:
        if text in seen:
            continue
        seen.add(text)
        outputs.append((text, note))
    return outputs


def _parse_message_with_repairs(
    raw: Any,
    error_type: Any,
    enable_deserialization_repair: bool,
) -> tuple[dict[str, Any] | None, str | None, list[str]]:
    notes: list[str] = []
    message, parse_error = _parse_message(raw)
    if message is not None:
        return message, None, notes

    if parse_error and enable_deserialization_repair:
        if isinstance(raw, str):
            if not raw.strip():
                notes.append("deserialization_repair_wrapped_empty_payload")
                return {"_empty_payload": True}, None, notes
            sanitized_raw, changed = _sanitize_json_control_chars(raw)
            if changed:
                repaired_message, candidate_error = _parse_message(sanitized_raw)
                if repaired_message is not None:
                    notes.append("deserialization_repair_sanitized_json_control_chars")
                    return repaired_message, None, notes
                repaired, parse_note = _try_repair_json_object_string(sanitized_raw)
                if repaired is not None:
                    notes.append("deserialization_repair_sanitized_json_control_chars")
                    if parse_note:
                        notes.append(f"deserialization_repair_{parse_note}")
                    return repaired, None, notes
                if candidate_error:
                    parse_error = candidate_error
            repaired, parse_note = _try_repair_json_object_string(raw)
            if repaired is not None:
                if parse_note:
                    notes.append(f"deserialization_repair_{parse_note}")
                return repaired, None, notes
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, list):
                    notes.append("deserialization_repair_wrapped_array_payload")
                    return {"_array_payload": parsed}, None, notes
                if parsed is not None:
                    notes.append("deserialization_repair_wrapped_scalar_payload")
                    return {"_scalar_payload": parsed}, None, notes
            except json.JSONDecodeError:
                pass
        elif isinstance(raw, list):
            notes.append("deserialization_repair_wrapped_array_payload")
            return {"_array_payload": raw}, None, notes
        elif raw is None:
            notes.append("deserialization_repair_wrapped_empty_payload")
            return {"_empty_payload": True}, None, notes

    if not _is_transport_error_type(error_type):
        return None, parse_error, notes

    text_candidates: list[tuple[str, str]] = []
    byte_candidates: list[tuple[bytes, str]] = []

    if isinstance(raw, str):
        text_candidates.append((raw, "raw_text"))
        sanitized_raw, changed = _sanitize_transport_text(raw)
        if changed:
            text_candidates.append((sanitized_raw, "sanitized_control_chars"))
        byte_candidates.extend(_decode_base64_variants(raw))
    elif isinstance(raw, bytes):
        byte_candidates.append((raw, "raw_bytes"))
    else:
        return None, parse_error, notes

    expanded_bytes: list[tuple[bytes, str]] = list(byte_candidates)
    for raw_bytes, source_note in byte_candidates:
        for dec_bytes, dec_note in _decompress_variants(raw_bytes):
            expanded_bytes.append((dec_bytes, f"{source_note}_{dec_note}"))

    for raw_bytes, source_note in expanded_bytes:
        for text, text_note in _decode_text_variants(raw_bytes):
            text_candidates.append((text, f"{source_note}_{text_note}"))
            sanitized_text, changed = _sanitize_transport_text(text)
            if changed:
                text_candidates.append((sanitized_text, f"{source_note}_{text_note}_sanitized_control_chars"))

    seen_text: set[str] = set()
    for candidate, note in text_candidates:
        if candidate in seen_text:
            continue
        seen_text.add(candidate)

        repaired_message, candidate_error = _parse_message(candidate)
        if repaired_message is not None:
            notes.append(f"transport_repair_{note}")
            return repaired_message, None, notes

        if enable_deserialization_repair:
            repaired, parse_note = _try_repair_json_object_string(candidate)
            if repaired is not None:
                notes.append(f"transport_repair_{note}")
                if parse_note:
                    notes.append(f"deserialization_repair_{parse_note}")
                return repaired, None, notes

        if candidate_error:
            parse_error = candidate_error

    return None, parse_error or "transport_repair_failed", notes


def _try_repair_json_object_string(raw: str) -> tuple[dict[str, Any] | None, str | None]:
    s = raw.strip()
    if not s.startswith("{"):
        return None, None

    candidates: list[tuple[str, str]] = []

    # 0) Close unbalanced braces when payload starts as object but is truncated.
    if s.count("{") > s.count("}"):
        closed = s + ("}" * (s.count("{") - s.count("}")))
        candidates.append((closed, "closed_unbalanced_braces"))

    # 1) Remove trailing commas before object/array close.
    no_trailing = re.sub(r",\s*([}\]])", r"\1", s)
    if no_trailing != s:
        candidates.append((no_trailing, "removed_trailing_commas"))

    # 1b) Add missing commas between adjacent key/value lines.
    lines = s.splitlines()
    if len(lines) > 2:
        fixed_lines = list(lines)
        changed = False
        for i in range(len(lines) - 1):
            current = lines[i].rstrip()
            nxt = lines[i + 1].lstrip()
            if not current or not nxt:
                continue
            if current.endswith(("{", "[", ",")):
                continue
            if current.strip() in {"{", "}", "]"}:
                continue
            if nxt.startswith(("}", "]", ",")):
                continue
            if nxt.startswith('"') and ":" in nxt:
                fixed_lines[i] = current + ","
                changed = True
        if changed:
            candidates.append(("\n".join(fixed_lines), "added_missing_commas_between_fields"))

    # 2) Quote unquoted object keys.
    quoted_keys = re.sub(r'([{\s,])([A-Za-z_][A-Za-z0-9_]*)\s*:', r'\1"\2":', s)
    if quoted_keys != s:
        candidates.append((quoted_keys, "quoted_unquoted_keys"))

    # 3) Quote bareword string values (but keep true/false/null).
    def _quote_bareword(m: re.Match[str]) -> str:
        value = m.group(2)
        if value in {"true", "false", "null"}:
            return f"{m.group(1)}{value}{m.group(3)}"
        return f'{m.group(1)}"{value}"{m.group(3)}'

    quoted_values = re.sub(r'(:\s*)([A-Za-z_][A-Za-z0-9_\-]*)(\s*[,}])', _quote_bareword, s)
    if quoted_values != s:
        candidates.append((quoted_values, "quoted_bareword_values"))

    # 4) Combined key/value/trailing-comma normalization.
    combined = s
    combined = re.sub(r'([{\s,])([A-Za-z_][A-Za-z0-9_]*)\s*:', r'\1"\2":', combined)
    combined = re.sub(r'(:\s*)([A-Za-z_][A-Za-z0-9_\-]*)(\s*[,}])', _quote_bareword, combined)
    combined = re.sub(r",\s*([}\]])", r"\1", combined)
    if combined != s:
        candidates.append((combined, "combined_json_normalization"))

    seen: set[str] = set()
    for candidate, note in candidates:
        if candidate in seen:
            continue
        seen.add(candidate)
        try:
            parsed = json.loads(candidate)
            if isinstance(parsed, dict):
                return parsed, note
        except json.JSONDecodeError:
            continue
    return None, None


def _extract_attempted_table(error_details: Any) -> str | None:
    if not isinstance(error_details, str) or not error_details.strip():
        return None
    try:
        parsed = json.loads(error_details)
        attempted = parsed.get("attempted_table")
        return attempted if isinstance(attempted, str) and attempted.strip() else None
    except json.JSONDecodeError:
        return None


def _expand_fanout_payloads(
    payload: dict[str, Any],
    schema_by_name: dict[str, Any],
    merge_key: str,
    fanout_enabled: bool,
    fanout_fields: tuple[str, ...],
) -> tuple[list[dict[str, Any]], list[str]]:
    if not fanout_enabled or not fanout_fields:
        return [payload], []

    expanded = [payload]
    notes: list[str] = []
    for field_name in fanout_fields:
        schema_field = schema_by_name.get(field_name)
        if schema_field is None or schema_field.mode == "REPEATED":
            continue

        next_expanded: list[dict[str, Any]] = []
        for base in expanded:
            value = base.get(field_name)
            if not isinstance(value, list) or len(value) <= 1:
                next_expanded.append(base)
                continue

            base_key = str(base.get(merge_key) or "")
            for idx, item in enumerate(value):
                child = dict(base)
                child[field_name] = item
                child[merge_key] = f"{base_key}__fanout_{field_name}_{idx}"
                next_expanded.append(child)
            notes.append(f"fanout_{field_name}_{len(value)}")
        expanded = next_expanded

    return expanded, notes


def _is_operational_bq_insert_error(error_type: Any, error_message: Any) -> bool:
    if str(error_type or "").upper() != "BIGQUERY_INSERT_FAILED":
        return False
    msg = str(error_message or "").lower()
    patterns = (
        "permission denied",
        "access denied",
        "quota exceeded",
        "rate limit exceeded",
        "internal error",
        "backend error",
        "service unavailable",
        "deadline exceeded",
        "resources exceeded",
    )
    return any(p in msg for p in patterns)


def _is_payload_merge_error(error_message: Any) -> bool:
    msg = str(error_message or "").lower()
    patterns = (
        "cannot convert value",
        "invalid int64 value",
        "invalid float",
        "invalid numeric",
        "invalid bignumeric",
        "invalid bool",
        "invalid timestamp",
        "invalid date",
        "invalid datetime",
        "missing required",
        "type mismatch",
        "is not a record",
        "array specified for non-repeated field",
        "invalid value",
        "invalid cast from json",
        "cannot cast",
        "could not cast",
        "no matching signature for function parse_json",
        "failed to parse input string",
        "invalid json",
        "expected array",
        "expected struct",
        "failed writing rows to staging table",
    )
    return any(p in msg for p in patterns)


def _is_operational_merge_error(error_message: Any) -> bool:
    msg = str(error_message or "").lower()
    patterns = (
        "permission denied",
        "access denied",
        "quota exceeded",
        "rate limit exceeded",
        "internal error",
        "backend error",
        "service unavailable",
        "deadline exceeded",
        "resources exceeded",
        "resource exhausted",
        "connection error",
        "http 503",
    )
    return any(p in msg for p in patterns)


def _is_requeue_error_type(error_type: Any) -> bool:
    et = str(error_type or "").strip().upper()
    return et in {
        "SERVICE_ACCOUNT_EXPIRED",
        "IAM_POLICY_CHANGED",
        "TOKEN_REFRESH_FAILED",
        "PERMISSION_DENIED",
        "QUOTA_EXCEEDED",
        "RATE_LIMITED",
        "DEADLINE_EXCEEDED",
        "BACKEND_ERROR",
        "NETWORK_PARTITION",
        "DNS_RESOLUTION_FAILED",
        "SCHEMA_REGISTRY_UNAVAILABLE",
        "MISSING_ROUTING_CONFIG",
        "UNMAPPED_TABLE",
        "DESTINATION_TABLE_NOT_FOUND",
        "WRONG_DATASET",
    }


def _is_explicit_poison_pill_error_type(error_type: Any) -> bool:
    et = str(error_type or "").strip().upper()
    return et in {
        "MAX_RETRY_EXCEEDED",
        "REPROCESSING_LOOP_DETECTED",
        "DLQ_REQUEUE_LIMIT_HIT",
    }


def _is_manual_intervention_error_type(error_type: Any) -> bool:
    et = str(error_type or "").strip().upper()
    return et in {
        "FK_NOT_FOUND",
        "BUSINESS_RULE_VIOLATION",
        "CONDITIONAL_FIELD_MISSING",
        "DATA_CLASSIFICATION_VIOLATION",
    }


def _is_dlq_publish_failed_error_type(error_type: Any) -> bool:
    return str(error_type or "").strip().upper() == "DLQ_PUBLISH_FAILED"


def _is_observability_manual_error_type(error_type: Any) -> bool:
    et = str(error_type or "").strip().upper()
    return et in {
        "LOGGING_SINK_ERROR",
        "METRICS_EXPORT_ERROR",
    }


def _extract_error_details_json(error_details: Any) -> dict[str, Any]:
    if isinstance(error_details, dict):
        return error_details
    if isinstance(error_details, str) and error_details.strip():
        try:
            parsed = json.loads(error_details)
            return parsed if isinstance(parsed, dict) else {}
        except json.JSONDecodeError:
            return {}
    return {}


def _try_fill_missing_merge_key(
    payload: dict[str, Any],
    merge_key: str,
    error_details: Any,
) -> str | None:
    if payload.get(merge_key) not in (None, ""):
        return None

    details = _extract_error_details_json(error_details)
    detail_candidates = (
        details.get("merge_key_value"),
        details.get("key_value"),
        details.get("primary_key_value"),
        details.get("id"),
    )
    for candidate in detail_candidates:
        if candidate not in (None, ""):
            payload[merge_key] = candidate
            return f"filled_{merge_key}_from_error_details"

    alias_keys = [merge_key.lower(), merge_key.upper(), merge_key.replace("-", "_")]
    if merge_key.endswith("_id"):
        alias_keys.append("id")
    alias_keys.extend(["event_id", "record_id", "key"])
    for alias in alias_keys:
        if alias == merge_key:
            continue
        candidate = payload.get(alias)
        if candidate not in (None, ""):
            payload[merge_key] = candidate
            return f"filled_{merge_key}_from_alias_{alias}"

    id_like = [k for k, v in payload.items() if k.endswith("_id") and v not in (None, "")]
    if len(id_like) == 1:
        alias = id_like[0]
        payload[merge_key] = payload.get(alias)
        return f"filled_{merge_key}_from_unique_id_like_field_{alias}"
    return None


def _is_partition_decorator_failure(
    error_type: Any,
    error_message: Any,
    error_details: Any = None,
) -> bool:
    et = str(error_type or "").strip().upper()
    msg = str(error_message or "").lower()
    details = str(error_details or "").lower()
    combined = f"{msg} {details}"
    patterns = (
        "partition decorator",
        "invalid partition decorator",
        "table decorator",
        "invalid decorator",
        "cannot use partition decorator",
        "partitioning specification",
        "partitioned table",
    )
    if any(p in combined for p in patterns):
        return True

    # Heuristic: explicit table decorator syntax in attempted table/detail text.
    if re.search(r"\$[0-9]{8}\b", combined):
        return True

    return et == "PARTITION_ERROR" and "decorator" in combined


def process_batch(
    config: AppConfig,
    table_name: str | None,
    limit: int | None,
    dry_run_override: bool | None,
    fix_only: bool = False,
) -> dict[str, int]:
    repo = BigQueryRepository(config)
    dry_run = config.dry_run if dry_run_override is None else dry_run_override
    batch_limit = min(limit or config.batch_size, config.max_records)

    rows = repo.fetch_dlq_rows(limit=batch_limit, table_name=table_name)
    logging.info("Fetched %s DLQ records", len(rows))

    counters = {
        "fetched": len(rows),
        "fixed_only": 0,
        "replayed": 0,
        "fixed_and_replayed": 0,
        "discarded": 0,
        "failed": 0,
    }
    pending_replays_by_target: dict[str, dict[str, Any]] = {}

    for row_obj in rows:
        row = row_to_dict(row_obj)
        row_for_log = {
            "table_name": row.get("table_name"),
            "error_type": row.get("error_type"),
            "failed_timestamp": str(row.get("failed_timestamp")),
        }
        decision = "DISCARD"
        discard_reason = None
        fix_notes: list[str] = []
        target_table = None
        effective_error_message = row.get("error_message")

        try:
            retry_count_raw = row.get("retry_count")
            try:
                retry_count = int(retry_count_raw) if retry_count_raw is not None else 0
            except (TypeError, ValueError):
                retry_count = 0
            if _is_explicit_poison_pill_error_type(row.get("error_type")):
                parsed_for_manual: Any = row.get("original_message")
                if isinstance(parsed_for_manual, str):
                    try:
                        parsed_for_manual = json.loads(parsed_for_manual)
                    except json.JSONDecodeError:
                        parsed_for_manual = {"_raw_original_message": parsed_for_manual}
                elif not isinstance(parsed_for_manual, (dict, list)):
                    parsed_for_manual = {"_raw_original_message": str(parsed_for_manual)}
                reason = f"manual_intervention_poison_pill_error_type:{str(row.get('error_type') or '')}"
                repo.write_manual_intervention_result(
                    row=row,
                    fixed_message=parsed_for_manual,
                    reason=reason,
                    target_table=row.get("table_name"),
                )
                outcome = ReplayOutcome(
                    decision="DISCARD",
                    fix_applied="manual_intervention_poison_pill_error_type_route",
                    discard_reason=reason,
                    target_table=None,
                )
                repo.mark_terminal(row, outcome.decision, outcome.fix_applied, outcome.discard_reason, dry_run=dry_run)

                counters["discarded"] += 1
                logging.warning("Discarded DLQ row by explicit poison-pill error_type: %s", {**row_for_log, **asdict(outcome)})
                continue
            if retry_count >= config.max_retry_count:
                parsed_for_manual: Any = row.get("original_message")
                if isinstance(parsed_for_manual, str):
                    try:
                        parsed_for_manual = json.loads(parsed_for_manual)
                    except json.JSONDecodeError:
                        parsed_for_manual = {"_raw_original_message": parsed_for_manual}
                elif not isinstance(parsed_for_manual, (dict, list)):
                    parsed_for_manual = {"_raw_original_message": str(parsed_for_manual)}
                reason = f"manual_intervention_poison_pill_max_retry_exceeded:{retry_count}"
                repo.write_manual_intervention_result(
                    row=row,
                    fixed_message=parsed_for_manual,
                    reason=reason,
                    target_table=row.get("table_name"),
                )
                outcome = ReplayOutcome(
                    decision="DISCARD",
                    fix_applied="manual_intervention_poison_pill_max_retry_route",
                    discard_reason=reason,
                    target_table=None,
                )
                repo.mark_terminal(row, outcome.decision, outcome.fix_applied, outcome.discard_reason, dry_run=dry_run)
                counters["discarded"] += 1
                logging.warning("Discarded DLQ row by retry guard: %s", {**row_for_log, **asdict(outcome)})
                continue
            if (
                retry_count >= config.reprocessing_loop_retry_threshold
                and _is_operational_bq_insert_error(row.get("error_type"), row.get("error_message"))
            ):
                parsed_for_manual: Any = row.get("original_message")
                if isinstance(parsed_for_manual, str):
                    try:
                        parsed_for_manual = json.loads(parsed_for_manual)
                    except json.JSONDecodeError:
                        parsed_for_manual = {"_raw_original_message": parsed_for_manual}
                elif not isinstance(parsed_for_manual, (dict, list)):
                    parsed_for_manual = {"_raw_original_message": str(parsed_for_manual)}
                reason = f"manual_intervention_poison_pill_reprocessing_loop_detected:{retry_count}"
                repo.write_manual_intervention_result(
                    row=row,
                    fixed_message=parsed_for_manual,
                    reason=reason,
                    target_table=row.get("table_name"),
                )
                outcome = ReplayOutcome(
                    decision="DISCARD",
                    fix_applied="manual_intervention_poison_pill_reprocessing_loop_route",
                    discard_reason=reason,
                    target_table=None,
                )
                repo.mark_terminal(row, outcome.decision, outcome.fix_applied, outcome.discard_reason, dry_run=dry_run)
                counters["discarded"] += 1
                logging.warning("Discarded DLQ row by loop-detection guard: %s", {**row_for_log, **asdict(outcome)})
                continue

            if _is_cdc_pending_error_type(row.get("error_type")):
                parsed_payload, parse_error, _ = _parse_message_with_repairs(
                    raw=row.get("original_message"),
                    error_type=row.get("error_type"),
                    enable_deserialization_repair=config.enable_deserialization_repair,
                )
                cdc_payload: Any = parsed_payload
                if cdc_payload is None:
                    raw_original = row.get("original_message")
                    if isinstance(raw_original, str):
                        cdc_payload = {"_raw_original_message": raw_original}
                    else:
                        cdc_payload = {"_raw_original_message": str(raw_original)}
                route_reason = f"cdc_pending_routed:{str(row.get('error_type') or '')}"
                if parse_error:
                    route_reason = f"{route_reason}:parse_note:{parse_error}"
                repo.route_cdc_pending(
                    row=row,
                    payload=cdc_payload,
                    route_reason=route_reason,
                    merge_key=config.merge_key,
                    max_retry_count=config.max_retry_count,
                    dry_run=dry_run,
                )
                outcome = ReplayOutcome(
                    decision="CDC_PENDING_ROUTED",
                    fix_applied="cdc_pending_route",
                    discard_reason=route_reason,
                    target_table=str(row.get("table_name") or ""),
                )
                repo.mark_terminal(row, outcome.decision, outcome.fix_applied, outcome.discard_reason, dry_run=dry_run)
                logging.info("Routed CDC DLQ row to pending table: %s", {**row_for_log, **asdict(outcome)})
                continue

            if _is_dlq_publish_failed_error_type(row.get("error_type")):
                parsed_for_manual: Any = row.get("original_message")
                if isinstance(parsed_for_manual, str):
                    try:
                        parsed_for_manual = json.loads(parsed_for_manual)
                    except json.JSONDecodeError:
                        parsed_for_manual = {"_raw_original_message": parsed_for_manual}
                reason = f"manual_intervention_dlq_publish_failed:{str(row.get('error_message') or '')[:200]}"
                repo.write_manual_intervention_result(
                    row=row,
                    fixed_message=parsed_for_manual,
                    reason=reason,
                    target_table=row.get("table_name"),
                )
                outcome = ReplayOutcome(
                    decision="DISCARD",
                    fix_applied="manual_intervention_dlq_publish_failed_route",
                    discard_reason=reason,
                    target_table=None,
                )
                repo.mark_terminal(row, outcome.decision, outcome.fix_applied, outcome.discard_reason, dry_run=dry_run)
                counters["discarded"] += 1
                logging.warning("Routed DLQ_PUBLISH_FAILED row to manual intervention: %s", {**row_for_log, **asdict(outcome)})
                continue

            if _is_observability_manual_error_type(row.get("error_type")):
                parsed_for_manual: Any = row.get("original_message")
                if isinstance(parsed_for_manual, str):
                    try:
                        parsed_for_manual = json.loads(parsed_for_manual)
                    except json.JSONDecodeError:
                        parsed_for_manual = {"_raw_original_message": parsed_for_manual}
                reason = f"manual_intervention_observability_error_type:{row.get('error_type')}:{str(row.get('error_message') or '')[:200]}"
                repo.write_manual_intervention_result(
                    row=row,
                    fixed_message=parsed_for_manual,
                    reason=reason,
                    target_table=row.get("table_name"),
                )
                outcome = ReplayOutcome(
                    decision="DISCARD",
                    fix_applied="manual_intervention_observability_error_type_route",
                    discard_reason=reason,
                    target_table=None,
                )
                repo.mark_terminal(row, outcome.decision, outcome.fix_applied, outcome.discard_reason, dry_run=dry_run)
                counters["discarded"] += 1
                logging.warning("Routed observability error row to manual intervention: %s", {**row_for_log, **asdict(outcome)})
                continue

            if _is_partition_decorator_failure(
                row.get("error_type"),
                row.get("error_message"),
                row.get("error_details"),
            ):
                parsed_for_manual: Any = row.get("original_message")
                if isinstance(parsed_for_manual, str):
                    try:
                        parsed_for_manual = json.loads(parsed_for_manual)
                    except json.JSONDecodeError:
                        parsed_for_manual = {"_raw_original_message": parsed_for_manual}
                reason = (
                    "manual_intervention_partition_decorator_failure:"
                    f"{str(row.get('error_message') or '')[:200]}"
                )
                repo.write_manual_intervention_result(
                    row=row,
                    fixed_message=parsed_for_manual,
                    reason=reason,
                    target_table=row.get("table_name"),
                )
                outcome = ReplayOutcome(
                    decision="DISCARD",
                    fix_applied="manual_intervention_partition_decorator_route",
                    discard_reason=reason,
                    target_table=None,
                )
                repo.mark_terminal(row, outcome.decision, outcome.fix_applied, outcome.discard_reason, dry_run=dry_run)
                counters["discarded"] += 1
                logging.warning("Routed DLQ row to manual intervention by partition decorator failure: %s", {**row_for_log, **asdict(outcome)})
                continue

            if _is_manual_intervention_error_type(row.get("error_type")):
                parsed_for_manual: Any = row.get("original_message")
                if isinstance(parsed_for_manual, str):
                    try:
                        parsed_for_manual = json.loads(parsed_for_manual)
                    except json.JSONDecodeError:
                        parsed_for_manual = {"_raw_original_message": parsed_for_manual}
                reason = f"manual_intervention_error_type:{row.get('error_type')}:{str(row.get('error_message') or '')[:200]}"
                repo.write_manual_intervention_result(
                    row=row,
                    fixed_message=parsed_for_manual,
                    reason=reason,
                    target_table=row.get("table_name"),
                )
                outcome = ReplayOutcome(
                    decision="DISCARD",
                    fix_applied="manual_intervention_error_type_route",
                    discard_reason=reason,
                    target_table=None,
                )
                repo.mark_terminal(row, outcome.decision, outcome.fix_applied, outcome.discard_reason, dry_run=dry_run)
                counters["discarded"] += 1
                logging.warning("Routed DLQ row to manual intervention by error_type: %s", {**row_for_log, **asdict(outcome)})
                continue

            if config.retry_input_topic and _is_requeue_error_type(row.get("error_type")):
                published = repo.publish_to_retry_input_topic(
                    payload=row.get("original_message"),
                    row=row,
                    reason="requeue_error_type",
                    dry_run=dry_run,
                )
                if published:
                    repo.mark_requeued_terminal(
                        row=row,
                        reason=f"requeued_by_error_type:{row.get('error_type')}",
                        fix_applied="requeue_by_error_type_to_input_topic",
                        dry_run=dry_run,
                    )
                    counters["replayed"] += 1
                    logging.info(
                        "Requeued DLQ row to input topic by error_type: %s",
                        {**row_for_log, "error_type": row.get("error_type")},
                    )
                    continue
                repo.mark_retry_pending(
                    row=row,
                    pending_reason=f"retry_pending_requeue_publish_failed:{row.get('error_type')}",
                    fix_applied="requeue_by_error_type_to_input_topic",
                    dry_run=dry_run,
                )
                logging.warning(
                    "Requeue publish failed; marked retry pending for error_type: %s",
                    {**row_for_log, "error_type": row.get("error_type")},
                )
                continue

            message, parse_error, parse_notes = _parse_message_with_repairs(
                raw=row.get("original_message"),
                error_type=row.get("error_type"),
                enable_deserialization_repair=config.enable_deserialization_repair,
            )
            if parse_error:
                if _is_transport_error_type(row.get("error_type")):
                    parsed_for_manual: Any = row.get("original_message")
                    if isinstance(parsed_for_manual, str):
                        try:
                            parsed_for_manual = json.loads(parsed_for_manual)
                        except json.JSONDecodeError:
                            parsed_for_manual = {"_raw_original_message": parsed_for_manual}
                    elif not isinstance(parsed_for_manual, (dict, list)):
                        parsed_for_manual = {"_raw_original_message": str(parsed_for_manual)}

                    reason = (
                        "manual_intervention_unrecoverable_transport_error:"
                        f"{str(row.get('error_type') or '')}:{parse_error}"
                    )
                    repo.write_manual_intervention_result(
                        row=row,
                        fixed_message=parsed_for_manual,
                        reason=reason,
                        target_table=row.get("table_name"),
                    )
                    outcome = ReplayOutcome(
                        decision="DISCARD",
                        fix_applied="manual_intervention_unrecoverable_transport_error_route",
                        discard_reason=reason,
                        target_table=None,
                    )
                    repo.mark_terminal(
                        row,
                        outcome.decision,
                        outcome.fix_applied,
                        outcome.discard_reason,
                        dry_run=dry_run,
                    )
                    counters["discarded"] += 1
                    logging.warning("Routed unrecoverable transport error to manual intervention: %s", {**row_for_log, **asdict(outcome)})
                    continue

                discard_reason = parse_error
                outcome = ReplayOutcome(decision=decision, fix_applied="", discard_reason=discard_reason, target_table=None)
                repo.mark_terminal(row, outcome.decision, outcome.fix_applied, outcome.discard_reason, dry_run=dry_run)
                counters["discarded"] += 1
                logging.info("Discarded DLQ row: %s", {**row_for_log, **asdict(outcome)})
                continue

            source_table_name = (row.get("table_name") or "").strip()
            try:
                target_table = config.resolve_target_table(source_table_name)
            except ValueError:
                outcome = ReplayOutcome(
                    decision="DISCARD",
                    fix_applied="routing_config_validation",
                    discard_reason=f"unmapped_table_name:{source_table_name or 'empty'}",
                    target_table=None,
                )
                repo.mark_terminal(row, outcome.decision, outcome.fix_applied, outcome.discard_reason, dry_run=dry_run)
                counters["discarded"] += 1
                logging.warning("Discarded DLQ row by routing validation: %s", {**row_for_log, **asdict(outcome)})
                continue

            try:
                schema = repo.get_schema(target_table)
            except NotFound:
                attempted = _extract_attempted_table(row.get("error_details"))
                if attempted:
                    try:
                        target_table = config.resolve_target_table(attempted)
                    except ValueError:
                        outcome = ReplayOutcome(
                            decision="DISCARD",
                            fix_applied="routing_config_validation",
                            discard_reason=f"unmapped_attempted_table:{attempted}",
                            target_table=None,
                        )
                        repo.mark_terminal(
                            row,
                            outcome.decision,
                            outcome.fix_applied,
                            outcome.discard_reason,
                            dry_run=dry_run,
                        )
                        counters["discarded"] += 1
                        logging.warning("Discarded DLQ row by attempted-table routing validation: %s", {**row_for_log, **asdict(outcome)})
                        continue
                    try:
                        schema = repo.get_schema(target_table)
                    except NotFound:
                        outcome = ReplayOutcome(
                            decision="DISCARD",
                            fix_applied="routing_config_validation",
                            discard_reason=f"destination_table_not_found:{target_table}",
                            target_table=target_table,
                        )
                        repo.mark_terminal(
                            row,
                            outcome.decision,
                            outcome.fix_applied,
                            outcome.discard_reason,
                            dry_run=dry_run,
                        )
                        counters["discarded"] += 1
                        logging.warning("Discarded DLQ row by missing destination table: %s", {**row_for_log, **asdict(outcome)})
                        continue
                else:
                    outcome = ReplayOutcome(
                        decision="DISCARD",
                        fix_applied="routing_config_validation",
                        discard_reason=f"destination_table_not_found:{target_table}",
                        target_table=target_table,
                    )
                    repo.mark_terminal(
                        row,
                        outcome.decision,
                        outcome.fix_applied,
                        outcome.discard_reason,
                        dry_run=dry_run,
                    )
                    counters["discarded"] += 1
                    logging.warning("Discarded DLQ row by missing destination table: %s", {**row_for_log, **asdict(outcome)})
                    continue

            if (not fix_only) and _is_operational_bq_insert_error(row.get("error_type"), row.get("error_message")):
                filtered_payload = {k: v for k, v in (message or {}).items() if k in schema}
                try:
                    repo.merge_payload(
                        table_id=target_table,
                        payload=filtered_payload,
                        schema_by_name=schema,
                        merge_key=config.merge_key,
                        dry_run=dry_run,
                    )
                    outcome = ReplayOutcome(
                        decision="REPLAY",
                        fix_applied="operational_error_direct_merge_attempt",
                        discard_reason=None,
                        target_table=target_table,
                    )
                    repo.mark_terminal(row, outcome.decision, outcome.fix_applied, outcome.discard_reason, dry_run=dry_run)
                    counters["replayed"] += 1
                    logging.info("Replayed DLQ row via operational direct merge: %s", {**row_for_log, **asdict(outcome)})
                    continue
                except Exception as merge_exc:
                    merge_err = str(merge_exc)
                    if _is_payload_merge_error(merge_err):
                        # Fallback: try full fix pipeline based on the fresh merge failure reason.
                        effective_error_message = merge_err
                        logging.info(
                            "Operational direct merge failed with payload-like error; falling back to fix path: %s",
                            {**row_for_log, "merge_error": merge_err},
                        )
                    elif _is_operational_merge_error(merge_err):
                        if retry_count + 1 >= config.max_retry_count:
                            parsed_for_manual: Any = row.get("original_message")
                            if isinstance(parsed_for_manual, str):
                                try:
                                    parsed_for_manual = json.loads(parsed_for_manual)
                                except json.JSONDecodeError:
                                    parsed_for_manual = {"_raw_original_message": parsed_for_manual}
                            elif not isinstance(parsed_for_manual, (dict, list)):
                                parsed_for_manual = {"_raw_original_message": str(parsed_for_manual)}
                            reason = f"manual_intervention_poison_pill_dlq_requeue_limit_hit:{retry_count + 1}"
                            repo.write_manual_intervention_result(
                                row=row,
                                fixed_message=parsed_for_manual,
                                reason=reason,
                                target_table=target_table,
                            )
                            outcome = ReplayOutcome(
                                decision="DISCARD",
                                fix_applied="manual_intervention_poison_pill_dlq_requeue_limit_route",
                                discard_reason=reason,
                                target_table=target_table,
                            )
                            repo.mark_terminal(row, outcome.decision, outcome.fix_applied, outcome.discard_reason, dry_run=dry_run)
                            counters["discarded"] += 1
                            logging.warning(
                                "Discarded DLQ row by requeue-limit guard: %s",
                                {**row_for_log, **asdict(outcome), "merge_error": merge_err},
                            )
                            continue
                        if config.retry_input_topic:
                            published = repo.publish_to_retry_input_topic(
                                payload=row.get("original_message"),
                                row=row,
                                reason="operational_merge_error",
                                dry_run=dry_run,
                            )
                            if published:
                                repo.mark_requeued_terminal(
                                    row=row,
                                    reason="requeued_after_operational_merge_error",
                                    fix_applied="operational_error_requeued_to_input_topic",
                                    dry_run=dry_run,
                                )
                                counters["replayed"] += 1
                                logging.info(
                                    "Requeued DLQ row to input topic after operational merge error: %s",
                                    {**row_for_log, "merge_error": merge_err},
                                )
                                continue
                        repo.mark_retry_pending(
                            row=row,
                            pending_reason=f"retry_pending_operational_merge_error:{merge_err[:200]}",
                            fix_applied="operational_error_direct_merge_attempt",
                            dry_run=dry_run,
                        )
                        logging.warning(
                            "Operational direct merge failed; marked pending retry: %s",
                            {**row_for_log, "retry_count_next": retry_count + 1, "merge_error": merge_err},
                        )
                        continue
                    else:
                        raise

            fixed_payload, fix_notes = apply_deterministic_fixes(
                message or {},
                row.get("error_type"),
                schema,
                failed_timestamp=row.get("failed_timestamp"),
                error_details=row.get("error_details"),
                error_message=effective_error_message,
            )
            merge_key_note = _try_fill_missing_merge_key(
                fixed_payload,
                config.merge_key,
                row.get("error_details"),
            )
            if merge_key_note:
                fix_notes.append(merge_key_note)
            expanded_payloads, fanout_notes = _expand_fanout_payloads(
                payload=fixed_payload,
                schema_by_name=schema,
                merge_key=config.merge_key,
                fanout_enabled=config.fanout_enabled,
                fanout_fields=config.fanout_fields,
            )
            fix_notes.extend(fanout_notes)
            fix_notes.extend(parse_notes)

            manual_intervention_notes = [
                n for n in fix_notes if n.startswith("manual_intervention_required_")
            ]
            if manual_intervention_notes:
                reason = ";".join(manual_intervention_notes[:5])
                repo.write_manual_intervention_result(
                    row=row,
                    fixed_message=fixed_payload,
                    reason=reason,
                    target_table=target_table,
                )
                outcome = ReplayOutcome(
                    decision="DISCARD",
                    fix_applied=",".join(fix_notes),
                    discard_reason=reason,
                    target_table=target_table,
                )
                repo.mark_terminal(row, outcome.decision, outcome.fix_applied, outcome.discard_reason, dry_run=dry_run)
                counters["discarded"] += 1
                logging.warning("Routed DLQ row to manual intervention: %s", {**row_for_log, **asdict(outcome)})
                continue

            repo.write_fix_only_result(
                row=row,
                original_message=message,
                fixed_message=fixed_payload,
                fixes_applied=fix_notes,
                target_table=target_table,
                fanout_payloads_count=len(expanded_payloads),
                decision="FIX_ONLY" if fix_only else "PENDING_REPLAY",
                discard_reason=None,
            )
            if fix_only:
                counters["fixed_only"] += 1
                logging.info(
                    "Fix-only result: %s",
                    {
                        **row_for_log,
                        "target_table": target_table,
                        "schema_columns_count": len(schema),
                        "fix_applied": ",".join(fix_notes),
                        "original_message": message,
                        "fixed_message": fixed_payload,
                        "fanout_payloads_count": len(expanded_payloads),
                    },
                )
                continue

            filtered_payloads: list[dict[str, Any]] = []
            for expanded_payload in expanded_payloads:
                filtered_payload = {k: v for k, v in expanded_payload.items() if k in schema}

                errors = validate_payload(
                    payload=filtered_payload,
                    schema_by_name=schema,
                    required_fields=config.required_fields,
                    regex_rules=config.regex_rules,
                    merge_key=config.merge_key,
                )
                if errors:
                    discard_reason = ";".join(errors[:5])
                    outcome = ReplayOutcome(
                        decision="DISCARD",
                        fix_applied=",".join(fix_notes),
                        discard_reason=discard_reason,
                        target_table=target_table,
                    )
                    repo.mark_terminal(row, outcome.decision, outcome.fix_applied, outcome.discard_reason, dry_run=dry_run)
                    counters["discarded"] += 1
                    logging.info("Discarded DLQ row: %s", {**row_for_log, **asdict(outcome)})
                    break

                filtered_payloads.append(filtered_payload)
            else:
                pending = pending_replays_by_target.setdefault(
                    target_table,
                    {
                        "schema": schema,
                        "entries": [],
                    },
                )
                pending["entries"].append(
                    {
                        "row": row,
                        "row_for_log": row_for_log,
                        "fix_notes": list(fix_notes),
                        "payloads": filtered_payloads,
                    }
                )
                continue

            continue

        except Exception as exc:
            counters["failed"] += 1
            try:
                repo.mark_retry_pending(
                    row=row,
                    pending_reason=f"retry_pending_unexpected_processing_error:{str(exc)[:200]}",
                    fix_applied="unexpected_processing_error",
                    dry_run=dry_run,
                )
            except Exception:
                pass
            logging.exception(
                "Failed processing DLQ row",
                extra={
                    "row": row_for_log,
                    "target_table": target_table,
                    "decision": decision,
                    "discard_reason": discard_reason,
                    "fix_applied": ",".join(fix_notes),
                },
            )

    for target_table, pending in pending_replays_by_target.items():
        schema = pending["schema"]
        entries = pending["entries"]
        payloads_to_merge = [payload for entry in entries for payload in entry["payloads"]]
        try:
            logging.info(
                "Running set-based merge: target_table=%s replay_rows=%s payloads=%s merge_key=%s",
                target_table,
                len(entries),
                len(payloads_to_merge),
                config.merge_key,
            )
            repo.merge_payloads_set_based(
                table_id=target_table,
                payloads=payloads_to_merge,
                schema_by_name=schema,
                merge_key=config.merge_key,
                dry_run=dry_run,
            )
            for entry in entries:
                decision = "FIX_AND_REPLAY" if entry["fix_notes"] else "REPLAY"
                outcome = ReplayOutcome(
                    decision=decision,
                    fix_applied=",".join(entry["fix_notes"]),
                    discard_reason=None,
                    target_table=target_table,
                )
                repo.mark_terminal(
                    entry["row"],
                    outcome.decision,
                    outcome.fix_applied,
                    outcome.discard_reason,
                    dry_run=dry_run,
                )
                if decision == "FIX_AND_REPLAY":
                    counters["fixed_and_replayed"] += 1
                else:
                    counters["replayed"] += 1
                logging.info("Replayed DLQ row: %s", {**entry["row_for_log"], **asdict(outcome)})
        except Exception:
            logging.exception(
                "Set-based merge failed for target table; falling back to row-by-row merge",
                extra={"target_table": target_table, "payload_count": len(payloads_to_merge)},
            )
            for entry in entries:
                try:
                    for payload in entry["payloads"]:
                        repo.merge_payload(
                            table_id=target_table,
                            payload=payload,
                            schema_by_name=schema,
                            merge_key=config.merge_key,
                            dry_run=dry_run,
                        )
                    decision = "FIX_AND_REPLAY" if entry["fix_notes"] else "REPLAY"
                    outcome = ReplayOutcome(
                        decision=decision,
                        fix_applied=",".join(entry["fix_notes"]),
                        discard_reason=None,
                        target_table=target_table,
                    )
                    repo.mark_terminal(
                        entry["row"],
                        outcome.decision,
                        outcome.fix_applied,
                        outcome.discard_reason,
                        dry_run=dry_run,
                    )
                    if decision == "FIX_AND_REPLAY":
                        counters["fixed_and_replayed"] += 1
                    else:
                        counters["replayed"] += 1
                    logging.info("Replayed DLQ row: %s", {**entry["row_for_log"], **asdict(outcome)})
                except Exception:
                    counters["failed"] += 1
                    logging.exception(
                        "Failed processing DLQ row during row-by-row fallback",
                        extra={
                            "row": entry["row_for_log"],
                            "target_table": target_table,
                            "fix_applied": ",".join(entry["fix_notes"]),
                        },
                    )

    logging.info("Replay summary: %s", counters)
    return counters


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="DLQ replay Cloud Run Job")
    parser.add_argument("--table-name", default=None, help="Optional table_name filter from DLQ")
    parser.add_argument("--limit", type=int, default=None, help="Max DLQ rows for this run")
    parser.add_argument("--dry-run", action="store_true", help="Compute decisions without MERGE or DLQ updates")
    parser.add_argument(
        "--fix-only",
        action="store_true",
        help="Read and apply deterministic fixes only (no schema lookup, validation, merge, or DLQ updates)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = AppConfig.from_env()
    _setup_logging(config.log_level)
    logging.info(
        "Replay config: merge_key=%s dlq_table=%s default_target_table=%s fix_only=%s dry_run=%s",
        config.merge_key,
        config.dlq_table,
        config.default_target_table,
        args.fix_only,
        args.dry_run or config.dry_run,
    )

    counters = process_batch(
        config=config,
        table_name=args.table_name,
        limit=args.limit,
        dry_run_override=True if args.dry_run else None,
        fix_only=args.fix_only,
    )

    # Exit non-zero only for processing errors to surface in scheduler alerts.
    if counters["failed"] > 0:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
