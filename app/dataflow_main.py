import argparse
import json
import logging
from typing import Any

import apache_beam as beam
from apache_beam.io.filesystems import FileSystems
from apache_beam.metrics import Metrics
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from google.api_core.exceptions import NotFound

from app.bigquery_repo import BigQueryRepository
from app.config import AppConfig
from app.main import (
    _expand_fanout_payloads,
    _extract_attempted_table,
    _is_cdc_pending_error_type,
    _is_dlq_publish_failed_error_type,
    _is_explicit_poison_pill_error_type,
    _is_manual_intervention_error_type,
    _is_observability_manual_error_type,
    _is_operational_bq_insert_error,
    _is_operational_merge_error,
    _is_partition_decorator_failure,
    _is_payload_merge_error,
    _is_requeue_error_type,
    _is_transport_error_type,
    _parse_message_with_repairs,
    _setup_logging,
    _try_fill_missing_merge_key,
)
from app.models import ReplayOutcome, row_to_dict
from app.fixes import apply_deterministic_fixes
from app.validators import validate_payload



class ReplayRowDoFn(beam.DoFn):
    def __init__(self, config_dict: dict[str, Any], fix_only: bool = False):
        self._config_dict = config_dict
        self._fix_only = fix_only
        self._repo: BigQueryRepository | None = None

        self._fetched = Metrics.counter("dlq_replay", "fetched")
        self._fixed_only = Metrics.counter("dlq_replay", "fixed_only")
        self._replayed = Metrics.counter("dlq_replay", "replayed")
        self._fixed_and_replayed = Metrics.counter("dlq_replay", "fixed_and_replayed")
        self._discarded = Metrics.counter("dlq_replay", "discarded")
        self._cdc_pending_routed = Metrics.counter("dlq_replay", "cdc_pending_routed")
        self._manual_intervention_routed = Metrics.counter("dlq_replay", "manual_intervention_routed")
        self._failed = Metrics.counter("dlq_replay", "failed")

    def setup(self) -> None:
        config = AppConfig(**self._config_dict)
        _setup_logging(config.log_level)
        self._repo = BigQueryRepository(config)

    def _route_to_manual_intervention(self, row: dict[str, Any], reason: str, parsed_for_manual: Any = None):
        """Routes the row to manual intervention."""
        assert self._repo is not None
        config = self._repo.config

        if parsed_for_manual is None:
            parsed_for_manual = row.get("original_message")
            if isinstance(parsed_for_manual, str):
                try:
                    parsed_for_manual = json.loads(parsed_for_manual)
                except json.JSONDecodeError:
                    parsed_for_manual = {"_raw_original_message": parsed_for_manual}
            elif not isinstance(parsed_for_manual, (dict, list)):
                parsed_for_manual = {"_raw_original_message": str(parsed_for_manual)}

        self._repo.write_manual_intervention_result(
            row=row,
            fixed_message=parsed_for_manual,
            reason=reason,
            target_table=row.get("table_name"),
        )
        self._manual_intervention_routed.inc()
        outcome = ReplayOutcome(
            decision="DISCARD",
            fix_applied="manual_intervention_route",
            discard_reason=reason,
            target_table=None,
        )
        self._repo.mark_terminal(row, outcome.decision, outcome.fix_applied, outcome.discard_reason, dry_run=config.dry_run)
        self._discarded.inc()
        logging.warning("Routed to manual intervention: %s", {**self._row_for_log(row), "discard_reason": outcome.discard_reason})

    def _requeue_to_input_topic(self, row: dict[str, Any], reason: str):
        """Requeues the row to the input topic."""
        assert self._repo is not None
        config = self._repo.config

        published = self._repo.publish_to_retry_input_topic(
            payload=row.get("original_message"),
            row=row,
            reason=reason,
            dry_run=config.dry_run,
        )
        if published:
            self._repo.mark_requeued_terminal(
                row=row,
                reason=reason,
                fix_applied="requeue_to_input_topic",
                dry_run=config.dry_run,
            )
            self._replayed.inc()
            logging.info("Requeued to input topic: %s", {**self._row_for_log(row), "reason": reason})
            return True
        self._repo.mark_retry_pending(
            row=row,
            pending_reason=f"retry_pending_requeue_publish_failed:{reason}",
            fix_applied="requeue_to_input_topic",
            dry_run=config.dry_run,
        )
        logging.warning("Requeue publish failed; marked retry pending: %s", {**self._row_for_log(row), "reason": reason})
        return False

    def _mark_terminal_discard(self, row: dict[str, Any], reason: str, fix_applied: str):
        assert self._repo is not None
        config = self._repo.config
        outcome = ReplayOutcome(decision="DISCARD", fix_applied=fix_applied, discard_reason=reason, target_table=None)
        self._repo.mark_terminal(row, outcome.decision, outcome.fix_applied, outcome.discard_reason, dry_run=config.dry_run)
        self._discarded.inc()
        logging.info("Discarded DLQ row: %s", {**self._row_for_log(row), **asdict(outcome)})

    def process(self, row_obj: dict[str, Any]):
        assert self._repo is not None
        config = self._repo.config

        self._fetched.inc()
        row = row_to_dict(row_obj)
        row_for_log = {
            "table_name": row.get("table_name"),
            "error_type": row.get("error_type"),
            "failed_timestamp": str(row.get("failed_timestamp")),
        }
        
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
                reason = f"manual_intervention_poison_pill_error_type:{str(row.get('error_type') or '')}"
                self._route_to_manual_intervention(row, reason)
                return
            if retry_count >= config.max_retry_count:
                reason = f"manual_intervention_poison_pill_max_retry_exceeded:{retry_count}"
                self._repo.write_manual_intervention_result(
                    row=row,
                    fixed_message=parsed_for_manual,
                    reason=reason,
                    target_table=row.get("table_name"),
                )
                self._manual_intervention_routed.inc()
                outcome = ReplayOutcome(
                    decision="DISCARD",
                    fix_applied="manual_intervention_poison_pill_max_retry_route",
                    discard_reason=reason,
                    target_table=None,
                )
                self._repo.mark_terminal(row, outcome.decision, outcome.fix_applied, outcome.discard_reason, dry_run=config.dry_run)
                self._discarded.inc()
                logging.warning("Discarded DLQ row by retry guard: %s", {**row_for_log, "discard_reason": outcome.discard_reason})
                return
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
                self._repo.write_manual_intervention_result(
                    row=row,
                    fixed_message=parsed_for_manual,
                    reason=reason,
                    target_table=row.get("table_name"),
                )
                self._manual_intervention_routed.inc()
                outcome = ReplayOutcome(
                    decision="DISCARD",
                    fix_applied="manual_intervention_poison_pill_reprocessing_loop_route",
                    discard_reason=reason,
                    target_table=None,
                )
                self._repo.mark_terminal(row, outcome.decision, outcome.fix_applied, outcome.discard_reason, dry_run=config.dry_run)
                self._discarded.inc()
                logging.warning("Discarded DLQ row by loop-detection guard: %s", {**row_for_log, "discard_reason": outcome.discard_reason})
                return

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
                self._repo.route_cdc_pending(
                    row=row,
                    payload=cdc_payload,
                    route_reason=route_reason,
                    merge_key=config.merge_key,
                    max_retry_count=config.max_retry_count,
                    dry_run=config.dry_run,
                )
                outcome = ReplayOutcome(
                    decision="CDC_PENDING_ROUTED",
                    fix_applied="cdc_pending_route",
                    discard_reason=route_reason,
                    target_table=str(row.get("table_name") or ""),
                )
                self._repo.mark_terminal(
                    row,
                    outcome.decision,
                    outcome.fix_applied,
                    outcome.discard_reason,
                    dry_run=config.dry_run,
                )
                self._cdc_pending_routed.inc()
                return




            if _is_dlq_publish_failed_error_type(row.get("error_type")):
                parsed_for_manual: Any = row.get("original_message")
                if isinstance(parsed_for_manual, str):
                    try:
                        parsed_for_manual = json.loads(parsed_for_manual)
                    except json.JSONDecodeError:
                        parsed_for_manual = {"_raw_original_message": parsed_for_manual}
                reason = f"manual_intervention_dlq_publish_failed:{str(row.get('error_message') or '')[:200]}"
                self._repo.write_manual_intervention_result(
                    row=row,
                    fixed_message=parsed_for_manual,
                    reason=reason,
                    target_table=row.get("table_name"),
                )
                self._manual_intervention_routed.inc()
                outcome = ReplayOutcome(
                    decision="DISCARD",
                    fix_applied="manual_intervention_dlq_publish_failed_route",
                    discard_reason=reason,
                    target_table=None,
                )
                self._repo.mark_terminal(
                    row,
                    outcome.decision,
                    outcome.fix_applied,
                    outcome.discard_reason,
                    dry_run=config.dry_run,
                )
                self._discarded.inc()
                return

            if _is_observability_manual_error_type(row.get("error_type")):
                parsed_for_manual: Any = row.get("original_message")
                if isinstance(parsed_for_manual, str):
                    try:
                        parsed_for_manual = json.loads(parsed_for_manual)
                    except json.JSONDecodeError:
                        parsed_for_manual = {"_raw_original_message": parsed_for_manual}
                reason = f"manual_intervention_observability_error_type:{row.get('error_type')}:{str(row.get('error_message') or '')[:200]}"
                self._repo.write_manual_intervention_result(
                    row=row,
                    fixed_message=parsed_for_manual,
                    reason=reason,
                    target_table=row.get("table_name"),
                )
                self._manual_intervention_routed.inc()
                outcome = ReplayOutcome(
                    decision="DISCARD",
                    fix_applied="manual_intervention_observability_error_type_route",
                    discard_reason=reason,
                    target_table=None,
                )
                self._repo.mark_terminal(
                    row,
                    outcome.decision,
                    outcome.fix_applied,
                    outcome.discard_reason,
                    dry_run=config.dry_run,
                )
                self._discarded.inc()
                return

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
                self._repo.write_manual_intervention_result(
                    row=row,
                    fixed_message=parsed_for_manual,
                    reason=reason,
                    target_table=row.get("table_name"),
                )
                self._manual_intervention_routed.inc()
                outcome = ReplayOutcome(
                    decision="DISCARD",
                    fix_applied="manual_intervention_partition_decorator_route",
                    discard_reason=reason,
                    target_table=None,
                )
                self._repo.mark_terminal(
                    row,
                    outcome.decision,
                    outcome.fix_applied,
                    outcome.discard_reason,
                    dry_run=config.dry_run,
                )
                self._discarded.inc()
                return

            if _is_manual_intervention_error_type(row.get("error_type")):
                parsed_for_manual: Any = row.get("original_message")
                if isinstance(parsed_for_manual, str):
                    try:
                        parsed_for_manual = json.loads(parsed_for_manual)
                    except json.JSONDecodeError:
                        parsed_for_manual = {"_raw_original_message": parsed_for_manual}
                reason = f"manual_intervention_error_type:{row.get('error_type')}:{str(row.get('error_message') or '')[:200]}"
                self._repo.write_manual_intervention_result(
                    row=row,
                    fixed_message=parsed_for_manual,
                    reason=reason,
                    target_table=row.get("table_name"),
                )
                self._manual_intervention_routed.inc()
                outcome = ReplayOutcome(
                    decision="DISCARD",
                    fix_applied="manual_intervention_error_type_route",
                    discard_reason=reason,
                    target_table=None,
                )
                self._repo.mark_terminal(
                    row,
                    outcome.decision,
                    outcome.fix_applied,
                    outcome.discard_reason,
                    dry_run=config.dry_run,
                )
                self._discarded.inc()
                return

            if config.retry_input_topic and _is_requeue_error_type(row.get("error_type")):
                published = self._repo.publish_to_retry_input_topic(
                    payload=row.get("original_message"),
                    row=row,
                    reason="requeue_error_type",
                    dry_run=config.dry_run,
                )
                if published:
                    self._repo.mark_requeued_terminal(
                        row=row,
                        reason=f"requeued_by_error_type:{row.get('error_type')}",
                        fix_applied="requeue_by_error_type_to_input_topic",
                        dry_run=config.dry_run,
                    )
                    self._replayed.inc()
                    return
                self._repo.mark_retry_pending(
                    row=row,
                    pending_reason=f"retry_pending_requeue_publish_failed:{row.get('error_type')}",
                    fix_applied="requeue_by_error_type_to_input_topic",
                    dry_run=config.dry_run,
                )
                return

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
                    self._repo.write_manual_intervention_result(
                        row=row,
                        fixed_message=parsed_for_manual,
                        reason=reason,
                        target_table=row.get("table_name"),
                    )
                    self._manual_intervention_routed.inc()
                    outcome = ReplayOutcome(
                        decision="DISCARD",
                        fix_applied="manual_intervention_unrecoverable_transport_error_route",
                        discard_reason=reason,
                        target_table=None,
                    )
                    self._repo.mark_terminal(
                        row,
                        outcome.decision,
                        outcome.fix_applied,
                        outcome.discard_reason,
                        dry_run=config.dry_run,
                    )
                    self._discarded.inc()
                    return

                discard_reason = parse_error
                outcome = ReplayOutcome(decision=decision, fix_applied="", discard_reason=discard_reason, target_table=None)
                self._repo.mark_terminal(row, outcome.decision, outcome.fix_applied, outcome.discard_reason, dry_run=config.dry_run)
                self._discarded.inc()
                return

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
                self._repo.mark_terminal(row, outcome.decision, outcome.fix_applied, outcome.discard_reason, dry_run=config.dry_run)
                self._discarded.inc()
                return

            try:
                schema = self._repo.get_schema(target_table)
            except NotFound:
                attempted = _extract_attempted_table(row.get("error_details"))
                if attempted:
                    try:
                        target_table = config.resolve_target_table(attempted)
                        schema = self._repo.get_schema(target_table)
                    except (ValueError, NotFound):
                        outcome = ReplayOutcome(
                            decision="DISCARD",
                            fix_applied="routing_config_validation",
                            discard_reason=f"unmapped_attempted_table:{attempted}",
                            target_table=None,
                        )
                        self._repo.mark_terminal(row, outcome.decision, outcome.fix_applied, outcome.discard_reason, dry_run=config.dry_run)
                        self._discarded.inc()
                        return
                else:
                    outcome = ReplayOutcome(
                        decision="DISCARD",
                        fix_applied="routing_config_validation",
                        discard_reason=f"destination_table_not_found:{target_table}",
                        target_table=target_table,
                    )
                    self._repo.mark_terminal(row, outcome.decision, outcome.fix_applied, outcome.discard_reason, dry_run=config.dry_run)
                    self._discarded.inc()
                    return

            if (not self._fix_only) and _is_operational_bq_insert_error(row.get("error_type"), row.get("error_message")):
                filtered_payload = {k: v for k, v in (message or {}).items() if k in schema}
                try:
                    self._repo.merge_payload(
                        table_id=target_table,
                        payload=filtered_payload,
                        schema_by_name=schema,
                        merge_key=config.merge_key,
                        dry_run=config.dry_run,
                    )
                    outcome = ReplayOutcome(
                        decision="REPLAY",
                        fix_applied="operational_error_direct_merge_attempt",
                        discard_reason=None,
                        target_table=target_table,
                    )
                    self._repo.mark_terminal(row, outcome.decision, outcome.fix_applied, outcome.discard_reason, dry_run=config.dry_run)
                    self._replayed.inc()
                    return
                except Exception as merge_exc:
                    merge_err = str(merge_exc)
                    if _is_payload_merge_error(merge_err):
                        effective_error_message = merge_err
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
                            self._repo.write_manual_intervention_result(
                                row=row,
                                fixed_message=parsed_for_manual,
                                reason=reason,
                                target_table=target_table,
                            )
                            self._manual_intervention_routed.inc()
                            outcome = ReplayOutcome(
                                decision="DISCARD",
                                fix_applied="manual_intervention_poison_pill_dlq_requeue_limit_route",
                                discard_reason=reason,
                                target_table=target_table,
                            )
                            self._repo.mark_terminal(
                                row,
                                outcome.decision,
                                outcome.fix_applied,
                                outcome.discard_reason,
                                dry_run=config.dry_run,
                            )
                            self._discarded.inc()
                            return
                        if config.retry_input_topic:
                            published = self._repo.publish_to_retry_input_topic(
                                payload=row.get("original_message"),
                                row=row,
                                reason="operational_merge_error",
                                dry_run=config.dry_run,
                            )
                            if published:
                                self._repo.mark_requeued_terminal(
                                    row=row,
                                    reason="requeued_after_operational_merge_error",
                                    fix_applied="operational_error_requeued_to_input_topic",
                                    dry_run=config.dry_run,
                                )
                                self._replayed.inc()
                                return
                        self._repo.mark_retry_pending(
                            row=row,
                            pending_reason=f"retry_pending_operational_merge_error:{merge_err[:200]}",
                            fix_applied="operational_error_direct_merge_attempt",
                            dry_run=config.dry_run,
                        )
                        # Keep pending for next retry cycle.
                        return
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
                self._repo.write_manual_intervention_result(
                    row=row,
                    fixed_message=fixed_payload,
                    reason=reason,
                    target_table=target_table,
                )
                self._manual_intervention_routed.inc()
                outcome = ReplayOutcome(
                    decision="DISCARD",
                    fix_applied=",".join(fix_notes),
                    discard_reason=reason,
                    target_table=target_table,
                )
                self._repo.mark_terminal(
                    row,
                    outcome.decision,
                    outcome.fix_applied,
                    outcome.discard_reason,
                    dry_run=config.dry_run,
                )
                self._discarded.inc()
                return

            self._repo.write_fix_only_result(
                row=row,
                original_message=message,
                fixed_message=fixed_payload,
                fixes_applied=fix_notes,
                target_table=target_table,
                fanout_payloads_count=len(expanded_payloads),
                decision="FIX_ONLY" if self._fix_only else "PENDING_REPLAY",
                discard_reason=None,
            )

            if self._fix_only:
                self._fixed_only.inc()
                return

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
                    outcome = ReplayOutcome(
                        decision="DISCARD",
                        fix_applied=",".join(fix_notes),
                        discard_reason=";".join(errors[:5]),
                        target_table=target_table,
                    )
                    self._repo.mark_terminal(row, outcome.decision, outcome.fix_applied, outcome.discard_reason, dry_run=config.dry_run)
                    self._discarded.inc()
                    return

                try:
                    self._repo.merge_payload(
                        table_id=target_table,
                        payload=filtered_payload,
                        schema_by_name=schema,
                        merge_key=config.merge_key,
                        dry_run=config.dry_run,
                    )
                except Exception as merge_exc:
                    merge_err = str(merge_exc)
                    if _is_payload_merge_error(merge_err):
                        outcome = ReplayOutcome(
                            decision="DISCARD",
                            fix_applied=",".join(fix_notes),
                            discard_reason=merge_err[:500],
                            target_table=target_table,
                        )
                        self._repo.mark_terminal(
                            row,
                            outcome.decision,
                            outcome.fix_applied,
                            outcome.discard_reason,
                            dry_run=config.dry_run,
                        )
                        self._discarded.inc()
                        return
                    if _is_operational_merge_error(merge_err):
                        self._repo.mark_retry_pending(
                            row=row,
                            pending_reason=f"retry_pending_operational_merge_error:{merge_err[:200]}",
                            fix_applied=",".join(fix_notes),
                            dry_run=config.dry_run,
                        )
                        # Keep pending for a future replay run.
                        return
                    raise

            outcome = ReplayOutcome(
                decision="FIX_AND_REPLAY" if fix_notes else "REPLAY",
                fix_applied=",".join(fix_notes),
                discard_reason=None,
                target_table=target_table,
            )
            self._repo.mark_terminal(row, outcome.decision, outcome.fix_applied, outcome.discard_reason, dry_run=config.dry_run)
            if outcome.decision == "FIX_AND_REPLAY":
                self._fixed_and_replayed.inc()
            else:
                self._replayed.inc()
            yield {"decision": outcome.decision, "table_name": target_table}

        except Exception as exc:
            self._failed.inc()
            try:
                self._repo.mark_retry_pending(
                    row=row,
                    pending_reason=f"retry_pending_unexpected_processing_error:{str(exc)[:200]}",
                    fix_applied="unexpected_processing_error",
                    dry_run=config.dry_run,
                )
            except Exception:
                pass
            logging.exception(
                "Failed processing DLQ row in Dataflow",
                extra={
                    "row": row_for_log,
                    "target_table": target_table,
                    "decision": decision,
                    "discard_reason": discard_reason,
                    "fix_applied": ",".join(fix_notes),
                },
            )


def _parse_csv(value: str) -> tuple[str, ...]:
    return tuple(x.strip() for x in (value or "").split(",") if x.strip())


def _row_for_log(row: dict[str, Any]) -> dict[str, Any]:
    return {"table_name": row.get("table_name"), "error_type": row.get("error_type"), "failed_timestamp": str(row.get("failed_timestamp"))}


def _load_yaml_config(path: str) -> dict[str, Any]:
    import yaml

    with FileSystems.open(path) as f:
        raw = yaml.safe_load(f.read().decode("utf-8")) or {}
    return {str(k): v for k, v in raw.items()}


def _make_app_config_dict(args: argparse.Namespace) -> dict[str, Any]:
    discard_error_types = _parse_csv(args.discard_error_types)
    if args.enable_deserialization_repair:
        discard_error_types = tuple(x for x in discard_error_types if x != "DESERIALIZATION_ERROR")

    required_fields = _parse_csv(args.required_fields) or (args.merge_key,)
    fanout_fields = _parse_csv(args.fanout_fields)
    regex_rules = json.loads(args.regex_rules_json or "{}")

    return {
        "project_id": args.project_id,
        "location": args.location,
        "dlq_table": args.dlq_table,
        "default_dataset": args.default_dataset or "",
        "default_target_table": args.default_target_table or "",
        "merge_key": args.merge_key,
        "batch_size": args.batch_size,
        "max_records": args.max_records,
        "max_retry_count": args.max_retry_count,
        "reprocessing_loop_retry_threshold": args.reprocessing_loop_retry_threshold,
        "retry_input_topic": args.retry_input_topic or "",
        "dlq_retry_count_attribute": args.dlq_retry_count_attribute or "dlq_retry_count",
        "dry_run": args.dry_run,
        "discard_error_types": discard_error_types,
        "required_fields": tuple(required_fields),
        "regex_rules": regex_rules,
        "dlq_id_column": args.dlq_id_column or "",
        "fanout_enabled": args.fanout_enabled,
        "fanout_fields": tuple(fanout_fields),
        "fix_only_output_table": args.fix_only_output_table or "",
        "manual_intervention_table": args.manual_intervention_table or "",
        "cdc_pending_table": args.cdc_pending_table or "",
        "enable_deserialization_repair": args.enable_deserialization_repair,
        "log_level": args.log_level.upper(),
    }


def _build_dlq_query(config: dict[str, Any], table_name: str | None, limit: int | None) -> str:
    def _sql_quote(s: str) -> str:
        return "'" + s.replace("\\", "\\\\").replace("'", "\\'") + "'"

    discard_values = [x for x in config["discard_error_types"] if x]
    discard_list = ", ".join([_sql_quote(x) for x in discard_values])
    columns = [
        "original_message",
        "error_type",
        "error_message",
        "error_details",
        "failed_timestamp",
        "pipeline_job_id",
        "retry_count",
        "table_name",
        "reprocessed",
        "reprocessed_timestamp",
    ]
    if config["dlq_id_column"]:
        columns.append(config["dlq_id_column"])

    table_filter = ""
    if table_name:
        table_filter = f" AND table_name = {_sql_quote(table_name)}"

    lim = int(limit) if limit else int(config["batch_size"])
    lim = min(lim, int(config["max_records"]))
    discard_filter = ""
    if discard_values:
        discard_filter = f"\n  AND error_type NOT IN ({discard_list})"

    return f"""
SELECT {', '.join(columns)}
FROM `{config["dlq_table"]}`
WHERE reprocessed = FALSE
  {discard_filter}
  {table_filter}
ORDER BY failed_timestamp
LIMIT {lim}
"""


def parse_args() -> tuple[argparse.Namespace, list[str]]:
    # Keep Beam/Dataflow flags (e.g. --project) in beam_args by disabling
    # argparse long-option abbreviation against app flags like --project-id.
    parser = argparse.ArgumentParser(description="DLQ replay Dataflow batch job", allow_abbrev=False)
    parser.add_argument("--config-yaml", default=None, help="Optional YAML file with replay config keys")
    parser.add_argument("--project-id", default="")
    parser.add_argument("--location", default="us-central1")
    parser.add_argument("--dlq-table", default="")
    parser.add_argument("--default-dataset", default="")
    parser.add_argument("--default-target-table", default="")
    parser.add_argument("--merge-key", default="event_id")
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--max-records", type=int, default=5000)
    parser.add_argument("--max-retry-count", type=int, default=5)
    parser.add_argument("--reprocessing-loop-retry-threshold", type=int, default=3)
    parser.add_argument("--retry-input-topic", default="")
    parser.add_argument("--dlq-retry-count-attribute", default="dlq_retry_count")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--discard-error-types", default="DESERIALIZATION_ERROR")
    parser.add_argument("--required-fields", default="event_id")
    parser.add_argument("--regex-rules-json", default='{"event_id":"^[A-Za-z0-9_.:-]+$"}')
    parser.add_argument("--dlq-id-column", default="")
    parser.add_argument("--fanout-enabled", action="store_true")
    parser.add_argument("--fanout-fields", default="")
    parser.add_argument("--fix-only-output-table", default="")
    parser.add_argument("--manual-intervention-table", default="")
    parser.add_argument("--cdc-pending-table", default="")
    parser.add_argument("--enable-deserialization-repair", action="store_true")
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument("--table-name", default=None, help="Optional DLQ table_name filter")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--fix-only", action="store_true")

    args, beam_args = parser.parse_known_args()
    if args.config_yaml:
        cfg = _load_yaml_config(args.config_yaml)
        for key, value in cfg.items():
            normalized = key.lower().replace("-", "_")
            if not hasattr(args, normalized):
                continue
            current = getattr(args, normalized)
            if current in ("", None, False) or (
                isinstance(current, int) and current in {0, 3, 5, 500, 5000}
            ):
                if isinstance(current, bool):
                    setattr(args, normalized, str(value).strip().lower() == "true")
                elif isinstance(current, int):
                    setattr(args, normalized, int(value))
                else:
                    setattr(args, normalized, value)
    return args, beam_args


def run() -> None:
    args, beam_args = parse_args()
    config_dict = _make_app_config_dict(args)
    _setup_logging(config_dict["log_level"])

    missing = [k for k in ("project_id", "dlq_table") if not config_dict[k]]
    if missing:
        raise ValueError(f"Missing required options: {', '.join(missing)}")

    query = _build_dlq_query(config_dict, args.table_name, args.limit)
    logging.info("Starting Dataflow replay with query limit and filter applied.")

    pipeline_options = PipelineOptions(beam_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "ReadDLQRows" >> beam.io.ReadFromBigQuery(query=query, use_standard_sql=True)
            | "ProcessReplayRows" >> beam.ParDo(ReplayRowDoFn(config_dict=config_dict, fix_only=args.fix_only))
        )



if __name__ == "__main__":
    run()
