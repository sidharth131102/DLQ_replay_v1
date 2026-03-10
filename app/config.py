import json
import os
from dataclasses import dataclass


@dataclass(frozen=True)
class AppConfig:
    project_id: str
    location: str
    dlq_table: str
    default_dataset: str
    default_target_table: str
    merge_key: str
    batch_size: int
    max_records: int
    max_retry_count: int
    reprocessing_loop_retry_threshold: int
    retry_input_topic: str
    dlq_retry_count_attribute: str
    dry_run: bool
    discard_error_types: tuple[str, ...]
    required_fields: tuple[str, ...]
    regex_rules: dict[str, str]
    dlq_id_column: str
    fanout_enabled: bool
    fanout_fields: tuple[str, ...]
    fix_only_output_table: str
    manual_intervention_table: str
    cdc_pending_table: str
    enable_deserialization_repair: bool
    log_level: str
    dlq_update_grace_minutes: int

    @staticmethod
    def from_env() -> "AppConfig":
        project_id = os.getenv("PROJECT_ID", "").strip()
        location = os.getenv("LOCATION", "us-central1").strip()
        dlq_table = os.getenv("DLQ_TABLE", "").strip()
        default_dataset = os.getenv("DEFAULT_DATASET", "").strip()
        default_target_table = os.getenv("DEFAULT_TARGET_TABLE", "").strip()
        merge_key = os.getenv("MERGE_KEY", "event_id").strip()
        batch_size = int(os.getenv("BATCH_SIZE", "500"))
        max_records = int(os.getenv("MAX_RECORDS", str(batch_size)))
        max_retry_count = int(os.getenv("MAX_RETRY_COUNT", "5"))
        reprocessing_loop_retry_threshold = int(os.getenv("REPROCESSING_LOOP_RETRY_THRESHOLD", "3"))
        retry_input_topic = os.getenv("RETRY_INPUT_TOPIC", "").strip()
        dlq_retry_count_attribute = os.getenv("DLQ_RETRY_COUNT_ATTRIBUTE", "dlq_retry_count").strip()
        dry_run = os.getenv("DRY_RUN", "false").strip().lower() == "true"
        discard_error_types = tuple(
            x.strip() for x in os.getenv("DISCARD_ERROR_TYPES", "DESERIALIZATION_ERROR").split(",") if x.strip()
        )
        enable_deserialization_repair = (
            os.getenv("ENABLE_DESERIALIZATION_REPAIR", "false").strip().lower() == "true"
        )
        if enable_deserialization_repair:
            discard_error_types = tuple(x for x in discard_error_types if x != "DESERIALIZATION_ERROR")
        required_fields = tuple(
            x.strip() for x in os.getenv("REQUIRED_FIELDS", merge_key).split(",") if x.strip()
        )
        regex_rules_raw = os.getenv("REGEX_RULES_JSON", "{}").strip() or "{}"
        regex_rules = json.loads(regex_rules_raw)
        dlq_id_column = os.getenv("DLQ_ID_COLUMN", "").strip()
        fanout_enabled = os.getenv("FANOUT_ENABLED", "false").strip().lower() == "true"
        fanout_fields = tuple(
            x.strip() for x in os.getenv("FANOUT_FIELDS", "").split(",") if x.strip()
        )
        fix_only_output_table = os.getenv("FIX_ONLY_OUTPUT_TABLE", "").strip()
        manual_intervention_table = os.getenv("MANUAL_INTERVENTION_TABLE", "").strip()
        cdc_pending_table = os.getenv("CDC_PENDING_TABLE", "").strip()
        log_level = os.getenv("LOG_LEVEL", "INFO").strip().upper()
        dlq_update_grace_minutes = int(os.getenv("DLQ_UPDATE_GRACE_MINUTES", "90"))

        missing = [
            name
            for name, value in (
                ("PROJECT_ID", project_id),
                ("DLQ_TABLE", dlq_table),
            )
            if not value
        ]
        if missing:
            raise ValueError(f"Missing required env vars: {', '.join(missing)}")

        return AppConfig(
            project_id=project_id,
            location=location,
            dlq_table=dlq_table,
            default_dataset=default_dataset,
            default_target_table=default_target_table,
            merge_key=merge_key,
            batch_size=batch_size,
            max_records=max_records,
            max_retry_count=max_retry_count,
            reprocessing_loop_retry_threshold=reprocessing_loop_retry_threshold,
            retry_input_topic=retry_input_topic,
            dlq_retry_count_attribute=dlq_retry_count_attribute,
            dry_run=dry_run,
            discard_error_types=discard_error_types,
            required_fields=required_fields,
            regex_rules=regex_rules,
            dlq_id_column=dlq_id_column,
            fanout_enabled=fanout_enabled,
            fanout_fields=fanout_fields,
            fix_only_output_table=fix_only_output_table,
            manual_intervention_table=manual_intervention_table,
            cdc_pending_table=cdc_pending_table,
            enable_deserialization_repair=enable_deserialization_repair,
            log_level=log_level,
            dlq_update_grace_minutes=dlq_update_grace_minutes,
        )

    def resolve_target_table(self, table_name: str) -> str:
        raw = (table_name or "").strip()
        if not raw:
            if not self.default_target_table:
                raise ValueError("No target table on row and DEFAULT_TARGET_TABLE is empty")
            raw = self.default_target_table

        # Accept both project.dataset.table and legacy project:dataset.table.
        normalized = raw.replace(":", ".")

        if normalized.count(".") == 2:
            return normalized
        if normalized.count(".") == 1:
            return f"{self.project_id}.{normalized}"
        if self.default_dataset:
            return f"{self.project_id}.{self.default_dataset}.{normalized}"
        raise ValueError(f"Cannot resolve target table: {raw}")
