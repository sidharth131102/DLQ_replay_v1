import json
import hashlib
import logging
import uuid
import time
from datetime import datetime, timezone
from typing import Any

from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from google.cloud import pubsub_v1

from app.config import AppConfig


def _to_string_value(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (dict, list)):
        return json.dumps(value, separators=(",", ":"), ensure_ascii=False)
    return str(value)


def _scalar_sql_type(field: bigquery.SchemaField) -> str:
    t = field.field_type.upper()
    if t in {"INT64", "INTEGER"}:
        return "INT64"
    if t in {"FLOAT64", "FLOAT"}:
        return "FLOAT64"
    if t in {"BOOL", "BOOLEAN"}:
        return "BOOL"
    if t in {"NUMERIC", "BIGNUMERIC", "STRING", "BYTES", "TIMESTAMP", "DATE", "DATETIME", "TIME", "JSON"}:
        return t
    return t


def _field_sql_type(field: bigquery.SchemaField) -> str:
    if field.field_type.upper() == "RECORD":
        inner = ", ".join([f"`{f.name}` {_field_sql_type(f)}" for f in field.fields])
        struct_type = f"STRUCT<{inner}>"
        if field.mode == "REPEATED":
            return f"ARRAY<{struct_type}>"
        return struct_type

    base = _scalar_sql_type(field)
    if field.mode == "REPEATED":
        return f"ARRAY<{base}>"
    return base


def _as_sql_expr(field: bigquery.SchemaField, param_name: str) -> str:
    field_type = field.field_type.upper()
    if field.mode == "REPEATED" or field_type == "RECORD":
        # Complex types are passed as JSON text and cast to exact target type.
        return f"CAST(PARSE_JSON(@{param_name}) AS {_field_sql_type(field)})"
    if field_type in {"STRING", "BYTES"}:
        return f"@{param_name}"
    if field_type in {"INT64", "INTEGER"}:
        return f"CAST(@{param_name} AS INT64)"
    if field_type in {"FLOAT64", "FLOAT"}:
        return f"CAST(@{param_name} AS FLOAT64)"
    if field_type == "NUMERIC":
        return f"CAST(@{param_name} AS NUMERIC)"
    if field_type == "BIGNUMERIC":
        return f"CAST(@{param_name} AS BIGNUMERIC)"
    if field_type in {"BOOL", "BOOLEAN"}:
        return f"CAST(@{param_name} AS BOOL)"
    if field_type == "TIMESTAMP":
        return f"TIMESTAMP(@{param_name})"
    if field_type == "DATE":
        return f"DATE(@{param_name})"
    if field_type == "DATETIME":
        return f"DATETIME(@{param_name})"
    if field_type == "TIME":
        return f"TIME(@{param_name})"
    if field_type == "JSON":
        return f"PARSE_JSON(@{param_name})"
    return f"@{param_name}"


class BigQueryRepository:
    def __init__(self, config: AppConfig):
        self.config = config
        self.client = bigquery.Client(project=config.project_id)
        self._dlq_table = self.client.get_table(config.dlq_table)
        self._dlq_columns = {field.name for field in self._dlq_table.schema}
        self._schema_cache: dict[str, dict[str, bigquery.SchemaField]] = {}
        self._fix_only_output_table_ready = False
        self._manual_intervention_table_ready = False
        self._cdc_pending_table_ready = False
        self._publisher = pubsub_v1.PublisherClient() if config.retry_input_topic else None

    @staticmethod
    def _next_retry_count_from_row(row: dict[str, Any]) -> int:
        raw = row.get("retry_count")
        try:
            current = int(raw) if raw is not None else 0
        except (TypeError, ValueError):
            current = 0
        return max(0, current) + 1

    def _resolve_cdc_pending_table(self) -> str:
        if self.config.cdc_pending_table:
            return self.config.resolve_target_table(self.config.cdc_pending_table)
        # Default alongside DLQ table dataset.
        project, dataset, _ = self.config.dlq_table.replace(":", ".").split(".")
        return f"{project}.{dataset}.cdc_pending_replay"

    @staticmethod
    def _to_iso_ts(value: Any) -> str | None:
        if value is None:
            return None
        if isinstance(value, datetime):
            dt = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return None
            try:
                if text.endswith("Z"):
                    dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
                else:
                    dt = datetime.fromisoformat(text)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
            except ValueError:
                return None
        return None

    @staticmethod
    def _normalize_cdc_operation(raw_op: Any) -> str | None:
        if raw_op is None:
            return None
        op = str(raw_op).strip().upper()
        mapping = {
            "I": "INSERT",
            "C": "INSERT",
            "INSERT": "INSERT",
            "U": "UPDATE",
            "UPDATE": "UPDATE",
            "D": "DELETE",
            "DELETE": "DELETE",
        }
        return mapping.get(op)

    @staticmethod
    def _parse_int(value: Any) -> int | None:
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _payload_hash(payload: Any) -> str:
        normalized = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        return hashlib.sha1(normalized.encode("utf-8")).hexdigest()

    def _ensure_cdc_pending_table(self) -> None:
        if self._cdc_pending_table_ready:
            return
        table_id = self._resolve_cdc_pending_table()
        try:
            self.client.get_table(table_id)
            self._cdc_pending_table_ready = True
            return
        except NotFound:
            pass

        schema = [
            bigquery.SchemaField("pending_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("pending_key", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("source_dlq_table", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("source_dlq_row_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("target_table", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("merge_key", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("merge_key_value", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("operation", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("event_version", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("event_timestamp", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("event_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("payload_json", "JSON", mode="NULLABLE"),
            bigquery.SchemaField("payload_hash", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("error_type", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("error_message", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("route_reason", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("retry_count", "INT64", mode="REQUIRED"),
            bigquery.SchemaField("max_retry_count", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("first_seen_at", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("last_attempt_at", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("next_retry_at", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("resolved_at", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("resolution", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("manual_reason", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("source_metadata_json", "JSON", mode="NULLABLE"),
            bigquery.SchemaField("created_at", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("updated_at", "TIMESTAMP", mode="NULLABLE"),
        ]
        table = bigquery.Table(table_id, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="created_at",
        )
        table.clustering_fields = ["status", "error_type", "target_table", "merge_key_value"]
        self.client.create_table(table)
        self._cdc_pending_table_ready = True

    def route_cdc_pending(
        self,
        row: dict[str, Any],
        payload: Any,
        route_reason: str,
        merge_key: str,
        max_retry_count: int,
        dry_run: bool = False,
    ) -> None:
        table_id = self._resolve_cdc_pending_table()
        if dry_run:
            return

        self._ensure_cdc_pending_table()

        payload_obj = payload if isinstance(payload, dict) else {}
        merge_key_value = _to_string_value(payload_obj.get(merge_key)) if payload_obj else None
        operation = self._normalize_cdc_operation(payload_obj.get("operation") or payload_obj.get("op"))
        event_version = self._parse_int(payload_obj.get("version") or payload_obj.get("event_version"))
        event_timestamp = self._to_iso_ts(payload_obj.get("timestamp") or payload_obj.get("event_timestamp"))
        event_id = _to_string_value(payload_obj.get("event_id"))
        payload_hash = self._payload_hash(payload)
        source_dlq_row_id = _to_string_value(row.get(self.config.dlq_id_column)) if self.dlq_has_id else None
        source_metadata = {}
        if isinstance(row.get("error_details"), str):
            try:
                source_metadata = json.loads(row["error_details"]) or {}
            except json.JSONDecodeError:
                source_metadata = {}

        target_table = _to_string_value(row.get("table_name"))
        raw_pending_key = {
            "dlq_table": self.config.dlq_table,
            "dlq_row_id": source_dlq_row_id,
            "target_table": target_table,
            "merge_key": merge_key,
            "merge_key_value": merge_key_value,
            "operation": operation,
            "event_version": event_version,
            "event_id": event_id,
            "payload_hash": payload_hash,
            "error_type": _to_string_value(row.get("error_type")),
        }
        pending_key = hashlib.sha1(
            json.dumps(raw_pending_key, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        ).hexdigest()
        pending_id = str(uuid.uuid4())

        query = f"""
        MERGE `{table_id}` T
        USING (
          SELECT
            @pending_key AS pending_key,
            @pending_id AS pending_id,
            @source_dlq_table AS source_dlq_table,
            @source_dlq_row_id AS source_dlq_row_id,
            @target_table AS target_table,
            @merge_key AS merge_key,
            @merge_key_value AS merge_key_value,
            @operation AS operation,
            @event_version AS event_version,
            TIMESTAMP(@event_timestamp) AS event_timestamp,
            @event_id AS event_id,
            PARSE_JSON(@payload_json) AS payload_json,
            @payload_hash AS payload_hash,
            @error_type AS error_type,
            @error_message AS error_message,
            @route_reason AS route_reason,
            @max_retry_count AS max_retry_count,
            PARSE_JSON(@source_metadata_json) AS source_metadata_json,
            CURRENT_TIMESTAMP() AS now_ts
        ) S
        ON T.pending_key = S.pending_key
        WHEN MATCHED THEN UPDATE SET
          source_dlq_table = S.source_dlq_table,
          source_dlq_row_id = S.source_dlq_row_id,
          target_table = S.target_table,
          merge_key = S.merge_key,
          merge_key_value = S.merge_key_value,
          operation = S.operation,
          event_version = S.event_version,
          event_timestamp = S.event_timestamp,
          event_id = S.event_id,
          payload_json = S.payload_json,
          payload_hash = S.payload_hash,
          error_type = S.error_type,
          error_message = S.error_message,
          status = 'PENDING',
          route_reason = S.route_reason,
          max_retry_count = S.max_retry_count,
          next_retry_at = S.now_ts,
          source_metadata_json = S.source_metadata_json,
          updated_at = S.now_ts
        WHEN NOT MATCHED THEN
          INSERT (
            pending_id, pending_key, source_dlq_table, source_dlq_row_id, target_table,
            merge_key, merge_key_value, operation, event_version, event_timestamp, event_id,
            payload_json, payload_hash, error_type, error_message, status, route_reason,
            retry_count, max_retry_count, first_seen_at, next_retry_at,
            source_metadata_json, created_at, updated_at
          )
          VALUES (
            S.pending_id, S.pending_key, S.source_dlq_table, S.source_dlq_row_id, S.target_table,
            S.merge_key, S.merge_key_value, S.operation, S.event_version, S.event_timestamp, S.event_id,
            S.payload_json, S.payload_hash, S.error_type, S.error_message, 'PENDING', S.route_reason,
            0, S.max_retry_count, S.now_ts, S.now_ts,
            S.source_metadata_json, S.now_ts, S.now_ts
          )
        """
        params = [
            bigquery.ScalarQueryParameter("pending_key", "STRING", pending_key),
            bigquery.ScalarQueryParameter("pending_id", "STRING", pending_id),
            bigquery.ScalarQueryParameter("source_dlq_table", "STRING", self.config.dlq_table),
            bigquery.ScalarQueryParameter("source_dlq_row_id", "STRING", source_dlq_row_id),
            bigquery.ScalarQueryParameter("target_table", "STRING", target_table),
            bigquery.ScalarQueryParameter("merge_key", "STRING", merge_key),
            bigquery.ScalarQueryParameter("merge_key_value", "STRING", merge_key_value),
            bigquery.ScalarQueryParameter("operation", "STRING", operation),
            bigquery.ScalarQueryParameter("event_version", "INT64", event_version),
            bigquery.ScalarQueryParameter("event_timestamp", "STRING", event_timestamp),
            bigquery.ScalarQueryParameter("event_id", "STRING", event_id),
            bigquery.ScalarQueryParameter(
                "payload_json",
                "STRING",
                json.dumps(payload, separators=(",", ":"), ensure_ascii=False),
            ),
            bigquery.ScalarQueryParameter("payload_hash", "STRING", payload_hash),
            bigquery.ScalarQueryParameter("error_type", "STRING", _to_string_value(row.get("error_type"))),
            bigquery.ScalarQueryParameter("error_message", "STRING", _to_string_value(row.get("error_message"))),
            bigquery.ScalarQueryParameter("route_reason", "STRING", route_reason),
            bigquery.ScalarQueryParameter("max_retry_count", "INT64", int(max_retry_count)),
            bigquery.ScalarQueryParameter(
                "source_metadata_json",
                "STRING",
                json.dumps(source_metadata, separators=(",", ":"), ensure_ascii=False),
            ),
        ]
        self.client.query(query, job_config=bigquery.QueryJobConfig(query_parameters=params)).result()

    @property
    def dlq_has_decision(self) -> bool:
        return "decision" in self._dlq_columns

    @property
    def dlq_has_discard_reason(self) -> bool:
        return "discard_reason" in self._dlq_columns

    @property
    def dlq_has_fix_applied(self) -> bool:
        return "fix_applied" in self._dlq_columns

    @property
    def dlq_has_id(self) -> bool:
        return bool(self.config.dlq_id_column) and self.config.dlq_id_column in self._dlq_columns

    @staticmethod
    def _manual_payload_from_row(row: dict[str, Any]) -> Any:
        original = row.get("original_message")
        if isinstance(original, dict):
            return original
        if isinstance(original, str):
            try:
                return json.loads(original)
            except json.JSONDecodeError:
                return {"_raw_original_message": original}
        if isinstance(original, (list, tuple)):
            return {"_raw_original_message": json.dumps(original, separators=(",", ":"), ensure_ascii=False)}
        if original is None:
            return {}
        return {"_raw_original_message": str(original)}

    def fetch_dlq_rows(self, limit: int, table_name: str | None = None) -> list[bigquery.table.Row]:
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
        if self.dlq_has_id:
            columns.append(self.config.dlq_id_column)

        table_filter = ""
        params: list[bigquery.ArrayQueryParameter | bigquery.ScalarQueryParameter] = [
            bigquery.ArrayQueryParameter("discard_types", "STRING", list(self.config.discard_error_types)),
            bigquery.ScalarQueryParameter("limit", "INT64", limit),
            bigquery.ScalarQueryParameter("dlq_update_grace_minutes", "INT64", self.config.dlq_update_grace_minutes),
        ]
        if table_name:
            table_filter = "AND table_name = @table_name"
            params.append(bigquery.ScalarQueryParameter("table_name", "STRING", table_name))

        query = f"""
        SELECT {', '.join(columns)}
        FROM `{self.config.dlq_table}`
        WHERE reprocessed = FALSE
          AND error_type NOT IN UNNEST(@discard_types)
          AND failed_timestamp < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @dlq_update_grace_minutes MINUTE)
          {table_filter}
        ORDER BY failed_timestamp
        LIMIT @limit
        """

        job_config = bigquery.QueryJobConfig(query_parameters=params)
        return list(self.client.query(query, job_config=job_config).result())

    def write_fix_only_result(
        self,
        row: dict[str, Any],
        original_message: dict[str, Any] | None,
        fixed_message: dict[str, Any],
        fixes_applied: list[str],
        target_table: str | None,
        fanout_payloads_count: int,
        decision: str | None = None,
        discard_reason: str | None = None,
    ) -> None:
        if not self.config.fix_only_output_table:
            return

        self._ensure_fix_only_output_table()
        output_table = self.config.resolve_target_table(self.config.fix_only_output_table)
        item = {
            "raw_original_message": _to_string_value(row.get("original_message")),
            "original_message": json.dumps(original_message or {}, separators=(",", ":"), ensure_ascii=False),
            "fixed_message": json.dumps(fixed_message, separators=(",", ":"), ensure_ascii=False),
            "fixes_applied": ",".join(fixes_applied),
            "error_type": _to_string_value(row.get("error_type")),
            "error_message": _to_string_value(row.get("error_message")),
            "decision": _to_string_value(decision),
            "discard_reason": _to_string_value(discard_reason),
        }
        errors = self.client.insert_rows_json(output_table, [item])
        if errors:
            raise RuntimeError(f"Failed writing fix-only result row: {errors}")

    def _ensure_fix_only_output_table(self) -> None:
        if self._fix_only_output_table_ready:
            return
        output_table = self.config.resolve_target_table(self.config.fix_only_output_table)
        try:
            existing_table = self.client.get_table(output_table)
            existing_columns = {field.name for field in existing_table.schema}
            if "decision" not in existing_columns:
                self.client.query(
                    f"ALTER TABLE `{output_table}` ADD COLUMN decision STRING"
                ).result()
            if "discard_reason" not in existing_columns:
                self.client.query(
                    f"ALTER TABLE `{output_table}` ADD COLUMN discard_reason STRING"
                ).result()
            self._fix_only_output_table_ready = True
            return
        except NotFound:
            pass

        schema = [
            bigquery.SchemaField("raw_original_message", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("original_message", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("fixed_message", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("fixes_applied", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("error_type", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("error_message", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("decision", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("discard_reason", "STRING", mode="NULLABLE"),
        ]
        table = bigquery.Table(output_table, schema=schema)
        self.client.create_table(table)
        self._fix_only_output_table_ready = True

    def write_manual_intervention_result(
        self,
        row: dict[str, Any],
        fixed_message: Any,
        reason: str,
        target_table: str | None,
    ) -> None:
        if not self.config.manual_intervention_table:
            return
        self._ensure_manual_intervention_table()
        output_table = self.config.resolve_target_table(self.config.manual_intervention_table)
        item = {
            "raw_original_message": _to_string_value(row.get("original_message")),
            "fixed_message": json.dumps(fixed_message, separators=(",", ":"), ensure_ascii=False),
            "error_type": _to_string_value(row.get("error_type")),
            "error_message": _to_string_value(row.get("error_message")),
            "reason": reason,
            "target_table": _to_string_value(target_table),
            "created_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        }
        errors = self.client.insert_rows_json(output_table, [item])
        if errors:
            raise RuntimeError(f"Failed writing manual intervention row: {errors}")

    def _ensure_manual_intervention_table(self) -> None:
        if self._manual_intervention_table_ready:
            return
        output_table = self.config.resolve_target_table(self.config.manual_intervention_table)
        try:
            self.client.get_table(output_table)
            self._manual_intervention_table_ready = True
            return
        except NotFound:
            pass

        schema = [
            bigquery.SchemaField("raw_original_message", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("fixed_message", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("error_type", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("error_message", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("reason", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("target_table", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("created_at", "TIMESTAMP", mode="NULLABLE"),
        ]
        table = bigquery.Table(output_table, schema=schema)
        self.client.create_table(table)
        self._manual_intervention_table_ready = True

    def get_schema(self, table_id: str) -> dict[str, bigquery.SchemaField]:
        if table_id in self._schema_cache:
            return self._schema_cache[table_id]
        table = self.client.get_table(table_id)
        schema = {field.name: field for field in table.schema}
        self._schema_cache[table_id] = schema
        return schema

    def merge_payload(
        self,
        table_id: str,
        payload: dict[str, Any],
        schema_by_name: dict[str, bigquery.SchemaField],
        merge_key: str,
        dry_run: bool = False,
    ) -> None:
        if merge_key not in payload or payload[merge_key] in (None, ""):
            raise ValueError(f"Missing merge key in payload: {merge_key}")

        ordered_columns = [field.name for field in schema_by_name.values() if field.name in payload]
        if merge_key not in ordered_columns:
            ordered_columns.insert(0, merge_key)

        if not ordered_columns:
            raise ValueError("No payload columns intersect target schema")

        has_complex_columns = any(
            (
                schema_by_name[column].mode == "REPEATED"
                or schema_by_name[column].field_type.upper() == "RECORD"
            )
            for column in ordered_columns
        )
        if has_complex_columns:
            # Inline CAST(PARSE_JSON(...)) is brittle for complex columns and can
            # fail on valid repaired payloads. Route through staging-table merge
            # so BigQuery ingests typed JSON rows directly.
            self.merge_payloads_set_based(
                table_id=table_id,
                payloads=[payload],
                schema_by_name=schema_by_name,
                merge_key=merge_key,
                dry_run=dry_run,
            )
            return

        select_exprs: list[str] = []
        params: list[bigquery.ScalarQueryParameter] = []
        for idx, column in enumerate(ordered_columns):
            param_name = f"p_{idx}"
            field = schema_by_name[column]
            value = payload.get(column)
            if field.mode == "REPEATED" or field.field_type.upper() == "RECORD":
                param_value = json.dumps(value, separators=(",", ":"), ensure_ascii=False) if value is not None else None
            elif field.field_type.upper() == "JSON":
                param_value = json.dumps(value, separators=(",", ":"), ensure_ascii=False) if isinstance(value, (dict, list)) else _to_string_value(value)
            else:
                param_value = _to_string_value(value)

            select_exprs.append(f"{_as_sql_expr(field, param_name)} AS `{column}`")
            params.append(bigquery.ScalarQueryParameter(param_name, "STRING", param_value))

        updates = ",\n  ".join([f"`{c}` = S.`{c}`" for c in ordered_columns])
        insert_cols = ", ".join([f"`{c}`" for c in ordered_columns])
        insert_vals = ", ".join([f"S.`{c}`" for c in ordered_columns])

        query = f"""
        MERGE `{table_id}` T
        USING (
          SELECT
            {', '.join(select_exprs)}
        ) S
        ON T.`{merge_key}` = S.`{merge_key}`
        WHEN MATCHED THEN UPDATE SET
          {updates}
        WHEN NOT MATCHED THEN
          INSERT ({insert_cols}) VALUES ({insert_vals})
        """

        if dry_run:
            return

        job_config = bigquery.QueryJobConfig(query_parameters=params)
        self.client.query(query, job_config=job_config).result()

    def _staging_table_for_target(self, table_id: str, suffix: str | None = None) -> str:
        project, dataset, table = table_id.split(".")
        if suffix:
            return f"{project}.{dataset}.__dlq_replay_staging_{table}_{suffix}"
        return f"{project}.{dataset}.__dlq_replay_staging_{table}"

    def _prepare_staging_table(self, target_table_id: str, staging_table_id: str, dry_run: bool = False) -> None:
        if dry_run:
            return
        query = f"CREATE OR REPLACE TABLE `{staging_table_id}` LIKE `{target_table_id}`"
        self.client.query(query).result()

    def merge_payloads_set_based(
        self,
        table_id: str,
        payloads: list[dict[str, Any]],
        schema_by_name: dict[str, bigquery.SchemaField],
        merge_key: str,
        dry_run: bool = False,
    ) -> None:
        if not payloads:
            return

        for payload in payloads:
            if merge_key not in payload or payload[merge_key] in (None, ""):
                raise ValueError(f"Missing merge key in payload: {merge_key}")

        # Keep a single source row per merge key to prevent duplicate inserts
        # when multiple DLQ rows for the same key are processed in one batch.
        deduped_by_key: dict[str, dict[str, Any]] = {}
        for payload in payloads:
            deduped_by_key[str(payload[merge_key])] = payload
        deduped_payloads = list(deduped_by_key.values())

        # Use unique staging table per merge to avoid worker collisions when
        # Dataflow processes rows in parallel.
        staging_table = self._staging_table_for_target(table_id, suffix=uuid.uuid4().hex[:12])
        self._prepare_staging_table(table_id, staging_table, dry_run=dry_run)

        if not dry_run:
            rows_to_insert = [{k: v for k, v in payload.items() if k in schema_by_name} for payload in deduped_payloads]
            try:
                errors = self.client.insert_rows_json(staging_table, rows_to_insert)
            except NotFound as exc:
                # BigQuery can briefly return "Table is truncated" right after
                # CREATE OR REPLACE. Recreate and retry once.
                msg = str(exc).lower()
                if "table is truncated" not in msg:
                    raise
                time.sleep(1.0)
                self._prepare_staging_table(table_id, staging_table, dry_run=dry_run)
                errors = self.client.insert_rows_json(staging_table, rows_to_insert)
            if errors:
                raise RuntimeError(f"Failed writing rows to staging table {staging_table}: {errors}")

        ordered_columns = []
        for field_name in schema_by_name:
            if field_name == merge_key or any(field_name in payload for payload in deduped_payloads):
                ordered_columns.append(field_name)

        if merge_key not in ordered_columns:
            ordered_columns.insert(0, merge_key)
        if not ordered_columns:
            raise ValueError("No payload columns intersect target schema")

        updates = ",\n  ".join([f"`{c}` = S.`{c}`" for c in ordered_columns])
        insert_cols = ", ".join([f"`{c}`" for c in ordered_columns])
        insert_vals = ", ".join([f"S.`{c}`" for c in ordered_columns])
        select_cols = ", ".join([f"`{c}`" for c in ordered_columns])

        query = f"""
        MERGE `{table_id}` T
        USING (
          SELECT {select_cols}
          FROM `{staging_table}`
        ) S
        ON T.`{merge_key}` = S.`{merge_key}`
        WHEN MATCHED THEN UPDATE SET
          {updates}
        WHEN NOT MATCHED THEN
          INSERT ({insert_cols}) VALUES ({insert_vals})
        """

        if dry_run:
            return

        try:
            self.client.query(query).result()
        finally:
            # Best effort cleanup to avoid accumulating staging artifacts.
            try:
                self.client.query(f"DROP TABLE `{staging_table}`").result()
            except Exception:
                pass

    def mark_terminal(
        self,
        row: dict[str, Any],
        decision: str,
        fix_applied: str,
        discard_reason: str | None,
        dry_run: bool = False,
    ) -> None:
        if dry_run:
            return

        # Global policy: archive every discarded row into manual intervention,
        # unless it was already explicitly routed there by caller logic.
        if (
            decision == "DISCARD"
            and self.config.manual_intervention_table
            and not str(fix_applied or "").startswith("manual_intervention_")
            and not str(discard_reason or "").startswith("manual_intervention_")
        ):
            reason = f"auto_manual_from_discard:{str(discard_reason or '')[:500]}"
            try:
                self.write_manual_intervention_result(
                    row=row,
                    fixed_message=self._manual_payload_from_row(row),
                    reason=reason,
                    target_table=row.get("table_name"),
                )
            except Exception:
                # Preserve main terminal path even if manual archival fails.
                pass

        set_clauses = [
            "reprocessed = TRUE",
            "reprocessed_timestamp = CURRENT_TIMESTAMP()",
        ]
        params: list[bigquery.ScalarQueryParameter] = []

        if self.dlq_has_decision:
            set_clauses.append("decision = @decision")
            params.append(bigquery.ScalarQueryParameter("decision", "STRING", decision))
        if self.dlq_has_fix_applied:
            set_clauses.append("fix_applied = @fix_applied")
            params.append(bigquery.ScalarQueryParameter("fix_applied", "STRING", fix_applied))
        if self.dlq_has_discard_reason:
            set_clauses.append("discard_reason = @discard_reason")
            params.append(bigquery.ScalarQueryParameter("discard_reason", "STRING", discard_reason))

        if self.dlq_has_id:
            where_clause = f"{self.config.dlq_id_column} = @dlq_id"
            params.append(
                bigquery.ScalarQueryParameter(
                    "dlq_id",
                    "STRING",
                    _to_string_value(row.get(self.config.dlq_id_column)),
                )
            )
        else:
            where_clause = """
              original_message = @original_message
              AND error_type = @error_type
              AND failed_timestamp = @failed_timestamp
              AND IFNULL(table_name, '') = IFNULL(@table_name, '')
              AND IFNULL(pipeline_job_id, '') = IFNULL(@pipeline_job_id, '')
              AND IFNULL(retry_count, -1) = IFNULL(@retry_count, -1)
              AND reprocessed = FALSE
            """
            params.extend(
                [
                    bigquery.ScalarQueryParameter("original_message", "STRING", _to_string_value(row.get("original_message"))),
                    bigquery.ScalarQueryParameter("error_type", "STRING", _to_string_value(row.get("error_type"))),
                    bigquery.ScalarQueryParameter("failed_timestamp", "TIMESTAMP", row.get("failed_timestamp")),
                    bigquery.ScalarQueryParameter("table_name", "STRING", _to_string_value(row.get("table_name"))),
                    bigquery.ScalarQueryParameter("pipeline_job_id", "STRING", _to_string_value(row.get("pipeline_job_id"))),
                    bigquery.ScalarQueryParameter("retry_count", "INT64", row.get("retry_count")),
                ]
            )

        query = f"""
        UPDATE `{self.config.dlq_table}`
        SET {', '.join(set_clauses)}
        WHERE {where_clause}
        """
        job_config = bigquery.QueryJobConfig(query_parameters=params)
        self.client.query(query, job_config=job_config).result()

    def mark_retry_pending(
        self,
        row: dict[str, Any],
        pending_reason: str,
        fix_applied: str = "",
        dry_run: bool = False,
    ) -> None:
        if dry_run:
            return

        set_clauses = [
            "retry_count = IFNULL(retry_count, 0) + 1",
        ]
        params: list[bigquery.ScalarQueryParameter] = []

        if self.dlq_has_decision:
            set_clauses.append("decision = @decision")
            params.append(bigquery.ScalarQueryParameter("decision", "STRING", "RETRY_PENDING"))
        if self.dlq_has_fix_applied:
            set_clauses.append("fix_applied = @fix_applied")
            params.append(bigquery.ScalarQueryParameter("fix_applied", "STRING", fix_applied))
        if self.dlq_has_discard_reason:
            set_clauses.append("discard_reason = @discard_reason")
            params.append(bigquery.ScalarQueryParameter("discard_reason", "STRING", pending_reason))

        if self.dlq_has_id:
            where_clause = f"{self.config.dlq_id_column} = @dlq_id"
            params.append(
                bigquery.ScalarQueryParameter(
                    "dlq_id",
                    "STRING",
                    _to_string_value(row.get(self.config.dlq_id_column)),
                )
            )
        else:
            where_clause = """
              original_message = @original_message
              AND error_type = @error_type
              AND failed_timestamp = @failed_timestamp
              AND IFNULL(table_name, '') = IFNULL(@table_name, '')
              AND IFNULL(pipeline_job_id, '') = IFNULL(@pipeline_job_id, '')
              AND IFNULL(retry_count, -1) = IFNULL(@retry_count, -1)
              AND reprocessed = FALSE
            """
            params.extend(
                [
                    bigquery.ScalarQueryParameter("original_message", "STRING", _to_string_value(row.get("original_message"))),
                    bigquery.ScalarQueryParameter("error_type", "STRING", _to_string_value(row.get("error_type"))),
                    bigquery.ScalarQueryParameter("failed_timestamp", "TIMESTAMP", row.get("failed_timestamp")),
                    bigquery.ScalarQueryParameter("table_name", "STRING", _to_string_value(row.get("table_name"))),
                    bigquery.ScalarQueryParameter("pipeline_job_id", "STRING", _to_string_value(row.get("pipeline_job_id"))),
                    bigquery.ScalarQueryParameter("retry_count", "INT64", row.get("retry_count")),
                ]
            )

        query = f"""
        UPDATE `{self.config.dlq_table}`
        SET {', '.join(set_clauses)}
        WHERE {where_clause}
        """
        job_config = bigquery.QueryJobConfig(query_parameters=params)
        self.client.query(query, job_config=job_config).result()

    def publish_to_retry_input_topic(
        self,
        payload: Any,
        row: dict[str, Any],
        reason: str,
        dry_run: bool = False,
    ) -> bool:
        topic = self.config.retry_input_topic
        if not topic:
            return False
        if dry_run:
            return True
        if self._publisher is None:
            return False

        if isinstance(payload, bytes):
            body = payload
        elif isinstance(payload, str):
            body = payload.encode("utf-8")
        else:
            body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        retry_attr_name = self.config.dlq_retry_count_attribute or "dlq_retry_count"
        attrs = {
            "source": "dlq_replay",
            "reason": reason[:200],
            "dlq_error_type": _to_string_value(row.get("error_type")) or "",
            "dlq_table_name": _to_string_value(row.get("table_name")) or "",
            retry_attr_name: str(self._next_retry_count_from_row(row)),
        }
        future = self._publisher.publish(topic, body, **attrs)
        future.result()
        return True

    def _extract_event_id(self, payload: Any, row: dict[str, Any]) -> str:
        if isinstance(payload, dict):
            event_id = payload.get("event_id")
            if event_id not in (None, ""):
                return str(event_id)
        if isinstance(payload, str):
            try:
                parsed = json.loads(payload)
                if isinstance(parsed, dict):
                    event_id = parsed.get("event_id")
                    if event_id not in (None, ""):
                        return str(event_id)
            except json.JSONDecodeError:
                pass
        return str(row.get(self.config.dlq_id_column) or "") if self.dlq_has_id else ""

    def mark_requeued_terminal(
        self,
        row: dict[str, Any],
        reason: str,
        fix_applied: str,
        dry_run: bool = False,
    ) -> None:
        if dry_run:
            return

        set_clauses = [
            "retry_count = IFNULL(retry_count, 0) + 1",
            "reprocessed = TRUE",
            "reprocessed_timestamp = CURRENT_TIMESTAMP()",
        ]
        params: list[bigquery.ScalarQueryParameter] = []

        if self.dlq_has_decision:
            set_clauses.append("decision = @decision")
            params.append(bigquery.ScalarQueryParameter("decision", "STRING", "REQUEUED_TO_INPUT_TOPIC"))
        if self.dlq_has_fix_applied:
            set_clauses.append("fix_applied = @fix_applied")
            params.append(bigquery.ScalarQueryParameter("fix_applied", "STRING", fix_applied))
        if self.dlq_has_discard_reason:
            set_clauses.append("discard_reason = @discard_reason")
            params.append(bigquery.ScalarQueryParameter("discard_reason", "STRING", reason))

        if self.dlq_has_id:
            where_clause = f"{self.config.dlq_id_column} = @dlq_id"
            params.append(
                bigquery.ScalarQueryParameter(
                    "dlq_id",
                    "STRING",
                    _to_string_value(row.get(self.config.dlq_id_column)),
                )
            )
        else:
            where_clause = """
              original_message = @original_message
              AND error_type = @error_type
              AND failed_timestamp = @failed_timestamp
              AND IFNULL(table_name, '') = IFNULL(@table_name, '')
              AND IFNULL(pipeline_job_id, '') = IFNULL(@pipeline_job_id, '')
              AND IFNULL(retry_count, -1) = IFNULL(@retry_count, -1)
              AND reprocessed = FALSE
            """
            params.extend(
                [
                    bigquery.ScalarQueryParameter("original_message", "STRING", _to_string_value(row.get("original_message"))),
                    bigquery.ScalarQueryParameter("error_type", "STRING", _to_string_value(row.get("error_type"))),
                    bigquery.ScalarQueryParameter("failed_timestamp", "TIMESTAMP", row.get("failed_timestamp")),
                    bigquery.ScalarQueryParameter("table_name", "STRING", _to_string_value(row.get("table_name"))),
                    bigquery.ScalarQueryParameter("pipeline_job_id", "STRING", _to_string_value(row.get("pipeline_job_id"))),
                    bigquery.ScalarQueryParameter("retry_count", "INT64", row.get("retry_count")),
                ]
            )

        query = f"""
        UPDATE `{self.config.dlq_table}`
        SET {', '.join(set_clauses)}
        WHERE {where_clause}
        """
        job_config = bigquery.QueryJobConfig(query_parameters=params)
        self.client.query(query, job_config=job_config).result()
