import datetime as dt
import json
import os
from typing import Any

from flask import Flask, jsonify, request
from google.auth import default
from google.auth.transport.requests import AuthorizedSession


app = Flask(__name__)


def _env(name: str, default_value: str = "") -> str:
    return os.getenv(name, default_value).strip()


def _optional_int(name: str) -> int | None:
    raw = _env(name)
    if not raw:
        return None
    return int(raw)


def _bool_string(value: Any) -> str:
    return "true" if str(value).strip().lower() == "true" else "false"


def _utc_suffix() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d-%H%M%S")


def _build_launch_payload(body: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    project_id = _env("PROJECT_ID")
    region = _env("REGION", "us-central1")
    template_path = _env("FLEX_TEMPLATE_SPEC_GCS_PATH")
    config_yaml_gcs_path = body.get("config_yaml") or _env("CONFIG_YAML_GCS_PATH")
    job_name_prefix = body.get("job_name_prefix") or _env("JOB_NAME_PREFIX", "dlq-replay")
    dataflow_service_account = _env("DATAFLOW_SERVICE_ACCOUNT")
    temp_location = _env("TEMP_LOCATION")
    staging_location = _env("STAGING_LOCATION")

    if not project_id:
        raise ValueError("PROJECT_ID is required")
    if not template_path:
        raise ValueError("FLEX_TEMPLATE_SPEC_GCS_PATH is required")
    if not config_yaml_gcs_path:
        raise ValueError("CONFIG_YAML_GCS_PATH or request config_yaml is required")
    if not temp_location:
        raise ValueError("TEMP_LOCATION is required")
    if not staging_location:
        raise ValueError("STAGING_LOCATION is required")

    parameters: dict[str, str] = {
        "config-yaml": config_yaml_gcs_path,
    }
    if body.get("table_name"):
        parameters["table-name"] = str(body["table_name"])
    if body.get("limit") not in (None, ""):
        parameters["limit"] = str(body["limit"])
    if body.get("fix_only") is not None:
        parameters["fix-only"] = _bool_string(body["fix_only"])

    environment: dict[str, Any] = {
        "tempLocation": temp_location,
        "stagingLocation": staging_location,
    }
    if dataflow_service_account:
        environment["serviceAccountEmail"] = dataflow_service_account
    if _env("SUBNETWORK"):
        environment["subnetwork"] = _env("SUBNETWORK")
    if _env("NETWORK"):
        environment["network"] = _env("NETWORK")
    if _env("MACHINE_TYPE"):
        environment["machineType"] = _env("MACHINE_TYPE")
    if _optional_int("MAX_WORKERS") is not None:
        environment["maxWorkers"] = _optional_int("MAX_WORKERS")
    if _optional_int("NUM_WORKERS") is not None:
        environment["numWorkers"] = _optional_int("NUM_WORKERS")
    if _env("ADDITIONAL_EXPERIMENTS"):
        environment["additionalExperiments"] = [
            item.strip() for item in _env("ADDITIONAL_EXPERIMENTS").split(",") if item.strip()
        ]

    job_name = f"{job_name_prefix}-{_utc_suffix()}".lower()
    launch_payload = {
        "launchParameter": {
            "jobName": job_name,
            "containerSpecGcsPath": template_path,
            "parameters": parameters,
            "environment": environment,
        }
    }
    endpoint = (
        f"https://dataflow.googleapis.com/v1b3/projects/{project_id}/locations/{region}/flexTemplates:launch"
    )
    return endpoint, launch_payload


@app.get("/healthz")
def healthz():
    return jsonify({"status": "ok"})


@app.post("/launch")
def launch():
    body = request.get_json(silent=True) or {}
    try:
        endpoint, payload = _build_launch_payload(body)
    except (TypeError, ValueError) as exc:
        return jsonify({"error": str(exc)}), 400

    credentials, _ = default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    session = AuthorizedSession(credentials)
    response = session.post(endpoint, json=payload, timeout=60)

    if response.status_code >= 400:
        return (
            jsonify(
                {
                    "error": "dataflow_launch_failed",
                    "status_code": response.status_code,
                    "response": response.text,
                    "request": payload,
                }
            ),
            response.status_code,
        )

    return jsonify(
        {
            "status": "submitted",
            "request": payload,
            "dataflow_response": response.json(),
        }
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")))
