# DLQ Replay (Dataflow Flex Template + Scheduler)

This repository implements deterministic DLQ replay with two runtime options:
- **Local / ad hoc batch** (`python -m app.main` or manual Dataflow submission)
- **Scheduled GCP deployment**: `Cloud Scheduler -> Cloud Run launcher -> Dataflow Flex Template`

Operational runbook:
- [DLQ_REPLAY_RUNBOOK.md](/c:/Users/sreer/Desktop/da/dlq_replay/DLQ_REPLAY_RUNBOOK.md)

## What It Does

1. Reads unreprocessed records from a BigQuery DLQ table.
2. Applies deterministic fixes.
3. Validates repaired payloads against target table schema + required/regex rules.
4. Executes explicit column-based BigQuery `MERGE` into target table.
5. Marks each DLQ row terminal as replayed/fixed+replayed/discarded.

## Repo Layout

- `app/main.py`: Cloud Run/local batch entrypoint
- `app/dataflow_main.py`: Dataflow batch entrypoint
- `app/bigquery_repo.py`: BigQuery reads, MERGE, terminal updates
- `app/fixes.py`: deterministic fix rules
- `app/validators.py`: payload validation rules
- `app/config.py`: env config
- `deploy/dataflow_flex_template_metadata.json`: Flex Template parameter spec
- `launchers/dataflow_replay_launcher/main.py`: Cloud Run launcher that starts the Flex Template
- `scripts/setup_replay_infra.ps1`: create service accounts, APIs, IAM, topics
- `scripts/deploy_replay_flextemplate.ps1`: build the replay Flex Template and upload config
- `scripts/deploy_replay_launcher.ps1`: deploy the Cloud Run launcher
- `scripts/create_replay_scheduler.ps1`: create the Cloud Scheduler job for the launcher
- `scripts/deploy_manual_intervention_alert.ps1`: create a Cloud Monitoring alert on the Dataflow manual-intervention counter
- `scripts/deploy_job.ps1`: legacy Cloud Run Job deployment
- `scripts/run_dataflow_replay.ps1`: launch Dataflow replay
- `scripts/create_scheduler_job.ps1`: legacy Cloud Run Job scheduler
- `.env.job.yaml.example`: sample env vars for job

## Prerequisites

- `gcloud` authenticated and configured.
- APIs enabled:
  - `run.googleapis.com`
  - `cloudscheduler.googleapis.com`
  - `bigquery.googleapis.com`
  - `artifactregistry.googleapis.com`
  - `dataflow.googleapis.com`
- A service account for the job with:
  - `roles/bigquery.dataViewer`
  - `roles/bigquery.dataEditor`
  - `roles/bigquery.jobUser`

## Environment Variables

Copy `.env.job.yaml.example` to `.env.job.yaml` and set real values.

Required:

- `PROJECT_ID`
- `DLQ_TABLE` (fully-qualified: `project.dataset.table`)

Important optional:

- `DEFAULT_DATASET`
- `DEFAULT_TARGET_TABLE`
- `MERGE_KEY` (default: `event_id`)
- `MAX_RETRY_COUNT` (default: `5`; rows at/above this `retry_count` are terminal-discarded as poison-pill protection)
- `DISCARD_ERROR_TYPES` (default includes `DESERIALIZATION_ERROR`)
- `DLQ_ID_COLUMN` (recommended if available for precise terminal update)
- `FANOUT_ENABLED` (default `false`; opt-in one-to-many expansion)
- `FANOUT_FIELDS` (comma-separated fields eligible for fan-out, e.g. `tags`)
- `FIX_ONLY_OUTPUT_TABLE` (optional BigQuery table to store fix-only results)
- `ENABLE_DESERIALIZATION_REPAIR` (default `false`; opt-in safe JSON normalization before discard)
- `RETRY_INPUT_TOPIC` (Pub/Sub topic used to republish operational/retryable DLQ messages)
- `MANUAL_INTERVENTION_ALERT_ENABLED` (optional; enable Cloud Monitoring alert deployment)
- `MANUAL_INTERVENTION_ALERT_THRESHOLD` (optional; threshold for `manual_intervention_routed`)
- `MANUAL_INTERVENTION_ALERT_NOTIFICATION_CHANNEL` (optional; Monitoring notification channel ID/resource name)

## Recommended GCP Deployment

Use this path when you want replay to run on a schedule in GCP:

1. `Cloud Scheduler` sends an authenticated HTTP request to the launcher service.
2. `Cloud Run launcher` calls the Dataflow Flex Template launch API.
3. `Dataflow Flex Template` runs the replay pipeline using the YAML config stored in GCS.
4. Retryable operational rows are republished to the original input topic.

### 1. Setup Infra

```powershell
./scripts/setup_replay_infra.ps1 `
  -ProjectId "your-project" `
  -Region "us-central1" `
  -TempBucket "gs://your-bucket" `
  -StagingBucket "gs://your-bucket" `
  -TemplateBucket "gs://your-bucket" `
  -RetryInputTopic "projects/your-project/topics/your-main-input-topic"
```

This script:
- enables the required APIs
- creates the Dataflow worker, launcher, and scheduler service accounts
- grants IAM roles for Dataflow, BigQuery, Storage, Pub/Sub, and Eventarc
- creates the retry topics if needed

### 2. Build the Replay Flex Template

```powershell
./scripts/deploy_replay_flextemplate.ps1 `
  -ProjectId "your-project" `
  -Region "us-central1" `
  -TemplateBucket "gs://your-bucket" `
  -ConfigYaml ".env.job.yaml"
```

This script:
- uploads `.env.job.yaml` to GCS
- builds the Dataflow Flex Template container
- writes the Flex Template spec JSON to GCS

### 3. Deploy the Cloud Run Launcher

```powershell
./scripts/deploy_replay_launcher.ps1 `
  -ProjectId "your-project" `
  -Region "us-central1" `
  -ServiceName "dlq-replay-launcher" `
  -TemplateSpecGcsPath "gs://your-bucket/templates/dlq-replay-flex.json" `
  -ConfigYamlGcsPath "gs://your-bucket/config/dlq-replay-config.yaml" `
  -TempLocation "gs://your-bucket/dataflow/tmp" `
  -StagingLocation "gs://your-bucket/dataflow/staging" `
  -DataflowServiceAccount "dlq-replay-dataflow-sa@your-project.iam.gserviceaccount.com"
```

The launcher exposes:
- `GET /healthz`
- `POST /launch`

Optional POST body overrides:
- `table_name`
- `limit`
- `fix_only`
- `config_yaml`
- `job_name_prefix`

### 4. Create the Scheduler Job

```powershell
./scripts/create_replay_scheduler.ps1 `
  -ProjectId "your-project" `
  -Region "us-central1" `
  -SchedulerJobName "run-dlq-replay-nightly" `
  -LauncherServiceName "dlq-replay-launcher" `
  -Schedule "0 23 * * *" `
  -TimeZone "Asia/Calcutta"
```

This creates a scheduler job that calls:
- `POST https://<launcher-service-url>/launch`

with OIDC authentication using the scheduler service account.

### 5. Create the Manual Intervention Alert

If you want Cloud Monitoring to send an email through an existing notification channel when too many rows are routed to manual intervention in a replay run, deploy the alert policy:

```powershell
./scripts/deploy_manual_intervention_alert.ps1 `
  -ProjectId "your-project" `
  -Region "us-central1" `
  -ConfigYaml ".env.job.yaml"
```

This policy watches the Dataflow user counter:
- `dataflow.googleapis.com/job/user_counter`
- `metric_name = manual_intervention_routed`

and sends notifications through the configured Cloud Monitoring notification channel when the threshold is exceeded.

## Legacy Cloud Run Job Deployment

This path is still available, but it is not the recommended scheduled deployment model.

```powershell
./scripts/deploy_job.ps1 \
  -ProjectId "your-project" \
  -Region "us-central1" \
  -JobName "dlq-replay-job" \
  -Image "us-central1-docker.pkg.dev/your-project/data-jobs/dlq-replay:latest" \
  -ServiceAccount "dlq-replay-job-sa@your-project.iam.gserviceaccount.com" \
  -EnvFile ".env.job.yaml"
```

## Legacy Cloud Scheduler Trigger For Cloud Run Job

```powershell
./scripts/create_scheduler_job.ps1 \
  -ProjectId "your-project" \
  -Region "us-central1" \
  -SchedulerJobName "run-dlq-replay-daily" \
  -RunJobName "dlq-replay-job" \
  -SchedulerServiceAccount "scheduler-invoker@your-project.iam.gserviceaccount.com" \
  -Schedule "0 2 * * *" \
  -TimeZone "Etc/UTC"
```

This scheduler calls the Cloud Run Jobs API endpoint:
`POST https://run.googleapis.com/v2/projects/{project}/locations/{region}/jobs/{job}:run`

## Manual Dataflow Replay Batch

```powershell
./scripts/run_dataflow_replay.ps1 `
  -ProjectId "your-project" `
  -Region "us-central1" `
  -JobName "dlq-replay-batch-$(Get-Date -Format yyyyMMdd-HHmmss)" `
  -TempLocation "gs://your-bucket/dataflow/tmp" `
  -StagingLocation "gs://your-bucket/dataflow/staging" `
  -ConfigYaml ".env.job.yaml" `
  -TableName "your-project.your_dataset.your_target_table" `
  -Limit 1000
```

For Flex Template and manual Dataflow runs, replay behavior remains driven by the same `.env.job.yaml`.

## Notes

- Job is idempotent through key-based `MERGE`.
- Fan-out is disabled by default. If enabled, non-repeated array fields in `FANOUT_FIELDS` are expanded into multiple rows with deterministic merge-key suffixes.
- Retryable operational errors are republished to `RETRY_INPUT_TOPIC`.
- The Dataflow worker service account is granted `roles/monitoring.metricWriter` so Beam custom counters are exported to Cloud Monitoring.
- If `decision`, `discard_reason`, `fix_applied` columns exist, they are updated.
- If `DLQ_ID_COLUMN` is missing, terminal updates use a conservative composite row match.

## Fix-Only Output Table (Optional)

If `FIX_ONLY_OUTPUT_TABLE` is set and you run with `--fix-only`, the job writes one row per processed DLQ message into that table.

Expected columns:

- `raw_original_message` STRING
- `original_message` STRING
- `fixed_message` STRING
- `fixes_applied` STRING
- `error_type` STRING
- `error_message` STRING
