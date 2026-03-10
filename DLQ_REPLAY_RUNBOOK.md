# DLQ Replay Runbook

## Contents

1. DLQ Replay Overview  
1.1 Introduction  
1.2 Scope in This Repository  
1.3 Environment Behaviour Model  
1.4 Core Terms  

2. Replay Configuration  
2.1 Required Configuration Controls  
2.2 Configuration Intent  
2.3 Operational Implications of Changing Core Controls  
2.4 Change-Control Checklist  

3. Replay Data Flow and Routing  
3.1 Architecture Overview  
3.2 End-to-End Replay Flow  
3.3 Routing Behaviour by Error Family  
3.4 Expected Behaviour Across Environments  

4. Storage Contracts  
4.1 Purpose of the DLQ Table  
4.2 Fix-Only Output Table Contract  
4.3 Manual Intervention Table Contract  
4.4 CDC Pending Replay Table Contract  
4.5 Auditability Expectations  

5. Replay Infrastructure  
5.1 Why the Scheduled Replay Stack Exists  
5.2 Runtime Components  
5.3 Infra Preparation Intent  
5.4 Readiness Criteria Before Replay Runs  
5.5 Ordering and Idempotency Principles  

6. Replay Lifecycle  
6.1 Core State Model  
6.2 Lifecycle Semantics  
6.3 Batch Processing Model  
6.4 Idempotency Expectations  
6.5 Completion Criteria for an Operational Run  
6.6 Partial Replay Handling  

7. Operational Queries and Evidence Views  
7.1 Primary Operational Surfaces  
7.2 Interpretation Guidance  
7.3 Evidence Capture Expectations  

8. Operations Procedures  
8.1 Daily Health Procedure  
8.2 Incident-Time Operating Procedure  
8.3 Replay Trigger Procedure  
8.4 Post-Replay Verification Procedure  
8.5 Pre-Release / Post-Release Procedure  
8.6 Historical Replay / Backfill Procedure  
8.7 Operational Anti-Patterns to Avoid  

9. Troubleshooting  
9.1 Problem: DLQ Backlog Grows Continuously  
9.2 Problem: Rows Stay `reprocessed = false`  
9.3 Problem: CDC Pending Backlog Grows  
9.4 Problem: Manual Intervention Volume Spikes  
9.5 Problem: Scheduler / Launcher / Flex Template Fails  
9.6 Problem: Replay Completes But Target Data Looks Wrong  

10. DLQ Replay Execution Procedure  


## 1. DLQ Replay Overview

### 1.1 Introduction
This repository implements deterministic replay of failed pipeline records from a BigQuery DLQ table back into target BigQuery tables or back into the original Pub/Sub input topic, depending on the failure type.

The replay path exists to prevent permanent data loss while avoiding unsafe blind retries. It standardises how to:
- classify DLQ failures
- repair payloads when deterministic repair is possible
- route non-repairable records to manual intervention
- route CDC-specific sequencing/idempotency cases to a pending table
- requeue operational failures back to the main input topic
- execute replay on a schedule using Dataflow

### 1.2 Scope in This Repository
This repository currently implements:
- deterministic fix logic for transport, deserialization, schema, coercion, and storage-related payload issues
- merge-based replay into target BigQuery tables
- manual intervention routing for non-repairable cases
- CDC pending routing for CDC-specific sequencing, idempotency, and contract-validation cases
- requeue of operational/auth/routing failures to the original Pub/Sub input topic
- scheduled deployment model using:
  - Cloud Scheduler
  - Cloud Run launcher
  - Dataflow Flex Template

### 1.3 Environment Behaviour Model
Replay behaviour is configuration-driven, not environment-hardcoded.

Operationally, the same DLQ record can behave differently across environments because of:
- different DLQ tables
- different target tables
- different Pub/Sub retry topics
- different `MAX_RETRY_COUNT`
- different manual intervention tables
- different replay schedule cadence
- different target schemas

Incident Review Note  
Always record:
- environment
- DLQ table
- target table
- replay config version
- `MAX_RETRY_COUNT`
- whether replay was manual or scheduled

Environment confusion is a common source of incorrect incident conclusions.

### 1.4 Core Terms

| Term | Purpose / Notes |
|---|---|
| DLQ table | BigQuery table storing failed records from the main pipeline |
| Replay | Controlled reprocessing of DLQ rows |
| Fix-only output | Table storing repaired payloads and repair notes before merge |
| Manual intervention table | Holding table for non-repairable records requiring engineer review |
| CDC pending table | Holding table for CDC-specific replay cases that require state-aware handling |
| Requeue | Republishing the original message to the main Pub/Sub input topic |
| Merge | BigQuery upsert/merge into the target table |
| Retry count | DLQ replay attempt count used for poison-pill protection |
| Poison pill | Record that repeatedly fails and must not loop indefinitely |


## 2. Replay Configuration

### 2.1 Required Configuration Controls
Before operating replay, validate:
- `PROJECT_ID`
- `LOCATION`
- `DLQ_TABLE`
- `DEFAULT_TARGET_TABLE`
- `MERGE_KEY`
- `BATCH_SIZE`
- `MAX_RECORDS`
- `MAX_RETRY_COUNT`
- `RETRY_INPUT_TOPIC`
- `FIX_ONLY_OUTPUT_TABLE`
- `MANUAL_INTERVENTION_TABLE`
- `CDC_PENDING_TABLE`
- `ENABLE_DESERIALIZATION_REPAIR`
- `DISCARD_ERROR_TYPES`
- `REQUIRED_FIELDS`
- `REGEX_RULES_JSON`
- `DLQ_ID_COLUMN` if available
- `MANUAL_INTERVENTION_ALERT_ENABLED` if Cloud Monitoring alerting is desired
- `MANUAL_INTERVENTION_ALERT_THRESHOLD`
- `MANUAL_INTERVENTION_ALERT_NOTIFICATION_CHANNEL`

### 2.2 Configuration Intent

| Setting | Purpose / Notes |
|---|---|
| `DLQ_TABLE` | Source of unreprocessed failed records |
| `DEFAULT_TARGET_TABLE` | Default merge destination when row-level table resolution is absent |
| `MERGE_KEY` | Primary merge key used for replay merge semantics |
| `BATCH_SIZE` | Number of records fetched per replay batch |
| `MAX_RECORDS` | Upper cap for a single replay run |
| `MAX_RETRY_COUNT` | Poison-pill protection threshold |
| `RETRY_INPUT_TOPIC` | Destination for operational/auth/routing retries |
| `FIX_ONLY_OUTPUT_TABLE` | Stores fixed payloads and repair notes |
| `MANUAL_INTERVENTION_TABLE` | Stores rows that must not be auto-merged |
| `CDC_PENDING_TABLE` | Stores CDC cases that need separate state-aware replay |
| `ENABLE_DESERIALIZATION_REPAIR` | Enables safe JSON normalization heuristics |
| `REQUIRED_FIELDS` | Fields that must remain present after repair |
| `REGEX_RULES_JSON` | Regex validation for key fields like `event_id` |
| `MANUAL_INTERVENTION_ALERT_THRESHOLD` | Threshold for Dataflow manual-intervention routing alert |
| `MANUAL_INTERVENTION_ALERT_NOTIFICATION_CHANNEL` | Existing Cloud Monitoring notification channel used for email delivery |

### 2.3 Operational Implications of Changing Core Controls

**Raising `MAX_RETRY_COUNT`**
- more opportunities for transient recovery
- higher risk of prolonged replay churn
- larger time to identify true poison-pill records

**Lowering `MAX_RETRY_COUNT`**
- faster quarantine/manual routing
- less retry churn
- higher operator involvement

**Raising `BATCH_SIZE` / `MAX_RECORDS`**
- faster backlog burn-down
- higher Dataflow resource usage
- higher blast radius if a merge-path issue is introduced

**Lowering `BATCH_SIZE` / `MAX_RECORDS`**
- safer operational control
- slower backlog reduction
- useful during incident isolation

### 2.4 Change-Control Checklist
Before changing replay config:
- confirm the target environment
- confirm target schema compatibility
- confirm retry topic exists and is correct
- confirm manual intervention table exists
- confirm CDC pending table exists
- document old and new values
- avoid changing multiple core controls in one release


## 3. Replay Data Flow and Routing

### 3.1 Architecture Overview
The scheduled replay architecture is:

1. Cloud Scheduler triggers the launcher on a schedule.
2. Cloud Run launcher receives authenticated HTTP request.
3. Launcher calls the Dataflow Flex Template launch API.
4. Dataflow starts the replay batch job.
5. Replay reads unreprocessed DLQ rows from BigQuery.
6. Replay decides one of:
   - merge to target table
   - route to manual intervention
   - route to CDC pending
   - republish to input topic
   - mark retry-pending for later rerun

### 3.2 End-to-End Replay Flow
Each DLQ row follows this path:
1. Row is fetched from `DLQ_TABLE` where `reprocessed = false`.
2. Replay checks poison-pill guards.
3. Replay checks special-routing categories:
   - CDC pending types
   - manual-only business-rule types
   - observability/manual-only types
   - DLQ publish failed
   - partition decorator failures
   - requeue-only operational/auth/routing types
4. If payload processing is required:
   - parse original message
   - attempt deterministic repairs
   - validate against target schema
   - write fix-only result
   - merge repaired payload
5. Replay updates DLQ row state:
   - terminal reprocessed
   - retry-pending
   - manual-routed
   - CDC-pending-routed

### 3.3 Routing Behaviour by Error Family

| Error family | Current handling |
|---|---|
| Transport / Message-Level | deterministic repair if possible, else manual intervention |
| Deserialization | heuristic repair if possible, else terminal/manual depending on case |
| Schema / Coercion / Storage | deterministic repair, validate, merge or manual |
| Infrastructure / Auth / Routing operational failures | republish to `RETRY_INPUT_TOPIC` |
| CDC idempotency / sequencing / contract validation | route to `CDC_PENDING_TABLE` |
| Domain / business-rule validation | manual intervention |
| Poison pill / reprocessing loop | manual intervention / terminal block |
| DLQ observability failures | manual intervention |

### 3.4 Expected Behaviour Across Environments
The same DLQ message may replay differently across environments because:
- target schema may differ
- manual intervention table may differ
- retry topic may differ
- CDC pending table may differ
- replay schedule timing may differ

This is expected and must be documented during incident review.


## 4. Storage Contracts

### 4.1 Purpose of the DLQ Table
The DLQ table is the durable source of replay work. It is not a table to be manually cleaned as a first response.

Critical Distinction  
Do not delete rows from the DLQ table as remediation.  
A DLQ row is an operational record of failed business data and replay state.

### 4.2 Fix-Only Output Table Contract
The fix-only output table exists to make repair behaviour observable before merge.

Expected purpose:
- store original message
- store fixed message
- store repair notes
- support fix verification during testing and incidents

Operationally, it is the first place to check whether a repair was produced correctly.

### 4.3 Manual Intervention Table Contract
The manual intervention table exists to capture:
- non-repairable payloads
- policy-blocked records
- CDC records requiring engineer review after threshold
- poison-pill records
- transport/deserialization rows that cannot be safely recovered

Operationally, it is the authoritative holding area for records that must not be auto-merged.

### 4.4 CDC Pending Replay Table Contract
The CDC pending table exists to isolate CDC-specific replay complexity from generic replay.

It stores cases such as:
- `DUPLICATE_MERGE_KEY`
- `BATCH_DUPLICATE`
- `VERSION_CONFLICT`
- `OUT_OF_ORDER_EVENT`
- `SEQUENCE_GAP_DETECTED`
- `CAUSALITY_VIOLATION`
- `MISSING_OPERATION_TYPE`
- `INVALID_CDC_EVENT`
- `MISSING_METADATA`
- `TOMBSTONE_WITHOUT_KEY`
- `LATE_ARRIVING_EVENT`

These records are terminal for generic replay but not resolved; they are queued for separate CDC-aware handling.

### 4.5 Auditability Expectations
Operations must preserve:
- DLQ failed timestamp
- replay attempt count
- whether row was reprocessed
- fixed payload evidence
- manual routing reason
- CDC pending routing reason
- main-table outcome where applicable


## 5. Replay Infrastructure

### 5.1 Why the Scheduled Replay Stack Exists
The scheduled stack exists to make replay:
- repeatable
- isolated from developer workstations
- deployable through GCP-native controls
- schedulable without manual intervention

### 5.2 Runtime Components

| Component | Purpose / Notes |
|---|---|
| Dataflow Flex Template | Runs the actual replay Beam pipeline |
| Cloud Run launcher | Lightweight control-plane service that starts the Flex Template |
| Cloud Scheduler | Calls the launcher on a schedule |
| GCS template spec | Stores the Flex Template spec |
| GCS config YAML | Stores replay config used by scheduled Dataflow runs |

### 5.3 Infra Preparation Intent
`setup_replay_infra.ps1` prepares the deployment prerequisites:
- enables required APIs
- creates service accounts
- grants IAM roles
- grants bucket access
- creates retry input topic if needed

### 5.4 Readiness Criteria Before Replay Runs
Replay is ready only when all are true:
- DLQ table exists and is queryable
- target tables exist and schema is current
- manual intervention table exists
- CDC pending table exists
- retry topic exists if requeue is enabled
- Flex Template is built
- Cloud Run launcher is deployed
- scheduler target is correct
- Dataflow worker service account has required permissions

Pre-Replay Gate  
Check readiness before every production replay release, not just initial deployment.

### 5.5 Ordering and Idempotency Principles
Replay must preserve deterministic outcomes:
- generic merge is key-based and idempotent
- set-based merge is preferred where possible
- complex type payloads use staging merge path
- CDC cases are not blindly replayed in the generic path
- poison-pill protection prevents infinite loops


## 6. Replay Lifecycle

### 6.1 Core State Model
There are two operational state models.

**DLQ row state**
- `reprocessed = false`: eligible for replay
- `reprocessed = true`: terminal for generic replay

**CDC pending state**
- `PENDING`
- later CDC replay states such as:
  - `REPLAYED`
  - `DISCARDED_DUPLICATE`
  - `MANUAL_ESCALATED`
  - `PENDING` with next retry

### 6.2 Lifecycle Semantics
For generic replay:
- rows start as `reprocessed = false`
- after replay action they become terminal or pending for next run
- operational failures may increment retry and stay pending
- manual and CDC routing are terminal for the generic replay path

### 6.3 Batch Processing Model
A standard generic replay batch:
1. fetches unreprocessed DLQ rows ordered by `failed_timestamp`
2. processes row-level routing and repairs
3. batches mergeable payloads by target table
4. runs set-based merge first
5. falls back to row-by-row merge if needed
6. updates DLQ state

### 6.4 Idempotency Expectations
Replay runs must tolerate:
- reruns of the same Dataflow job scope
- repeated scheduler invocations across days
- previously fixed rows re-entering the pipeline as new DLQ rows
- requeue of operational rows followed by later data-fix replay

### 6.5 Completion Criteria for an Operational Run
A replay run is operationally complete when:
- targeted DLQ rows are no longer unexpectedly left at `reprocessed = false`
- manual-routed rows are visible in the manual intervention table
- CDC-routed rows are visible in the CDC pending table
- retryable operational rows have been republished successfully where applicable
- target-table rows appear with correct repaired values

### 6.6 Partial Replay Handling
If only part of the backlog succeeds:
- identify whether failures are merge-path, schema, or routing related
- inspect fix-only output
- inspect manual intervention growth
- inspect CDC pending growth
- rerun after root-cause correction


## 7. Operational Queries and Evidence Views

### 7.1 Primary Operational Surfaces
This project currently relies on BigQuery operational queries rather than prebuilt dashboards.

Primary evidence surfaces:
- DLQ table
- fix-only output table
- manual intervention table
- CDC pending table
- Cloud Monitoring metric `dataflow.googleapis.com/job/user_counter` filtered to `manual_intervention_routed`
- Cloud Monitoring dashboard for replay counters
- Dataflow job logs
- Cloud Run launcher logs
- Scheduler execution history

### 7.2 Interpretation Guidance
- Rising DLQ with low manual volume suggests replay has not run or target merge path is blocked.
- Rising manual volume suggests schema, payload, or policy drift.
- Rising CDC pending volume suggests state-aware CDC cases are accumulating faster than they are being handled.
- Reprocessed growth without target-table change suggests replay is requeueing or terminal-routing rather than merging.

### 7.3 Evidence Capture Expectations
For incident closure, capture:
- scheduler run time
- launcher invocation result
- Dataflow job ID
- number of rows fetched
- number merged
- number manual-routed
- number CDC-routed
- number requeued
- residual backlog size


## 8. Operations Procedures

### 8.1 Daily Health Procedure
- confirm scheduler ran at expected time
- confirm launcher accepted request
- confirm Dataflow job launched successfully
- confirm no unusual DLQ growth
- confirm manual table growth is within expected bounds
- confirm CDC pending backlog is stable

### 8.2 Incident-Time Operating Procedure
- identify whether the incident is generic replay, CDC routing, or requeue-related
- confirm latest replay deployment version
- inspect fix-only output for a representative failing row
- inspect Dataflow worker error for merge/path issues
- verify target schema for failing rows
- determine whether rerun is safe or whether config/code correction is required first

### 8.3 Replay Trigger Procedure
For manual immediate replay:
- use `run_dataflow_replay.ps1` for ad hoc testing
- or call the deployed launcher endpoint once manually in GCP

For scheduled replay:
- Cloud Scheduler triggers Cloud Run launcher
- launcher starts Dataflow Flex Template
- no workstation is required

### 8.4 Post-Replay Verification Procedure
- verify expected rows in target table
- verify repaired rows in fix-only output
- verify manual-routed rows in manual table
- verify CDC-routed rows in pending table
- verify DLQ retry counts and reprocessed status

### 8.5 Pre-Release / Post-Release Procedure

**Before release**
- validate `.env.job.yaml`
- validate Flex Template build path
- validate launcher environment variables
- validate scheduler target and cadence
- validate target schemas for expected replay cases

**After release**
- trigger one controlled replay
- verify Dataflow launch success
- verify one repaired merge case
- verify one manual-routing case
- verify one requeue case if enabled

### 8.6 Historical Replay / Backfill Procedure
When replaying historical backlog:
- reduce batch size initially
- verify target schema stability first
- monitor manual table growth closely
- confirm CDC cases are not being forced through generic replay
- scale up only after correctness is confirmed

### 8.7 Operational Anti-Patterns to Avoid

Anti-Pattern 1  
Running repeated replay jobs without checking whether failures are deterministic payload issues or operational issues.

Anti-Pattern 2  
Deleting DLQ records to reduce backlog appearance.

Anti-Pattern 3  
Treating CDC sequencing errors as generic replay cases.

Anti-Pattern 4  
Changing replay config and target schema at the same time without controlled validation.

Anti-Pattern 5  
Ignoring fix-only output during troubleshooting.


## 9. Troubleshooting

### 9.1 Problem: DLQ Backlog Grows Continuously

**Likely Causes**
- replay scheduler not firing
- launcher failing to start Dataflow
- Dataflow jobs failing before merge
- high upstream failure rate

**Diagnosis**
- check scheduler execution history
- check Cloud Run launcher logs
- check Dataflow job state
- query `DLQ_TABLE` for oldest unreprocessed rows

**Resolution**
- restore scheduler or launcher path
- fix deployment/config issues
- rerun replay manually with limited scope
- confirm backlog begins to decline

### 9.2 Problem: Rows Stay `reprocessed = false`

**Likely Causes**
- merge failure
- target schema mismatch
- operational merge error marked retry-pending
- Dataflow job using stale code/template

**Diagnosis**
- inspect Dataflow worker logs for exact row failure
- inspect fix-only output for repaired payload
- confirm target schema at runtime
- confirm Flex Template was rebuilt after code changes

**Resolution**
- fix merge-path or schema issue
- rebuild Flex Template
- rerun replay
- verify row becomes terminal

### 9.3 Problem: CDC Pending Backlog Grows

**Likely Causes**
- many CDC sequencing/idempotency errors reaching generic replay
- no CDC-specific replay consumer yet
- source stream contract drift

**Diagnosis**
- query pending table by `error_type`
- inspect route reasons
- identify whether issue is sequence, version, or envelope related

**Resolution**
- stabilise upstream CDC contract
- implement/operate CDC-specific replay worker
- manually review and resolve oldest pending records first

### 9.4 Problem: Manual Intervention Volume Spikes

**Likely Causes**
- target schema drift
- new payload format
- transport corruption spike
- bad deployment or wrong environment config

**Diagnosis**
- inspect `reason` values in manual table
- group by error type
- compare to recent schema/config changes

**Resolution**
- fix schema/config mismatch
- update deterministic repairs if safe
- rerun replay after correction

### 9.5 Problem: Scheduler / Launcher / Flex Template Fails

**Likely Causes**
- wrong GCS template path
- wrong config YAML GCS path
- missing IAM on launcher or Dataflow SA
- invalid Cloud Run launcher env vars

**Diagnosis**
- check Cloud Run launcher response and logs
- check Dataflow launch API error
- verify template and config files exist in GCS
- verify service account permissions

**Resolution**
- correct GCS paths
- grant missing IAM
- redeploy launcher or rebuild Flex Template
- rerun one controlled launch

### 9.6 Problem: Replay Completes But Target Data Looks Wrong

**Likely Causes**
- repair logic produced technically valid but semantically wrong data
- merge key mismatch
- wrong target table resolution
- CDC-style cases were merged through generic path unexpectedly

**Diagnosis**
- compare original message, fixed message, and target row
- validate merge key and target table
- inspect fix notes in fix-only output
- inspect worker logs for fallback merge behaviour

**Resolution**
- correct fix logic or routing rule
- rebuild template
- rerun affected scope with controlled limit


## 10. DLQ Replay Execution Procedure

### Step 1. Prepare Environment
Update [.env.job.yaml](/c:/Users/sreer/Desktop/da/dlq_replay/.env.job.yaml) with the target environment values.

Core values to validate:
```yaml
PROJECT_ID: "stream-accelerator-3"
LOCATION: "us-central1"
DLQ_TABLE: "stream-accelerator-3.dataengineering.streaming_data_dlq"
DEFAULT_TARGET_TABLE: "stream-accelerator-3.dataengineering.streaming_data"
MERGE_KEY: "event_id"
RETRY_INPUT_TOPIC: "projects/stream-accelerator-3/topics/dlq_replay_retry_test"
MANUAL_INTERVENTION_TABLE: "stream-accelerator-3.dataengineering.manual_intervention"
CDC_PENDING_TABLE: "stream-accelerator-3.dataengineering.cdc_pending_replay"
```

### Step 2. Prepare Replay Infrastructure
```powershell
.\scripts\setup_replay_infra.ps1 `
  -ProjectId "stream-accelerator-3" `
  -Region "us-central1" `
  -TempBucket "gs://dlq_replay" `
  -StagingBucket "gs://dlq_replay" `
  -TemplateBucket "gs://dlq_replay" `
  -RetryInputTopic "projects/stream-accelerator-3/topics/dlq_replay_retry_test"
```

### Step 3. Build and Stage the Flex Template
```powershell
.\scripts\deploy_replay_flextemplate.ps1 `
  -ProjectId "stream-accelerator-3" `
  -Region "us-central1" `
  -TemplateBucket "gs://dlq_replay" `
  -ConfigYaml ".env.job.yaml"
```

### Step 4. Deploy the Launcher
```powershell
.\scripts\deploy_replay_launcher.ps1 `
  -ProjectId "stream-accelerator-3" `
  -Region "us-central1" `
  -ServiceName "dlq-replay-launcher" `
  -TemplateSpecGcsPath "gs://dlq_replay/templates/dlq-replay-flex.json" `
  -ConfigYamlGcsPath "gs://dlq_replay/config/dlq-replay-config.yaml" `
  -TempLocation "gs://dlq_replay/dataflow/tmp" `
  -StagingLocation "gs://dlq_replay/dataflow/staging" `
  -DataflowServiceAccount "dlq-replay-dataflow-sa@stream-accelerator-3.iam.gserviceaccount.com"
```

### Step 5. Create the Scheduler
```powershell
.\scripts\create_replay_scheduler.ps1 `
  -ProjectId "stream-accelerator-3" `
  -Region "us-central1" `
  -SchedulerJobName "run-dlq-replay-nightly" `
  -LauncherServiceName "dlq-replay-launcher" `
  -Schedule "0 23 * * *" `
  -TimeZone "Asia/Calcutta"
```

### Step 6. Create the Manual Intervention Alert
```powershell
.\scripts\deploy_manual_intervention_alert.ps1 `
  -ProjectId "stream-accelerator-3" `
  -Region "us-central1" `
  -ConfigYaml ".env.job.yaml"
```

### Step 7. Deploy the Replay Dashboard
```powershell
.\scripts\deploy_replay_dashboard.ps1 `
  -ProjectId "stream-accelerator-3" `
  -Region "us-central1"
```

### Step 8. Trigger Replay Manually (Immediate Run)
Option 1: submit ad hoc Dataflow directly
```powershell
.\scripts\run_dataflow_replay.ps1 `
  -ProjectId "stream-accelerator-3" `
  -Region "us-central1" `
  -JobName "dlq-replay-manual-$(Get-Date -Format yyyyMMdd-HHmmss)" `
  -TempLocation "gs://dlq_replay/dataflow/tmp" `
  -StagingLocation "gs://dlq_replay/dataflow/staging" `
  -ConfigYaml ".env.job.yaml" `
  -Limit 100 `
  -PythonExe "python"
```

Option 2: trigger the deployed scheduler job
```powershell
gcloud scheduler jobs run run-dlq-replay-nightly `
  --location us-central1 `
  --project stream-accelerator-3
```

### Step 9. Check DLQ Replay Status
```sql
SELECT
  COUNT(*) AS total_unreprocessed,
  COUNTIF(error_type = 'BIGQUERY_INSERT_FAILED') AS bq_insert_failed,
  COUNTIF(error_type = 'DESERIALIZATION_ERROR') AS deserialization_errors,
  COUNTIF(error_type IN ('OUT_OF_ORDER_EVENT','SEQUENCE_GAP_DETECTED','CAUSALITY_VIOLATION')) AS cdc_ordering_errors
FROM `stream-accelerator-3.dataengineering.streaming_data_dlq`
WHERE reprocessed = FALSE;
```

### Step 10. Check CDC Pending Queue
```sql
SELECT
  status,
  error_type,
  COUNT(*) AS cnt
FROM `stream-accelerator-3.dataengineering.cdc_pending_replay`
GROUP BY status, error_type
ORDER BY cnt DESC;
```

### Step 11. Verify Recent Fixes
```sql
SELECT
  error_type,
  fixes_applied,
  target_table,
  fixed_message
FROM `stream-accelerator-3.dataengineering.fix_only_results`
ORDER BY processed_at DESC
LIMIT 50;
```

### Step 12. Verify Manual Intervention Volume
```sql
SELECT
  reason,
  COUNT(*) AS cnt
FROM `stream-accelerator-3.dataengineering.manual_intervention`
GROUP BY reason
ORDER BY cnt DESC
LIMIT 50;
```

### Step 13. Validate Main Table After Replay
```sql
SELECT
  event_id,
  COUNT(*) AS cnt
FROM `stream-accelerator-3.dataengineering.streaming_data`
GROUP BY event_id
HAVING cnt > 1
LIMIT 20;
```

### Step 14. Recovery if Replay Fails Mid-Run
Use the latest Dataflow job logs first.  
Do not reset or delete DLQ rows blindly.  
Correct the underlying issue, rebuild the Flex Template if code changed, and rerun replay with a limited scope first.
