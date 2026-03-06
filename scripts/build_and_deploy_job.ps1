param(
  [Parameter(Mandatory = $true)][string]$ProjectId,
  [Parameter(Mandatory = $true)][string]$Region,
  [Parameter(Mandatory = $true)][string]$JobName,
  [Parameter(Mandatory = $true)][string]$RepositoryPath, # e.g. us-central1-docker.pkg.dev/stream-accelerator-3/dlq-replay
  [string]$ImageName = "dlq-replay",
  [string]$ImageTag = "latest",
  [string]$ServiceAccountName = "dlq-replay-job-sa",
  [Parameter(Mandatory = $true)][string]$EnvFile,
  [switch]$SkipTestRun
)

$ErrorActionPreference = "Stop"

function Invoke-GCloud {
  param(
    [Parameter(Mandatory = $true)]
    [string[]]$GcloudArgs
  )
  Write-Host "`n> gcloud $($GcloudArgs -join ' ')"
  & gcloud @GcloudArgs
  if ($LASTEXITCODE -ne 0) {
    throw "gcloud command failed: gcloud $($GcloudArgs -join ' ')"
  }
}

Write-Host "Starting build + deploy flow for Cloud Run Job '$JobName' in '$Region'..."

$serviceAccountEmail = "$ServiceAccountName@$ProjectId.iam.gserviceaccount.com"
$imageUri = "$RepositoryPath/$ImageName`:$ImageTag"

# Ensure required APIs.
Invoke-GCloud @(
  "services", "enable",
  "run.googleapis.com",
  "bigquery.googleapis.com",
  "artifactregistry.googleapis.com",
  "cloudbuild.googleapis.com",
  "--project", $ProjectId
)

# Create service account if it does not exist.
Write-Host "`nChecking service account: $serviceAccountEmail"
gcloud iam service-accounts describe $serviceAccountEmail --project $ProjectId 1>$null 2>$null
if ($LASTEXITCODE -ne 0) {
  Write-Host "Service account not found. Creating..."
  Invoke-GCloud @(
    "iam", "service-accounts", "create", $ServiceAccountName,
    "--project", $ProjectId,
    "--display-name", "DLQ Replay Cloud Run Job SA"
  )
} else {
  Write-Host "Service account already exists."
}

# Grant runtime permissions for BigQuery replay operations.
foreach ($role in @(
  "roles/bigquery.jobUser",
  "roles/bigquery.dataEditor",
  "roles/bigquery.dataViewer"
)) {
  Invoke-GCloud @(
    "projects", "add-iam-policy-binding", $ProjectId,
    "--member", "serviceAccount:$serviceAccountEmail",
    "--role", $role
  )
}

# Build and push container image using Cloud Build.
Invoke-GCloud @(
  "builds", "submit",
  "--project", $ProjectId,
  "--tag", $imageUri
)

# Deploy Cloud Run Job with env vars and runtime service account.
Invoke-GCloud @(
  "run", "jobs", "deploy", $JobName,
  "--project", $ProjectId,
  "--region", $Region,
  "--image", $imageUri,
  "--service-account", $serviceAccountEmail,
  "--env-vars-file", $EnvFile,
  "--max-retries", "1",
  "--task-timeout", "7200s",
  "--tasks", "1",
  "--execute-now=false"
)

if (-not $SkipTestRun) {
  Write-Host "`nExecuting one manual test run before scheduler setup..."
  Invoke-GCloud @(
    "run", "jobs", "execute", $JobName,
    "--project", $ProjectId,
    "--region", $Region,
    "--wait"
  )
  Write-Host "`nTest run finished."
} else {
  Write-Host "`nSkipped manual test run (SkipTestRun requested)."
}

Write-Host "`nDone. Scheduler is not created by this script."
Write-Host "When ready, create scheduler separately via scripts/create_scheduler_job.ps1."
