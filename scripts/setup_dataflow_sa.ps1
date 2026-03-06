param(
  [Parameter(Mandatory = $true)][string]$ProjectId,
  [Parameter(Mandatory = $true)][string]$ServiceAccountName,
  [string]$TempBucket = "",      # e.g. gs://my-bucket
  [string]$StagingBucket = "",   # e.g. gs://my-bucket
  [string]$RetryInputTopic = "", # e.g. projects/my-project/topics/main-input
  [switch]$SkipEnableApis
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

function Normalize-BucketName {
  param([string]$BucketUriOrName)
  if (-not $BucketUriOrName) { return "" }
  $name = $BucketUriOrName.Trim()
  if ($name.StartsWith("gs://")) {
    $name = $name.Substring(5)
  }
  return $name.TrimEnd("/")
}

function Normalize-TopicName {
  param([string]$TopicPathOrName)
  if (-not $TopicPathOrName) { return "" }
  $name = $TopicPathOrName.Trim()
  if ($name -match "^projects\/[^\/]+\/topics\/([^\/]+)$") {
    return $Matches[1]
  }
  return $name
}

$serviceAccountEmail = "$ServiceAccountName@$ProjectId.iam.gserviceaccount.com"

Write-Host "Setting up Dataflow worker service account: $serviceAccountEmail"

if (-not $SkipEnableApis) {
  # Enable required APIs.
  Invoke-GCloud @(
    "services", "enable",
    "dataflow.googleapis.com",
    "bigquery.googleapis.com",
    "storage.googleapis.com",
    "pubsub.googleapis.com",
    "monitoring.googleapis.com",
    "--project", $ProjectId
  )
} else {
  Write-Host "Skipping API enable step (SkipEnableApis requested)."
}

# Create SA if missing.
$saExists = $false
try {
  & gcloud iam service-accounts describe $serviceAccountEmail --project $ProjectId 1>$null 2>$null
  if ($LASTEXITCODE -eq 0) {
    $saExists = $true
  }
} catch {
  $saExists = $false
}

if (-not $saExists) {
  Invoke-GCloud @(
    "iam", "service-accounts", "create", $ServiceAccountName,
    "--project", $ProjectId,
    "--display-name", "DLQ Replay Dataflow Worker SA"
  )
} else {
  Write-Host "Service account already exists."
}

# Project-level roles.
$projectRoles = @(
  "roles/dataflow.worker",
  "roles/bigquery.jobUser",
  "roles/bigquery.dataEditor",
  "roles/bigquery.dataViewer",
  "roles/monitoring.metricWriter"
)

foreach ($role in $projectRoles) {
  Invoke-GCloud @(
    "projects", "add-iam-policy-binding", $ProjectId,
    "--member", "serviceAccount:$serviceAccountEmail",
    "--role", $role
  )
}

# Optional bucket IAM for temp/staging.
$tempBucketName = Normalize-BucketName $TempBucket
$stagingBucketName = Normalize-BucketName $StagingBucket
$bucketNames = @($tempBucketName, $stagingBucketName) | Where-Object { $_ } | Select-Object -Unique

foreach ($bucket in $bucketNames) {
  Invoke-GCloud @(
    "storage", "buckets", "add-iam-policy-binding", "gs://$bucket",
    "--member", "serviceAccount:$serviceAccountEmail",
    "--role", "roles/storage.objectAdmin",
    "--project", $ProjectId
  )
}

# Optional topic IAM for retry publishing.
$topicNames = @(
  (Normalize-TopicName $RetryInputTopic)
) | Where-Object { $_ } | Select-Object -Unique

foreach ($topic in $topicNames) {
  Invoke-GCloud @(
    "pubsub", "topics", "add-iam-policy-binding", $topic,
    "--project", $ProjectId,
    "--member", "serviceAccount:$serviceAccountEmail",
    "--role", "roles/pubsub.publisher"
  )
}

Write-Host "`nDataflow worker service account setup complete."
Write-Host "Service Account: $serviceAccountEmail"
