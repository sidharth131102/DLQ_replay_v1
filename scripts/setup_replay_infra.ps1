param(
  [Parameter(Mandatory = $true)][string]$ProjectId,
  [string]$Region = "us-central1",
  [string]$DataflowServiceAccountName = "dlq-replay-dataflow-sa",
  [string]$LauncherServiceAccountName = "dlq-replay-launcher-sa",
  [string]$SchedulerServiceAccountName = "dlq-replay-scheduler-sa",
  [string]$TempBucket = "",
  [string]$StagingBucket = "",
  [string]$TemplateBucket = "",
  [string]$RetryInputTopic = "",
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

function Ensure-ServiceAccount {
  param(
    [Parameter(Mandatory = $true)][string]$Name,
    [Parameter(Mandatory = $true)][string]$DisplayName
  )
  $email = "$Name@$ProjectId.iam.gserviceaccount.com"
  & gcloud iam service-accounts describe $email --project $ProjectId 1>$null 2>$null
  if ($LASTEXITCODE -ne 0) {
    Invoke-GCloud @(
      "iam", "service-accounts", "create", $Name,
      "--project", $ProjectId,
      "--display-name", $DisplayName
    )
  } else {
    Write-Host "Service account already exists: $email"
  }
  return $email
}

function Add-ProjectRole {
  param(
    [Parameter(Mandatory = $true)][string]$ServiceAccountEmail,
    [Parameter(Mandatory = $true)][string]$Role
  )
  Invoke-GCloud @(
    "projects", "add-iam-policy-binding", $ProjectId,
    "--member", "serviceAccount:$ServiceAccountEmail",
    "--role", $Role
  )
}

if (-not $SkipEnableApis) {
  Invoke-GCloud @(
    "services", "enable",
    "dataflow.googleapis.com",
    "bigquery.googleapis.com",
    "storage.googleapis.com",
    "pubsub.googleapis.com",
    "monitoring.googleapis.com",
    "run.googleapis.com",
    "cloudscheduler.googleapis.com",
    "cloudbuild.googleapis.com",
    "artifactregistry.googleapis.com",
    "iamcredentials.googleapis.com",
    "--project", $ProjectId
  )
}

$dataflowServiceAccount = Ensure-ServiceAccount -Name $DataflowServiceAccountName -DisplayName "DLQ Replay Dataflow Worker SA"
$launcherServiceAccount = Ensure-ServiceAccount -Name $LauncherServiceAccountName -DisplayName "DLQ Replay Launcher SA"
$schedulerServiceAccount = Ensure-ServiceAccount -Name $SchedulerServiceAccountName -DisplayName "DLQ Replay Scheduler SA"

foreach ($role in @(
  "roles/dataflow.worker",
  "roles/bigquery.jobUser",
  "roles/bigquery.dataEditor",
  "roles/bigquery.dataViewer",
  "roles/monitoring.metricWriter"
)) {
  Add-ProjectRole -ServiceAccountEmail $dataflowServiceAccount -Role $role
}

foreach ($role in @(
  "roles/dataflow.admin",
  "roles/logging.logWriter"
)) {
  Add-ProjectRole -ServiceAccountEmail $launcherServiceAccount -Role $role
}

$bucketNames = @(
  (Normalize-BucketName $TempBucket),
  (Normalize-BucketName $StagingBucket),
  (Normalize-BucketName $TemplateBucket)
) | Where-Object { $_ } | Select-Object -Unique

foreach ($bucket in $bucketNames) {
  Invoke-GCloud @(
    "storage", "buckets", "add-iam-policy-binding", "gs://$bucket",
    "--member", "serviceAccount:$dataflowServiceAccount",
    "--role", "roles/storage.objectAdmin",
    "--project", $ProjectId
  )
  Invoke-GCloud @(
    "storage", "buckets", "add-iam-policy-binding", "gs://$bucket",
    "--member", "serviceAccount:$launcherServiceAccount",
    "--role", "roles/storage.objectViewer",
    "--project", $ProjectId
  )
}

$topicNames = @(
  (Normalize-TopicName $RetryInputTopic)
) | Where-Object { $_ } | Select-Object -Unique

foreach ($topic in $topicNames) {
  & gcloud pubsub topics describe $topic --project $ProjectId 1>$null 2>$null
  if ($LASTEXITCODE -ne 0) {
    Invoke-GCloud @(
      "pubsub", "topics", "create", $topic,
      "--project", $ProjectId
    )
  }

  Invoke-GCloud @(
    "pubsub", "topics", "add-iam-policy-binding", $topic,
    "--project", $ProjectId,
    "--member", "serviceAccount:$dataflowServiceAccount",
    "--role", "roles/pubsub.publisher"
  )
}

Invoke-GCloud @(
  "iam", "service-accounts", "add-iam-policy-binding", $dataflowServiceAccount,
  "--project", $ProjectId,
  "--member", "serviceAccount:$launcherServiceAccount",
  "--role", "roles/iam.serviceAccountUser"
)

Write-Host "`nReplay infrastructure setup complete."
Write-Host "Dataflow worker SA: $dataflowServiceAccount"
Write-Host "Launcher SA: $launcherServiceAccount"
Write-Host "Scheduler SA: $schedulerServiceAccount"
