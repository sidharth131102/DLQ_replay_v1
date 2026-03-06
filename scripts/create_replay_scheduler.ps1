param(
  [Parameter(Mandatory = $true)][string]$ProjectId,
  [Parameter(Mandatory = $true)][string]$Region,
  [Parameter(Mandatory = $true)][string]$SchedulerJobName,
  [Parameter(Mandatory = $true)][string]$LauncherServiceName,
  [string]$SchedulerServiceAccountName = "dlq-replay-scheduler-sa",
  [string]$Schedule = "0 23 * * *",
  [string]$TimeZone = "Asia/Calcutta",
  [string]$RequestBody = "{}"
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

$schedulerServiceAccount = "$SchedulerServiceAccountName@$ProjectId.iam.gserviceaccount.com"

Invoke-GCloud @(
  "services", "enable",
  "cloudscheduler.googleapis.com",
  "run.googleapis.com",
  "iamcredentials.googleapis.com",
  "--project", $ProjectId
)

& gcloud iam service-accounts describe $schedulerServiceAccount --project $ProjectId 1>$null 2>$null
if ($LASTEXITCODE -ne 0) {
  Invoke-GCloud @(
    "iam", "service-accounts", "create", $SchedulerServiceAccountName,
    "--project", $ProjectId,
    "--display-name", "DLQ Replay Scheduler SA"
  )
}

$serviceUrl = & gcloud run services describe $LauncherServiceName --project $ProjectId --region $Region --format "value(status.url)"
if ($LASTEXITCODE -ne 0 -or -not $serviceUrl) {
  throw "Unable to resolve Cloud Run service URL for $LauncherServiceName"
}
$launchUrl = "$serviceUrl/launch"

Invoke-GCloud @(
  "run", "services", "add-iam-policy-binding", $LauncherServiceName,
  "--project", $ProjectId,
  "--region", $Region,
  "--member", "serviceAccount:$schedulerServiceAccount",
  "--role", "roles/run.invoker"
)

$projectNumber = & gcloud projects describe $ProjectId --format "value(projectNumber)"
if ($LASTEXITCODE -ne 0 -or -not $projectNumber) {
  throw "Unable to resolve project number for $ProjectId"
}
$cloudSchedulerServiceAgent = "service-$projectNumber@gcp-sa-cloudscheduler.iam.gserviceaccount.com"
Invoke-GCloud @(
  "iam", "service-accounts", "add-iam-policy-binding", $schedulerServiceAccount,
  "--project", $ProjectId,
  "--member", "serviceAccount:$cloudSchedulerServiceAgent",
  "--role", "roles/iam.serviceAccountTokenCreator"
)

& gcloud scheduler jobs describe $SchedulerJobName --project $ProjectId --location $Region 1>$null 2>$null
$jobExists = $LASTEXITCODE -eq 0

$commonArgs = @(
  "--project", $ProjectId,
  "--location", $Region,
  "--schedule", $Schedule,
  "--time-zone", $TimeZone,
  "--uri", $launchUrl,
  "--http-method", "POST",
  "--oidc-service-account-email", $schedulerServiceAccount,
  "--oidc-token-audience", $serviceUrl,
  "--message-body", $RequestBody
)

if ($jobExists) {
  $args = @("scheduler", "jobs", "update", "http", $SchedulerJobName) + $commonArgs
  Invoke-GCloud -GcloudArgs $args
} else {
  $args = @("scheduler", "jobs", "create", "http", $SchedulerJobName) + $commonArgs
  Invoke-GCloud -GcloudArgs $args
}

Write-Host "`nReplay scheduler setup complete."
Write-Host "Scheduler target: $launchUrl"
