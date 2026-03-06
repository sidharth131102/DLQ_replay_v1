param(
  [Parameter(Mandatory = $true)][string]$ProjectId,
  [Parameter(Mandatory = $true)][string]$Region,
  [Parameter(Mandatory = $true)][string]$ServiceName,
  [Parameter(Mandatory = $true)][string]$TemplateSpecGcsPath,
  [Parameter(Mandatory = $true)][string]$ConfigYamlGcsPath,
  [Parameter(Mandatory = $true)][string]$TempLocation,
  [Parameter(Mandatory = $true)][string]$StagingLocation,
  [string]$DataflowServiceAccount = "",
  [string]$LauncherServiceAccountName = "dlq-replay-launcher-sa",
  [string]$DataflowServiceAccountName = "dlq-replay-dataflow-sa",
  [string]$JobNamePrefix = "dlq-replay",
  [string]$SourceDir = ".\\launchers\\dataflow_replay_launcher",
  [string]$Subnetwork = "",
  [string]$Network = "",
  [string]$MachineType = "",
  [string]$AdditionalExperiments = "use_runner_v2",
  [string]$MaxWorkers = "",
  [string]$NumWorkers = ""
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

if (-not $DataflowServiceAccount) {
  $DataflowServiceAccount = "$DataflowServiceAccountName@$ProjectId.iam.gserviceaccount.com"
}
$launcherServiceAccount = "$LauncherServiceAccountName@$ProjectId.iam.gserviceaccount.com"

Invoke-GCloud @(
  "services", "enable",
  "run.googleapis.com",
  "cloudbuild.googleapis.com",
  "artifactregistry.googleapis.com",
  "dataflow.googleapis.com",
  "iamcredentials.googleapis.com",
  "--project", $ProjectId
)

& gcloud iam service-accounts describe $launcherServiceAccount --project $ProjectId 1>$null 2>$null
if ($LASTEXITCODE -ne 0) {
  Invoke-GCloud @(
    "iam", "service-accounts", "create", $LauncherServiceAccountName,
    "--project", $ProjectId,
    "--display-name", "DLQ Replay Launcher SA"
  )
}

Invoke-GCloud @(
  "projects", "add-iam-policy-binding", $ProjectId,
  "--member", "serviceAccount:$launcherServiceAccount",
  "--role", "roles/dataflow.admin"
)
Invoke-GCloud @(
  "projects", "add-iam-policy-binding", $ProjectId,
  "--member", "serviceAccount:$launcherServiceAccount",
  "--role", "roles/logging.logWriter"
)
Invoke-GCloud @(
  "iam", "service-accounts", "add-iam-policy-binding", $DataflowServiceAccount,
  "--project", $ProjectId,
  "--member", "serviceAccount:$launcherServiceAccount",
  "--role", "roles/iam.serviceAccountUser"
)

$envVars = @(
  "PROJECT_ID=$ProjectId",
  "REGION=$Region",
  "FLEX_TEMPLATE_SPEC_GCS_PATH=$TemplateSpecGcsPath",
  "CONFIG_YAML_GCS_PATH=$ConfigYamlGcsPath",
  "TEMP_LOCATION=$TempLocation",
  "STAGING_LOCATION=$StagingLocation",
  "DATAFLOW_SERVICE_ACCOUNT=$DataflowServiceAccount",
  "JOB_NAME_PREFIX=$JobNamePrefix"
)
if ($Subnetwork) { $envVars += "SUBNETWORK=$Subnetwork" }
if ($Network) { $envVars += "NETWORK=$Network" }
if ($MachineType) { $envVars += "MACHINE_TYPE=$MachineType" }
if ($AdditionalExperiments) { $envVars += "ADDITIONAL_EXPERIMENTS=$AdditionalExperiments" }
if ($MaxWorkers) { $envVars += "MAX_WORKERS=$MaxWorkers" }
if ($NumWorkers) { $envVars += "NUM_WORKERS=$NumWorkers" }

Invoke-GCloud @(
  "run", "deploy", $ServiceName,
  "--project", $ProjectId,
  "--region", $Region,
  "--source", $SourceDir,
  "--service-account", $launcherServiceAccount,
  "--set-env-vars", ($envVars -join ","),
  "--no-allow-unauthenticated"
)

$serviceUrl = & gcloud run services describe $ServiceName --project $ProjectId --region $Region --format "value(status.url)"
if ($LASTEXITCODE -ne 0) {
  throw "Failed to fetch Cloud Run service URL for $ServiceName"
}

Write-Host "`nReplay launcher deployment complete."
Write-Host "Service URL: $serviceUrl"
