param(
  [Parameter(Mandatory = $true)][string]$ProjectId,
  [string]$Region = "us-central1",
  [string]$DashboardTemplatePath = ".\\monitoring\\dlq_replay_dashboard.json",
  [string]$DashboardDisplayName = "DLQ Replay Dashboard"
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

if (-not (Test-Path $DashboardTemplatePath)) {
  throw "Dashboard template not found: $DashboardTemplatePath"
}

Invoke-GCloud @(
  "services", "enable",
  "monitoring.googleapis.com",
  "--project", $ProjectId
)

$dashboardJson = Get-Content $DashboardTemplatePath -Raw

try {
  $dashboardObject = $dashboardJson | ConvertFrom-Json
} catch {
  throw "Dashboard template is not valid JSON: $DashboardTemplatePath"
}

$dashboardObject.displayName = $DashboardDisplayName

$tmpDashboardFile = Join-Path $env:TEMP "dlq-replay-dashboard.json"
$dashboardObject | ConvertTo-Json -Depth 50 | Set-Content -Path $tmpDashboardFile -Encoding UTF8

$existingDashboardsJson = & gcloud monitoring dashboards list --project $ProjectId --format json
if ($LASTEXITCODE -ne 0) {
  throw "Failed to list existing dashboards."
}

$existingDashboards = @()
if ($existingDashboardsJson) {
  $parsed = $existingDashboardsJson | ConvertFrom-Json
  if ($parsed -is [System.Array]) {
    $existingDashboards = $parsed
  } elseif ($parsed) {
    $existingDashboards = @($parsed)
  }
}

$existingDashboard = $existingDashboards | Where-Object { $_.displayName -eq $DashboardDisplayName } | Select-Object -First 1

if ($existingDashboard) {
  Invoke-GCloud @(
    "monitoring", "dashboards", "update", $existingDashboard.name,
    "--project", $ProjectId,
    "--config-from-file", $tmpDashboardFile
  )
  Write-Host "`nUpdated dashboard: $DashboardDisplayName"
} else {
  Invoke-GCloud @(
    "monitoring", "dashboards", "create",
    "--project", $ProjectId,
    "--config-from-file", $tmpDashboardFile
  )
  Write-Host "`nCreated dashboard: $DashboardDisplayName"
}
