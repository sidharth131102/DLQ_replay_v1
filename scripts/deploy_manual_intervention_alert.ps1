param(
  [Parameter(Mandatory = $true)][string]$ProjectId,
  [string]$Region = "us-central1",
  [string]$ConfigYaml = ".\\.env.job.yaml",
  [string]$PolicyDisplayName = "DLQ Replay Manual Intervention Threshold",
  [string]$Threshold = "",
  [string]$NotificationChannel = "",
  [string]$AlignmentPeriod = "",
  [string]$Duration = "",
  [string]$JobNamePrefix = "dlq-replay"
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

function Get-SimpleYamlValue {
  param(
    [Parameter(Mandatory = $true)][string]$Path,
    [Parameter(Mandatory = $true)][string]$Key
  )
  if (-not (Test-Path $Path)) {
    return ""
  }
  foreach ($line in Get-Content $Path) {
    if ($line -match "^\s*$Key\s*:\s*(.+?)\s*$") {
      $value = $Matches[1].Trim()
      if (($value.StartsWith('"') -and $value.EndsWith('"')) -or ($value.StartsWith("'") -and $value.EndsWith("'"))) {
        return $value.Substring(1, $value.Length - 2)
      }
      return $value
    }
  }
  return ""
}

function Normalize-NotificationChannel {
  param([string]$Channel)
  if (-not $Channel) { return "" }
  $value = $Channel.Trim()
  if ($value -match "^projects\/[^\/]+\/notificationChannels\/[^\/]+$") {
    return $value
  }
  return "projects/$ProjectId/notificationChannels/$value"
}

$enabledRaw = Get-SimpleYamlValue -Path $ConfigYaml -Key "MANUAL_INTERVENTION_ALERT_ENABLED"
$enabled = $enabledRaw.Trim().ToLower() -eq "true"

if (-not $Threshold) {
  $Threshold = Get-SimpleYamlValue -Path $ConfigYaml -Key "MANUAL_INTERVENTION_ALERT_THRESHOLD"
}
if (-not $NotificationChannel) {
  $NotificationChannel = Get-SimpleYamlValue -Path $ConfigYaml -Key "MANUAL_INTERVENTION_ALERT_NOTIFICATION_CHANNEL"
}
if (-not $AlignmentPeriod) {
  $AlignmentPeriod = Get-SimpleYamlValue -Path $ConfigYaml -Key "MANUAL_INTERVENTION_ALERT_ALIGNMENT_PERIOD"
}
if (-not $Duration) {
  $Duration = Get-SimpleYamlValue -Path $ConfigYaml -Key "MANUAL_INTERVENTION_ALERT_DURATION"
}

if (-not $AlignmentPeriod) { $AlignmentPeriod = "300s" }
if (-not $Duration) { $Duration = "0s" }

if (-not $enabled) {
  Write-Host "MANUAL_INTERVENTION_ALERT_ENABLED is not true. Skipping alert deployment."
  return
}

if (-not $Threshold) {
  throw "Manual intervention alert threshold is required."
}
if (-not $NotificationChannel) {
  throw "Manual intervention notification channel is required."
}

$notificationChannelName = Normalize-NotificationChannel $NotificationChannel

Invoke-GCloud @(
  "services", "enable",
  "monitoring.googleapis.com",
  "--project", $ProjectId
)

$policy = @{
  displayName = $PolicyDisplayName
  documentation = @{
    content = @"
Triggers when the summed delta of the Dataflow custom counter `manual_intervention_routed`
exceeds the configured threshold for replay jobs in project `$ProjectId` and region `$Region`.

Expected action:
1. Inspect the latest Dataflow replay job.
2. Check fix-only output and manual intervention tables.
3. Confirm whether the spike is due to schema drift, payload corruption, or policy routing.
"@
    mimeType = "text/markdown"
  }
  combiner = "OR"
  enabled = $true
  notificationChannels = @($notificationChannelName)
  conditions = @(
    @{
      displayName = "Manual intervention routed count > $Threshold"
      conditionThreshold = @{
        filter = @"
metric.type="dataflow.googleapis.com/job/user_counter"
resource.type="dataflow_job"
resource.labels.project_id="$ProjectId"
resource.labels.region="$Region"
metric.labels.metric_name="manual_intervention_routed"
"@ -replace "`r?`n", " "
        aggregations = @(
          @{
            alignmentPeriod = $AlignmentPeriod
            perSeriesAligner = "ALIGN_DELTA"
            crossSeriesReducer = "REDUCE_SUM"
            groupByFields = @()
          }
        )
        comparison = "COMPARISON_GT"
        thresholdValue = [double]$Threshold
        duration = $Duration
        trigger = @{
          count = 1
        }
      }
    }
  )
  userLabels = @{
    service = "dlq_replay"
    metric = "manual_intervention_routed"
    launcher = $JobNamePrefix.Replace("_", "-")
  }
}

$tmpPolicyFile = Join-Path $env:TEMP "dlq-replay-manual-intervention-alert.json"
$policy | ConvertTo-Json -Depth 20 | Set-Content -Path $tmpPolicyFile -Encoding UTF8

$existingPolicy = & gcloud alpha monitoring policies list `
  --project $ProjectId `
  --format "value(name)" `
  --filter "displayName=""$PolicyDisplayName""" 2>$null

if ($LASTEXITCODE -ne 0) {
  throw "Failed to list existing monitoring policies."
}

if ($existingPolicy) {
  Write-Host "Existing policy found. Replacing: $existingPolicy"
  Invoke-GCloud @(
    "alpha", "monitoring", "policies", "delete", $existingPolicy,
    "--project", $ProjectId,
    "--quiet"
  )
}

Invoke-GCloud @(
  "alpha", "monitoring", "policies", "create",
  "--project", $ProjectId,
  "--policy-from-file", $tmpPolicyFile
)

Write-Host "`nManual intervention alert policy deployment complete."
Write-Host "Policy display name: $PolicyDisplayName"
Write-Host "Threshold: $Threshold"
Write-Host "Notification channel: $notificationChannelName"
