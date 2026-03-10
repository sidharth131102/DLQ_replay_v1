param(
  [Parameter(Mandatory = $true)][string]$ProjectId,
  [string]$Region = "us-central1",
  [string]$ConfigYaml = ".\\.env.job.yaml",
  [string]$PolicyDisplayName = "DLQ Replay Dataflow Job Failed",
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

$enabledRaw = Get-SimpleYamlValue -Path $ConfigYaml -Key "REPLAY_JOB_FAILURE_ALERT_ENABLED"
$enabled = $enabledRaw.Trim().ToLower() -eq "true"

if (-not $Threshold) {
  $Threshold = Get-SimpleYamlValue -Path $ConfigYaml -Key "REPLAY_JOB_FAILURE_ALERT_THRESHOLD"
}
if (-not $NotificationChannel) {
  $NotificationChannel = Get-SimpleYamlValue -Path $ConfigYaml -Key "REPLAY_JOB_FAILURE_ALERT_NOTIFICATION_CHANNEL"
}
if (-not $NotificationChannel) {
  $NotificationChannel = Get-SimpleYamlValue -Path $ConfigYaml -Key "MANUAL_INTERVENTION_ALERT_NOTIFICATION_CHANNEL"
}
if (-not $AlignmentPeriod) {
  $AlignmentPeriod = Get-SimpleYamlValue -Path $ConfigYaml -Key "REPLAY_JOB_FAILURE_ALERT_ALIGNMENT_PERIOD"
}
if (-not $Duration) {
  $Duration = Get-SimpleYamlValue -Path $ConfigYaml -Key "REPLAY_JOB_FAILURE_ALERT_DURATION"
}

if (-not $AlignmentPeriod) { $AlignmentPeriod = "300s" }
if (-not $Duration) { $Duration = "0s" }
if (-not $Threshold) { $Threshold = "0.5" }

if (-not $enabled) {
  Write-Host "REPLAY_JOB_FAILURE_ALERT_ENABLED is not true. Skipping alert deployment."
  return
}

if (-not $NotificationChannel) {
  throw "Replay job failure notification channel is required."
}

$notificationChannelName = Normalize-NotificationChannel $NotificationChannel
$jobNameRegex = "{0}.*" -f [Regex]::Escape($JobNamePrefix.Trim())

Invoke-GCloud @(
  "services", "enable",
  "monitoring.googleapis.com",
  "--project", $ProjectId
)

$policy = @{
  displayName = $PolicyDisplayName
  documentation = @{
    content = @"
Triggers when a Dataflow job in this project reports the standard Dataflow failure metric.

Expected action:
1. Open the failed replay Dataflow job.
2. Inspect worker and job-message logs for the first root-cause exception.
3. Verify whether the failure is due to target-table merge, DLQ update, IAM, or template/config drift.
"@
    mimeType = "text/markdown"
  }
  combiner = "OR"
  enabled = $true
  notificationChannels = @($notificationChannelName)
  conditions = @(
    @{
      displayName = "Replay Dataflow job failed"
      conditionThreshold = @{
        filter = @"
metric.type="dataflow.googleapis.com/job/is_failed"
resource.type="dataflow_job"
resource.labels.job_name=monitoring.regex.full_match("$jobNameRegex")
"@ -replace "`r?`n", " "
        aggregations = @(
          @{
            alignmentPeriod = $AlignmentPeriod
            perSeriesAligner = "ALIGN_MAX"
            crossSeriesReducer = "REDUCE_MAX"
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
    metric = "dataflow_job_failed"
    job_prefix = $JobNamePrefix.Replace("-", "_")
    region = $Region.Replace("-", "_")
  }
}

$tmpPolicyFile = Join-Path $env:TEMP "dlq-replay-job-failure-alert.json"
$policy | ConvertTo-Json -Depth 20 | Set-Content -Path $tmpPolicyFile -Encoding UTF8

$existingPolicy = $null

try {
  $existingPolicy = & gcloud monitoring policies list `
    --project $ProjectId `
    --format="value(name)" `
    --filter="displayName=\"$PolicyDisplayName\"" `
    --quiet 2>$null
}
catch {
  $existingPolicy = $null
}

if ($existingPolicy) {
  Write-Host "Existing policy found. Replacing: $existingPolicy"
  Invoke-GCloud @(
    "monitoring", "policies", "delete", $existingPolicy,
    "--project", $ProjectId,
    "--quiet"
  )
}

Invoke-GCloud @(
  "monitoring", "policies", "create",
  "--project", $ProjectId,
  "--policy-from-file", $tmpPolicyFile
)

Write-Host "`nReplay job failure alert policy deployment complete."
Write-Host "Policy display name: $PolicyDisplayName"
Write-Host "Threshold: $Threshold"
Write-Host "Job name prefix: $JobNamePrefix"
Write-Host "Notification channel: $notificationChannelName"
