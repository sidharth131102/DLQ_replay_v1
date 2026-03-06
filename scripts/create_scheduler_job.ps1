param(
  [Parameter(Mandatory = $true)][string]$ProjectId,
  [Parameter(Mandatory = $true)][string]$Region,
  [Parameter(Mandatory = $true)][string]$SchedulerJobName,
  [Parameter(Mandatory = $true)][string]$RunJobName,
  [Parameter(Mandatory = $true)][string]$SchedulerServiceAccount,
  [string]$Schedule = "0 2 * * *",
  [string]$TimeZone = "Etc/UTC"
)

$ErrorActionPreference = "Stop"

$uri = "https://run.googleapis.com/v2/projects/$ProjectId/locations/$Region/jobs/$RunJobName:run"

Write-Host "Creating Cloud Scheduler job $SchedulerJobName"

# Requires scheduler SA permission to run the Cloud Run Job via Run API.
gcloud scheduler jobs create http $SchedulerJobName `
  --project $ProjectId `
  --location $Region `
  --schedule $Schedule `
  --time-zone $TimeZone `
  --uri $uri `
  --http-method POST `
  --oauth-service-account-email $SchedulerServiceAccount `
  --oauth-token-scope https://www.googleapis.com/auth/cloud-platform `
  --message-body "{}"

Write-Host "Cloud Scheduler job created."
