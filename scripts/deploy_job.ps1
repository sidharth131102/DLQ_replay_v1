param(
  [Parameter(Mandatory = $true)][string]$ProjectId,
  [Parameter(Mandatory = $true)][string]$Region,
  [Parameter(Mandatory = $true)][string]$JobName,
  [Parameter(Mandatory = $true)][string]$Image,
  [Parameter(Mandatory = $true)][string]$ServiceAccount,
  [Parameter(Mandatory = $true)][string]$EnvFile
)

$ErrorActionPreference = "Stop"

Write-Host "Deploying Cloud Run Job $JobName in $Region"

gcloud run jobs deploy $JobName `
  --project $ProjectId `
  --region $Region `
  --image $Image `
  --service-account $ServiceAccount `
  --env-vars-file $EnvFile `
  --max-retries 1 `
  --task-timeout 3600s `
  --tasks 1 `
  --execute-now=false

Write-Host "Cloud Run Job deployment complete."
