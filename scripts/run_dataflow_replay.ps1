param(
  [Parameter(Mandatory = $true)][string]$ProjectId,
  [Parameter(Mandatory = $true)][string]$Region,
  [Parameter(Mandatory = $true)][string]$JobName,
  [Parameter(Mandatory = $true)][string]$TempLocation,    # gs://bucket/tmp
  [Parameter(Mandatory = $true)][string]$StagingLocation, # gs://bucket/staging
  [Parameter(Mandatory = $true)][string]$ConfigYaml,      # .env.job.yaml
  [string]$Runner = "DataflowRunner",
  [string]$PythonExe = "python",
  [string]$TableName = "",
  [int]$Limit = 0,
  [switch]$FixOnly
)

$ErrorActionPreference = "Stop"

Write-Host "Launching Dataflow replay job '$JobName' in '$Region'..."

$cmd = @(
  $PythonExe,
  "-m", "app.dataflow_main",
  "--config-yaml", $ConfigYaml,
  "--setup_file", ".\\setup.py",
  "--requirements_file", ".\\requirements.txt",
  "--runner", $Runner,
  "--project", $ProjectId,
  "--region", $Region,
  "--job_name", $JobName,
  "--temp_location", $TempLocation,
  "--staging_location", $StagingLocation,
  "--experiments=use_runner_v2"
)

if ($TableName) {
  $cmd += @("--table-name", $TableName)
}
if ($Limit -gt 0) {
  $cmd += @("--limit", "$Limit")
}
if ($FixOnly) {
  $cmd += @("--fix-only")
}

Write-Host "`n> $($cmd -join ' ')"
try {
  & $cmd[0] $cmd[1..($cmd.Length - 1)]
} catch {
  throw "Failed launching with Python executable '$PythonExe'. Pass -PythonExe with an explicit path if needed."
}

if ($LASTEXITCODE -ne 0) {
  throw "Dataflow replay launch failed."
}

Write-Host "Dataflow replay launch submitted."
