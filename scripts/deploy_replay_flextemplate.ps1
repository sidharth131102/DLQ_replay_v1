param(
  [Parameter(Mandatory = $true)][string]$ProjectId,
  [Parameter(Mandatory = $true)][string]$Region,
  [Parameter(Mandatory = $true)][string]$TemplateBucket,
  [Parameter(Mandatory = $true)][string]$ConfigYaml,
  [string]$ArtifactRepositoryName = "dlq-replay",
  [string]$ImageName = "dlq-replay-flex",
  [string]$ImageTag = "latest",
  [string]$TemplateObjectPath = "templates/dlq-replay-flex.json",
  [string]$ConfigObjectPath = "config/dlq-replay-config.yaml",
  [string]$MetadataFile = ".\\deploy\\dataflow_flex_template_metadata.json"
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
  $name = $BucketUriOrName.Trim()
  if ($name.StartsWith("gs://")) {
    $name = $name.Substring(5)
  }
  return $name.TrimEnd("/")
}

$bucketName = Normalize-BucketName $TemplateBucket
$templateSpecPath = "gs://$bucketName/$TemplateObjectPath"
$configYamlGcsPath = if ($ConfigYaml.StartsWith("gs://")) {
  $ConfigYaml
} else {
  "gs://$bucketName/$ConfigObjectPath"
}
$imageUri = "$Region-docker.pkg.dev/$ProjectId/$ArtifactRepositoryName/$ImageName`:$ImageTag"

Invoke-GCloud @(
  "services", "enable",
  "dataflow.googleapis.com",
  "cloudbuild.googleapis.com",
  "artifactregistry.googleapis.com",
  "storage.googleapis.com",
  "--project", $ProjectId
)

& gcloud artifacts repositories describe $ArtifactRepositoryName `
  --project $ProjectId `
  --location $Region 1>$null 2>$null
if ($LASTEXITCODE -ne 0) {
  Invoke-GCloud @(
    "artifacts", "repositories", "create", $ArtifactRepositoryName,
    "--project", $ProjectId,
    "--location", $Region,
    "--repository-format", "docker",
    "--description", "DLQ replay containers"
  )
}

if (-not $ConfigYaml.StartsWith("gs://")) {
  Invoke-GCloud @(
    "storage", "cp", $ConfigYaml, $configYamlGcsPath,
    "--project", $ProjectId
  )
}

Invoke-GCloud @(
  "dataflow", "flex-template", "build", $templateSpecPath,
  "--project", $ProjectId,
  "--image-gcr-path", $imageUri,
  "--sdk-language", "PYTHON",
  "--flex-template-base-image", "PYTHON3",
  "--metadata-file", $MetadataFile,
  "--py-path", ".",
  "--env", "FLEX_TEMPLATE_PYTHON_PY_FILE=app/dataflow_main.py",
  "--env", "FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE=requirements.txt",
  "--env", "FLEX_TEMPLATE_PYTHON_SETUP_FILE=setup.py"
)

Write-Host "`nReplay Flex Template build complete."
Write-Host "Template spec: $templateSpecPath"
Write-Host "Config YAML: $configYamlGcsPath"
Write-Host "Container image: $imageUri"
