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

$prevErrorAction = $ErrorActionPreference
$ErrorActionPreference = "SilentlyContinue"
& gcloud artifacts repositories describe $ArtifactRepositoryName `
  --project $ProjectId `
  --location $Region 1>$null 2>$null
$exitCode = $LASTEXITCODE
$ErrorActionPreference = $prevErrorAction
if ($exitCode -ne 0) {
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
  "auth", "configure-docker",
  "$Region-docker.pkg.dev",
  "--quiet"
)


Write-Host "`nBuilding Dataflow container image..."
docker build -f Dockerfile.dataflow -t $imageUri .

if ($LASTEXITCODE -ne 0) {
  throw "Docker build failed."
}

Write-Host "`nPushing container image..."
docker push $imageUri

if ($LASTEXITCODE -ne 0) {
  throw "Docker push failed."
}


Invoke-GCloud @(
  "dataflow", "flex-template", "build", $templateSpecPath,
  "--project", $ProjectId,
  "--image", $imageUri,
  "--sdk-language", "PYTHON",
  "--metadata-file", $MetadataFile
)

Write-Host "`nReplay Flex Template build complete."
Write-Host "Template spec: $templateSpecPath"
Write-Host "Config YAML: $configYamlGcsPath"
Write-Host "Container image: $imageUri"
