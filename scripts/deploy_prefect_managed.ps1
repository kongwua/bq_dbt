param(
  [string]$PythonExe = "D:\conda\envs\data\python.exe",
  [string]$DeploymentName = "dbt-sync-hourly"
)

$ErrorActionPreference = "Stop"
if (-not (Test-Path $PythonExe)) {
  $PythonExe = "python"
}

Write-Host "[1/3] Prefect version"
& $PythonExe -m prefect version | Out-Host

Write-Host "[2/3] Deploy using prefect.yaml"
& $PythonExe -m prefect deploy --prefect-file .\prefect.yaml -n $DeploymentName
if ($LASTEXITCODE -ne 0) {
  throw "Managed deployment failed with exit code $LASTEXITCODE"
}

Write-Host "[3/3] Done. Trigger once with:"
Write-Host "  $PythonExe -m prefect deployment run \"dbt-sync-flow/$DeploymentName\""
