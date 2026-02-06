param(
  [string]$WorkPool = "local-process-pool",
  [string]$PythonExe = "D:\conda\envs\data\python.exe"
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path $PythonExe)) {
  $PythonExe = "python"
}

Write-Host "Starting Prefect process worker on pool: $WorkPool"
& $PythonExe -m prefect worker start --pool $WorkPool --type process
