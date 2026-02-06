param(
  [string]$WorkPool = "local-process-pool",
  [string]$Cron = "0 * * * *",
  [string]$DeploymentName = "dbt-sync-hourly",
  [string]$ConfigPath = "sync_scheduler_config.json",
  [string]$PythonExe = "D:\conda\envs\data\python.exe"
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path $PythonExe)) {
  $PythonExe = "python"
}

Write-Host "[1/4] Checking prefect CLI..."
& $PythonExe -m prefect version | Out-Host

Write-Host "[2/4] Applying deployment via prefect_deploy.py..."
& $PythonExe .\prefect_deploy.py --work-pool $WorkPool --cron "$Cron" --deployment-name $DeploymentName --config-path $ConfigPath
if ($LASTEXITCODE -ne 0) {
  throw "Deployment failed with exit code $LASTEXITCODE"
}

Write-Host "[3/4] Ensure Cloud Variables (set in Prefect Cloud UI if missing):"
Write-Host "  DBT_SYNC_PROJECT_DIR"
Write-Host "  DBT_SYNC_COMMAND"
Write-Host "  DBT_SYNC_TIMEOUT_MINUTES"
Write-Host "  DBT_SYNC_LOG_DIR"

Write-Host "[4/4] Done. Next: start worker script scripts/run_worker.ps1"
