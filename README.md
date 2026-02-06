# dbt Sync with Prefect Cloud (Process Worker)

This project runs dbt sync as a Prefect flow and is scheduled by Prefect Deployment cron.

## What changed

- `local_sync_scheduler.py` is now a single-run Prefect flow (`dbt_sync_flow`)
- Internal `while` loop scheduler was removed
- Schedule moved to Prefect deployment cron
- Config source priority is:
  1. Prefect Variables
  2. Environment variables
  3. Optional JSON fallback (`sync_scheduler_config.json`)
  4. Built-in defaults

## Required Prefect Variables

Create these in Prefect Cloud workspace:

- `DBT_SYNC_PROJECT_DIR`
- `DBT_SYNC_COMMAND`
- `DBT_SYNC_TIMEOUT_MINUTES`
- `DBT_SYNC_LOG_DIR`

Example values:

- `DBT_SYNC_PROJECT_DIR=d:/project/bq_dbt/bq_dbt`
- `DBT_SYNC_COMMAND=conda run -n data dbt build --select +marts`
- `DBT_SYNC_TIMEOUT_MINUTES=180`
- `DBT_SYNC_LOG_DIR=d:/project/bq_dbt/bq_dbt/logs`

## Connect to Prefect Cloud

```powershell
prefect cloud login
prefect profile use <cloud-profile>
prefect config set PREFECT_API_URL="https://api.prefect.cloud/api/accounts/<account_id>/workspaces/<workspace_id>"
```

## Deploy and run

1. Create a Process work pool in Prefect Cloud, example name: `local-process-pool`
2. Deploy flow:

```powershell
.\scripts\deploy_prefect.ps1 -WorkPool local-process-pool -Cron "0 * * * *"
```

3. Start worker on this machine:

```powershell
.\scripts\run_worker.ps1 -WorkPool local-process-pool
```

## Manual single-run

```powershell
python .\local_sync_scheduler.py --config .\sync_scheduler_config.json
```

## Verification checklist

- Success path: flow run is `Completed`, dbt output appears in Prefect logs and `logs/sync_scheduler_YYYYMMDD.log`
- Failure path: wrong command should trigger flow retries (2 times, 10 minutes apart)
- Timeout path: small timeout value should kill process and fail run
- Concurrency: deployment concurrency limit is `1` so runs do not overlap

## About docker-compose

`d:\prefect\docker-compose.yml` is a local Prefect Server setup.
For Prefect Cloud mode, it is not required.
