from __future__ import annotations

import argparse
import shlex
import subprocess
import sys
from typing import Sequence


def run_command(cmd: Sequence[str]) -> tuple[int, str]:
    process = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace")
    output = (process.stdout or "") + (process.stderr or "")
    return process.returncode, output


def deploy(
    work_pool: str,
    cron: str,
    deployment_name: str,
    config_path: str,
) -> int:
    entrypoint = "local_sync_scheduler.py:dbt_sync_flow"
    prefect_cmd = [sys.executable, "-m", "prefect", "deploy"]

    commands = [
        prefect_cmd + [
            entrypoint,
            "--name",
            deployment_name,
            "--pool",
            work_pool,
            "--cron",
            cron,
            "--param",
            f"config_path={config_path}",
            "--concurrency-limit",
            "1",
        ],
        prefect_cmd + [
            entrypoint,
            "--name",
            deployment_name,
            "--work-pool",
            work_pool,
            "--cron",
            cron,
            "--param",
            f"config_path={config_path}",
            "--concurrency-limit",
            "1",
        ],
    ]

    for cmd in commands:
        print(f"Trying: {shlex.join(cmd)}")
        code, output = run_command(cmd)
        print(output)
        if code == 0:
            return 0

    print("Deployment failed. Check prefect CLI version and argument names.", file=sys.stderr)
    return 1


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Deploy dbt sync flow to Prefect Cloud")
    parser.add_argument("--work-pool", default="local-process-pool")
    parser.add_argument("--cron", default="0 * * * *")
    parser.add_argument("--deployment-name", default="dbt-sync-hourly")
    parser.add_argument("--config-path", default="sync_scheduler_config.json")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    return deploy(
        work_pool=args.work_pool,
        cron=args.cron,
        deployment_name=args.deployment_name,
        config_path=args.config_path,
    )


if __name__ == "__main__":
    raise SystemExit(main())
