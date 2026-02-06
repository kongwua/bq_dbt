from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

try:
    from prefect import flow, get_run_logger, task
    PREFECT_AVAILABLE = True
except Exception:
    PREFECT_AVAILABLE = False

    def flow(*_args, **_kwargs):  # type: ignore
        def _decorator(func):
            return func

        return _decorator

    def task(*_args, **_kwargs):  # type: ignore
        def _decorator(func):
            return func

        return _decorator

    def get_run_logger() -> logging.Logger:  # type: ignore
        return logging.getLogger("dbt_sync_fallback")

try:
    from prefect.variables import Variable
except Exception:  # pragma: no cover
    Variable = None


DEFAULT_PROJECT_DIR = "d:/project/bq_dbt/bq_dbt"
DEFAULT_SYNC_COMMAND = "conda run -n data dbt build --select +marts"
DEFAULT_LOG_DIR = "d:/project/bq_dbt/bq_dbt/logs"
DEFAULT_TIMEOUT_MINUTES = 180


@dataclass
class SyncConfig:
    dbt_project_dir: str = DEFAULT_PROJECT_DIR
    sync_command: str = DEFAULT_SYNC_COMMAND
    command_timeout_minutes: int = DEFAULT_TIMEOUT_MINUTES
    log_dir: str = DEFAULT_LOG_DIR


def _safe_int(value: Any, default: int) -> int:
    try:
        return max(1, int(value))
    except (TypeError, ValueError):
        return default


def _read_json_config(config_path: Optional[str]) -> dict[str, Any]:
    if not config_path:
        return {}
    path = Path(config_path)
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as file:
        data = json.load(file)
    return data if isinstance(data, dict) else {}


def _get_prefect_variable(name: str) -> Optional[str]:
    if Variable is None:
        return None
    try:
        value = Variable.get(name, default=None)
    except Exception:
        return None
    if value is None:
        return None
    return str(value)


def _resolve_value(
    key: str,
    env_name: str,
    variable_name: str,
    json_data: dict[str, Any],
    default: Any,
) -> Any:
    variable_value = _get_prefect_variable(variable_name)
    if variable_value not in (None, ""):
        return variable_value
    env_value = os.getenv(env_name)
    if env_value not in (None, ""):
        return env_value
    if key in json_data and json_data[key] not in (None, ""):
        return json_data[key]
    return default


def load_config(config_path: Optional[str]) -> SyncConfig:
    json_data = _read_json_config(config_path)
    project_dir = _resolve_value(
        key="dbt_project_dir",
        env_name="DBT_SYNC_PROJECT_DIR",
        variable_name="DBT_SYNC_PROJECT_DIR",
        json_data=json_data,
        default=DEFAULT_PROJECT_DIR,
    )
    command = _resolve_value(
        key="sync_command",
        env_name="DBT_SYNC_COMMAND",
        variable_name="DBT_SYNC_COMMAND",
        json_data=json_data,
        default=DEFAULT_SYNC_COMMAND,
    )
    timeout_value = _resolve_value(
        key="command_timeout_minutes",
        env_name="DBT_SYNC_TIMEOUT_MINUTES",
        variable_name="DBT_SYNC_TIMEOUT_MINUTES",
        json_data=json_data,
        default=DEFAULT_TIMEOUT_MINUTES,
    )
    log_dir = _resolve_value(
        key="log_dir",
        env_name="DBT_SYNC_LOG_DIR",
        variable_name="DBT_SYNC_LOG_DIR",
        json_data=json_data,
        default=DEFAULT_LOG_DIR,
    )
    return SyncConfig(
        dbt_project_dir=str(project_dir),
        sync_command=str(command),
        command_timeout_minutes=_safe_int(timeout_value, DEFAULT_TIMEOUT_MINUTES),
        log_dir=str(log_dir),
    )


def _build_file_logger(log_dir: str) -> logging.Logger:
    os.makedirs(log_dir, exist_ok=True)
    logger_name = f"dbt_sync_file_logger_{os.getpid()}"
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    logger.propagate = False

    log_file = os.path.join(log_dir, f"sync_scheduler_{datetime.now().strftime('%Y%m%d')}.log")
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return logger


def _log_line(prefect_logger: logging.Logger, file_logger: logging.Logger, message: str) -> None:
    prefect_logger.info(message)
    file_logger.info(message)


@task(name="run-dbt-sync", retries=0)
def run_sync_once(config: SyncConfig) -> None:
    prefect_logger = get_run_logger()
    file_logger = _build_file_logger(config.log_dir)
    start = time.time()

    _log_line(prefect_logger, file_logger, "Starting dbt sync task.")
    _log_line(prefect_logger, file_logger, f"Command: {config.sync_command}")
    _log_line(prefect_logger, file_logger, f"Project dir: {config.dbt_project_dir}")
    _log_line(
        prefect_logger,
        file_logger,
        f"Timeout (minutes): {config.command_timeout_minutes}",
    )

    try:
        process = subprocess.Popen(
            config.sync_command,
            cwd=config.dbt_project_dir,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
    except Exception as exc:
        raise RuntimeError(f"Failed to start sync command: {exc}") from exc

    timeout_seconds = config.command_timeout_minutes * 60
    deadline = time.time() + timeout_seconds

    try:
        while True:
            line = process.stdout.readline() if process.stdout else ""
            if line:
                _log_line(prefect_logger, file_logger, f"dbt output: {line.rstrip()}")

            if process.poll() is not None:
                break

            if time.time() > deadline:
                process.kill()
                raise TimeoutError(
                    f"Sync command timed out after {config.command_timeout_minutes} minutes."
                )
    finally:
        if process.stdout:
            process.stdout.close()

    duration = time.time() - start
    if process.returncode != 0:
        raise RuntimeError(
            f"Sync command failed with exit code={process.returncode}, duration={duration:.2f}s."
        )

    _log_line(prefect_logger, file_logger, f"Sync command completed in {duration:.2f}s.")


@flow(
    name="dbt-sync-flow",
    retries=2,
    retry_delay_seconds=600,
    log_prints=True,
)
def dbt_sync_flow(config_path: Optional[str] = None) -> None:
    config = load_config(config_path=config_path)
    run_sync_once(config)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run dbt sync as a Prefect flow once.")
    parser.add_argument(
        "--config",
        default="sync_scheduler_config.json",
        help="Optional JSON fallback config path. Prefect Variables are used first.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not PREFECT_AVAILABLE:
        print("Prefect is not installed. Install it first, e.g. `pip install prefect`.")
        return 1
    dbt_sync_flow(config_path=args.config)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
