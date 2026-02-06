# -*- coding: utf-8 -*-
"""
本地定时同步脚本

功能：
1. 按配置文件的时间间隔定时执行同步命令
2. 支持启动时立即执行一次
3. 同步日志同时输出到控制台与文件
4. 全部关键提示为中文
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class SchedulerConfig:
    enabled: bool = True
    interval_minutes: int = 1440
    run_on_start: bool = True
    dbt_project_dir: str = "d:/project/bq_dbt/bq_dbt"
    sync_command: str = "conda run -n data dbt build --select +marts"
    log_dir: str = "d:/project/bq_dbt/bq_dbt/logs"
    command_timeout_minutes: int = 180


def load_config(config_path: str) -> SchedulerConfig:
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"未找到配置文件: {config_path}")

    with open(config_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    return SchedulerConfig(
        enabled=bool(data.get("enabled", True)),
        interval_minutes=max(1, int(data.get("interval_minutes", 1440))),
        run_on_start=bool(data.get("run_on_start", True)),
        dbt_project_dir=str(data.get("dbt_project_dir", "d:/project/bq_dbt/bq_dbt")),
        sync_command=str(data.get("sync_command", "conda run -n data dbt build --select +marts")),
        log_dir=str(data.get("log_dir", "d:/project/bq_dbt/bq_dbt/logs")),
        command_timeout_minutes=max(1, int(data.get("command_timeout_minutes", 180))),
    )


def setup_logger(log_dir: str) -> logging.Logger:
    os.makedirs(log_dir, exist_ok=True)
    logger = logging.getLogger("local_sync_scheduler")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    log_file = os.path.join(log_dir, f"sync_scheduler_{datetime.now().strftime('%Y%m%d')}.log")
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger


def run_sync_once(config: SchedulerConfig, logger: logging.Logger) -> bool:
    start = time.time()
    logger.info("开始执行数据同步任务")
    logger.info("执行命令: %s", config.sync_command)
    logger.info("工作目录: %s", config.dbt_project_dir)

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
    except Exception as e:
        logger.error("启动同步命令失败: %s", e)
        return False

    timeout_seconds = config.command_timeout_minutes * 60
    deadline = time.time() + timeout_seconds

    try:
        while True:
            line = process.stdout.readline() if process.stdout else ""
            if line:
                logger.info("同步输出: %s", line.rstrip())

            if process.poll() is not None:
                break

            if time.time() > deadline:
                process.kill()
                logger.error("同步任务超时，已终止（超时 %s 分钟）", config.command_timeout_minutes)
                return False
    finally:
        if process.stdout:
            process.stdout.close()

    duration = time.time() - start
    if process.returncode == 0:
        logger.info("同步任务执行成功，耗时 %.2f 秒", duration)
        return True

    logger.error("同步任务执行失败，退出码=%s，耗时 %.2f 秒", process.returncode, duration)
    return False


class LocalSyncScheduler:
    def __init__(self, config: SchedulerConfig, logger: logging.Logger) -> None:
        self.config = config
        self.logger = logger
        self.stop_event = threading.Event()
        self.running_lock = threading.Lock()

    def stop(self) -> None:
        self.stop_event.set()
        self.logger.info("收到停止信号，调度器即将退出")

    def run_loop(self) -> None:
        if not self.config.enabled:
            self.logger.warning("配置 enabled=false，调度器不执行任务")
            return

        interval_seconds = self.config.interval_minutes * 60
        self.logger.info("本地定时同步已启动，间隔=%s 分钟", self.config.interval_minutes)

        if self.config.run_on_start and not self.stop_event.is_set():
            with self.running_lock:
                run_sync_once(self.config, self.logger)

        while not self.stop_event.is_set():
            for _ in range(interval_seconds):
                if self.stop_event.is_set():
                    break
                time.sleep(1)

            if self.stop_event.is_set():
                break

            if self.running_lock.locked():
                self.logger.warning("上一次同步尚未结束，本次触发跳过")
                continue

            with self.running_lock:
                run_sync_once(self.config, self.logger)

        self.logger.info("本地定时同步已停止")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="本地 dbt 定时同步工具")
    parser.add_argument(
        "--config",
        default="sync_scheduler_config.json",
        help="配置文件路径（默认: sync_scheduler_config.json）",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="只执行一次同步后退出（忽略定时循环）",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    try:
        config = load_config(args.config)
    except Exception as e:
        print(f"[错误] 配置加载失败: {e}")
        return 1

    logger = setup_logger(config.log_dir)
    logger.info("配置加载成功: %s", args.config)

    if args.once:
        ok = run_sync_once(config, logger)
        return 0 if ok else 1

    scheduler = LocalSyncScheduler(config, logger)

    def _handle_signal(signum: int, frame: Optional[object]) -> None:
        _ = frame
        logger.info("收到系统信号: %s", signum)
        scheduler.stop()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    try:
        scheduler.run_loop()
    except Exception as e:
        logger.exception("调度器发生未处理异常: %s", e)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
