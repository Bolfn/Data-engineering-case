from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

from prefect import flow, get_run_logger, task


PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = PROJECT_ROOT / "src"


def run_script(script_name: str) -> None:
    logger = get_run_logger()
    script_path = SRC_DIR / script_name

    logger.info("Running %s", script_path)
    completed = subprocess.run(
        [sys.executable, str(script_path)],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
    )

    if completed.stdout:
        logger.info("Stdout from %s:\n%s", script_name, completed.stdout.strip())

    if completed.stderr:
        logger.warning("Stderr from %s:\n%s", script_name, completed.stderr.strip())

    if completed.returncode != 0:
        raise RuntimeError(
            f"{script_name} failed with exit code {completed.returncode}"
        )


def run_command(command: list[str], command_name: str) -> None:
    logger = get_run_logger()
    logger.info("Running command: %s", " ".join(command))
    completed = subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
    )

    if completed.stdout:
        logger.info("Stdout from %s:\n%s", command_name, completed.stdout.strip())

    if completed.stderr:
        logger.warning("Stderr from %s:\n%s", command_name, completed.stderr.strip())

    if completed.returncode != 0:
        raise RuntimeError(
            f"{command_name} failed with exit code {completed.returncode}"
        )


def validate_cron_expression(cron_expression: str) -> str:
    cron_parts = cron_expression.split()
    if len(cron_parts) != 5:
        raise ValueError(
            "Invalid PREFECT_SCHEDULE_CRON. Expected 5 cron fields, "
            f"got {len(cron_parts)}: {cron_expression}"
        )
    return cron_expression


@task(name="build_source_db", retries=1, retry_delay_seconds=5)
def build_source_db_task() -> None:
    run_command(["make", "build"], "make build")


@task(name="extract", retries=1, retry_delay_seconds=5)
def extract_task() -> None:
    run_script("extract.py")


@task(name="weather", retries=2, retry_delay_seconds=10)
def weather_task() -> None:
    run_script("weather.py")


@task(name="region_mapping", retries=1, retry_delay_seconds=5)
def region_mapping_task() -> None:
    run_script("region_mapping.py")


@task(name="data_quality", retries=1, retry_delay_seconds=5)
def data_quality_task() -> None:
    run_script("data_quality.py")


@task(name="load", retries=1, retry_delay_seconds=5)
def load_task() -> None:
    run_script("load.py")


@flow(name="northwind-etl-pipeline", log_prints=True)
def northwind_etl_pipeline() -> None:
    logger = get_run_logger()
    logger.info("Starting Northwind ETL pipeline")

    build_source_db_task()
    extract_task()
    weather_task()
    region_mapping_task()
    data_quality_task()
    load_task()

    logger.info("Northwind ETL pipeline completed successfully")


if __name__ == "__main__":
    schedule_cron = os.getenv("PREFECT_SCHEDULE_CRON")
    if schedule_cron:
        validated_cron = validate_cron_expression(schedule_cron)
        northwind_etl_pipeline.serve(
            name="northwind-etl-schedule",
            cron=validated_cron,
        )
    else:
        northwind_etl_pipeline()
