#!/usr/bin/env sh
set -eu

if [ -n "${PREFECT_SCHEDULE_CRON:-}" ]; then
  exec python src/pipeline.py
fi

exec python src/pipeline.py
