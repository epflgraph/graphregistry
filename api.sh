#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

source .venv.registry/bin/activate

export PYTHONPATH="$PWD:${PYTHONPATH:-}"
export GRAPHREGISTRY_ROOT="$PWD"

exec uvicorn graphregistry.entrypoints.api.main:create_app \
  --host 127.0.0.1 \
  --port 9999 \
  --workers 1 \
  --proxy-headers \
  --forwarded-allow-ips 127.0.0.1 \
  --factory
