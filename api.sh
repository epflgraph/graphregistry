#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

source .venv.registry/bin/activate

export PYTHONPATH="$PWD:${PYTHONPATH:-}"
export GRAPHREGISTRY_ROOT="$PWD"

# The local API script is only allowed to start in development mode so it does
# not accidentally write to production tables. Read mysql.mode from the global
# config (respecting GRAPH_REGISTRY_CONFIG_GLOBAL if set) and abort if it is not
# 'dev'.
config_global_path="${GRAPH_REGISTRY_CONFIG_GLOBAL:-config/config_global.yaml}"
mysql_mode=$(python -c "import yaml; print(yaml.safe_load(open('${config_global_path}'))['mysql']['mode'])")
if [[ "$mysql_mode" != "dev" ]]; then
    echo "ERROR: api.sh refused to start: ${config_global_path} has mysql.mode='${mysql_mode}', expected 'dev'." >&2
    exit 1
fi

exec uvicorn graphregistry.entrypoints.api.main:create_app \
  --host 127.0.0.1 \
  --port 9999 \
  --workers 1 \
  --proxy-headers \
  --forwarded-allow-ips 127.0.0.1 \
  --factory
