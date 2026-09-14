#!/usr/bin/env bash
set -euo pipefail

cd /app

export PYTHONPATH="/app:${PYTHONPATH:-}"
export GRAPHREGISTRY_ROOT="${GRAPHREGISTRY_ROOT:-/app}"

REQUIRED_FILES=(
  "/app/config/application/config_airflow.json"
  "/app/config/application/config_api.json"
  "/app/config/application/config_index.json"
  "/app/config/application/config_scores.json"
  "/app/config/environment/config_genai.yml"
  "/app/config/environment/config_graphai.yml"
  "/app/config/environment/config_graphdb.yml"
  "/app/config/environment/config_graphes.yml"
  "/app/config/environment/config_registry.yml"
)

for config_file in "${REQUIRED_FILES[@]}"; do
  if [[ ! -f "${config_file}" ]]; then
    echo "ERROR: required config file not found: ${config_file}"
    if [[ "${config_file}" == "/app/config/application/config_api.json" ]]; then
      echo "       Copy it from the repository: cp config/application/config_api.json /app/config/application/config_api.json"
    fi
    exit 1
  fi
  echo "[graphregistry] Config check OK: ${config_file}"
done
echo "[graphregistry] All required config files are present."

if [[ $# -gt 0 ]]; then
  exec "$@"
fi

ROLE="${GRAPHREGISTRY_ROLE:-api}"
echo "[graphregistry] ROLE=${ROLE}"

case "${ROLE}" in
  api)
    echo "[graphregistry] Starting API..."

    exec uvicorn \
      "${APP_MODULE:-graphregistry.entrypoints.api.main:create_app}" \
      --host "${API_HOST:-0.0.0.0}" \
      --port "${API_PORT:-28800}" \
      --workers "${API_WORKERS:-1}" \
      ${API_PROXY_HEADERS:+--proxy-headers} \
      --forwarded-allow-ips "${API_FORWARDED_ALLOW_IPS:-127.0.0.1}" \
      --factory
    ;;

  shell)
    exec /bin/bash
    ;;

  *)
    echo "ERROR: Unknown GRAPHREGISTRY_ROLE='${ROLE}'"
    echo "Valid values: api, shell"
    exit 1
    ;;
esac
