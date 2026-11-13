#!/usr/bin/env bash
set -euo pipefail

# Resolve paths
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Run commands from the project root
cd "$PROJECT_ROOT"

# Execute sequence
graphregistry airflow config "$PROJECT_ROOT/config/application/config_airflow.json"
graphregistry airflow expire --types Publication --scores -d 1
graphregistry airflow plan -l 5000 -shr
graphregistry airflow status
graphregistry kgraph compute
graphregistry kgraph patch
graphregistry airflow rollover
