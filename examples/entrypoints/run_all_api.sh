#!/usr/bin/env bash
# Run all examples under examples/entrypoints in a logical order:
#   1. create nodes
#   2. read/verify nodes
#   3. create edges
#   4. read/verify edges
#   5. delete edges
#   6. delete nodes
#
# This script mutates the configured registry database. Make sure you are
# pointing at the intended environment (dev/test) before running it.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# Use the project virtualenv if it exists.
if [[ -f "${REPO_ROOT}/.venv.registry/bin/activate" ]]; then
    # shellcheck source=/dev/null
    source "${REPO_ROOT}/.venv.registry/bin/activate"
fi

export PYTHONPATH="${REPO_ROOT}${PYTHONPATH:+:${PYTHONPATH}}"
export GRAPHREGISTRY_ROOT="${REPO_ROOT}"

run_example() {
    local name="$1"
    local api_script="${SCRIPT_DIR}/${name}/api.sh"

    if [[ ! -f "${api_script}" ]]; then
        echo "⚠️  Skipping ${name}: no api.sh found"
        return
    fi

    echo ""
    echo "▶️  Running ${name}..."
    bash "${api_script}"
    echo "✅ ${name} completed"
}

echo "Running Graph Registry entrypoint examples"
echo "GRAPHREGISTRY_ROOT=${GRAPHREGISTRY_ROOT}"

# -------------------------
# 1. Create nodes
# -------------------------
run_example "node_save"
run_example "node_save_many"

# -------------------------
# 2. Read/verify nodes
# -------------------------
run_example "node_exists"
run_example "node_exists_many"
run_example "node_get"
run_example "node_get_many"
run_example "node_list"

# -------------------------
# 3. Create edges
# -------------------------
run_example "edge_save"
run_example "edge_save_many"

# -------------------------
# 4. Read/verify edges
# -------------------------
run_example "edge_exists"
run_example "edge_exists_many"
run_example "edge_get"
run_example "edge_get_many"
run_example "edge_list"

# -------------------------
# 5. Delete edges
# -------------------------
run_example "edge_delete"
run_example "edge_delete_many"

# -------------------------
# 6. Delete nodes
# -------------------------
run_example "node_delete"
run_example "node_delete_many"

echo ""
echo "🏁 All examples completed successfully."
