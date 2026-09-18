#!/usr/bin/env bash
# Run the manual end-to-end pipeline test.
#
# This script triggers tests/end2end_tests/test_full_pipeline.py, which in turn
# executes tests/end2end_tests/helpers/run_cli_sequence.sh against the configured
# _1_DEV_* schemas and compares the output to
# tests/end2end_tests/data/ground_truth/.
#
# The test is destructive: it mutates the dev schemas.  It is guarded by the
# dev-mode check inside the pytest test itself.
#
# Usage:
#   ./scripts/run_e2e_test.sh
#
# Compare-only mode (skip the pipeline and compare existing test_output/ to ground_truth/):
#   ./scripts/run_e2e_test.sh --compare-only

set -euo pipefail

echo "🧪 GraphRegistry e2e test wrapper"
echo "   Started at: $(date +'%Y-%m-%d %H:%M:%S')"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Use the project virtualenv if it exists.
if [[ -f "${REPO_ROOT}/.venv.registry/bin/activate" ]]; then
    echo "🐍 Activating project virtualenv..."
    # shellcheck source=/dev/null
    source "${REPO_ROOT}/.venv.registry/bin/activate"
fi

export PYTHONPATH="${REPO_ROOT}${PYTHONPATH:+:${PYTHONPATH}}"
export GRAPHREGISTRY_RUN_MANUAL_E2E=1

cd "${REPO_ROOT}"

# Parse optional --compare-only flag and forward the remaining args to pytest.
COMPARE_ONLY=0
PYTEST_ARGS=()
for arg in "$@"; do
    if [[ "$arg" == "--compare-only" ]]; then
        COMPARE_ONLY=1
    else
        PYTEST_ARGS+=("$arg")
    fi
done

if [[ "$COMPARE_ONLY" == "1" ]]; then
    export GRAPHREGISTRY_E2E_COMPARE_ONLY=1
    echo "🔄 Mode: compare-only (skip pipeline, compare existing test_output/ to ground_truth/)"
else
    echo "🚀 Mode: full pipeline (mutates _1_DEV_* schemas)"
fi

echo "▶️  Starting pytest..."
exec pytest tests/end2end_tests/test_full_pipeline.py \
    --override-ini="addopts=" \
    -v \
    -s \
    "${PYTEST_ARGS[@]}"
