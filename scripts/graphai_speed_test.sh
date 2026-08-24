#!/usr/bin/env bash
#
# GraphAI speed test — measure TTFB for translation submit + status endpoints.
#
# Usage:
#   export GRAPHAI_TOKEN="<bearer token>"
#   ./scripts/graphai_speed_test.sh [iterations]
#
# If GRAPHAI_TOKEN is not set, the script reads config/config_graphai.json,
# calls POST /token once, and caches the token in /tmp/graphai_token.cache for 25 min.
#
# Requirements: curl, jq
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
GRAPHAI_CONFIG="${GRAPHAI_CONFIG:-${REPO_ROOT}/config/config_graphai.json}"
GRAPHAI_HOST="${GRAPHAI_HOST:-}"
GRAPHAI_TOKEN="${GRAPHAI_TOKEN:-}"
TOKEN_CACHE="/tmp/graphai_token.cache"

ITERATIONS="${1:-3}"

CURL_FMT='\n=== timing ===\n DNS lookup:        %{time_namelookup}s\n TCP connect:       %{time_connect}s\n SSL handshake:     %{time_appconnect}s\n Pre-transfer:      %{time_pretransfer}s\n TTFB:              %{time_starttransfer}s\n Total:             %{time_total}s\n=== sizes ===\n Request size:      %{size_request}b\n Response size:     %{size_download}b\n HTTP code:         %{http_code}\n'

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

command -v curl >/dev/null 2>&1 || { echo "ERROR: curl is required"; exit 1; }
command -v jq >/dev/null 2>&1 || { echo "ERROR: jq is required"; exit 1; }

if [[ ! -f "${GRAPHAI_CONFIG}" && -z "${GRAPHAI_HOST}" ]]; then
    echo "ERROR: GraphAI config not found at ${GRAPHAI_CONFIG}"
    echo "Set GRAPHAI_CONFIG, GRAPHAI_HOST+GRAPHAI_TOKEN, or run from repo root."
    exit 1
fi

if [[ -z "${GRAPHAI_HOST}" ]]; then
    raw_host="$(jq -r '.host' "${GRAPHAI_CONFIG}")"
    raw_port="$(jq -r '.port' "${GRAPHAI_CONFIG}")"
    # Config may already contain a scheme (e.g. https://host) or just a hostname.
    if [[ "${raw_host}" == http://* || "${raw_host}" == https://* ]]; then
        BASE_URL="${raw_host}"
        # Only add the port if it is non-standard and not already present.
        if [[ "${raw_port}" != "443" && "${raw_port}" != "80" && "${BASE_URL}" != *:${raw_port} ]]; then
            BASE_URL="${BASE_URL}:${raw_port}"
        fi
    else
        BASE_URL="https://${raw_host}:${raw_port}"
    fi
else
    BASE_URL="https://${GRAPHAI_HOST}"
fi

cache_token() {
    local token="$1"
    printf '%s\n%d\n' "${token}" "$(date +%s)" > "${TOKEN_CACHE}"
    chmod 600 "${TOKEN_CACHE}"
}

load_cached_token() {
    if [[ ! -f "${TOKEN_CACHE}" ]]; then
        return 1
    fi
    local cached_at
    cached_at="$(sed -n '2p' "${TOKEN_CACHE}" 2>/dev/null || echo 0)"
    local now
    now="$(date +%s)"
    # Tokens are valid for 30 min; reuse for 25 min.
    if (( now - cached_at > 1500 )); then
        return 1
    fi
    sed -n '1p' "${TOKEN_CACHE}"
}

fetch_token() {
    echo "[auth] No GRAPHAI_TOKEN set; fetching token from ${BASE_URL}/token ..."
    local user password
    user="$(jq -r '.user' "${GRAPHAI_CONFIG}")"
    password="$(jq -r '.password' "${GRAPHAI_CONFIG}")"

    local token_response
    token_response="$(curl -s -X POST "${BASE_URL}/token" \
        -H 'Content-Type: application/x-www-form-urlencoded' \
        --data-urlencode "username=${user}" \
        --data-urlencode "password=${password}" \
        -w "\n%{http_code}")"

    local http_code
    http_code="$(echo "${token_response}" | tail -n1)"
    if [[ "${http_code}" != "200" ]]; then
        echo "ERROR: /token returned HTTP ${http_code}"
        echo "${token_response}" | head -n -1
        exit 1
    fi

    local body
    body="$(echo "${token_response}" | sed '$d')"
    GRAPHAI_TOKEN="$(echo "${body}" | jq -r '.access_token')"
    cache_token "${GRAPHAI_TOKEN}"
}

ensure_token() {
    if [[ -n "${GRAPHAI_TOKEN}" ]]; then
        return 0
    fi
    if cached_token="$(load_cached_token)"; then
        GRAPHAI_TOKEN="${cached_token}"
        echo "[auth] Using cached token (expires in < 25 min)."
        return 0
    fi
    fetch_token
}

# ---------------------------------------------------------------------------
# Main benchmark
# ---------------------------------------------------------------------------

ensure_token

echo "================================================"
echo "GraphAI speed test"
echo "Base URL: ${BASE_URL}"
echo "Iterations: ${ITERATIONS}"
echo "================================================"
echo

# Helper: measure one endpoint and return TTFB (seconds) on stdout.
# All diagnostic output goes to stderr so callers can capture the number cleanly.
# Usage: measure_endpoint <label> <url> [method] [output_file] [extra curl args...]
measure_endpoint() {
    local label="$1"
    local url="$2"
    local method="${3:-GET}"
    local output_file="${4:-/dev/null}"
    shift 4 || true

    local curl_args=(-w "${CURL_FMT}" -o "${output_file}" -s -X "${method}")
    if [[ $# -gt 0 ]]; then
        curl_args+=("$@")
    fi
    curl_args+=("${url}")

    echo "[${label}] ${method} ${url}" >&2
    local response
    response="$(curl "${curl_args[@]}")"
    echo "${response}" >&2
    local ttfb
    # Take the last TTFB value in case curl reports multiple transactions (redirects/retries).
    ttfb="$(echo "${response}" | awk '/TTFB:/{print $2}' | tr -d 's' | tail -n 1)"
    echo "[${label}] TTFB: ${ttfb}s" >&2
    echo >&2
    echo "${ttfb}"
}

# Control endpoint: /openapi.json is auto-generated by FastAPI and exercises
# no custom route logic. It helps distinguish global middleware/instrumentation/
# worker saturation from translation-specific overhead.
echo "--- Control endpoint (1 iteration) ---"
OPENAPI_TTFB="$(measure_endpoint "openapi" "${BASE_URL}/openapi.json" GET /tmp/graphai_openapi.json)"

echo "================================================"
echo "Control endpoint summary"
echo "Average /openapi.json TTFB: ${OPENAPI_TTFB}s"
echo "================================================"
echo

REQUEST_BODY='{
  "text": "Machine learning is a branch of artificial intelligence. Natural language processing enables machines to understand text. And more so.",
  "source": "fr",
  "target": "en",
  "force": true,
  "no_cache": false,
  "skip_segmentation": false,
  "clean": false
}'

TOTAL_SUBMIT=0
TOTAL_STATUS=0

echo "--- Translation submit/status (${ITERATIONS} iterations) ---"
for i in $(seq 1 "${ITERATIONS}"); do
    echo "--- Iteration ${i}/${ITERATIONS} ---"

    SUBMIT_TTFB="$(measure_endpoint \
        "submit" \
        "${BASE_URL}/translation/translate" \
        "POST" \
        /tmp/graphai_submit.json \
        -H 'accept: application/json' \
        -H "Authorization: Bearer ${GRAPHAI_TOKEN}" \
        -H 'Content-Type: application/json' \
        -d "${REQUEST_BODY}")"
    TOTAL_SUBMIT="$(awk "BEGIN {print ${TOTAL_SUBMIT} + ${SUBMIT_TTFB}}")"

    TASK_ID="$(jq -r '.task_id' /tmp/graphai_submit.json 2>/dev/null || echo '')"
    if [[ -z "${TASK_ID}" || "${TASK_ID}" == "null" ]]; then
        echo "ERROR: did not get a task_id"
        cat /tmp/graphai_submit.json
        continue
    fi
    echo "[submit] task_id: ${TASK_ID}"
    echo

    STATUS_TTFB="$(measure_endpoint \
        "status" \
        "${BASE_URL}/translation/translate/status/${TASK_ID}" \
        "GET" \
        /tmp/graphai_status.json \
        -H 'accept: application/json' \
        -H "Authorization: Bearer ${GRAPHAI_TOKEN}")"
    TOTAL_STATUS="$(awk "BEGIN {print ${TOTAL_STATUS} + ${STATUS_TTFB}}")"

    echo -n "[status] result: "
    jq -c '.task_status, .task_result.result' /tmp/graphai_status.json 2>/dev/null || cat /tmp/graphai_status.json
    echo
    echo

done

AVG_SUBMIT="$(awk "BEGIN {print ${TOTAL_SUBMIT} / ${ITERATIONS}}")"
AVG_STATUS="$(awk "BEGIN {print ${TOTAL_STATUS} / ${ITERATIONS}}")"

echo "================================================"
echo "Translation endpoint summary (${ITERATIONS} iterations)"
echo "Average submit TTFB: ${AVG_SUBMIT}s"
echo "Average status TTFB: ${AVG_STATUS}s"
echo "================================================"
echo

# Soft target / warning
SLOW_CONTROL=0
if awk "BEGIN {exit !(${OPENAPI_TTFB} > 0.5)}"; then
    echo "WARNING: Control endpoint /openapi.json is > 0.5s. There is a global middleware/worker/instrumentation bottleneck."
    SLOW_CONTROL=1
fi

SLOW_TRANSLATION=0
if awk "BEGIN {exit !(${AVG_SUBMIT} > 0.5 || ${AVG_STATUS} > 0.5)}"; then
    echo "WARNING: Translation endpoints are > 0.5s. The rate-limiter/Redis/MySQL overhead likely still exists."
    SLOW_TRANSLATION=1
fi

if [[ ${SLOW_CONTROL} -eq 0 && ${SLOW_TRANSLATION} -eq 0 ]]; then
    echo "OK: All endpoints are within target (< 0.5s TTFB)."
else
    exit 2
fi
