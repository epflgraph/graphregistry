#!/usr/bin/env bash
# Diagnostic script for /video/get_file hang.
# Runs the curl matrix requested by GraphAI and captures timing, headers,
# file sizes, and checksums.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

TOKEN="${TOKEN:-177581471705447505981400.mp4}"
URL="${URL:-}"
AUTH="${AUTH:-}"
MAX_TIME="${MAX_TIME:-120}"
OUT_DIR="${OUT_DIR:-/tmp/graphai_video_get_file_diagnose}"

usage() {
  cat <<EOF
Usage: TOKEN=<token> URL=<url> AUTH=<bearer_token> [OUT_DIR=<dir>] ./diagnose.sh

Environment variables:
  URL     GraphAI /video/get_file URL, e.g. https://graphai.example.com/video/get_file
  AUTH    Bearer token for GraphAI
  TOKEN   Video token to download (default: 177581471705447505981400.mp4)
  MAX_TIME  Max time per curl in seconds (default: 120)
  OUT_DIR Output directory for captured files and logs (default: /tmp/graphai_video_get_file_diagnose)

Example:
  URL=https://graphai.example.com/video/get_file AUTH="Bearer abc123" ./diagnose.sh
EOF
  exit 1
}

if [[ -z "$URL" || -z "$AUTH" ]]; then
  echo "ERROR: URL and AUTH must be set." >&2
  usage
fi

mkdir -p "$OUT_DIR"
OUT_DIR="$(cd "$OUT_DIR" && pwd)"

echo "========================================"
echo "GraphAI /video/get_file diagnostic"
echo "URL:    $URL"
echo "TOKEN:  $TOKEN"
echo "OUTPUT: $OUT_DIR"
echo "TIME:   $(date -Iseconds)"
echo "========================================"
echo

run_curl() {
  local name="$1"
  shift
  local out_file="$OUT_DIR/${name}.mp4"
  local log_file="$OUT_DIR/${name}.log"
  local header_file="$OUT_DIR/${name}.headers"

  echo "--- Running: $name ---"
  echo "Command: curl -w '@$CURL_FORMAT_FILE' -D \"$header_file\" -o \"$out_file\" -sS --max-time $MAX_TIME $*" | tee "$log_file"

  local rc=0
  # -v captures request headers, TLS handshake, and response headers in the log.
  # -D still writes response headers to a separate file for easy parsing.
  curl -w "@$CURL_FORMAT_FILE" \
       -D "$header_file" \
       -o "$out_file" \
       -sS \
       -v \
       --max-time "$MAX_TIME" \
       "$@" \
       2>&1 | tee -a "$log_file" || rc=$?

  echo "Exit code: $rc" | tee -a "$log_file"
  echo
  return $rc
}

# Create a temporary curl write-out format file. curl interprets literal
# backslash-n sequences in the file as newlines, so we build it with printf.
CURL_FORMAT_FILE="$(mktemp)"
printf '%s\\n' \
  '--- curl timing ---' \
  'http_code:          %{http_code}' \
  'content_type:       %{content_type}' \
  'name_lookup:        %{time_namelookup}s' \
  'connect:            %{time_connect}s' \
  'app_connect:        %{time_appconnect}s' \
  'pre_transfer:       %{time_pretransfer}s' \
  'redirect:           %{time_redirect}s' \
  'start_transfer:     %{time_starttransfer}s' \
  'total:              %{time_total}s' \
  'size_download:      %{size_download} bytes' \
  'size_header:        %{size_header} bytes' \
  'speed_download:     %{speed_download} bytes/s' \
  > "$CURL_FORMAT_FILE"

cleanup_format() { rm -f "$CURL_FORMAT_FILE"; }
trap cleanup_format EXIT

echo "Test payload: {\"token\":\"$TOKEN\"}" > "$OUT_DIR/payload.json"

# ---------------------------------------------------------------------------
# Test 1: HTTP/1.1 keep-alive (default)
# ---------------------------------------------------------------------------
run_curl "http11_keepalive" \
  --http1.1 \
  -X POST "$URL" \
  -H "Authorization: $AUTH" \
  -H "Content-Type: application/json" \
  -d "{\"token\":\"$TOKEN\"}" || true

# ---------------------------------------------------------------------------
# Test 2: Connection: close
# ---------------------------------------------------------------------------
run_curl "http11_close" \
  --http1.1 \
  -X POST "$URL" \
  -H "Authorization: $AUTH" \
  -H "Content-Type: application/json" \
  -H "Connection: close" \
  -d "{\"token\":\"$TOKEN\"}" || true

# ---------------------------------------------------------------------------
# Test 3: HTTP/1.0
# ---------------------------------------------------------------------------
run_curl "http10" \
  --http1.0 \
  -X POST "$URL" \
  -H "Authorization: $AUTH" \
  -H "Content-Type: application/json" \
  -d "{\"token\":\"$TOKEN\"}" || true

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo "========================================"
echo "Summary"
echo "========================================"

for name in http11_keepalive http11_close http10; do
  out_file="$OUT_DIR/${name}.mp4"
  header_file="$OUT_DIR/${name}.headers"
  log_file="$OUT_DIR/${name}.log"

  echo
  echo "--- $name ---"
  if [[ -f "$out_file" ]]; then
    echo "Output size: $(stat -c%s "$out_file" 2>/dev/null || stat -f%z "$out_file" 2>/dev/null || echo 'unknown') bytes"
    if command -v sha256sum >/dev/null 2>&1; then
      echo "SHA256:      $(sha256sum "$out_file" | cut -d' ' -f1)"
    elif command -v shasum >/dev/null 2>&1; then
      echo "SHA256:      $(shasum -a 256 "$out_file" | cut -d' ' -f1)"
    fi
  else
    echo "Output file missing"
  fi

  if [[ -f "$header_file" ]]; then
    echo "Response headers:"
    sed -n '/^HTTP\|^[Cc]onnection:\|^[Cc]ontent-[Ll]ength:\|^[Tt]ransfer-[Ee]ncoding:/p' "$header_file" | sed 's/^/  /'
  else
    echo "Header file missing"
  fi

  if [[ -f "$log_file" ]]; then
    echo "Timing (from log):"
    sed -n '/--- curl timing ---/,$p' "$log_file" | sed 's/^/  /'
  fi
done

echo
echo "All artifacts saved to: $OUT_DIR"
echo "Share the .log, .headers, and .mp4 files (or their sizes) with GraphAI."
