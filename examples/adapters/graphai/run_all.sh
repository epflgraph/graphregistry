#!/usr/bin/env bash
# Run all GraphAI adapter examples in a logical order and report results.
# Inherits the current environment (e.g. GOOGLE_CLOUD_API_KEY from .env).
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Where to save the video download_file example output.
DOWNLOAD_OUTPUT="${DOWNLOAD_OUTPUT:-/tmp/graphai_video_download_example.mp4}"

PASS=0
FAIL=0
SKIPPED=0

# ANSI colors (no-op if not a terminal)
if [[ -t 1 ]]; then
  BOLD='\033[1m'
  RESET='\033[0m'
else
  BOLD=''
  RESET=''
fi

print_header() {
  echo
  echo -e "${BOLD}📂 $1${RESET}"
  echo "$(printf '%.0s-' {1..60})"
}

run_example() {
  local name="$1"
  local run_script="$2"
  shift 2

  echo
  echo "▶️  $name"
  echo "   command: $run_script $*"

  local rc=0
  local output
  local start_time end_time elapsed

  start_time=$(date +%s.%N)
  output=$("$run_script" "$@" 2>&1) || rc=$?
  end_time=$(date +%s.%N)
  elapsed=$(awk "BEGIN { printf \"%.2f\", $end_time - $start_time }")

  # Print the example output, indented.
  if [[ -n "$output" ]]; then
    echo "$output" | sed 's/^/   /'
  fi

  if [[ $rc -eq 0 ]]; then
    echo "   ✅ Success  ⏱️  ${elapsed}s"
    PASS=$((PASS + 1))
  elif [[ $rc -eq 127 ]]; then
    echo "   ⏭️  Skipped (script not found)  ⏱️  ${elapsed}s"
    SKIPPED=$((SKIPPED + 1))
  else
    echo "   ❌ Failed (exit code $rc)  ⏱️  ${elapsed}s"
    FAIL=$((FAIL + 1))
  fi

  return $rc
}

# ---------------------------------------------------------------------------
# Start
# ---------------------------------------------------------------------------
TOTAL_START=$(date +%s.%N)
echo
echo -e "${BOLD}🚀 Running all GraphAI adapter examples${RESET}"
echo "   started: $(date -Iseconds)"
echo "   script dir: $SCRIPT_DIR"
echo "   download output: $DOWNLOAD_OUTPUT"

# ---------------------------------------------------------------------------
# Text
# ---------------------------------------------------------------------------
print_header "Text"
run_example "Wiki search" "$SCRIPT_DIR/text/wiki_search/run.sh"
run_example "Extract keywords" "$SCRIPT_DIR/text/extract_keywords/run.sh"
run_example "Detect concepts" "$SCRIPT_DIR/text/detect_concepts/run.sh"

# ---------------------------------------------------------------------------
# Translation
# ---------------------------------------------------------------------------
print_header "Translation"
run_example "Translate text" "$SCRIPT_DIR/translation/translate_text/run.sh"
run_example "Translate multilingual" "$SCRIPT_DIR/translation/translate_multilingual/run.sh"

# ---------------------------------------------------------------------------
# Embedding
# ---------------------------------------------------------------------------
print_header "Embedding"
run_example "Embed text (string)" "$SCRIPT_DIR/embedding/embed_text_str/run.sh"
run_example "Embed text (list)" "$SCRIPT_DIR/embedding/embed_text_list/run.sh"

# ---------------------------------------------------------------------------
# Image
# ---------------------------------------------------------------------------
print_header "Image"
run_example "Image fingerprint" "$SCRIPT_DIR/image/fingerprint/run.sh"
run_example "Extract text (OCR)" "$SCRIPT_DIR/image/extract_text/run.sh"

# ---------------------------------------------------------------------------
# Video
# ---------------------------------------------------------------------------
print_header "Video"
run_example "Get video" "$SCRIPT_DIR/video/get_video/run.sh"
run_example "Video fingerprint" "$SCRIPT_DIR/video/fingerprint/run.sh"
# run_example "Download video file" "$SCRIPT_DIR/video/download_file/run.sh" "$DOWNLOAD_OUTPUT"
run_example "Extract audio" "$SCRIPT_DIR/video/extract_audio/run.sh"
run_example "Extract slides" "$SCRIPT_DIR/video/extract_slides/run.sh"
run_example "Process slides" "$SCRIPT_DIR/video/process_slides/run.sh"

# ---------------------------------------------------------------------------
# Voice
# ---------------------------------------------------------------------------
print_header "Voice"
run_example "Detect language" "$SCRIPT_DIR/voice/detect_language/run.sh"
run_example "Voice fingerprint" "$SCRIPT_DIR/voice/fingerprint/run.sh"
run_example "Transcribe audio" "$SCRIPT_DIR/voice/transcribe_audio/run.sh"

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
TOTAL_END=$(date +%s.%N)
TOTAL_ELAPSED=$(awk "BEGIN { printf \"%.2f\", $TOTAL_END - $TOTAL_START }")

echo
print_header "Summary"
echo "   ✅ Passed:   $PASS"
echo "   ❌ Failed:   $FAIL"
echo "   ⏭️  Skipped: $SKIPPED"
echo "   ⏱️  Total time: ${TOTAL_ELAPSED}s"
echo "   finished: $(date -Iseconds)"

if [[ $FAIL -gt 0 ]]; then
  echo
  echo -e "${BOLD}⚠️  Some examples failed.${RESET}"
  exit 1
fi

echo
echo -e "${BOLD}🎉 All examples completed successfully.${RESET}"
