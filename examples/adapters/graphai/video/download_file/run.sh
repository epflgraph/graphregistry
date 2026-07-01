#!/usr/bin/env bash
set -euo pipefail

# Default output file if none is provided. Override with:
#   ./run.sh /path/to/output.mp4
OUTPUT_FILE="${1:-deleteme.mp4}"

python3 "$(dirname "$0")/example.py" --output "$OUTPUT_FILE"
