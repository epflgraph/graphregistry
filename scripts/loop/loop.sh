#!/usr/bin/env bash

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

trap 'echo "Loop stopped at $(date)"; exit 0' TERM INT

while true; do
    echo "========================================"
    echo "Iteration started: $(date)"
    echo "========================================"

    bash "$DIR/sequence.sh"
    STATUS=$?

    echo "Iteration finished: $(date) (exit code: $STATUS)"
    echo ""

    sleep 1
done
