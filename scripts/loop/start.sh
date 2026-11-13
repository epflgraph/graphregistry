#!/usr/bin/env bash
set -e

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PID_FILE="$DIR/loop.pid"
LOG_FILE="$DIR/loop.log"

if [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
    echo "Loop already running (PID $(cat "$PID_FILE"))"
    exit 1
fi

cd "$DIR"

nohup bash "$DIR/loop.sh" >> "$LOG_FILE" 2>&1 &
echo $! > "$PID_FILE"

echo "Loop started (PID $!)"
echo "Logs: $LOG_FILE"
