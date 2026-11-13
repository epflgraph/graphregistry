#!/usr/bin/env bash

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PID_FILE="$DIR/loop.pid"
LOG_FILE="$DIR/loop.log"

if [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
    echo "🟢 Loop running (PID $(cat "$PID_FILE"))"
else
    echo "🔴 Loop not running"
fi

echo "📋 Monitoring logs: $LOG_FILE"
echo "Press Ctrl+C to stop monitoring (loop keeps running)"
echo "----------------------------------------"

touch "$LOG_FILE"
tail -n 100 -f "$LOG_FILE"
