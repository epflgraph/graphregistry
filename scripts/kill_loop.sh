#!/usr/bin/env bash

pkill -TERM -f '[./]scripts/run_loop\.sh' 2>/dev/null || true
pkill -TERM -f 'while true; do ./scripts/run_loop\.sh' 2>/dev/null || true

sleep 1

pkill -KILL -f '[./]scripts/run_loop\.sh' 2>/dev/null || true
pkill -KILL -f 'while true; do ./scripts/run_loop\.sh' 2>/dev/null || true

ps aux | grep '[r]un_loop'
