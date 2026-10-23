#!/usr/bin/env bash

# Real-time, NO-SUDO monitor for connections to the inference server.
#
# This does NOT inspect HTTPS contents. It watches TCP connections to the
# server and reports when they appear/disappear.
#
# No sudo required.

set -u

SERVER="${1:-inference.rcp.epfl.ch}"
PORT="${2:-443}"
INTERVAL="${MONITOR_INTERVAL:-0.2}"

# ── Colours ──────────────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
BOLD='\033[1m'
DIM='\033[2m'
RESET='\033[0m'

cleanup() {
    printf "\n\n${GREEN}👋 Monitor stopped.${RESET}\n"
    exit 0
}

trap cleanup INT TERM

clear

printf "${BOLD}${CYAN}📡 Inference Server Traffic Monitor${RESET}\n"
printf "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}\n"
printf "🌐 Server: ${BOLD}%s${RESET}\n" "$SERVER"
printf "🔌 Port:   ${BOLD}%s${RESET}\n" "$PORT"
printf "⏱️  Poll:   ${BOLD}%ss${RESET}\n" "$INTERVAL"

mapfile -t IPS < <(
    getent ahostsv4 "$SERVER" 2>/dev/null |
    awk '{print $1}' |
    sort -u
)

if ((${#IPS[@]} == 0)); then
    printf "\n${RED}❌ Could not resolve %s${RESET}\n" "$SERVER"
    exit 1
fi

printf "🎯 IP(s):  ${BOLD}%s${RESET}\n" "${IPS[*]}"
printf "\n${YELLOW}🔒 HTTPS contents are encrypted; monitoring TCP connections.${RESET}\n"
printf "${DIM}Press Ctrl+C to stop.${RESET}\n\n"

printf "${BOLD}${MAGENTA}EVENTS${RESET}\n"
printf "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}\n"

previous=""

printf "${GREEN}🟢 [%s] Monitoring — no active connection.${RESET}\n" \
    "$(date '+%H:%M:%S')"

while true; do
    current=""

    for ip in "${IPS[@]}"; do
        matches="$(
            ss -Htnp 2>/dev/null |
            awk -v target="$ip:$PORT" '$5 == target' ||
            true
        )"

        if [[ -n "$matches" ]]; then
            current+="$matches"$'\n'
        fi
    done

    current="${current%$'\n'}"

    # ── Connection appeared ──────────────────────────────────────────────────
    if [[ -n "$current" && -z "$previous" ]]; then

        printf "\n${BOLD}${RED}🚨 [%s] CONNECTION DETECTED${RESET}\n" \
            "$(date '+%H:%M:%S.%3N')"

        while IFS= read -r line; do
            [[ -z "$line" ]] && continue

            printf "${YELLOW}   %s${RESET}\n" "$line"

            pid="$(
                printf '%s\n' "$line" |
                sed -n 's/.*pid=\([0-9]\+\).*/\1/p'
            )"

            if [[ -n "$pid" && -r "/proc/$pid/cmdline" ]]; then
                cmd="$(
                    tr '\0' ' ' < "/proc/$pid/cmdline" 2>/dev/null
                )"

                printf "${CYAN}   🧩 PID: %s${RESET}\n" "$pid"
                printf "${CYAN}   📝 CMD: %s${RESET}\n" "$cmd"
            else
                printf "${DIM}   ℹ️  Process identity unavailable.${RESET}\n"
            fi

        done <<< "$current"

        printf "\n"

    # ── Connection disappeared ───────────────────────────────────────────────
    elif [[ -z "$current" && -n "$previous" ]]; then

        printf "${GREEN}🔚 [%s] Connection closed${RESET}\n" \
            "$(date '+%H:%M:%S.%3N')"

        printf "${GREEN}🟢 [%s] Monitoring — no active connection.${RESET}\n" \
            "$(date '+%H:%M:%S')"
    fi

    previous="$current"

    sleep "$INTERVAL"
done
