#!/usr/bin/env bash

# Thoroughly check whether ping_model.py (or something equivalent) is still
# running, holding its files open, connecting to the inference server, or
# configured to restart automatically.

set -u

SCRIPT_PATTERN="ping_model.py"
MODEL_PATTERN="Qwen/Qwen3-Coder-Next-fp8"
SCRIPT_FILE="scripts/ping_model.py"
LOG_FILE="scripts/ping_model.log"
SERVER="inference.rcp.epfl.ch"
OLD_PID="${1:-}"

# ── Colours ──────────────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
DIM='\033[2m'
RESET='\033[0m'

PASS=0
WARN=0
FAIL=0

header() {
    printf "\n${BOLD}${BLUE}━━━ %s ━━━${RESET}\n" "$1"
}

ok() {
    printf "${GREEN}✅ %s${RESET}\n" "$1"
    ((PASS++))
}

warn() {
    printf "${YELLOW}⚠️  %s${RESET}\n" "$1"
    ((WARN++))
}

fail() {
    printf "${RED}🚨 %s${RESET}\n" "$1"
    ((FAIL++))
}

info() {
    printf "${CYAN}ℹ️  %s${RESET}\n" "$1"
}

printf "\n${BOLD}${CYAN}🔎 Deep inference-pinger scan${RESET}\n"
printf "${DIM}Server: %s | Script: %s${RESET}\n" "$SERVER" "$SCRIPT_PATTERN"

# ── Process name / command line ──────────────────────────────────────────────
header "PROCESS SCAN"

MATCHES="$(pgrep -af "$SCRIPT_PATTERN" 2>/dev/null || true)"

if [[ -z "$MATCHES" ]]; then
    ok "No process matching '$SCRIPT_PATTERN'"
else
    fail "Found process(es) matching '$SCRIPT_PATTERN':"
    printf "%s\n" "$MATCHES"
fi

# ── /proc direct scan ────────────────────────────────────────────────────────
header "/proc KERNEL PROCESS SCAN"

PROC_MATCHES="$(
    grep -aH "$SCRIPT_PATTERN" /proc/[0-9]*/cmdline 2>/dev/null || true
)"

if [[ -z "$PROC_MATCHES" ]]; then
    ok "No /proc command line contains '$SCRIPT_PATTERN'"
else
    fail "Found '$SCRIPT_PATTERN' in /proc:"
    printf "%s\n" "$PROC_MATCHES"
fi

MODEL_MATCHES="$(
    grep -aH "$MODEL_PATTERN" /proc/[0-9]*/cmdline 2>/dev/null || true
)"

if [[ -z "$MODEL_MATCHES" ]]; then
    ok "No /proc command line contains model '$MODEL_PATTERN'"
else
    fail "Found model name in a running process:"
    printf "%s\n" "$MODEL_MATCHES"
fi

# ── Old PID ──────────────────────────────────────────────────────────────────
header "PID CHECK"

if [[ -n "$OLD_PID" ]]; then
    if kill -0 "$OLD_PID" 2>/dev/null; then
        fail "PID $OLD_PID still exists"
        ps -p "$OLD_PID" -o pid,ppid,user,stat,etime,cmd
    else
        ok "Old PID $OLD_PID does not exist"
    fi
else
    info "No old PID supplied"
    printf "${DIM}Usage: $0 790941${RESET}\n"
fi

# ── Open files ────────────────────────────────────────────────────────────────
header "OPEN FILE CHECK"

if command -v lsof >/dev/null 2>&1; then
    SCRIPT_OPEN="$(lsof "$SCRIPT_FILE" 2>/dev/null || true)"
    LOG_OPEN="$(lsof "$LOG_FILE" 2>/dev/null || true)"

    if [[ -z "$SCRIPT_OPEN" ]]; then
        ok "No visible process has $SCRIPT_FILE open"
    else
        warn "$SCRIPT_FILE is open:"
        printf "%s\n" "$SCRIPT_OPEN"
    fi

    if [[ -z "$LOG_OPEN" ]]; then
        ok "No visible process has $LOG_FILE open"
    else
        warn "$LOG_FILE is open:"
        printf "%s\n" "$LOG_OPEN"
    fi

    info "lsof can have limited visibility across Docker/mount namespaces."
else
    warn "lsof is not installed; skipping open-file checks"
fi

# ── Resolve inference server ─────────────────────────────────────────────────
header "NETWORK CHECK"

mapfile -t SERVER_IPS < <(
    getent ahostsv4 "$SERVER" 2>/dev/null |
    awk '{print $1}' |
    sort -u
)

if ((${#SERVER_IPS[@]} == 0)); then
    warn "Could not resolve $SERVER"
else
    info "$SERVER resolves to: ${SERVER_IPS[*]}"

    CONNECTION_FOUND=0

    for ip in "${SERVER_IPS[@]}"; do
        MATCH="$(ss -tnp 2>/dev/null | grep -F "$ip" || true)"

        if [[ -n "$MATCH" ]]; then
            CONNECTION_FOUND=1
            fail "Active TCP connection involving $ip:"
            printf "%s\n" "$MATCH"
        fi
    done

    if ((CONNECTION_FOUND == 0)); then
        ok "No current TCP connections to the inference server"
    fi
fi

# ── Cron ─────────────────────────────────────────────────────────────────────
header "AUTO-RESTART CHECK"

CRON="$(crontab -l 2>/dev/null || true)"

if printf '%s\n' "$CRON" | grep -qiE 'ping_model|Qwen3-Coder-Next'; then
    fail "Found a suspicious user cron entry:"
    printf "%s\n" "$CRON" | grep -iE 'ping_model|Qwen3-Coder-Next'
else
    ok "No matching user cron entry"
fi

# ── systemd ──────────────────────────────────────────────────────────────────
SYSTEMD_MATCH="$(
    grep -R -iE 'ping_model|Qwen3-Coder-Next' \
        "$HOME/.config/systemd" 2>/dev/null || true
)"

if [[ -z "$SYSTEMD_MATCH" ]]; then
    ok "No matching user systemd configuration"
else
    fail "Found matching user systemd configuration:"
    printf "%s\n" "$SYSTEMD_MATCH"
fi

# ── Related command lines ────────────────────────────────────────────────────
header "RELATED COMMAND SCAN"

RELATED="$(
    ps auxww |
    grep -E '[p]ing_model\.py|[Q]wen/Qwen3-Coder-Next-fp8' ||
    true
)"

if [[ -z "$RELATED" ]]; then
    ok "No related command lines found"
else
    fail "Related process command lines found:"
    printf "%s\n" "$RELATED"
fi

# ── File status ────────────────────────────────────────────────────────────────
header "FILE EXISTENCE CHECK"

if [[ ! -e "$SCRIPT_FILE" ]]; then
    ok "$SCRIPT_FILE does not exist"
else
    warn "$SCRIPT_FILE still exists"
fi

if [[ ! -e "$LOG_FILE" ]]; then
    ok "$LOG_FILE does not exist"
else
    info "$LOG_FILE still exists"
fi

# ── Log status ────────────────────────────────────────────────────────────────
header "LOG STATUS"

if [[ -f "$LOG_FILE" ]]; then
    info "Last modification:"
    stat -c '   📅 %y   📦 %s bytes' "$LOG_FILE"

    info "Last 5 log lines:"
    tail -n 5 "$LOG_FILE" | sed 's/^/   /'
else
    ok "$LOG_FILE does not exist"
fi

# ── Summary ──────────────────────────────────────────────────────────────────
header "SUMMARY"

printf "${GREEN}✅ Passed: %d${RESET}\n" "$PASS"
printf "${YELLOW}⚠️  Warnings: %d${RESET}\n" "$WARN"
printf "${RED}🚨 Failures: %d${RESET}\n" "$FAIL"

if ((FAIL == 0)); then
    printf "\n${BOLD}${GREEN}🟢 No evidence that the inference pinger is running.${RESET}\n\n"
    exit 0
else
    printf "\n${BOLD}${RED}🔴 Something needs investigation — see failures above.${RESET}\n\n"
    exit 1
fi
