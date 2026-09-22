#!/usr/bin/env bash

# ─────────────────────────────────────────────────────────────
# GraphRegistry processing loop
# ─────────────────────────────────────────────────────────────

commands=(
    "graphregistry airflow expire -d 30"
    "graphregistry airflow plan -l 10000 -shr"
    "graphregistry airflow status"
    # "graphregistry ai detect-concepts"
    "graphregistry kgraph compute"
    "graphregistry kgraph patch"
    "graphregistry airflow rollover"
)

# ─────────────────────────────────────────────────────────────

# Colours
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
CYAN='\033[0;36m'
DIM='\033[2m'
BOLD='\033[1m'
RESET='\033[0m'

iteration=0

while true; do
    ((iteration++))

    echo
    echo -e "${BOLD}${CYAN}🔄 GraphRegistry — iteration #${iteration}${RESET}"
    echo -e "${DIM}────────────────────────────────────────────────────────${RESET}"

    for command in "${commands[@]}"; do
        echo
        echo -e "${CYAN}▶  Running:${RESET} ${BOLD}${command}${RESET}"

        start=$SECONDS
        eval "$command"
        rc=$?
        elapsed=$((SECONDS - start))

        if (( rc == 0 )); then
            echo -e "${GREEN}✔  Success${RESET} ${DIM}(${elapsed}s)${RESET}"
        else
            echo -e "${RED}✖  Failed${RESET} ${DIM}(exit code: ${rc}, ${elapsed}s)${RESET}"
            echo
            echo -e "${RED}${BOLD}🛑 Stopping due to command failure.${RESET}"
            exit "$rc"
        fi
    done

    echo
    echo -e "${GREEN}${BOLD}✨ Iteration #${iteration} completed successfully.${RESET}"
done
