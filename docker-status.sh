#!/usr/bin/env bash

printf "\n📚 Graph Registry\n\n"

docker compose ps --format '{{.Service}}|{{.Status}}|{{.Ports}}' |
while IFS='|' read -r service status ports; do
    case "$status" in
        *healthy*)
            icon="🟢"
            ;;
        *starting*)
            icon="🟡"
            ;;
        *Restarting*|*unhealthy*)
            icon="🔴"
            ;;
        *Exited*|*Dead*)
            icon="🔴"
            ;;
        Up*)
            icon="🟢"
            ;;
        *)
            icon="🟡"
            ;;
    esac

    printf "%-3s %-16s %-32s %s\n" "$icon" "$service" "$status" "$ports"
done

echo
