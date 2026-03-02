#!/usr/bin/env bash
set -Eeuo pipefail
IFS=$'\n\t'

# -----------------------------
# Config
# -----------------------------
SEP=$'\n\n###########################################################################################\n\n'

# Memory limit for each step (KiB). 4 GiB = 4*1024*1024 KiB.
MEM_LIMIT_KIB=$((4*1024*1024))

# If you want a dry-run mode:
#   DRY_RUN=1 ./script.sh
DRY_RUN="${DRY_RUN:-0}"

# -----------------------------
# "Goodies"
# -----------------------------
# Nice error message on failure (works well with -E)
trap 'echo "ERROR: line $LINENO: $BASH_COMMAND" >&2' ERR

# Optional: show each command as it runs:
# set -x

# Ensure required binary exists before doing anything
command -v graphregistry >/dev/null 2>&1 || { echo "ERROR: graphregistry not found in PATH" >&2; exit 127; }

run_step() {
  # Usage: run_step "Human label" graphregistry ...
  local label="$1"; shift

  if [[ "$DRY_RUN" == "1" ]]; then
    echo "(dry-run) ulimit -v ${MEM_LIMIT_KIB} && $*" >&2
  else
    # Use && so the command won't run if ulimit fails.
    ( ulimit -v "${MEM_LIMIT_KIB}" && "$@" )
  fi

  printf "%s" "$SEP"
}

#=========================#
# MySQL data update steps #
#=========================#

run_step "airflow reset" \
  graphregistry airflow reset --options=typeflags,airflow,cache

run_step "airflow config" \
  graphregistry airflow config --typeflags=@airflow_config.json

run_step "airflow update_checksums" \
  graphregistry airflow update_checksums

run_step "airflow expire" \
  graphregistry airflow expire --older_than=90 --limit_per_type=1000

run_step "airflow refresh" \
  graphregistry airflow refresh --limit_per_type=1000

run_step "airflow status" \
  graphregistry airflow status

run_step "cache update (formulas)" \
  graphregistry cache update --formulas=fields,views,traversals,scores --actions=commit

run_step "cache update (matrix)" \
  graphregistry cache update --matrix --actions=commit

run_step "index build" \
  graphregistry index build --actions=commit

run_step "index patch" \
  graphregistry index patch --actions=commit

run_step "airflow rollover" \
  graphregistry airflow rollover --actions=commit

run_step "airflow update_dates" \
  graphregistry airflow update_dates --actions=commit

run_step "airflow reset (again)" \
  graphregistry airflow reset --options=typeflags,airflow

#=================================#
# ElasticSearch data update steps #
#=================================#

run_step "index generate (elasticsearch)" \
  graphregistry index generate --target=elasticsearch --index_date=2026-02-19 -r -f

run_step "es import" \
  graphregistry es import --env=xaas_coresrv \
    --input_folder=/home/dockerhost/data/es_exports/2026-02-19/es_fullindex_2026-02-19 \
    --rename_to=graphsearch_test_2026_02_19 -r -f

echo "End of script." >&2