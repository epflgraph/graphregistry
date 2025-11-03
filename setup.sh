#!/bin/bash

# Get the first command-line argument
env_name="$1"

# Check if argument is provided
if [ -z "$env_name" ]; then
  echo "Usage: $0 <env_name>"
  echo "   env_name: local, test"
  exit 1
fi

# Perform actions based on env_name
case "$env_name" in
  local)
    echo "Running local environment setup..."
    # Put your local-specific commands here
    # e.g., start local server, load .env.local, etc.
    cd config
    ln -sf ../../config/graphregistry/global_cfg.local.yaml config_global.yaml
    ln -sf ../../config/graphregistry/index_cfg.local.json  config_index.json
    ;;

  test)
    echo "Running test environment setup..."
    # Put your test-specific commands here
    # e.g., run unit tests, set environment variables, etc.
    cd config
    ln -sf ../../config/graphregistry/global_cfg.graphsearch.yaml config_global.yaml
    ln -sf ../../config/graphregistry/index_cfg.graphsearch.json  config_index.json
    ;;

  *)
    echo "Unknown environment: $env_name"
    echo "Please use one of: local, test"
    exit 1
    ;;
esac