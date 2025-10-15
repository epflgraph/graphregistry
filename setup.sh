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
    ln -sf ../config/graphregistry/config-local.yaml config.yaml
    ln -sf ../../../config/graphregistry/index_config-local.json database/config/index_config.json
    ;;

  test)
    echo "Running test environment setup..."
    # Put your test-specific commands here
    # e.g., run unit tests, set environment variables, etc.
    ln -sf ../config/graphregistry/config-graphtest.yaml config.yaml
    ln -sf ../../../config/graphregistry/index_config-graphtest.json database/config/index_config.json
    ;;

  *)
    echo "Unknown environment: $env_name"
    echo "Please use one of: local, test"
    exit 1
    ;;
esac