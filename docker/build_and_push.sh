#!/usr/bin/env bash
set -euo pipefail
# This script builds and pushes the Docker image for the Graph Registry project.

# Parameters
IMAGE="epflgraph/graphregistry"
BUILDER="multiarch"
# 0 = fully clean build (passes --no-cache, so even the local BuildKit cache is ignored)
# 1 = use registry-based aggressive caching (--cache-from / --cache-to)
AGRESSIVE_CACHING=0

# Get the version from pyproject.toml
VERSION=$(
python - <<'PY'
import tomllib
with open("pyproject.toml", "rb") as f:
    print(tomllib.load(f)["project"]["version"])
PY
)

# Print the version being built
echo "Building version: ${VERSION}"

# Create or reuse builder
if ! docker buildx inspect "$BUILDER" >/dev/null 2>&1; then
    echo "Creating buildx builder '$BUILDER'..."
    docker buildx create --name "$BUILDER" --driver docker-container --use
else
    docker buildx use "$BUILDER"
fi

# Bootstrap the builder to ensure it's ready
docker buildx inspect --bootstrap

# Print the caching mode that will be used
if [[ "$AGRESSIVE_CACHING" -eq 1 ]]; then
    echo "Caching:       Agressive"
else
    echo "Caching:       None"
fi

# Build and push the Docker image
echo
echo "Building and pushing:"
echo "  ${IMAGE}:${VERSION}"
echo "  ${IMAGE}:latest"
echo

# Define the cache image for aggressive caching
CACHE_IMAGE="${IMAGE}:buildcache"

# Use aggressive caching if enabled
if [[ "$AGRESSIVE_CACHING" -eq 1 ]]; then
    echo "Using aggressive caching with cache image: ${CACHE_IMAGE} ..."
    docker buildx build \
        --builder "$BUILDER" \
        --platform linux/amd64,linux/arm64 \
        --cache-from "type=registry,ref=${CACHE_IMAGE}" \
        --cache-to   "type=registry,ref=${CACHE_IMAGE},mode=max" \
        -t "${IMAGE}:${VERSION}" \
        -t "${IMAGE}:latest" \
        --push .
# Else, do a clean build: ignore the local builder cache and the registry cache.
else
    echo "Using default caching (clean build, --no-cache) ..."
    docker buildx build \
        --builder "$BUILDER" \
        --platform linux/amd64,linux/arm64 \
        --no-cache \
        -t "${IMAGE}:${VERSION}" \
        -t "${IMAGE}:latest" \
        --push .
fi

# Print success message
echo
echo "✅ Successfully pushed:"
echo "   ${IMAGE}:${VERSION}"
echo "   ${IMAGE}:latest"
