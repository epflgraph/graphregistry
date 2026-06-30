#!/usr/bin/env bash
set -euo pipefail

IMAGE="epflgraph/graphregistry"
BUILDER="multiarch"

VERSION=$(
python - <<'PY'
import tomllib
with open("pyproject.toml", "rb") as f:
    print(tomllib.load(f)["project"]["version"])
PY
)

echo "Building version: ${VERSION}"

# Create or reuse builder
if ! docker buildx inspect "$BUILDER" >/dev/null 2>&1; then
    echo "Creating buildx builder '$BUILDER'..."
    docker buildx create --name "$BUILDER" --driver docker-container --use
else
    docker buildx use "$BUILDER"
fi

docker buildx inspect --bootstrap

echo
echo "Building and pushing:"
echo "  ${IMAGE}:${VERSION}"
echo "  ${IMAGE}:latest"
echo

docker buildx build \
    --builder "$BUILDER" \
    --platform linux/amd64,linux/arm64 \
    -t "${IMAGE}:${VERSION}" \
    -t "${IMAGE}:latest" \
    --push .

echo
echo "✅ Successfully pushed:"
echo "   ${IMAGE}:${VERSION}"
echo "   ${IMAGE}:latest"
