#!/bin/bash

# Docker build script with automatic version detection
# Usage: ./build.sh [image-name] [tag]

set -e

# Default values
IMAGE_NAME=${1:-"table-sync"}
TAG=${2:-"latest"}

# Capture build-time information
BUILD_TIMESTAMP=$(date -u +"%Y-%m-%d_%H:%M:%S_UTC")
GIT_COMMIT=$(git rev-parse --short HEAD 2>/dev/null || echo "unknown")
GIT_TAG=$(git describe --tags --exact-match HEAD 2>/dev/null || git describe --tags --abbrev=0 2>/dev/null || echo "no-tag")
DOCKER_IMAGE_TAG="${IMAGE_NAME}:${TAG}"

echo "🏗️  Building Docker image with version information:"
echo "   📦 Image: ${DOCKER_IMAGE_TAG}"
echo "   🕐 Build time: ${BUILD_TIMESTAMP}"
echo "   🏷️  Git tag: ${GIT_TAG}"
echo "   📝 Git commit: ${GIT_COMMIT}"
echo

# Build the image with build arguments
docker build \
    --build-arg BUILD_TIMESTAMP="${BUILD_TIMESTAMP}" \
    --build-arg GIT_COMMIT="${GIT_COMMIT}" \
    --build-arg GIT_TAG="${GIT_TAG}" \
    --build-arg DOCKER_IMAGE_TAG="${DOCKER_IMAGE_TAG}" \
    -t "${DOCKER_IMAGE_TAG}" \
    .

echo
echo "✅ Build completed successfully!"
echo "   Image: ${DOCKER_IMAGE_TAG}"
echo "   Run with: docker run --rm ${DOCKER_IMAGE_TAG}"
echo

# Also tag as latest if not already
if [ "${TAG}" != "latest" ]; then
    echo "🏷️  Also tagging as latest..."
    docker tag "${DOCKER_IMAGE_TAG}" "${IMAGE_NAME}:latest"
fi

echo "🔍 Version information that will be detected at runtime:"
docker run --rm "${DOCKER_IMAGE_TAG}" python -c "
from version_utils import get_version_info
import json
info = get_version_info()
print(json.dumps(info, indent=2))
"