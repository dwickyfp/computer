#!/bin/bash
# =============================================================================
# ROSETTA BUILD SCRIPT
# =============================================================================
# Builds the single unified Rosetta Docker image.
# The same image is used by all three services (compute / web / worker);
# the MODE environment variable selects the runtime behaviour.
#
# Usage:
#   ./build.sh                    # Build unified image
#   ./build.sh --push             # Build and push to registry
# =============================================================================

set -e

# Configuration
IMAGE_NAME="rosetta-etl"
IMAGE_TAG="1.0.0"
DOCKERFILE="Dockerfile"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info()    { echo -e "${BLUE}[INFO]${NC}    $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
log_warning() { echo -e "${YELLOW}[WARNING]${NC} $1"; }
log_error()   { echo -e "${RED}[ERROR]${NC}   $1"; }

build_rosetta() {
    log_info "Building unified Rosetta image: ${IMAGE_NAME}:${IMAGE_TAG}..."

    # Optional Vite build args
    BUILD_ARGS=""
    if [ -n "${VITE_CLERK_PUBLISHABLE_KEY:-}" ]; then
        BUILD_ARGS="${BUILD_ARGS} --build-arg VITE_CLERK_PUBLISHABLE_KEY=${VITE_CLERK_PUBLISHABLE_KEY}"
    fi
    if [ -n "${VITE_API_URL:-}" ]; then
        BUILD_ARGS="${BUILD_ARGS} --build-arg VITE_API_URL=${VITE_API_URL}"
    fi

    docker build \
        --target rosetta \
        --tag "${IMAGE_NAME}:${IMAGE_TAG}" \
        --tag "${IMAGE_NAME}:latest" \
        ${BUILD_ARGS} \
        --file ${DOCKERFILE} \
        .
    log_success "Built ${IMAGE_NAME}:${IMAGE_TAG}"
}

show_images() {
    echo ""
    log_info "Built images:"
    echo "=============================================="
    docker images | grep ${IMAGE_NAME} || true
    echo "=============================================="
}

# Main
echo "=============================================="
echo "  ROSETTA ETL PLATFORM - BUILD SCRIPT"
echo "  Unified image: ${IMAGE_NAME}:${IMAGE_TAG}"
echo "=============================================="
echo ""

build_rosetta
show_images

echo ""
log_success "Build completed successfully!"
echo ""
echo "To run the containers:"
echo "  docker compose -f docker-compose-app.yml up -d"
