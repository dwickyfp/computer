#!/bin/bash
# =============================================================================
# ROSETTA BUILD SCRIPT
# =============================================================================
# Builds Docker images for the Rosetta ETL Platform
#
# Usage:
#   ./build.sh                    # Build all images
#   ./build.sh compute-node       # Build compute-node only
#   ./build.sh web                # Build web only
#   ./build.sh worker             # Build worker only
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
NC='\033[0m' # No Color

# Functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

build_compute_node() {
    log_info "Building compute-node image..."
    docker build \
        --target compute-node \
        --tag "${IMAGE_NAME}:${IMAGE_TAG}-compute" \
        --file ${DOCKERFILE} \
        .
    log_success "Built ${IMAGE_NAME}:${IMAGE_TAG}-compute"
}

build_web() {
    log_info "Building web image..."
    
    # Build args for frontend
    BUILD_ARGS=""
    if [ -n "${VITE_CLERK_PUBLISHABLE_KEY}" ]; then
        BUILD_ARGS="${BUILD_ARGS} --build-arg VITE_CLERK_PUBLISHABLE_KEY=${VITE_CLERK_PUBLISHABLE_KEY}"
    fi
    if [ -n "${VITE_API_URL}" ]; then
        BUILD_ARGS="${BUILD_ARGS} --build-arg VITE_API_URL=${VITE_API_URL}"
    fi
    
    docker build \
        --target web \
        --tag "${IMAGE_NAME}:${IMAGE_TAG}-web" \
        ${BUILD_ARGS} \
        --file ${DOCKERFILE} \
        .
    log_success "Built ${IMAGE_NAME}:${IMAGE_TAG}-web"
}

build_worker() {
    log_info "Building worker image..."
    docker build \
        --target worker \
        --tag "${IMAGE_NAME}:${IMAGE_TAG}-worker" \
        --file ${DOCKERFILE} \
        .
    log_success "Built ${IMAGE_NAME}:${IMAGE_TAG}-worker"
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
echo "  Image: ${IMAGE_NAME}:${IMAGE_TAG}"
echo "=============================================="
echo ""

TARGET=${1:-all}

case $TARGET in
    compute-node|compute)
        build_compute_node
        ;;
    web)
        build_web
        ;;
    worker)
        build_worker
        ;;
    all|"")
        build_compute_node
        build_web
        build_worker
        ;;
    *)
        log_error "Unknown target: $TARGET"
        echo "Usage: $0 [compute-node|web|worker|all]"
        exit 1
        ;;
esac

show_images

echo ""
log_success "Build completed successfully!"
echo ""
echo "To run the containers:"
echo "  docker compose -f docker-compose-app.yml up -d"
