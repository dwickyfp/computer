#!/bin/bash
set -euo pipefail

# Color output for better visibility
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

echo "=================================="
echo "  Rosetta Web Container Starting  "
echo "=================================="
log_info "MODE: ${MODE:-web}"
log_info "Environment: ${ENVIRONMENT:-production}"
echo "=================================="

# Validate required environment variables
if [ -z "${DATABASE_URL:-}" ] && [ "${RUN_MIGRATIONS:-false}" = "true" ]; then
    log_error "DATABASE_URL is required when RUN_MIGRATIONS=true"
    exit 1
fi

# Create log directories with proper permissions
log_info "Setting up log directories..."
mkdir -p /var/log/supervisor
mkdir -p /var/log/nginx
chown -R www-data:www-data /var/log/nginx

# Verify static files exist
if [ ! -f "/var/www/html/index.html" ]; then
    log_error "Frontend build not found at /var/www/html/index.html"
    exit 1
fi
log_info "Frontend files verified"

# Run database migrations if needed
if [ "${RUN_MIGRATIONS:-false}" = "true" ]; then
    log_info "Running database migrations..."
    cd /app/backend
    
    # Check if alembic is available
    if ! /app/.venv/bin/alembic --version &> /dev/null; then
        log_error "Alembic not found in virtual environment"
        exit 1
    fi
    
    # Run migrations with timeout
    timeout 300 /app/.venv/bin/alembic upgrade head || {
        log_error "Migration failed or timed out after 5 minutes"
        exit 1
    }
    log_info "Migrations completed successfully"
else
    log_warn "Skipping database migrations (RUN_MIGRATIONS=${RUN_MIGRATIONS:-false})"
fi

# Wait for backend to be ready (optional health check)
if [ "${WAIT_FOR_BACKEND:-false}" = "true" ]; then
    log_info "Waiting for backend to be ready..."
    max_attempts=30
    attempt=0
    while [ $attempt -lt $max_attempts ]; do
        if curl -f http://127.0.0.1:8000/health &> /dev/null; then
            log_info "Backend is ready!"
            break
        fi
        attempt=$((attempt + 1))
        sleep 2
    done
    
    if [ $attempt -eq $max_attempts ]; then
        log_warn "Backend health check timed out, continuing anyway..."
    fi
fi

# Display resource limits
log_info "System information:"
log_info "  CPU cores: $(nproc)"
log_info "  Memory: $(free -h | awk '/^Mem:/ {print $2}')"
log_info "  Uvicorn workers: ${WEB_CONCURRENCY:-4}"

# Start supervisord (manages nginx + uvicorn)
log_info "Starting supervisord..."
exec /usr/bin/supervisord -c /etc/supervisor/conf.d/supervisord.conf
