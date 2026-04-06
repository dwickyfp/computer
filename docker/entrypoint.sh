#!/bin/bash
set -euo pipefail

# =============================================================================
# ROSETTA UNIFIED ENTRYPOINT
# Dispatches to the correct service based on MODE env var:
#   MODE=compute  →  CDC Pipeline Engine (port 8001)
#   MODE=web      →  FastAPI Backend + React Frontend via Nginx (ports 80/8000)
#   MODE=worker   →  Celery Task Processor (port 8002)
# =============================================================================

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info()  { echo -e "${GREEN}[INFO]${NC}  $1"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC}  $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

MODE="${MODE:-web}"

echo "======================================================="
echo "  Rosetta ETL Platform"
echo "  MODE : ${MODE^^}"
echo "  TZ   : ${TZ:-UTC}"
echo "======================================================="

case "${MODE,,}" in

  # -----------------------------------------------------------
  # COMPUTE — CDC Pipeline Engine
  # -----------------------------------------------------------
  compute)
    log_info "Starting COMPUTE mode (CDC Pipeline Engine)"
    export PATH="/app/.venv-compute/bin:$PATH"
    export PYTHONPATH=/app/compute

    mkdir -p /app/tmp/offsets
    log_info "Launching pipeline manager..."
    exec python -m compute.main
    ;;

  # -----------------------------------------------------------
  # WORKER — Celery Task Processor
  # -----------------------------------------------------------
  worker)
    log_info "Starting WORKER mode (Celery Task Processor)"
    export PATH="/app/.venv-worker/bin:$PATH"
    export PYTHONPATH=/app/worker

    cd /app/worker

    CONCURRENCY="${WORKER_CONCURRENCY:-10}"
    LOGLEVEL="${LOG_LEVEL:-info}"
    QUEUES="preview,default,orchestration"

    # Resolve ADBC Snowflake driver path
    if [ -z "${SNOWFLAKE_ADBC_DRIVER_PATH:-}" ]; then
        _ADBC_SO="$(python3 -c "import adbc_driver_snowflake, os; print(os.path.join(os.path.dirname(adbc_driver_snowflake.__file__), 'libadbc_driver_snowflake.so'))" 2>/dev/null || true)"
        if [ -f "$_ADBC_SO" ]; then
            export SNOWFLAKE_ADBC_DRIVER_PATH="$_ADBC_SO"
        fi
    fi
    log_info "ADBC driver: ${SNOWFLAKE_ADBC_DRIVER_PATH:-not set}"
    log_info "Concurrency : ${CONCURRENCY}"
    log_info "Queues      : ${QUEUES}"

    # Start health API server in background
    log_info "Starting health API on port ${SERVER_PORT:-8002}..."
    python server.py &
    HEALTH_PID=$!
    trap "echo 'Stopping health API...'; kill ${HEALTH_PID} 2>/dev/null" EXIT INT TERM

    log_info "Starting Celery worker..."
    exec python -m celery -A main worker \
        --loglevel="${LOGLEVEL}" \
        -Q "${QUEUES}" \
        -c "${CONCURRENCY}" \
        --pool=threads
    ;;

  # -----------------------------------------------------------
  # WEB — FastAPI Backend + React Frontend via Nginx
  # -----------------------------------------------------------
  web)
    log_info "Starting WEB mode (FastAPI Backend + React Frontend)"
    export PATH="/app/.venv-backend/bin:$PATH"
    export PYTHONPATH=/app/backend

    # Set up log directories
    mkdir -p /var/log/supervisor /var/log/nginx
    chown -R www-data:www-data /var/log/nginx 2>/dev/null || true

    # Validate required env vars
    if [ -z "${DATABASE_URL:-}" ] && [ "${RUN_MIGRATIONS:-false}" = "true" ]; then
        log_error "DATABASE_URL is required when RUN_MIGRATIONS=true"
        exit 1
    fi

    # Verify frontend build
    if [ ! -f "/var/www/html/index.html" ]; then
        log_error "Frontend build not found at /var/www/html/index.html"
        exit 1
    fi
    log_info "Frontend files verified"

    # Run database migrations
    if [ "${RUN_MIGRATIONS:-false}" = "true" ]; then
        log_info "Running database migrations..."
        cd /app/backend
        timeout 300 alembic upgrade head || {
            log_error "Migration failed or timed out after 5 minutes"
            exit 1
        }
        log_info "Migrations completed successfully"
    else
        log_warn "Skipping migrations (RUN_MIGRATIONS=${RUN_MIGRATIONS:-false})"
    fi

    log_info "CPU cores       : $(nproc)"
    log_info "Uvicorn workers : ${WEB_CONCURRENCY:-4}"
    log_info "Starting supervisord (nginx + uvicorn)..."
    exec /usr/bin/supervisord -c /etc/supervisor/conf.d/supervisord.conf
    ;;

  *)
    log_error "Unknown MODE='${MODE}'. Valid values: compute | web | worker"
    exit 1
    ;;

esac
