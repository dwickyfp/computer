#!/bin/bash
# Start the Rosetta Worker service (HIGH PERFORMANCE)
# Usage: ./start.sh [options]
#
# Options:
#   --concurrency N    Number of worker threads (default: 8)
#   --queues Q         Comma-separated queue names (default: preview,default,orchestration)
#   --loglevel LEVEL   Log level (default: info)
#   --beat             Also start Celery Beat scheduler

set -e

cd "$(dirname "$0")"

if [ -x ".venv/bin/python" ]; then
    PYTHON_BIN="$(pwd)/.venv/bin/python"
else
    PYTHON_BIN="$(command -v python3 || command -v python)"
fi

if [ -z "$PYTHON_BIN" ]; then
    echo "Python interpreter not found"
    exit 1
fi

export PATH="$(dirname "$PYTHON_BIN"):$PATH"

CONCURRENCY=${WORKER_CONCURRENCY:-10}
QUEUES="preview,default,orchestration"
LOGLEVEL=${LOG_LEVEL:-info}
RUN_BEAT=false

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --concurrency) CONCURRENCY=$2; shift 2 ;;
        --queues) QUEUES=$2; shift 2 ;;
        --loglevel) LOGLEVEL=$2; shift 2 ;;
        --beat) RUN_BEAT=true; shift ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

# Expose ADBC Snowflake native driver so DuckDB can find it at runtime.
# Per docs (https://github.com/iqea-ai/duckdb-snowflake#adbc-driver-setup),
# DuckDB searches ~/.duckdb/extensions/<version>/<platform>/ automatically.
# The driver is installed there at Docker build time.
# SNOWFLAKE_ADBC_DRIVER_PATH is an explicit fallback recognized by the extension.
if [ -z "$SNOWFLAKE_ADBC_DRIVER_PATH" ]; then
    # Resolve from venv if not already set (local dev)
    _ADBC_SO="$("$PYTHON_BIN" -c "import adbc_driver_snowflake, os; print(os.path.join(os.path.dirname(adbc_driver_snowflake.__file__), 'libadbc_driver_snowflake.so'))" 2>/dev/null || true)"
    if [ -f "$_ADBC_SO" ]; then
        export SNOWFLAKE_ADBC_DRIVER_PATH="$_ADBC_SO"
        echo "  ADBC driver: $SNOWFLAKE_ADBC_DRIVER_PATH"
    fi
else
    echo "  ADBC driver: $SNOWFLAKE_ADBC_DRIVER_PATH"
fi

echo "Starting Rosetta Worker..."
echo "  Concurrency: $CONCURRENCY"
echo "  Queues: $QUEUES"
echo "  Log Level: $LOGLEVEL"
echo "  Python: $PYTHON_BIN"

# Start health API server in background
echo "Starting health API server on port ${SERVER_PORT:-8002}..."
"$PYTHON_BIN" server.py &
HEALTH_PID=$!
echo "  Health API PID: $HEALTH_PID"

# Trap to clean up health server on exit
trap "echo 'Stopping health API server...'; kill $HEALTH_PID 2>/dev/null" EXIT INT TERM

if [ "$RUN_BEAT" = true ]; then
    echo "  Beat: enabled"
    exec "$PYTHON_BIN" -m celery -A main worker \
        --loglevel=$LOGLEVEL \
        -Q $QUEUES \
        -c $CONCURRENCY \
        --pool=threads \
        --beat
else
    exec "$PYTHON_BIN" -m celery -A main worker \
        --loglevel=$LOGLEVEL \
        -Q $QUEUES \
        -c $CONCURRENCY \
        --pool=threads
fi
