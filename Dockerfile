# =============================================================================
# ROSETTA MULTI-STAGE DOCKERFILE
# =============================================================================
# This Dockerfile builds 4 applications:
# 1. Python compute-node (CDC pipeline manager)
# 2. FastAPI backend
# 3. React frontend
# 4. Celery worker (async task processor)
#
# Final images:
# - compute-node: Python CDC engine (mode=worker)
# - web: FastAPI + React served via Nginx (mode=web)
# - worker: Celery worker for async tasks (mode=worker)
# =============================================================================

# =============================================================================
# STAGE 1: COMPUTE DEPENDENCIES (PYTHON)
# =============================================================================
FROM python:3.12-slim-bookworm AS compute-deps

WORKDIR /app

# Install system dependencies
# git: for removing git+ dependencies if needed, or installing from git
# gcc, libpq-dev: for psycopg2
# default-jre-headless: for JPype1 (Debezium)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    git \
    default-jre-headless \
    && rm -rf /var/lib/apt/lists/*

# Copy uv binary
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Copy pyproject.toml
COPY compute/pyproject.toml ./

# Install dependencies using uv (fresh resolution, no lock file)
RUN uv sync --no-install-project


# =============================================================================
# STAGE 2: FRONTEND BUILDER
# =============================================================================
FROM node:22-alpine AS frontend-builder

WORKDIR /app

# Install build dependencies needed for native modules
RUN apk add --no-cache python3 make g++

# Install pnpm
RUN corepack enable && corepack prepare pnpm@latest --activate

# Copy package files first for dependency caching
COPY web/package.json ./

# Install dependencies (without lock file, will resolve fresh)
RUN pnpm install --no-frozen-lockfile

# Copy source code
COPY web/ ./

# Build the frontend
# Note: VITE_API_URL is left empty so client.ts uses window.location.origin dynamically
ARG VITE_CLERK_PUBLISHABLE_KEY=""
ENV VITE_CLERK_PUBLISHABLE_KEY=${VITE_CLERK_PUBLISHABLE_KEY}

RUN pnpm build

# =============================================================================
# STAGE 3: BACKEND DEPENDENCIES
# =============================================================================
FROM python:3.12-slim-bookworm AS backend-deps

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy pyproject.toml
COPY backend/pyproject.toml ./

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Install dependencies (fresh resolution, no lock file)
RUN uv sync --no-install-project

# =============================================================================
# STAGE 3.5: WORKER DEPENDENCIES
# =============================================================================
FROM python:3.12-slim-bookworm AS worker-deps

WORKDIR /app

# Install system dependencies for worker (Celery + DuckDB + ADBC Snowflake driver)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    curl \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Copy pyproject.toml
COPY worker/pyproject.toml ./

# Install dependencies (fresh resolution, no lock file)
RUN uv sync --no-install-project

# Install ADBC Snowflake driver into DuckDB's expected extensions directory.
# DuckDB looks for the driver at: ~/.duckdb/extensions/<version>/<platform>/libadbc_driver_snowflake.so
# ref: https://github.com/iqea-ai/duckdb-snowflake#adbc-driver-setup
RUN DUCKDB_VERSION=$(.venv/bin/python -c "import duckdb; print('v' + duckdb.__version__)") && \
    ARCH=$(uname -m) && \
    case "$ARCH" in \
        x86_64)  PLATFORM="linux_amd64" ;; \
        aarch64) PLATFORM="linux_arm64" ;; \
        *)       PLATFORM="linux_amd64" ;; \
    esac && \
    DRIVER_SRC=$(.venv/bin/python -c "import adbc_driver_snowflake, os; print(os.path.join(os.path.dirname(adbc_driver_snowflake.__file__), 'libadbc_driver_snowflake.so'))") && \
    DRIVER_DIR="/root/.duckdb/extensions/${DUCKDB_VERSION}/${PLATFORM}" && \
    mkdir -p "$DRIVER_DIR" && \
    cp "$DRIVER_SRC" "$DRIVER_DIR/" && \
    chmod +x "$DRIVER_DIR/libadbc_driver_snowflake.so" && \
    echo "Installed ADBC driver to $DRIVER_DIR"

# =============================================================================
# STAGE 4: COMPUTE-NODE (PYTHON RUNTIME)
# =============================================================================
FROM python:3.12-slim-bookworm AS compute-node

LABEL maintainer="Rosetta Team"
LABEL description="Rosetta Compute Node - Python Pipeline Manager"

# Install runtime dependencies
# libpq5: for psycopg2
# default-jre-headless: for JPype1 (Debezium)
# curl: for healthcheck
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    default-jre-headless \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy virtual environment from compute-deps
COPY --from=compute-deps /app/.venv /app/.venv

# Enable virtual environment
ENV PATH="/app/.venv/bin:$PATH"

# Copy compute source code
COPY compute/ ./compute/

# Copy migrations
COPY migrations ./migrations

# Set environment variables
ENV MODE=worker
ENV PYTHONPATH=/app/compute
ENV TZ=Asia/Jakarta
ENV SNOWFLAKE_HOME=/tmp/.snowflake

# Create directories with proper permissions
RUN mkdir -p /app/tmp/offsets && \
    useradd -m -u 1000 rosetta && \
    chown -R rosetta:rosetta /app

USER rosetta

# Run the application
CMD ["python", "-m", "compute.main"]

# Port 8001: serves the combined FastAPI server for:
#   - Health/pool endpoints  (/health, /health/pool)
#   - Rosetta Chain ingest   (/chain/ingest, /chain/schema, /chain/tables,
#                             /chain/databases, /chain/health)
# Remote Rosetta instances POST CDC batches to /chain/ingest on this port.
# Controlled by SERVER_PORT env var (default 8001).
EXPOSE 8001

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8001/health || exit 1

# =============================================================================
# STAGE 5: WEB (BACKEND + FRONTEND)
# =============================================================================
FROM python:3.12-slim-bookworm AS web

LABEL maintainer="Rosetta Team"
LABEL description="Rosetta Web - FastAPI Backend + React Frontend"

# Install runtime dependencies and nginx
RUN apt-get update && apt-get install -y --no-install-recommends \
    nginx \
    supervisor \
    libpq5 \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy virtual environment
COPY --from=backend-deps /app/.venv /app/.venv

# Enable virtual environment
ENV PATH="/app/.venv/bin:$PATH"

# Copy backend source code
COPY backend/ ./backend/

# Copy built frontend from frontend-builder
COPY --from=frontend-builder /app/dist /var/www/html

# Copy nginx configuration
COPY docker/nginx.conf /etc/nginx/nginx.conf

# Copy supervisor configuration
COPY docker/supervisord.conf /etc/supervisor/conf.d/supervisord.conf

# Copy entrypoint script
COPY docker/entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

# Set environment variables
ENV MODE=web
ENV HOST=0.0.0.0
ENV PORT=8000
ENV TZ=Asia/Jakarta
ENV PYTHONPATH=/app/backend
ENV SNOWFLAKE_HOME=/tmp/.snowflake

# Expose ports
EXPOSE 80 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Run entrypoint
CMD ["/app/entrypoint.sh"]

# =============================================================================
# STAGE 6: WORKER (CELERY)
# =============================================================================
FROM python:3.12-slim-bookworm AS worker

LABEL maintainer="Rosetta Team"
LABEL description="Rosetta Worker - Celery async task processor"

# Install runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy virtual environment from worker-deps
COPY --from=worker-deps /app/.venv /app/.venv

# Enable virtual environment
ENV PATH="/app/.venv/bin:$PATH"

# Copy worker source code
COPY worker/ ./worker/

# Make start.sh executable
RUN chmod +x ./worker/start.sh

# Set environment variables
ENV MODE=worker
ENV PYTHONPATH=/app/worker
ENV TZ=Asia/Jakarta
ENV C_FORCE_ROOT=true
# Set SNOWFLAKE_ADBC_DRIVER_PATH as an explicit fallback path for the driver
# DuckDB also auto-finds it from ~/.duckdb/extensions/<version>/<platform>/ (installed at build time in worker-deps)
ENV SNOWFLAKE_ADBC_DRIVER_PATH="/app/.venv/lib/python3.12/site-packages/adbc_driver_snowflake/libadbc_driver_snowflake.so"

# Copy DuckDB extensions (including installed ADBC driver) from worker-deps
COPY --from=worker-deps /root/.duckdb /root/.duckdb

# Create directories with proper permissions
RUN useradd -m -u 1001 celeryworker && \
    chown -R celeryworker:celeryworker /app && \
    cp -r /root/.duckdb /home/celeryworker/.duckdb && \
    chown -R celeryworker:celeryworker /home/celeryworker/.duckdb

USER celeryworker

# Start both health server and Celery worker via start.sh
# start.sh also dynamically appends ADBC lib path to LD_LIBRARY_PATH as a safety net
CMD ["./worker/start.sh"]

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:8002/health || exit 1

# =============================================================================
# STAGE 7: ROSETTA — UNIFIED IMAGE
# =============================================================================
# Single image that runs any service based on the MODE environment variable:
#   MODE=compute  →  CDC Pipeline Engine          (port 8001)
#   MODE=web      →  FastAPI + React via Nginx     (ports 80 / 8000)
#   MODE=worker   →  Celery Task Processor         (port 8002)
#
# Each service uses its own isolated virtual environment:
#   /app/.venv-compute   ← compute deps  (pydbzengine, psycopg2, httpx, duckdb …)
#   /app/.venv-backend   ← backend deps  (fastapi, sqlalchemy, alembic …)
#   /app/.venv-worker    ← worker deps   (celery, duckdb, pyarrow, adbc …)
# =============================================================================
FROM python:3.12-slim-bookworm AS rosetta

LABEL maintainer="Rosetta Team"
LABEL description="Rosetta ETL Platform - Unified Image (compute / web / worker)"

# Install all runtime system dependencies needed by any mode
RUN apt-get update && apt-get install -y --no-install-recommends \
    nginx \
    supervisor \
    libpq5 \
    default-jre-headless \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# ── Virtual environments (one per service, fully isolated) ──────────────────
COPY --from=compute-deps /app/.venv /app/.venv-compute
COPY --from=backend-deps /app/.venv /app/.venv-backend
COPY --from=worker-deps  /app/.venv /app/.venv-worker

# ── DuckDB ADBC extensions (worker mode) ────────────────────────────────────
COPY --from=worker-deps /root/.duckdb /root/.duckdb

# ── Application source code ─────────────────────────────────────────────────
COPY compute/   ./compute/
COPY backend/   ./backend/
COPY worker/    ./worker/
COPY migrations/ ./migrations/

# ── Frontend (web mode) ─────────────────────────────────────────────────────
COPY --from=frontend-builder /app/dist /var/www/html

# ── Nginx + Supervisor config (web mode) ────────────────────────────────────
COPY docker/nginx.conf        /etc/nginx/nginx.conf
COPY docker/supervisord.conf  /etc/supervisor/conf.d/supervisord.conf

# ── Unified entrypoint ───────────────────────────────────────────────────────
COPY docker/entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh /app/worker/start.sh

# ── Shared environment defaults ─────────────────────────────────────────────
ENV MODE=web
ENV TZ=Asia/Jakarta
ENV SNOWFLAKE_HOME=/tmp/.snowflake
ENV C_FORCE_ROOT=true
# Explicit fallback path for ADBC Snowflake driver (worker mode)
ENV SNOWFLAKE_ADBC_DRIVER_PATH="/app/.venv-worker/lib/python3.12/site-packages/adbc_driver_snowflake/libadbc_driver_snowflake.so"

# ── Runtime directories ──────────────────────────────────────────────────────
RUN mkdir -p /app/tmp/offsets /var/log/supervisor /var/log/nginx

# ── Ports ────────────────────────────────────────────────────────────────────
# 80    — Nginx (web mode, serves React frontend)
# 8000  — Uvicorn / FastAPI (web mode)
# 8001  — Compute health API + Chain ingest endpoints (compute mode)
# 8002  — Celery worker health API (worker mode)
EXPOSE 80 8000 8001 8002

CMD ["/app/entrypoint.sh"]
