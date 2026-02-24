FROM python:3.12-slim-bookworm

WORKDIR /app

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    curl \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# Install uv for package management
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Copy project files
COPY pyproject.toml .
COPY . .

# Install dependencies (includes adbc-driver-snowflake for DuckDB Snowflake extension)
RUN uv sync --no-dev

# Enable virtual environment
ENV PATH="/app/.venv/bin:$PATH"

# Install ADBC Snowflake driver into DuckDB's expected extensions directory.
# DuckDB looks for the driver at: ~/.duckdb/extensions/<version>/<platform>/libadbc_driver_snowflake.so
# The adbc-driver-snowflake Python package bundles the .so - we copy it to the right place.
# ref: https://github.com/iqea-ai/duckdb-snowflake#adbc-driver-setup
RUN DUCKDB_VERSION=$(python -c "import duckdb; print('v' + duckdb.__version__)") && \
    ARCH=$(uname -m) && \
    case "$ARCH" in \
        x86_64)  PLATFORM="linux_amd64" ;; \
        aarch64) PLATFORM="linux_arm64" ;; \
        *)       PLATFORM="linux_amd64" ;; \
    esac && \
    DRIVER_SRC=$(python -c "import adbc_driver_snowflake, os; print(os.path.join(os.path.dirname(adbc_driver_snowflake.__file__), 'libadbc_driver_snowflake.so'))") && \
    DRIVER_DIR="/root/.duckdb/extensions/${DUCKDB_VERSION}/${PLATFORM}" && \
    mkdir -p "$DRIVER_DIR" && \
    cp "$DRIVER_SRC" "$DRIVER_DIR/" && \
    chmod +x "$DRIVER_DIR/libadbc_driver_snowflake.so" && \
    echo "Installed ADBC driver to $DRIVER_DIR"

# Set SNOWFLAKE_ADBC_DRIVER_PATH as an explicit fallback path for the driver
ENV SNOWFLAKE_ADBC_DRIVER_PATH="/app/.venv/lib/python3.12/site-packages/adbc_driver_snowflake/libadbc_driver_snowflake.so"

# Runtime environment
ENV PYTHONPATH=/app
ENV TZ=Asia/Jakarta
ENV C_FORCE_ROOT=true

# Ensure start.sh is executable
RUN chmod +x /app/start.sh

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:8002/health || exit 1

# Start health API + Celery worker via start.sh
# (threads pool avoids DuckDB fork crashes)
CMD ["./start.sh"]
