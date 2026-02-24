# Run Backend

uv run uvicorn app.main:app --reload --host 0.0.0.0 --port 8000

# Run Compute

uv run main.py

# Run Compute with custom host/port (defaults: 0.0.0.0:8001)

SERVER_HOST=0.0.0.0 SERVER_PORT=8001 uv run main.py

# Run Compute on Windows (PowerShell)

$env:SERVER_HOST="0.0.0.0"; $env:SERVER_PORT="8001"; uv run main.py

# Other useful compute env vars:

# ROSETTA_DB_HOST, ROSETTA_DB_PORT, ROSETTA_DB_NAME, ROSETTA_DB_USER, ROSETTA_DB_PASSWORD

# REDIS_URL (default: redis://localhost:6379/0)

# CHAIN_ENABLED (default: false) — enable Rosetta Chain Arrow IPC ingestion

# CHAIN_AUTH_ENABLED (default: true) — validate X-Chain-Key header on chain endpoints

# LOG_LEVEL (default: INFO)

# Run Web

pnpm dev

# Start Worker (Linux/Mac)

./start.sh

# Start Worker Manual

# Start health server in background

uv run python server.py &

# Start Celery worker

uv run celery -A main worker --loglevel=info -Q preview,default -c 4 --pool=threads

# Kill Python Process

taskkill /IM python.exe /F
