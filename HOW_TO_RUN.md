# Run Backend

uv run uvicorn app.main:app --reload --host 0.0.0.0 --port 8000

# Run Compute

uv run main.py

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
