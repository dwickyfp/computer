# How to Run Rosetta Worker

## Setup

```bash
# Install dependencies
uv sync

# Copy environment file
cp .env.example .env
# Edit .env with your CREDENTIAL_ENCRYPTION_KEY (must match backend)
```

**If you get Pydantic version errors:**

Windows:

```powershell
# Close all Python terminals/processes first, then:
Remove-Item -Recurse -Force .venv
uv sync
```

Linux/Mac:

```bash
rm -rf .venv
uv sync
```

## Run with Scripts

**Linux/Mac:**

```bash
./start.sh
```

**Windows (PowerShell):**

```powershell
.\start.ps1
```

## Run with uv

```bash
# Start health server in background
uv run python server.py &

# Start Celery worker
uv run celery -A main worker --loglevel=info -Q preview,default -c 4 --pool=threads

# With Beat scheduler
uv run celery -A main worker --loglevel=info -Q preview,default -c 4 --pool=threads --beat
```

## Run Directly (without uv)

```bash
# Start health server in background
python server.py &

# Start Celery worker
celery -A main worker --loglevel=info -Q preview,default -c 4 --pool=threads
```
