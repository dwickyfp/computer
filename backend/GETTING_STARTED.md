# Rosetta ETL Platform - Getting Started

## Quick Start Guide

### 1. Environment Setup

Create a `.env` file based on `.env.example`:

```bash
cp .env.example .env
```

Update the following critical settings:

- `DATABASE_URL`: PostgreSQL connection string for application metadata
- `SECRET_KEY`: Generate a secure key for production
- `WAL_MONITOR_INTERVAL_SECONDS`: WAL check interval (default: 300 seconds)

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Database Setup

The application requires a PostgreSQL database for storing metadata. Use the existing SQL migrations:

```bash
# Connect to your PostgreSQL instance
psql -U postgres -d rosetta_metadata

# Run migrations from the migrations/ directory
\i ../migrations/001_create_table.sql
```

Alternatively, use Alembic for future migrations:

```bash
alembic upgrade head
```

### 4. Run the Application

```bash
# Development mode with auto-reload
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000

# Or use Python directly
python -m app.main
```

### 5. Access the API

- **API Documentation**: http://localhost:8000/docs
- **Alternative Docs**: http://localhost:8000/redoc
- **Health Check**: http://localhost:8000/health

## Architecture Overview

### Clean Architecture Layers

```
┌─────────────────────────────────────────┐
│          Presentation Layer             │
│    (API Endpoints, Request/Response)    │
│         app/api/v1/endpoints/           │
└─────────────────────────────────────────┘
                   ↓
┌─────────────────────────────────────────┐
│           Application Layer             │
│       (Use Cases, Services)             │
│         app/domain/services/            │
└─────────────────────────────────────────┘
                   ↓
┌─────────────────────────────────────────┐
│            Domain Layer                 │
│   (Entities, Business Logic)            │
│    app/domain/models/ & schemas/        │
└─────────────────────────────────────────┘
                   ↓
┌─────────────────────────────────────────┐
│         Infrastructure Layer            │
│  (Database, External Services, Tasks)   │
│   app/infrastructure/ & repositories/   │
└─────────────────────────────────────────┘
```

### Key Components

1. **Core Layer** (`app/core/`)

   - Configuration management
   - Database connection pooling
   - Exception handling
   - Logging

2. **Domain Layer** (`app/domain/`)

   - **Models**: SQLAlchemy ORM models
   - **Schemas**: Pydantic validation schemas
   - **Repositories**: Data access layer (Repository pattern)
   - **Services**: Business logic layer

3. **API Layer** (`app/api/`)

   - REST endpoints
   - Dependency injection
   - Request/response handling

4. **Infrastructure Layer** (`app/infrastructure/`)
   - Background task scheduler
   - WAL monitoring service

## API Usage Examples

### Create a Source

```bash
curl -X POST "http://localhost:8000/api/v1/sources" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "production-postgres",
    "pg_host": "postgres.example.com",
    "pg_port": 5432,
    "pg_database": "myapp_production",
    "pg_username": "replication_user",
    "pg_password": "SecurePassword123!",
    "publication_name": "dbz_publication",
    "replication_id": 1
  }'
```

### Create a Destination

```bash
curl -X POST "http://localhost:8000/api/v1/destinations" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "snowflake-production",
    "snowflake_account": "xy12345.us-east-1",
    "snowflake_user": "ETL_USER",
    "snowflake_database": "ANALYTICS",
    "snowflake_schema": "RAW_DATA",
    "snowflake_role": "SYSADMIN",
    "snowflake_private_key_path": "user/snowflake_key.p8",
    "snowflake_host": "xy12345.snowflakecomputing.com"
  }'
```

### Create a Pipeline

```bash
curl -X POST "http://localhost:8000/api/v1/pipelines" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "production-to-snowflake",
    "source_id": 1,
    "destination_id": 1,
    "status": "START"
  }'
```

### Get WAL Metrics

```bash
curl "http://localhost:8000/api/v1/metrics/wal?source_id=1&limit=10"
```

## Background Tasks

### WAL Monitoring

The application automatically monitors WAL size for all registered PostgreSQL sources:

- **Interval**: Configurable via `WAL_MONITOR_INTERVAL_SECONDS` (default: 5 minutes)
- **Timeout**: Configurable via `WAL_MONITOR_TIMEOUT_SECONDS` (default: 30 seconds)
- **Retries**: Configurable via `WAL_MONITOR_MAX_RETRIES` (default: 3)

The monitoring service:

1. Queries `pg_current_wal_lsn()` on each source
2. Calculates WAL size in bytes
3. Persists metrics to `wal_metrics` table
4. Implements exponential backoff on failures

## Database Connection Pooling

The application uses SQLAlchemy's async connection pool with the following safeguards:

- **Pool Size**: `DB_POOL_SIZE` (default: 20)
- **Max Overflow**: `DB_MAX_OVERFLOW` (default: 10)
- **Pool Timeout**: `DB_POOL_TIMEOUT` (default: 30 seconds)
- **Pool Recycle**: `DB_POOL_RECYCLE` (default: 1 hour)
- **Pre-Ping**: Enabled by default to test connections before use
- **LIFO**: Enabled for better connection reuse

## Error Handling

All errors follow a consistent format:

```json
{
  "error": "EntityNotFoundError",
  "message": "Source with id '123' not found",
  "details": {
    "entity_type": "Source",
    "entity_id": "123"
  },
  "timestamp": "2024-01-14T00:00:00Z"
}
```

## Production Deployment

### Environment Variables

Critical settings for production:

```bash
APP_ENV=production
DEBUG=False
SECRET_KEY=<generate-secure-key>
DATABASE_URL=postgresql+asyncpg://user:pass@host:5432/db
DB_POOL_SIZE=50
WAL_MONITOR_INTERVAL_SECONDS=300
LOG_LEVEL=INFO
```

### Running with Gunicorn

```bash
gunicorn app.main:app \
  --workers 4 \
  --worker-class uvicorn.workers.UvicornWorker \
  --bind 0.0.0.0:8000 \
  --access-logfile - \
  --error-logfile -
```

### Docker Deployment

```dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ ./app/
COPY alembic/ ./alembic/
COPY alembic.ini .

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

## Monitoring

- **Health Endpoint**: `/health` - Used by load balancers
- **Metrics**: Integration with Prometheus can be added
- **Logging**: Structured JSON logging for production observability

## Development

### Code Quality

```bash
# Format code
black app/
isort app/

# Lint
flake8 app/ --max-line-length=88

# Type checking
mypy app/

# Run tests
pytest tests/ -v --cov=app
```

### Database Migrations

```bash
# Create new migration
alembic revision --autogenerate -m "Description"

# Apply migrations
alembic upgrade head

# Rollback
alembic downgrade -1
```

## Troubleshooting

### Database Connection Issues

Check connection pool status in logs:

```python
from app.core.database import db_manager
status = await db_manager.get_pool_status()
```

### WAL Monitor Not Running

1. Check `WAL_MONITOR_ENABLED=True` in `.env`
2. Verify source database allows replication connections
3. Check logs for error messages

### High Memory Usage

Adjust connection pool settings:

- Reduce `DB_POOL_SIZE`
- Reduce `DB_MAX_OVERFLOW`
- Increase `DB_POOL_RECYCLE`

## Support

For issues or questions:

1. Check application logs in `logs/app.log`
2. Review API documentation at `/docs`
3. Check database connection pool status
