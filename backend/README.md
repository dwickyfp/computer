# Rosetta ETL Platform - Backend API

A production-ready FastAPI application for managing ETL pipeline configurations with PostgreSQL WAL monitoring capabilities.

## Architecture

This application follows **Clean Architecture** principles with clear separation of concerns:

```
backend/
├── app/
│   ├── api/              # Interface Layer (HTTP endpoints)
│   ├── core/             # Core Layer (config, database, shared utilities)
│   ├── domain/           # Domain Layer (business logic)
│   │   ├── models/       # SQLAlchemy ORM models
│   │   ├── schemas/      # Pydantic validation schemas
│   │   ├── repositories/ # Data access layer
│   │   └── services/     # Business logic services
│   └── infrastructure/   # Infrastructure Layer (external services, tasks)
```

## Key Features

- **Clean Architecture**: Strict separation between business logic and frameworks
- **Async-First**: Full async/await support for high-performance I/O operations
- **Connection Pooling**: Advanced PostgreSQL connection pool management with safeguards
- **Type Safety**: Comprehensive Pydantic models for request/response validation
- **Background Monitoring**: Automated PostgreSQL WAL size tracking
- **Error Handling**: Custom exception handlers for graceful error management
- **DDD Patterns**: Repository and Service patterns for maintainability

## Setup

1. **Install uv**

Follow the [official installation guide](https://docs.astral.sh/uv/getting-started/installation/) or:

```bash
# Windows (PowerShell)
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"

# macOS/Linux
curl -LsSf https://astral.sh/uv/install.sh | sh
```

2. **Install Dependencies**

```bash
uv sync
```

3. **Configure Environment**

```bash
cp .env.example .env
# Edit .env with your configuration
```

4. **Run Database Migrations**

```bash
uv run alembic upgrade head
```

5. **Start the Application**

```bash
uv run uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

## API Endpoints

### Sources

- `POST /api/v1/sources` - Create a new data source
- `GET /api/v1/sources` - List all sources
- `GET /api/v1/sources/{id}` - Get source by ID
- `PUT /api/v1/sources/{id}` - Update source
- `DELETE /api/v1/sources/{id}` - Delete source

### Destinations

- `POST /api/v1/destinations` - Create a new destination
- `GET /api/v1/destinations` - List all destinations
- `GET /api/v1/destinations/{id}` - Get destination by ID
- `PUT /api/v1/destinations/{id}` - Update destination
- `DELETE /api/v1/destinations/{id}` - Delete destination

### Pipelines

- `POST /api/v1/pipelines` - Create a new pipeline
- `GET /api/v1/pipelines` - List all pipelines
- `GET /api/v1/pipelines/{id}` - Get pipeline by ID
- `PUT /api/v1/pipelines/{id}` - Update pipeline
- `DELETE /api/v1/pipelines/{id}` - Delete pipeline
- `POST /api/v1/pipelines/{id}/start` - Start pipeline
- `POST /api/v1/pipelines/{id}/pause` - Pause pipeline

### Monitoring

- `GET /api/v1/metrics/wal` - Get WAL metrics history
- `GET /api/v1/health` - Health check endpoint

## Database Schema

The application uses a PostgreSQL database with the following main tables:

- `sources` - Data source configurations
- `destinations` - Data destination configurations
- `pipelines` - Pipeline mappings
- `pipeline_metadata` - Runtime pipeline information
- `wal_metrics` - Historical WAL size data
- `system_metrics` - System resource usage

## Configuration

Key configuration options in `.env`:

- **Database Pool**: `DB_POOL_SIZE`, `DB_MAX_OVERFLOW`, `DB_POOL_TIMEOUT`
- **WAL Monitoring**: `WAL_MONITOR_INTERVAL_SECONDS`, `WAL_MONITOR_TIMEOUT_SECONDS`
- **API**: `API_V1_PREFIX`, `HOST`, `PORT`

## Development

### Code Quality Tools

```bash
# Format code
black app/
isort app/

# Lint
flake8 app/

# Type checking
mypy app/

# Run tests
pytest tests/ -v --cov=app
```

## Production Considerations

1. **Connection Pool Tuning**: Adjust `DB_POOL_SIZE` based on expected concurrent connections
2. **WAL Monitor Interval**: Set `WAL_MONITOR_INTERVAL_SECONDS` based on monitoring needs
3. **Error Handling**: Review and customize exception handlers in `core/exceptions.py`
4. **Logging**: Configure structured logging in production environments
5. **Security**: Update `SECRET_KEY` and enable HTTPS in production

## License

Proprietary - All rights reserved
