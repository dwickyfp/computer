# Rosetta ETL Platform - Architecture Documentation

## Executive Summary

The Rosetta ETL Platform is a production-ready FastAPI application designed for managing ETL pipeline configurations with real-time PostgreSQL Write-Ahead Log (WAL) monitoring. Built using **Clean Architecture** principles and **Domain-Driven Design (DDD)** patterns, the system ensures high modularity, testability, and maintainability.

## Architecture Principles

### 1. Clean Architecture

The application strictly follows the **Dependency Rule**: dependencies point inward, with business logic isolated from frameworks and external concerns.

```
┌──────────────────────────────────────────────────────────┐
│                    PRESENTATION LAYER                     │
│  FastAPI Routes, HTTP Request/Response, OpenAPI Docs     │
│                    app/api/v1/endpoints/                  │
└────────────────────┬─────────────────────────────────────┘
                     ↓
┌──────────────────────────────────────────────────────────┐
│                   APPLICATION LAYER                       │
│    Business Logic, Use Cases, Orchestration              │
│              app/domain/services/                         │
└────────────────────┬─────────────────────────────────────┘
                     ↓
┌──────────────────────────────────────────────────────────┐
│                     DOMAIN LAYER                          │
│  Entities, Value Objects, Domain Logic (Framework-Free)  │
│         app/domain/models/ & schemas/                     │
└────────────────────┬─────────────────────────────────────┘
                     ↓
┌──────────────────────────────────────────────────────────┐
│                 INFRASTRUCTURE LAYER                      │
│  Database, External APIs, Background Tasks, Persistence  │
│      app/infrastructure/ & app/domain/repositories/       │
└──────────────────────────────────────────────────────────┘
```

### 2. Domain-Driven Design (DDD)

- **Entities**: `Source`, `Destination`, `Pipeline`, `PipelineMetadata`, `WALMetric`
- **Repositories**: Data access abstraction (Repository Pattern)
- **Services**: Business logic coordination
- **Value Objects**: Pydantic schemas for validation

### 3. SOLID Principles

- **Single Responsibility**: Each class has one reason to change
- **Open/Closed**: Open for extension, closed for modification
- **Liskov Substitution**: Base repository can be substituted
- **Interface Segregation**: Focused interfaces via dependency injection
- **Dependency Inversion**: Depend on abstractions, not concretions

## Project Structure

```
backend/
├── app/
│   ├── __init__.py
│   ├── main.py                      # Application entry point
│   │
│   ├── api/                         # PRESENTATION LAYER
│   │   ├── __init__.py
│   │   ├── deps.py                  # Dependency injection
│   │   └── v1/
│   │       ├── __init__.py
│   │       └── endpoints/
│   │           ├── sources.py       # Source CRUD endpoints
│   │           ├── destinations.py  # Destination CRUD endpoints
│   │           ├── pipelines.py     # Pipeline CRUD endpoints
│   │           ├── wal_metrics.py   # WAL metrics query endpoints
│   │           └── health.py        # Health check endpoint
│   │
│   ├── core/                        # CROSS-CUTTING CONCERNS
│   │   ├── __init__.py
│   │   ├── config.py                # Pydantic settings management
│   │   ├── database.py              # Connection pool management
│   │   ├── exceptions.py            # Custom exception hierarchy
│   │   └── logging.py               # Structured logging
│   │
│   ├── domain/                      # DOMAIN LAYER
│   │   ├── __init__.py
│   │   ├── models/                  # SQLAlchemy ORM models
│   │   │   ├── base.py              # Base model with common functionality
│   │   │   ├── source.py            # Source entity
│   │   │   ├── destination.py       # Destination entity
│   │   │   ├── pipeline.py          # Pipeline & PipelineMetadata entities
│   │   │   └── wal_metric.py        # WALMetric entity
│   │   │
│   │   ├── schemas/                 # Pydantic validation schemas
│   │   │   ├── common.py            # Shared schemas
│   │   │   ├── source.py            # Source schemas (Create, Update, Response)
│   │   │   ├── destination.py       # Destination schemas
│   │   │   ├── pipeline.py          # Pipeline schemas
│   │   │   └── wal_metric.py        # WAL metric schemas
│   │   │
│   │   ├── repositories/            # DATA ACCESS LAYER (Repository Pattern)
│   │   │   ├── base.py              # Generic CRUD repository
│   │   │   ├── source.py            # Source repository
│   │   │   ├── destination.py       # Destination repository
│   │   │   ├── pipeline.py          # Pipeline repository
│   │   │   └── wal_metric.py        # WAL metric repository
│   │   │
│   │   └── services/                # APPLICATION LAYER (Business Logic)
│   │       ├── source.py            # Source service
│   │       ├── destination.py       # Destination service
│   │       ├── pipeline.py          # Pipeline service
│   │       └── wal_monitor.py       # WAL monitoring service
│   │
│   └── infrastructure/              # INFRASTRUCTURE LAYER
│       └── tasks/
│           └── scheduler.py         # Background task scheduler
│
├── alembic/                         # Database migrations
│   ├── env.py
│   └── versions/
│
├── .env.example                     # Environment configuration template
├── requirements.txt                 # Python dependencies
├── alembic.ini                      # Alembic configuration
├── README.md                        # Project overview
└── GETTING_STARTED.md               # Quick start guide
```

## Core Components

### 1. Configuration Management (`app/core/config.py`)

**Technology**: Pydantic Settings v2

**Features**:

- Type-safe configuration with validation
- Environment variable loading
- Secure default values
- Connection pool tuning parameters

**Key Settings**:

```python
- DATABASE_URL: Async PostgreSQL connection string
- DB_POOL_SIZE: Connection pool size (default: 20)
- DB_MAX_OVERFLOW: Max overflow connections (default: 10)
- DB_POOL_TIMEOUT: Connection wait timeout (default: 30s)
- WAL_MONITOR_INTERVAL_SECONDS: WAL check interval (default: 300s)
```

### 2. Database Layer (`app/core/database.py`)

**Technology**: SQLAlchemy 2.0 + asyncpg

**Features**:

- **Async Connection Pooling**: QueuePool with LIFO strategy
- **Pool Health Monitoring**: Pre-ping and pool status tracking
- **Safeguards**: Timeout handling, overflow limits
- **Automatic Retries**: Connection failure recovery

**Connection Pool Architecture**:

```
┌─────────────────────────────────────────┐
│       Application (Multiple Workers)     │
└───────────────┬─────────────────────────┘
                ↓
┌─────────────────────────────────────────┐
│      SQLAlchemy Connection Pool         │
│  ┌─────┬─────┬─────┬─────┬─────┐       │
│  │ C1  │ C2  │ C3  │ ... │ C20 │       │  Pool Size
│  └─────┴─────┴─────┴─────┴─────┘       │
│  ┌─────┬─────┬─────┐                   │
│  │ O1  │ O2  │ ... │ Max Overflow: 10  │  Overflow
│  └─────┴─────┴─────┘                   │
└───────────────┬─────────────────────────┘
                ↓
┌─────────────────────────────────────────┐
│     PostgreSQL Database (asyncpg)       │
└─────────────────────────────────────────┘
```

**Pool Safeguards**:

1. **Pre-Ping**: Tests connections before use
2. **Pool Recycle**: Refreshes connections every hour
3. **Timeout**: 30-second wait for available connection
4. **LIFO**: Reuses recent connections for better cache hits

### 3. Exception Handling (`app/core/exceptions.py`)

**Custom Exception Hierarchy**:

```
RosettaException (Base)
├── DatabaseError
│   └── DatabaseConnectionError
├── EntityNotFoundError
├── ValidationError
├── DuplicateEntityError
├── PipelineOperationError
├── WALMonitorError
├── ConfigurationError
├── AuthenticationError
├── AuthorizationError
└── ExternalServiceError
```

**Benefits**:

- Consistent error responses
- Proper HTTP status code mapping
- Detailed error context
- Production-safe error messages

### 4. Repository Pattern (`app/domain/repositories/`)

**Base Repository** provides generic CRUD operations:

- `create()`: Create new entity
- `get_by_id()`: Retrieve by ID
- `get_by_name()`: Retrieve by name
- `get_all()`: List with pagination
- `update()`: Update entity
- `delete()`: Delete entity
- `exists()`: Check existence
- `count()`: Total count

**Specialized Repositories** extend base with domain-specific queries:

- `SourceRepository`: WAL metrics queries
- `PipelineRepository`: Relations loading, status filtering
- `WALMetricRepository`: Time-range queries, latest metrics

### 5. Service Layer (`app/domain/services/`)

**Responsibilities**:

- Business logic coordination
- Transaction management
- Validation orchestration
- Cross-repository operations

**Example**: `PipelineService.create_pipeline()`

1. Validates source exists
2. Validates destination exists
3. Creates pipeline
4. Creates associated metadata
5. Loads all relations
6. Returns complete pipeline

### 6. WAL Monitoring (`app/domain/services/wal_monitor.py`)

**Architecture**:

```
┌──────────────────────────────────────────┐
│      Background Scheduler (APScheduler)  │
│   Triggers every N seconds              │
└───────────────┬──────────────────────────┘
                ↓
┌──────────────────────────────────────────┐
│        WALMonitorService                 │
│  - Fetches all sources                   │
│  - Monitors each concurrently            │
│  - Implements retry logic                │
└───────────────┬──────────────────────────┘
                ↓
┌──────────────────────────────────────────┐
│   Per-Source Monitoring (Parallel)       │
│  1. Connect to source database           │
│  2. Query pg_current_wal_lsn()           │
│  3. Calculate size difference            │
│  4. Persist to wal_metrics table         │
│  5. Retry on failure (exponential)       │
└──────────────────────────────────────────┘
```

**WAL Size Query**:

```sql
SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')::bigint AS wal_size_bytes;
```

**Retry Strategy**:

- Max retries: 3 (configurable)
- Exponential backoff: 2^retry_count seconds
- Continues monitoring other sources on failure

### 7. API Layer (`app/api/v1/endpoints/`)

**RESTful Endpoints**:

**Sources** (`/api/v1/sources`):

- `POST /` - Create source
- `GET /` - List sources (paginated)
- `GET /{id}` - Get source by ID
- `PUT /{id}` - Update source
- `DELETE /{id}` - Delete source

**Destinations** (`/api/v1/destinations`):

- `POST /` - Create destination
- `GET /` - List destinations (paginated)
- `GET /{id}` - Get destination by ID
- `PUT /{id}` - Update destination
- `DELETE /{id}` - Delete destination

**Pipelines** (`/api/v1/pipelines`):

- `POST /` - Create pipeline
- `GET /` - List pipelines (paginated)
- `GET /{id}` - Get pipeline by ID
- `PUT /{id}` - Update pipeline
- `DELETE /{id}` - Delete pipeline
- `POST /{id}/start` - Start pipeline
- `POST /{id}/pause` - Pause pipeline
- `POST /{id}/refresh` - Refresh pipeline

**Metrics** (`/api/v1/metrics`):

- `GET /wal` - Query WAL metrics (with filters)

**Health** (`/api/v1/health`):

- `GET /` - Health check with dependency status

## Data Flow

### Example: Creating a Pipeline

```
1. HTTP Request
   ↓
2. FastAPI Route (pipelines.py)
   - Validates request with PipelineCreate schema
   ↓
3. Dependency Injection (deps.py)
   - Creates database session
   - Instantiates PipelineService
   ↓
4. PipelineService.create_pipeline()
   - Validates source exists (SourceRepository)
   - Validates destination exists (DestinationRepository)
   - Creates pipeline (PipelineRepository)
   - Creates metadata (PipelineMetadata)
   - Commits transaction
   ↓
5. Response
   - Converts to PipelineResponse schema
   - Returns JSON with 201 Created
```

### Example: WAL Monitoring Cycle

```
1. APScheduler Trigger (every 5 minutes)
   ↓
2. WALMonitorService.monitor_all_sources()
   - Fetches all sources from database
   ↓
3. Parallel Monitoring (asyncio.gather)
   - For each source:
     ├─ Connect to source database
     ├─ Execute WAL size query
     ├─ Calculate size in bytes
     ├─ Persist to wal_metrics table
     └─ Retry on failure (max 3 times)
   ↓
4. Metrics Available
   - Query via GET /api/v1/metrics/wal
```

## Database Schema

### Tables

**sources**:

- Connection details for PostgreSQL databases
- Fields: id, name, pg_host, pg_port, pg_database, pg_username, pg_password, publication_name, replication_id

**destinations**:

- Connection details for Snowflake warehouses
- Fields: id, name, snowflake_account, snowflake_user, snowflake_database, snowflake_schema, snowflake_role, snowflake_private_key_path, snowflake_host

**pipelines**:

- ETL pipeline configurations
- Fields: id, name, source_id, destination_id, status
- Status: START, PAUSE, REFRESH

**pipeline_metadata**:

- Runtime pipeline information
- Fields: id, pipeline_id, status, last_error, last_error_at, last_start_at
- Status: RUNNING, PAUSED, ERROR

**wal_metrics**:

- Historical WAL size data
- Fields: id, source_id, size_bytes, recorded_at

### Relationships

```
sources 1──────N pipelines
           │
           │
destinations 1──────N pipelines

pipelines 1──────1 pipeline_metadata

sources 1──────N wal_metrics
```

## Security Considerations

### Production Hardening

1. **Credential Management**:

   - Use secrets management (AWS Secrets Manager, HashiCorp Vault)
   - Encrypt passwords/passphrases before storage
   - Never log sensitive credentials

2. **API Security**:

   - Implement JWT authentication
   - Add rate limiting (SlowAPI)
   - Enable HTTPS/TLS
   - Restrict CORS origins

3. **Database Security**:

   - Use least-privilege database users
   - Enable SSL/TLS for connections
   - Implement connection pooling limits

4. **Monitoring**:
   - Log all access attempts
   - Monitor connection pool exhaustion
   - Alert on failed WAL monitoring

## Performance Optimization

### Connection Pool Tuning

```python
# Low traffic (< 100 req/min)
DB_POOL_SIZE=10
DB_MAX_OVERFLOW=5

# Medium traffic (100-1000 req/min)
DB_POOL_SIZE=20
DB_MAX_OVERFLOW=10

# High traffic (> 1000 req/min)
DB_POOL_SIZE=50
DB_MAX_OVERFLOW=20
```

### WAL Monitoring Optimization

- Adjust interval based on requirements
- Use connection pooling for source connections
- Implement metric aggregation for high-frequency monitoring

### Query Optimization

- Use `selectinload` for eager loading relations
- Implement pagination for all list endpoints
- Add database indexes on foreign keys (already included)

## Testing Strategy

### Unit Tests

- Test individual services
- Mock repository dependencies
- Validate business logic

### Integration Tests

- Test repository operations
- Validate database constraints
- Test transaction handling

### End-to-End Tests

- Test complete API flows
- Validate error handling
- Test background tasks

## Deployment

### Production Checklist

- [ ] Update `SECRET_KEY` to secure random value
- [ ] Set `APP_ENV=production` and `DEBUG=False`
- [ ] Configure production database
- [ ] Set appropriate connection pool sizes
- [ ] Enable HTTPS/TLS
- [ ] Configure logging aggregation
- [ ] Set up monitoring and alerting
- [ ] Implement backup strategy
- [ ] Configure WAL monitoring interval
- [ ] Test disaster recovery

### Scaling Considerations

**Horizontal Scaling**:

- Stateless application design
- Shared database for all instances
- Background tasks run on single instance (leader election)

**Vertical Scaling**:

- Increase connection pool size
- Add more CPU for concurrent requests
- Increase memory for larger datasets

## Maintenance

### Database Migrations

```bash
# Create migration
alembic revision --autogenerate -m "Add new field"

# Apply migrations
alembic upgrade head

# Rollback
alembic downgrade -1
```

### Log Rotation

Logs are automatically rotated:

- Max size: 10 MB per file
- Backup count: 5 files
- Location: `logs/app.log`

### Health Monitoring

Monitor these endpoints:

- `/health` - Overall application health
- `/api/v1/health` - Detailed component health

## Conclusion

The Rosetta ETL Platform demonstrates production-ready FastAPI development with:

- **Clean Architecture** for maintainability
- **Domain-Driven Design** for business alignment
- **Type Safety** with Pydantic
- **Async-First** for high performance
- **Robust Error Handling** for reliability
- **Background Monitoring** for observability

The modular structure ensures easy extension, comprehensive testing, and long-term maintainability.
