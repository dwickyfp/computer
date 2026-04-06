# Rosetta ETL Platform

Rosetta is a production-ready, real-time ETL platform with a modular three-service architecture for managing and executing Change Data Capture (CDC) pipelines from **PostgreSQL** to multiple destinations (**Snowflake**, **PostgreSQL**, and other **Rosetta instances**).

## Why Rosetta?

- ✅ **Multi-Destination Fan-Out**: Replicate one source to many destinations with a single CDC stream
- ✅ **Rosetta Chain**: Stream data between Rosetta instances over Arrow IPC — no Kafka or middleware required
- ✅ **Process Isolation**: Crash-resistant architecture where pipeline failures don't affect others
- ✅ **Historical Backfill**: DuckDB-powered batch processing for retroactive data sync
- ✅ **Failure Recovery**: Redis-based DLQ ensures no data loss with automatic retry
- ✅ **Clean Architecture**: Backend follows DDD with clear separation of concerns
- ✅ **Modern Stack**: FastAPI, React 19, TanStack ecosystem, SQLAlchemy 2.0
- ✅ **Production-Ready**: Connection pooling, structured logging, health checks, metrics
- ✅ **Developer-Friendly**: Comprehensive API docs, type safety, hot reload

## Table of Contents

- [Why Rosetta?](#why-rosetta)
- [Architecture Overview](#architecture-overview)
- [Key Features](#key-features)
  - [Multi-Destination Support](#-multi-destination-support)
  - [Backfill Feature](#-backfill-feature)
  - [Dead Letter Queue (DLQ)](#-dead-letter-queue-dlq)
  - [Process Isolation](#-process-isolation)
  - [Advanced Security](#-advanced-security)
- [Quick Start (TL;DR)](#quick-start-tldr)
- [Project Flow](#project-flow)
- [System Architecture](#system-architecture)
- [How to Run](#how-to-run)
- [Testing](#testing)
- [Port Reference](#port-reference)
- [Documentation](#documentation)
- [Common Use Cases](#common-use-cases)
- [Performance Characteristics](#performance-characteristics)
- [Troubleshooting](#troubleshooting)
- [Frequently Asked Questions](#frequently-asked-questions)
- [System Requirements](#system-requirements)

## Architecture Overview

The platform consists of three independent services:

- **Backend** (FastAPI/Python): RESTful API for managing sources, destinations, and pipeline configurations with Clean Architecture and DDD patterns
- **Compute** (Python/Debezium): CDC execution engine with process isolation that replicates data changes in real-time to multiple destinations
- **Web** (React/TypeScript/Vite): Feature-based admin dashboard built with TanStack ecosystem for pipeline monitoring and management

## Key Features

### 🎯 Multi-Destination Support

- **One-to-Many Replication**: A single pipeline can replicate from one PostgreSQL source to multiple Snowflake or PostgreSQL destinations simultaneously
- **Independent Health Tracking**: Each destination has its own health status, metrics, and error reporting via `pipeline_metadata`
- **Flexible Configuration**: Add or remove destinations on the fly through the `pipeline_destinations` join table
- **Efficient Processing**: Single Debezium connector fans out to all destinations, reducing resource overhead

### 🔗 Rosetta Chain (Rosetta-to-Rosetta Streaming)

- **No Middleware Required**: Stream CDC data directly from one Rosetta instance to another over **Apache Arrow IPC** — no Kafka, no message broker
- **Secure Authentication**: Each instance exposes a chain key; remote senders must present it in the `X-Chain-Key` header
- **Redis-Buffered Ingestion**: Incoming Arrow IPC batches are written to Redis Streams, then consumed by a dedicated `ChainPipelineEngine`
- **Pipeline Integration**: ROSETTA is a first-class source type — create pipelines with `source_type=ROSETTA` and choose a chain client instead of a PostgreSQL source
- **ROSETTA Destination**: Add a ROSETTA-type destination to any pipeline to push CDC changes to a remote instance
- **Schema Sync**: Table schemas are automatically pushed to the receiving instance on first connection
- **Managed via UI**: Configure chain keys, register remote clients, test connectivity, and browse remote tables from the _Rosetta Chain_ page in the dashboard
- **Environment**: Enable with `CHAIN_ENABLED=true`; server host/port controlled by `SERVER_HOST`/`SERVER_PORT`

### 📦 Backfill Feature

- **Historical Data Sync**: Retroactively synchronize historical data using DuckDB for efficient batch processing
- **Flexible Filtering**: Support for up to 5 WHERE clause filters per job with operators like `=`, `!=`, `>`, `<`, `LIKE`, `IS NULL`, etc.
- **Job Lifecycle Management**: Track jobs through states: PENDING → EXECUTING → COMPLETED/FAILED/CANCELLED
- **Memory-Safe Batching**: Processes 10,000 rows per batch (configurable) to prevent memory issues
- **Graceful Cancellation**: Cancel jobs at any time with proper cleanup
- **Real-time Progress**: Monitor progress via API endpoints and Web UI

### 🔁 Dead Letter Queue (DLQ)

- **Redis Streams-Based**: Failed CDC records stored in Redis Streams for reliable recovery
- **Granular Organization**: Separate streams per table/destination: `dlq:{source_id}:{table}:{dest_id}`
- **At-Least-Once Delivery**: Consumer groups ensure no data loss during recovery
- **Configurable Retry**: Customizable retry strategies and maximum retry counts
- **Recovery Endpoints**: Manual or automatic recovery via REST API
- **Visibility**: Track failed records, error reasons, and recovery status through Web UI

### 🔒 Process Isolation

- **Multiprocessing Architecture**: Each pipeline runs in an isolated `multiprocessing.Process`
- **Crash Isolation**: One pipeline failure doesn't affect others
- **Independent Resources**: Separate connection pools and memory space per pipeline
- **Graceful Shutdown**: Proper cleanup and state preservation on termination

### 🔐 Advanced Security

- **RSA Key-Pair Authentication**: Encrypted private keys (PKCS#8) for Snowflake connections
- **Secure Storage**: Private keys encrypted with passphrase using cryptography library
- **No Plain Passwords**: All sensitive credentials encrypted at rest

## Quick Start (TL;DR)

```bash
# 1. Start infrastructure
docker-compose up -d

# 2. Setup backend
cd backend
uv sync
uv run alembic upgrade head
uv run uvicorn app.main:app --reload --host 0.0.0.0 --port 8000 &

# 3. Setup compute
cd ../compute
pip install -r requirements.txt
python main.py &

# 4. Setup web
cd ../web
pnpm install
pnpm dev

# 5. Access dashboard
open http://localhost:5173
```

Then create your first pipeline through the Web UI or API!

## Project Flow

Data flows through the system in real-time with support for multiple destinations:

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Rosetta ETL Platform                        │
└─────────────────────────────────────────────────────────────────────┘

┌──────────────┐         ┌──────────────┐         ┌──────────────┐
│   Web UI     │────────>│   Backend    │────────>│  Config DB   │
│ (React/Vite) │  REST   │  (FastAPI)   │  Write  │ (PostgreSQL) │
└──────────────┘         └──────────────┘         └──────┬───────┘
                                                          │
                                                    Polls │ (10s)
                                                          │
                         ┌────────────────────────────────┴─────┐
                         │      Compute Service (Python)        │
                         │  ┌───────────────────────────────┐   │
                         │  │  PipelineManager              │   │
                         │  │  • Detects START pipelines    │   │
                         │  │  • Spawns Process per pipeline│   │
                         │  └───────────┬───────────────────┘   │
                         │              │                        │
                         │  ┌───────────▼───────────────────┐   │
                         │  │  Pipeline Process             │   │
                         │  │  (multiprocessing.Process)    │   │
                         │  │  ┌─────────────────────────┐  │   │
                         │  │  │  Debezium Engine        │  │   │
                         │  │  │  • Reads WAL changes    │  │   │
┌──────────────┐         │  │  │  • CDC event streaming  │  │   │
│  Source DB   │─────WAL─┼──┼─>│  • Fan-out to all dests│  │   │
│ (PostgreSQL) │ Logical │  │  └──────────┬──────────────┘  │   │
│  Replication │  Slot → │  │             │                  │   │
└──────────────┘         │  │    Success  │  Failure         │   │
                         │  │             │  │               │   │
                         │  │             ▼  ▼               │   │
                         │  │    ┌────────────────┐          │   │
                         │  │    │ Success │ DLQ  │          │   │
                         │  │    │  Path   │(Redis)          │   │
                         │  │    └────┬────┴───┬──┘          │   │
                         │  └─────────┼────────┼─────────────┘   │
                         └────────────┼────────┼─────────────────┘
                                      │        │
                        ┌─────────────▼─┐   ┌─▼─────────────┐
                        │  Destination  │   │  Destination  │
                        │  (Snowflake)  │   │ (PostgreSQL)  │
                        └───────────────┘   └───────────────┘

┌────────────────────────────────────────────────────────────────────┐
│  Backfill Manager (Separate Thread)                               │
│  • Polls queue_backfill_data every 5s                             │
│  • DuckDB for batch processing (10K rows/batch)                   │
│  • Historical data sync with filters                              │
└────────────────────────────────────────────────────────────────────┘
```

1.  **Configuration (Backend API)**: Define sources, destinations, and pipelines via REST API or Web UI
2.  **Source Connection (PostgreSQL)**: Compute service connects to PostgreSQL using logical replication slots to capture WAL changes (INSERT, UPDATE, DELETE)
3.  **CDC Processing (Compute/Debezium)**: The pydbzengine processes CDC events in isolated processes with:
    - Single Debezium connector per pipeline
    - Fan-out to multiple destinations simultaneously
    - Failed records automatically sent to Redis DLQ
4.  **Authentication**: Uses RSA Key-Pair Authentication (PKCS#8) to securely connect to Snowflake with encrypted private keys
5.  **Destinations (Snowflake/PostgreSQL)**: Processed data is ingested into multiple specified destinations with independent health tracking
6.  **Backfill (Optional)**: Historical data synchronized using DuckDB batch processing with flexible filtering

## System Architecture

### Configuration Database Pattern

Rosetta uses a shared configuration database pattern where all three services read/write to a central PostgreSQL database.

The system schema is defined in `migrations/001_create_table.sql`:

- **sources**: PostgreSQL source connection configurations
- **destinations**: Snowflake/PostgreSQL destination connection details
- **pipelines**: Pipeline definitions linking sources to destinations
- **pipeline_destinations**: Many-to-many relationship between pipelines and destinations
- **pipeline_metadata**: Real-time status, health metrics, and WAL monitoring data per destination
- **queue_backfill_data**: Backfill job queue and status tracking
- **data_flow_record_monitoring**: CDC event tracking for observability

### Service Communication Flow

```
Backend API
  ├─> Writes pipeline configs to PostgreSQL
  ├─> Creates pipeline_destinations for each target
  └─> Manages backfill job queue

Compute Service
  ├─> Polls DB every 10s for status='START' pipelines
  ├─> Spawns isolated Process per pipeline (multiprocessing.Process)
  ├─> Single Debezium connector per pipeline
  ├─> Replicates to ALL destinations via fan-out
  ├─> Updates pipeline_metadata per destination
  ├─> Sends failed records to Redis DLQ
  └─> Processes backfill jobs from queue (polled every 5s)

Web Dashboard
  ├─> Fetches data via Backend REST API
  ├─> Uses TanStack Query for caching
  └─> 300ms delay before cache invalidation for DB consistency

Redis (DLQ)
  ├─> Stores failed CDC records in Streams
  ├─> Organized by source_id:table:destination_id
  └─> Supports manual/automatic recovery
```

### Backend: Clean Architecture

The backend follows strict Clean Architecture with Domain-Driven Design:

```
api/v1/endpoints/          # Presentation Layer
  ├─> FastAPI routes
  └─> OpenAPI documentation

domain/services/           # Application Layer
  ├─> Business logic orchestration
  ├─> PipelineService, SourceService, DestinationService
  └─> BackfillService, SchemaMonitorService

domain/models/             # Domain Layer
  ├─> SQLAlchemy 2.0 ORM entities
  └─> Framework-free business logic

domain/schemas/            # Domain Layer
  └─> Pydantic validation schemas

domain/repositories/       # Infrastructure Layer
  ├─> Repository Pattern implementation
  └─> BaseRepository with generic CRUD

infrastructure/tasks/      # Infrastructure Layer
  └─> Background tasks (WAL monitoring)
```

### Compute: Event-Driven Processing

The compute service uses multiprocessing for pipeline isolation:

```python
PipelineManager
  ├─> Monitors pipeline table every 10s
  ├─> Spawns/terminates Process per pipeline
  └─> Handles process lifecycle

PipelineProcess (multiprocessing.Process)
  ├─> Isolated memory and resources
  ├─> Independent connection pool (max 3 connections)
  ├─> PipelineEngine with Debezium integration
  └─> Updates pipeline_metadata on changes

BackfillManager
  ├─> Polls backfill queue every 5s
  ├─> DuckDB for efficient batch processing
  ├─> Thread-based execution
  └─> Batch size: 10,000 rows

DLQManager
  ├─> Redis Streams for message persistence
  ├─> Consumer groups for recovery
  └─> Configurable retry strategies
```

### Web: Feature-Based Architecture

React frontend with modern TanStack ecosystem:

```
src/
├── features/
│   ├── pipelines/        # Pipeline management UI
│   │   ├── components/   # Table, drawer, forms
│   │   ├── pages/        # List, detail pages
│   │   └── data/         # Zod schemas, table configs
│   ├── destinations/     # Destination management
│   ├── sources/          # Source management
│   └── dashboard/        # Metrics & monitoring
├── repo/                 # API client layer (fetch-based)
│   ├── pipelines.ts
│   ├── backfill.ts
│   └── client.ts
└── components/ui/        # shadcn/ui components (RTL-enabled)
```

## How to Run

### Prerequisites

- **Python 3.11+** with [uv](https://docs.astral.sh/uv/) package manager (for Backend and Compute)
- **Node.js 18+** with [pnpm](https://pnpm.io/) (for Web)
- **Docker** & Docker Compose (for running local PostgreSQL instances)
- **OpenSSL** (for generating Snowflake authentication keys)

### Step 1: Generate Private & Public Keys

Rosetta uses Key-Pair Authentication for Snowflake.

1.  **Generate Encrypted Private Key** (remember the passphrase):
    ```bash
    openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -v2 des3 -out rsa_key.p8
    ```
2.  **Generate Public Key**:
    ```bash
    openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
    ```
3.  **Configure Snowflake User**:
    ```sql
    ALTER USER <YOUR_USER> SET RSA_PUBLIC_KEY='<CONTENT_OF_RSA_KEY_PUB>';
    ```

### Step 2: Set Environment Variables

Create environment files for each service:

**Backend** (`backend/.env`):

```bash
DATABASE_URL=postgresql+asyncpg://postgres:postgres@localhost:5433/postgres
SECRET_KEY=your-secret-key-here
WAL_MONITOR_INTERVAL_SECONDS=300
```

**Compute** (`compute/.env`):

```bash
CONFIG_DATABASE_URL=postgres://postgres:postgres@localhost:5433/postgres
DEBUG=false
LOG_LEVEL=INFO

# Redis DLQ Configuration
REDIS_URL=redis://localhost:6379/0
DLQ_KEY_PREFIX=rosetta:dlq
DLQ_CHECK_INTERVAL=30
DLQ_BATCH_SIZE=100
DLQ_MAX_RETRY_COUNT=10
DLQ_MAX_AGE_DAYS=7

# Pipeline Connection Pool
PIPELINE_POOL_MAX_CONN=3
```

### Step 3: Start Docker Services

Start the configuration and source PostgreSQL databases:

```bash
docker-compose up -d
```

This starts:

- **Config DB** on port 5433 (PostgreSQL 16 for pipeline configurations)
- **Source DB** on port 5434 (PostGIS 16-3.4 for CDC source)
- **Target DB** on port 5435 (PostGIS for PostgreSQL destination testing)
- **Redis** on port 6379 (Redis 7 for DLQ)

All PostgreSQL instances are configured with:

- `wal_level=logical` for CDC support
- `max_replication_slots=10`
- `max_wal_senders=10`
- Timezone: `Asia/Jakarta`

### Step 4: Setup and Run Backend

```bash
cd backend
uv sync                          # Install dependencies
uv run alembic upgrade head      # Apply database migrations
uv run uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

Backend API will be available at `http://localhost:8000/docs`

### Step 5: Setup and Run Compute

```bash
cd compute
python -m venv venv
source venv/bin/activate         # Windows: venv\Scripts\activate
pip install -r requirements.txt
python main.py
```

Compute service will start polling for active pipelines.

### Step 6: Setup and Run Web

```bash
cd web
pnpm install
pnpm dev
```

Web dashboard will be available at `http://localhost:5173`

### Via Web UI

Use the dashboard at `http://localhost:5173` to manage pipelines through a visual interface:

- **Pipeline Management**: Create, start, pause, delete pipelines
- **Multi-Destination**: Add/remove destinations to existing pipelines
- **Backfill Jobs**: Create and monitor historical data sync jobs
- **DLQ Recovery**: View and recover failed CDC records
- **Real-time Metrics**: Monitor WAL size, record counts, health status
- **Table Selection**: Choose which tables to sync per destination

### Via API

Backend provides comprehensive REST endpoints at `http://localhost:8000/api/v1`:

**Pipeline Management:**

- `POST /pipelines` - Create pipeline (initially in PAUSE status)
- `GET /pipelines` - List all pipelines with metadata
- `GET /pipelines/{id}` - Get pipeline details
- `PUT /pipelines/{id}` - Update pipeline configuration
- `DELETE /pipelines/{id}` - Delete pipeline
- `POST /pipelines/{id}/start` - Start pipeline
- `POST /pipelines/{id}/pause` - Pause pipeline

**Multi-Destination:**

- `POST /pipelines/{id}/destinations` - Add destination to pipeline
- `DELETE /pipelines/{id}/destinations/{dest_id}` - Remove destination
- `GET /pipelines/{id}/destinations/{dest_id}/metadata` - Get destination health

**Backfill:**

- `POST /pipelines/{id}/backfill` - Create backfill job with filters
- `GET /pipelines/{id}/backfill` - List backfill jobs
- `POST /pipelines/{id}/backfill/{job_id}/cancel` - Cancel job

**DLQ Recovery:**

- `GET /pipelines/{id}/destinations/{dest_id}/dlq/count` - Get failed record count
- `POST /pipelines/{id}/destinations/{dest_id}/recover-dlq` - Recover failed records

**Monitoring:**

- `GET /wal-metrics` - Get WAL size history
- `GET /pipelines/{id}/monitoring` - Get CDC event statistics

### Via Direct SQL

Compute service polls for status changes, so you can also control pipelines via SQL:

- **Pause**: `UPDATE pipelines SET status = 'PAUSE' WHERE name = '...';`
- **Start**: `UPDATE pipelines SET status = 'START' WHERE name = '...';`

Monitor pipeline health:

```sql
SELECT p.name, pm.health_status, pm.wal_size, pm.last_success_time
FROM pipelines p
JOIN pipeline_metadata pm ON p.id = pm.pipeline_id;
```

## Testing

### Backend Tests

```bash
cd backend
uv run pytest tests/             # Run all tests
uv run pytest tests/ --cov=app   # With coverage report
```

### Compute Tests

```bash
cd compute
pytest tests/
```

## Port Reference

| Service   | Port | Description                       |
| --------- | ---- | --------------------------------- |
| Backend   | 8000 | FastAPI REST API + OpenAPI docs   |
| Compute   | 8001 | Health check endpoint             |
| Web       | 5173 | Vite dev server                   |
| Config DB | 5433 | PostgreSQL config database        |
| Source DB | 5434 | PostgreSQL source (PostGIS)       |
| Target DB | 5435 | PostgreSQL destination (PostGIS)  |
| Redis     | 6379 | Redis for DLQ (Dead Letter Queue) |

## Documentation

- **Backend**: See `backend/ARCHITECTURE.md` for Clean Architecture details
- **Backend**: See `backend/GETTING_STARTED.md` for API usage guide
- **Backend**: See `backend/DIAGRAMS.md` for sequence diagrams
- **Compute**: See `compute/docs/` for troubleshooting guides
  - `DATABASE_CONNECTION_TROUBLESHOOTING.md`
  - `DEBEZIUM_TROUBLESHOOTING.md`
  - `PERFORMANCE_ANALYSIS.md`
  - `SNOWFLAKE_TIMEOUT_CONFIGURATION.md`
- **Backfill**: See `docs/BACKFILL_FEATURE.md` for complete backfill documentation
- **Web**: Based on [shadcn-admin](https://github.com/satnaing/shadcn-admin) template with TanStack ecosystem
- **AI Agents**: See `.github/copilot-instructions.md` for development guidelines

## Common Use Cases

### 1. Real-time CDC to Multiple Snowflake Accounts

```bash
# Create source
POST /sources
{
  "name": "production-db",
  "pg_host": "prod.example.com",
  "pg_database": "app",
  ...
}

# Create destinations
POST /destinations  # Analytics Snowflake
POST /destinations  # Data Science Snowflake

# Create pipeline with both destinations
POST /pipelines
{
  "name": "prod-to-analytics",
  "source_id": 1
}

POST /pipelines/1/destinations {"destination_id": 1}
POST /pipelines/1/destinations {"destination_id": 2}

# Start replication
POST /pipelines/1/start
```

### 2. Historical Backfill with Filtering

```bash
# Backfill orders from last 30 days
POST /pipelines/1/backfill
{
  "table_name": "orders",
  "filters": [
    "created_at >= '2026-01-01'",
    "status = 'completed'"
  ]
}
```

### 3. DLQ Recovery After Network Issues

```bash
# Check failed records
GET /pipelines/1/destinations/1/dlq/count
# Response: {"count": 150}

# Recover all failed records
POST /pipelines/1/destinations/1/recover-dlq
```

## Performance Characteristics

- **Throughput**: 10,000+ records/second per pipeline (depends on network and destination)
- **Latency**: Sub-second CDC latency for real-time changes
- **Scalability**: Horizontal scaling via multiple compute instances
- **Resource Usage**: ~200MB RAM per pipeline process
- **Connection Pooling**: Configurable per-pipeline pools (default: 3 connections)

## Troubleshooting

### Pipeline Not Starting

1. Check pipeline status: `SELECT * FROM pipelines WHERE id = X`
2. Check metadata errors: `SELECT * FROM pipeline_metadata WHERE pipeline_id = X`
3. Verify replication slot: `SELECT * FROM pg_replication_slots`
4. Check compute logs: Look for process crash or initialization errors

### High WAL Size

1. Check if pipeline is running: `SELECT status FROM pipelines`
2. Verify destination connectivity
3. Check for DLQ buildup: Query Redis stream length
4. Consider increasing `PIPELINE_POOL_MAX_CONN`

### Failed Records in DLQ

1. Check error patterns in Web UI DLQ tab
2. Verify destination schema matches source
3. Check destination connection credentials
4. Try manual recovery: `POST /pipelines/{id}/destinations/{dest_id}/recover-dlq`

## Frequently Asked Questions

### Can I add destinations to an existing pipeline?

Yes! Use `POST /pipelines/{id}/destinations` with `{"destination_id": X}` to add destinations dynamically without stopping the pipeline.

### What happens if one destination fails?

Other destinations continue operating normally. Failed records are sent to that destination's DLQ for later recovery. Each destination has independent health tracking.

### How do I migrate an existing database?

Create a pipeline, then use the Backfill feature via `POST /pipelines/{id}/backfill` to sync historical data. The pipeline will handle ongoing CDC.

### Can I filter which tables to replicate?

Yes! When adding a destination to a pipeline, you can specify `table_syncs` configuration to select specific tables and apply transformations.

### What's the latency for CDC?

Typically sub-second for individual records, depending on network conditions and destination write performance. Batch commits occur every few seconds.

### How do I scale for high throughput?

- Increase `PIPELINE_POOL_MAX_CONN` for more database connections
- Run multiple compute instances (horizontal scaling)
- Use separate pipelines per table/schema
- Tune Debezium batch sizes and commit intervals

## System Requirements

### Minimum

- **CPU**: 2 cores
- **RAM**: 4 GB (2 GB for compute, 1 GB for backend, 1 GB for web)
- **Storage**: 10 GB (for logs, Redis persistence)
- **Network**: 10 Mbps (source and destination connectivity)

### Recommended (Production)

- **CPU**: 4+ cores
- **RAM**: 8 GB (scales with number of pipelines)
- **Storage**: 50+ GB SSD (for WAL lag buffering)
- **Network**: 100+ Mbps with low latency to source/destination

### Per Pipeline Resource Usage

- **RAM**: ~200 MB baseline + data buffer
- **CPU**: 0.5-1 core under load
- **Network**: Depends on CDC volume (MB/s)
- **Database Connections**: 3 connections per pipeline (configurable)

## Contributing

This is a research project. For production use, please review:

- Security configurations (encryption keys, network policies)
- Resource limits (connection pools, memory, CPU)
- Monitoring and alerting setup
- Backup and disaster recovery procedures

## License

See LICENSE file for details.
