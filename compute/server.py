"""
FastAPI Server for Rosetta Compute Engine.

Provides health check, connection pool status, and chain ingestion endpoints.
"""

import json
import logging
import os
import threading
import time
import uvicorn
from collections import defaultdict
from fastapi import FastAPI, Header, Request, Response
from fastapi.responses import JSONResponse
from config.config import get_config

logger = logging.getLogger(__name__)

app = FastAPI(title="Rosetta Compute Engine")


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy"}


@app.get("/health/pool")
async def pool_health():
    """Connection pool health endpoint (C3)."""
    from core.database import get_pool_health

    return get_pool_health()


# ═══════════════════════════════════════════════════════════════════════════════
# R9: Simple in-memory sliding window rate limiter
# ═══════════════════════════════════════════════════════════════════════════════

_rate_limit_lock = threading.Lock()
_rate_limit_windows: dict[str, list[float]] = defaultdict(list)
_RATE_LIMIT_PER_MINUTE = int(os.getenv("CHAIN_RATE_LIMIT_PER_MINUTE", "60"))


def _check_rate_limit(chain_id: str) -> bool:
    """
    Check if a chain_id has exceeded its rate limit.

    Returns True if the request is allowed, False if rate-limited.
    Uses a sliding window of 60 seconds.
    """
    now = time.time()
    window_start = now - 60.0

    with _rate_limit_lock:
        timestamps = _rate_limit_windows[chain_id]
        # Remove expired entries
        _rate_limit_windows[chain_id] = [
            t for t in timestamps if t > window_start
        ]
        timestamps = _rate_limit_windows[chain_id]

        if len(timestamps) >= _RATE_LIMIT_PER_MINUTE:
            return False

        timestamps.append(now)
        return True


# ═══════════════════════════════════════════════════════════════════════════════
# Rosetta Chain — Arrow IPC Ingestion Endpoints
# ═══════════════════════════════════════════════════════════════════════════════

# Lazy-initialized singletons (thread-safe)
_ingest_manager = None
_schema_manager = None
_manager_lock = threading.Lock()

# Maximum request body size for chain ingest (50 MB)
_MAX_CHAIN_BODY_SIZE = 50 * 1024 * 1024


def _get_ingest_manager():
    global _ingest_manager
    if _ingest_manager is None:
        with _manager_lock:
            if _ingest_manager is None:
                from chain.ingest import ChainIngestManager
                _ingest_manager = ChainIngestManager()
    return _ingest_manager


def _get_schema_manager():
    global _schema_manager
    if _schema_manager is None:
        with _manager_lock:
            if _schema_manager is None:
                from chain.schema import ChainSchemaManager
                _schema_manager = ChainSchemaManager()
    return _schema_manager


@app.get("/chain/health")
async def chain_health():
    """Chain-specific health check."""
    config = get_config()
    if not config.chain.enabled:
        return JSONResponse(
            status_code=503,
            content={"status": "disabled", "message": "Chain ingestion is not enabled"},
        )

    return {
        "status": "healthy",
        "chain_enabled": True,
        "capabilities": ["arrow_ipc", "json", "schema_sync"],
    }


@app.post("/chain/ingest")
async def chain_ingest(
    request: Request,
    x_chain_id: str = Header(...),
    x_table_name: str = Header(...),
    x_operation_type: str = Header(default="c"),
):
    """
    Receive Arrow IPC data from a remote Rosetta instance.

    Content-Type should be application/vnd.apache.arrow.stream
    or application/json for JSON fallback.
    """
    config = get_config()
    if not config.chain.enabled:
        return JSONResponse(
            status_code=503,
            content={"error": "Chain ingestion is not enabled"},
        )

    # R9: Rate limiting per chain ID
    if not _check_rate_limit(x_chain_id):
        return JSONResponse(
            status_code=429,
            content={
                "error": f"Rate limit exceeded ({_RATE_LIMIT_PER_MINUTE} req/min)",
                "chain_id": x_chain_id,
            },
            headers={"Retry-After": "60"},
        )
    content_type = request.headers.get("content-type", "")

    # Enforce body size limit to prevent memory exhaustion (M6)
    content_length = request.headers.get("content-length")
    if content_length and int(content_length) > _MAX_CHAIN_BODY_SIZE:
        return JSONResponse(
            status_code=413,
            content={
                "error": f"Request body too large. Maximum size is {_MAX_CHAIN_BODY_SIZE // (1024*1024)} MB"
            },
        )

    # Use streaming read with size limit
    chunks = []
    total_size = 0
    async for chunk in request.stream():
        total_size += len(chunk)
        if total_size > _MAX_CHAIN_BODY_SIZE:
            return JSONResponse(
                status_code=413,
                content={
                    "error": f"Request body too large. Maximum size is {_MAX_CHAIN_BODY_SIZE // (1024*1024)} MB"
                },
            )
        chunks.append(chunk)
    body = b"".join(chunks)

    manager = _get_ingest_manager()

    try:
        if "arrow" in content_type or "octet-stream" in content_type:
            count = manager.ingest_arrow_ipc(
                body=body,
                chain_id=x_chain_id,
                table_name=x_table_name,
                operation_type=x_operation_type,
            )
        else:
            # JSON fallback
            records = json.loads(body)
            if not isinstance(records, list):
                records = [records]
            count = manager.ingest_json_records(
                records=records,
                chain_id=x_chain_id,
                table_name=x_table_name,
                operation_type=x_operation_type,
            )

        # Auto-populate source_chain_id on the matching chain client row so
        # ChainPipelineEngine can resolve the correct Redis stream pattern.
        _try_auto_map_source_chain_id(x_chain_id)

        return {"status": "ok", "records_ingested": count}

    except ValueError as e:
        return JSONResponse(status_code=400, content={"error": str(e)})
    except Exception as e:
        logger.error(f"Chain ingest error: {e}")
        return JSONResponse(status_code=500, content={"error": "Ingestion failed"})


def _try_auto_map_source_chain_id(chain_id: str) -> None:
    """
    Auto-populate source_chain_id on the rosetta_chain_clients row when
    exactly one client has no source_chain_id set yet.

    This covers the common single-client setup so users don't have to
    manually enter the sender's chain ID.  For multi-client setups the
    admin must set source_chain_id explicitly in the UI.
    """
    from core.database import DatabaseSession

    try:
        with DatabaseSession() as session:
            # Already mapped?
            session.execute(
                "SELECT id FROM rosetta_chain_clients "
                "WHERE source_chain_id = %s LIMIT 1",
                (chain_id,),
            )
            if session.fetchone():
                return  # Nothing to do

            # Count clients with no mapping yet
            session.execute(
                "SELECT id FROM rosetta_chain_clients WHERE source_chain_id IS NULL"
            )
            unmapped = session.fetchall()
            if len(unmapped) == 1:
                client_id = unmapped[0]["id"]
                session.execute(
                    "UPDATE rosetta_chain_clients "
                    "SET source_chain_id = %s, updated_at = NOW() "
                    "WHERE id = %s",
                    (chain_id, client_id),
                )
                logger.info(
                    f"Auto-mapped source_chain_id={chain_id!r} to "
                    f"rosetta_chain_clients.id={client_id}"
                )
    except Exception as e:
        logger.warning(f"Failed to auto-map source_chain_id={chain_id!r}: {e}")


@app.post("/chain/schema")
async def chain_push_schema(
    request: Request,
):
    """
    Receive a table schema definition from a remote Rosetta instance.

    Auto-creates the table entry in rosetta_chain_tables.
    """
    config = get_config()
    if not config.chain.enabled:
        return JSONResponse(
            status_code=503,
            content={"error": "Chain ingestion is not enabled"},
        )

    body = await request.json()
    table_name = body.get("table_name")
    schema_json = body.get("schema_json", {})
    chain_client_id = body.get("chain_client_id")  # optional for cross-instance calls
    source_chain_id = body.get("source_chain_id")
    database_name = body.get("database_name")  # logical database this table belongs to

    if not table_name:
        return JSONResponse(
            status_code=400, content={"error": "table_name is required"}
        )

    # For cross-instance registrations chain_client_id is absent (the sender's
    # local ID is meaningless in this DB).  Require source_chain_id in that case
    # so the remote table can still be uniquely identified.
    if not chain_client_id and not source_chain_id:
        return JSONResponse(
            status_code=400,
            content={
                "error": "source_chain_id is required when chain_client_id is absent"
            },
        )

    # Coerce to int if present
    if chain_client_id is not None:
        try:
            chain_client_id = int(chain_client_id)
        except (TypeError, ValueError):
            chain_client_id = None

    schema_mgr = _get_schema_manager()
    success = schema_mgr.upsert_table_schema(
        table_name=table_name,
        schema_json=schema_json,
        chain_client_id=chain_client_id,
        source_chain_id=str(source_chain_id) if source_chain_id is not None else None,
        database_name=database_name,
    )

    if success:
        return {"status": "ok", "table_name": table_name}
    else:
        return JSONResponse(status_code=500, content={"error": "Failed to save schema"})


@app.get("/chain/schema/{table_name}")
async def chain_get_schema(
    table_name: str,
    chain_client_id: int = None,
):
    """Check if a table schema exists and return it."""
    schema_mgr = _get_schema_manager()
    schema = schema_mgr.get_table_schema(table_name, chain_client_id)

    if schema is None:
        return JSONResponse(
            status_code=404, content={"error": f"Table {table_name} not found"}
        )

    return {"table_name": table_name, "schema_json": schema}


@app.get("/chain/tables")
async def chain_list_tables(
    chain_client_id: int = None,
):
    """List all chain tables available on this instance."""
    schema_mgr = _get_schema_manager()
    tables = schema_mgr.list_tables(chain_client_id)
    return tables


@app.get("/chain/databases")
async def chain_list_databases():
    """List all chain databases available on this instance."""
    schema_mgr = _get_schema_manager()
    databases = schema_mgr.list_databases()
    return databases


def run_server(host: str, port: int) -> None:
    """
    Run FastAPI server using Uvicorn.

    Args:
        host: Host to bind to
        port: Port to bind to
    """
    try:
        uvicorn.run(app, host=host, port=port, log_level="info")
    except Exception as e:
        logger.error(f"Failed to start API server: {e}")
