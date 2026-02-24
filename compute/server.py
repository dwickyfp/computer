"""
FastAPI Server for Rosetta Compute Engine.

Provides health check, connection pool status, and chain ingestion endpoints.
"""

import json
import logging
import uvicorn
from fastapi import FastAPI, Depends, Header, Request, Response
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
# Rosetta Chain — Arrow IPC Ingestion Endpoints
# ═══════════════════════════════════════════════════════════════════════════════

# Lazy-initialized singletons
_ingest_manager = None
_schema_manager = None


def _get_ingest_manager():
    global _ingest_manager
    if _ingest_manager is None:
        from chain.ingest import ChainIngestManager

        _ingest_manager = ChainIngestManager()
    return _ingest_manager


def _get_schema_manager():
    global _schema_manager
    if _schema_manager is None:
        from chain.schema import ChainSchemaManager

        _schema_manager = ChainSchemaManager()
    return _schema_manager


@app.get("/chain/health")
async def chain_health(x_chain_key: str = Header(default=None)):
    """
    Chain-specific health check.

    Validates the chain key and returns capabilities.
    """
    config = get_config()
    if not config.chain.enabled:
        return JSONResponse(
            status_code=503,
            content={"status": "disabled", "message": "Chain ingestion is not enabled"},
        )

    # Validate key if auth is enabled
    if config.chain.auth_enabled and x_chain_key:
        from chain.auth import validate_chain_key

        try:
            validate_chain_key(x_chain_key)
        except Exception:
            return JSONResponse(
                status_code=401,
                content={"status": "unauthorized", "message": "Invalid chain key"},
            )

    return {
        "status": "healthy",
        "chain_enabled": True,
        "capabilities": ["arrow_ipc", "json", "schema_sync"],
    }


@app.post("/chain/ingest")
async def chain_ingest(
    request: Request,
    x_chain_key: str = Header(...),
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

    # Validate key
    if config.chain.auth_enabled:
        from chain.auth import validate_chain_key

        validate_chain_key(x_chain_key)

    content_type = request.headers.get("content-type", "")
    body = await request.body()
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

        return {"status": "ok", "records_ingested": count}

    except ValueError as e:
        return JSONResponse(status_code=400, content={"error": str(e)})
    except Exception as e:
        logger.error(f"Chain ingest error: {e}")
        return JSONResponse(status_code=500, content={"error": "Ingestion failed"})


@app.post("/chain/schema")
async def chain_push_schema(
    request: Request,
    x_chain_key: str = Header(...),
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

    if config.chain.auth_enabled:
        from chain.auth import validate_chain_key

        validate_chain_key(x_chain_key)

    body = await request.json()
    table_name = body.get("table_name")
    schema_json = body.get("schema_json", {})
    chain_client_id = body.get("chain_client_id")
    source_chain_id = body.get("source_chain_id")

    if not table_name:
        return JSONResponse(
            status_code=400, content={"error": "table_name is required"}
        )
    if not chain_client_id:
        return JSONResponse(
            status_code=400, content={"error": "chain_client_id is required"}
        )

    schema_mgr = _get_schema_manager()
    success = schema_mgr.upsert_table_schema(
        chain_client_id=chain_client_id,
        table_name=table_name,
        schema_json=schema_json,
        source_chain_id=source_chain_id,
    )

    if success:
        return {"status": "ok", "table_name": table_name}
    else:
        return JSONResponse(status_code=500, content={"error": "Failed to save schema"})


@app.get("/chain/schema/{table_name}")
async def chain_get_schema(
    table_name: str,
    x_chain_key: str = Header(default=None),
    chain_client_id: int = None,
):
    """Check if a table schema exists and return it."""
    config = get_config()
    if config.chain.auth_enabled and x_chain_key:
        from chain.auth import validate_chain_key

        try:
            validate_chain_key(x_chain_key)
        except Exception:
            pass

    schema_mgr = _get_schema_manager()
    schema = schema_mgr.get_table_schema(table_name, chain_client_id)

    if schema is None:
        return JSONResponse(
            status_code=404, content={"error": f"Table {table_name} not found"}
        )

    return {"table_name": table_name, "schema_json": schema}


@app.get("/chain/tables")
async def chain_list_tables(
    x_chain_key: str = Header(default=None),
    chain_client_id: int = None,
):
    """List all chain tables available on this instance."""
    config = get_config()
    if config.chain.auth_enabled and x_chain_key:
        from chain.auth import validate_chain_key

        try:
            validate_chain_key(x_chain_key)
        except Exception:
            pass

    schema_mgr = _get_schema_manager()
    tables = schema_mgr.list_tables(chain_client_id)
    return tables


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
