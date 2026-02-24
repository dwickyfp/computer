"""
Rosetta Chain API endpoints for inter-instance streaming configuration.

Provides endpoints for:
- Chain key generation and management
- Remote client registration and testing
- Table discovery and synchronization
"""

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.api.deps import get_db, get_db_readonly
from app.domain.schemas.rosetta_chain import (
    ChainClientCreate,
    ChainClientResponse,
    ChainClientTestResponse,
    ChainClientUpdate,
    ChainKeyResponse,
    ChainTableResponse,
    ChainToggleActiveRequest,
)
from app.domain.services.rosetta_chain import RosettaChainService

router = APIRouter()


def get_chain_service(db: Session = Depends(get_db)) -> RosettaChainService:
    """Get chain service dependency (read-write)."""
    return RosettaChainService(db)


def get_chain_service_readonly(
    db: Session = Depends(get_db_readonly),
) -> RosettaChainService:
    """Get chain service dependency (read-only)."""
    return RosettaChainService(db)


# ─── Chain Key Endpoints ────────────────────────────────────────────────────────


@router.get("/key", response_model=ChainKeyResponse | None)
def get_chain_key(
    service: RosettaChainService = Depends(get_chain_service_readonly),
):
    """Get the current chain key (masked)."""
    return service.get_chain_key()


@router.get("/key/reveal")
def reveal_chain_key(
    service: RosettaChainService = Depends(get_chain_service_readonly),
):
    """Return the full decrypted chain key."""
    raw = service.get_chain_key_raw()
    if raw is None:
        return {"chain_key": None}
    return {"chain_key": raw}


@router.post("/generate-key")
def generate_chain_key(
    service: RosettaChainService = Depends(get_chain_service),
):
    """
    Generate a new chain key for this instance.

    Returns the raw key — store it securely, it will only be shown once.
    """
    raw_key = service.generate_chain_key()
    return {
        "chain_key": raw_key,
        "message": "Store this key securely. It will not be shown again.",
    }


@router.patch("/toggle-active")
def toggle_chain_active(
    body: ChainToggleActiveRequest,
    service: RosettaChainService = Depends(get_chain_service),
):
    """Toggle chain ingestion active state."""
    result = service.set_chain_active(body.is_active)
    if result is None:
        return {"message": "No chain key configured. Generate a key first."}
    return result


# ─── Client Endpoints ───────────────────────────────────────────────────────────


@router.get("/clients", response_model=list[ChainClientResponse])
def list_clients(
    service: RosettaChainService = Depends(get_chain_service_readonly),
):
    """List all registered remote Rosetta instances."""
    return service.get_clients()


@router.get("/clients/{client_id}", response_model=ChainClientResponse)
def get_client(
    client_id: int,
    service: RosettaChainService = Depends(get_chain_service_readonly),
):
    """Get a specific remote Rosetta instance."""
    return service.get_client(client_id)


@router.post("/clients", response_model=ChainClientResponse, status_code=201)
def create_client(
    data: ChainClientCreate,
    service: RosettaChainService = Depends(get_chain_service),
):
    """Register a new remote Rosetta instance."""
    return service.create_client(data)


@router.put("/clients/{client_id}", response_model=ChainClientResponse)
def update_client(
    client_id: int,
    data: ChainClientUpdate,
    service: RosettaChainService = Depends(get_chain_service),
):
    """Update a remote Rosetta instance registration."""
    return service.update_client(client_id, data)


@router.delete("/clients/{client_id}", status_code=204)
def delete_client(
    client_id: int,
    service: RosettaChainService = Depends(get_chain_service),
):
    """Remove a remote Rosetta instance (cascades to tables)."""
    service.delete_client(client_id)


@router.post(
    "/clients/{client_id}/test",
    response_model=ChainClientTestResponse,
)
def test_client_connection(
    client_id: int,
    service: RosettaChainService = Depends(get_chain_service),
):
    """Test connectivity to a remote Rosetta instance."""
    return service.test_client_connection(client_id)


# ─── Table Discovery Endpoints ──────────────────────────────────────────────────


@router.get(
    "/clients/{client_id}/tables",
    response_model=list[ChainTableResponse],
)
def get_client_tables(
    client_id: int,
    service: RosettaChainService = Depends(get_chain_service_readonly),
):
    """Get tables available on a chain client."""
    return service.get_client_tables(client_id)


@router.post(
    "/clients/{client_id}/sync-tables",
    response_model=list[ChainTableResponse],
)
def sync_client_tables(
    client_id: int,
    service: RosettaChainService = Depends(get_chain_service),
):
    """Fetch and sync table list from a remote Rosetta instance."""
    return service.sync_client_tables(client_id)


@router.post("/clients/sync-destinations")
def sync_destinations(
    service: RosettaChainService = Depends(get_chain_service),
):
    """
    Ensure every registered chain client has a linked ROSETTA destination.

    Idempotent — safe to call at any time. Creates missing destinations for
    clients that were registered before auto-creation was introduced.
    """
    created = service.sync_all_destinations()
    return {"created": created, "message": f"Synced {created} destination(s)"}
