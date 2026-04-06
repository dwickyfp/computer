"""
Internal Notification Config API endpoints.

Provides CRUD + enable/disable operations for internal notification configurations.
Also exposes a global on/off toggle stored in rosetta_setting_configuration.
"""

from typing import List

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.core.logging import get_logger
from app.domain.repositories.configuration_repo import ConfigurationRepository
from app.domain.repositories.internal_notification_repo import (
    InternalNotificationConfigRepository,
)
from app.domain.schemas.internal_notification_config import (
    InternalNotificationConfigCreate,
    InternalNotificationConfigResponse,
    InternalNotificationConfigUpdate,
    InternalNotificationGlobalStatusResponse,
    InternalNotificationGlobalToggleRequest,
)

logger = get_logger(__name__)

router = APIRouter(
    prefix="/internal-notifications",
    tags=["internal-notifications"],
)

_GLOBAL_KEY = "ENABLE_ALERT_NOTIFICATION_INTERNAL"


# ──────────────────────────────────────────────
# Global toggle
# ──────────────────────────────────────────────


@router.get(
    "/global-status",
    response_model=InternalNotificationGlobalStatusResponse,
    summary="Get global internal notification status",
)
def get_global_status(db: Session = Depends(get_db)):
    config_repo = ConfigurationRepository(db)
    value = config_repo.get_value(_GLOBAL_KEY, "FALSE").upper() == "TRUE"
    return InternalNotificationGlobalStatusResponse(is_active=value)


@router.patch(
    "/global-toggle",
    response_model=InternalNotificationGlobalStatusResponse,
    summary="Toggle global internal notification on/off",
)
def toggle_global(
    body: InternalNotificationGlobalToggleRequest,
    db: Session = Depends(get_db),
):
    config_repo = ConfigurationRepository(db)
    config_repo.set_value(_GLOBAL_KEY, "TRUE" if body.is_active else "FALSE")
    logger.info(f"Internal notification global toggle set to {body.is_active}")
    return InternalNotificationGlobalStatusResponse(is_active=body.is_active)


# ──────────────────────────────────────────────
# CRUD
# ──────────────────────────────────────────────


@router.get(
    "/",
    response_model=List[InternalNotificationConfigResponse],
    summary="List all internal notification configs",
)
def list_configs(db: Session = Depends(get_db)):
    repo = InternalNotificationConfigRepository(db)
    return repo.get_all()


@router.post(
    "/",
    response_model=InternalNotificationConfigResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a new internal notification config",
)
def create_config(
    payload: InternalNotificationConfigCreate,
    db: Session = Depends(get_db),
):
    repo = InternalNotificationConfigRepository(db)
    obj = repo.create(payload.dict())
    logger.info(f"Created internal notification config '{obj.name}' (id={obj.id})")
    return obj


@router.put(
    "/{config_id}",
    response_model=InternalNotificationConfigResponse,
    summary="Update an internal notification config",
)
def update_config(
    config_id: int,
    payload: InternalNotificationConfigUpdate,
    db: Session = Depends(get_db),
):
    repo = InternalNotificationConfigRepository(db)
    obj = repo.update(config_id, payload.dict(exclude_none=True))
    if not obj:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Config not found"
        )
    logger.info(f"Updated internal notification config id={config_id}")
    return obj


@router.delete(
    "/{config_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete an internal notification config",
)
def delete_config(config_id: int, db: Session = Depends(get_db)):
    repo = InternalNotificationConfigRepository(db)
    deleted = repo.delete(config_id)
    if not deleted:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Config not found"
        )
    logger.info(f"Deleted internal notification config id={config_id}")


@router.patch(
    "/{config_id}/toggle",
    response_model=InternalNotificationConfigResponse,
    summary="Toggle is_enabled for a single internal notification config",
)
def toggle_config(
    config_id: int,
    body: InternalNotificationGlobalToggleRequest,
    db: Session = Depends(get_db),
):
    repo = InternalNotificationConfigRepository(db)
    obj = repo.set_enabled(config_id, body.is_active)
    if not obj:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Config not found"
        )
    logger.info(
        f"Internal notification config id={config_id} set is_enabled={body.is_active}"
    )
    return obj


@router.post(
    "/{config_id}/test",
    summary="Send a test notification using a specific config",
)
def test_config(config_id: int, db: Session = Depends(get_db)):
    repo = InternalNotificationConfigRepository(db)
    cfg = repo.get_by_id(config_id)
    if not cfg:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Config not found"
        )

    from app.domain.services.notification_service import NotificationService

    svc = NotificationService(db)
    ok = svc.send_test_internal_notification(cfg)
    if not ok:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Failed to send test notification — check base_url and credentials",
        )
    return {"message": f"Test notification sent via config '{cfg.name}'"}
