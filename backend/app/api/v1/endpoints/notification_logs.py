"""
Notification Log API endpoints.
"""

from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.core.logging import get_logger
from app.domain.repositories.notification_log_repo import NotificationLogRepository
from app.domain.schemas.notification_log import NotificationLog

logger = get_logger(__name__)
router = APIRouter(prefix="/notifications", tags=["Notifications"])


@router.get(
    "/",
    response_model=List[NotificationLog],
    status_code=status.HTTP_200_OK,
    summary="List notifications",
    description="Get all active notifications, optionally filtered by read status.",
)
def list_notifications(
    skip: int = 0,
    limit: int = 100,
    is_read: Optional[bool] = Query(None, description="Filter by read status"),
    db: Session = Depends(get_db),
):
    """
    List notifications.
    """
    try:
        repo = NotificationLogRepository(db)
        return repo.get_all(skip=skip, limit=limit, is_read=is_read)
    except Exception as e:
        logger.error("Failed to list notifications", extra={"error": str(e)})
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to list notifications",
        )


@router.post(
    "/{notification_id}/read",
    response_model=NotificationLog,
    status_code=status.HTTP_200_OK,
    summary="Mark notification as read",
)
def mark_notification_as_read(
    notification_id: int,
    db: Session = Depends(get_db),
):
    """
    Mark a specific notification as read.
    """
    try:
        repo = NotificationLogRepository(db)
        notification = repo.mark_as_read(notification_id)
        if not notification:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Notification not found",
            )
        return notification
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to mark notification as read", extra={"error": str(e)})
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to mark notification as read",
        )


@router.post(
    "/read-all",
    status_code=status.HTTP_200_OK,
    summary="Mark all notifications as read",
)
def mark_all_notifications_as_read(
    db: Session = Depends(get_db),
):
    """
    Mark all active unread notifications as read.
    """
    try:
        repo = NotificationLogRepository(db)
        count = repo.mark_all_as_read()
        return {"message": "All notifications marked as read", "count": count}
    except Exception as e:
        logger.error(
            "Failed to mark all notifications as read", extra={"error": str(e)}
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to mark all notifications as read",
        )


@router.delete(
    "/clear-all",
    status_code=status.HTTP_200_OK,
    summary="Clear all notifications",
)
def clear_all_notifications(
    db: Session = Depends(get_db),
):
    """
    Soft delete all notifications.
    """
    try:
        repo = NotificationLogRepository(db)
        count = repo.soft_delete_all()
        return {"message": "All notifications cleared", "count": count}
    except Exception as e:
        logger.error("Failed to clear all notifications", extra={"error": str(e)})
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to clear all notifications",
        )


@router.delete(
    "/{notification_id}",
    response_model=NotificationLog,
    status_code=status.HTTP_200_OK,
    summary="Delete notification",
)
def delete_notification(
    notification_id: int,
    db: Session = Depends(get_db),
):
    """
    Soft delete a notification.
    """
    try:
        repo = NotificationLogRepository(db)
        notification = repo.soft_delete(notification_id)
        if not notification:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Notification not found",
            )
        return notification
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to delete notification", extra={"error": str(e)})
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to delete notification",
        )
