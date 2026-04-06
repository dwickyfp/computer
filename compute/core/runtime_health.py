"""
Runtime worker health registry.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from datetime import datetime, timezone


@dataclass
class WorkerState:
    name: str
    critical: bool
    status: str = "starting"
    message: str | None = None
    updated_at: datetime | None = None


_lock = threading.Lock()
_workers: dict[str, WorkerState] = {}


def mark_worker(name: str, status: str, critical: bool = True, message: str | None = None) -> None:
    with _lock:
        current = _workers.get(name)
        _workers[name] = WorkerState(
            name=name,
            critical=current.critical if current else critical,
            status=status,
            message=message,
            updated_at=datetime.now(timezone.utc),
        )


def snapshot() -> dict[str, dict]:
    with _lock:
        return {
            name: {
                "status": state.status,
                "critical": state.critical,
                "message": state.message,
                "updated_at": state.updated_at.isoformat() if state.updated_at else None,
            }
            for name, state in _workers.items()
        }


def overall_status() -> tuple[bool, dict[str, dict]]:
    workers = snapshot()
    unhealthy = any(
        state["critical"] and state["status"] != "running"
        for state in workers.values()
    )
    return (not unhealthy), workers

