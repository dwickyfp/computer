"""
Execute backend bridge jobs in an isolated subprocess.

The worker and backend both use a top-level ``app`` package, so importing
backend services directly inside the worker process is unsafe. This executor
launches a subprocess with PYTHONPATH pointing at the backend package instead.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import structlog

logger = structlog.get_logger(__name__)


def _backend_dir() -> Path:
    repo_root = Path(__file__).resolve().parents[4]
    backend_dir = repo_root / "backend"
    if not backend_dir.exists():
        raise FileNotFoundError(f"Backend directory not found: {backend_dir}")
    return backend_dir


def _backend_python(backend_dir: Path) -> str:
    """
    Prefer the backend virtualenv interpreter in local development.

    In containers there is usually no backend venv inside the worker image, so
    we fall back to the current interpreter.
    """
    if os.name == "nt":
        candidate = backend_dir / ".venv" / "Scripts" / "python.exe"
    else:
        candidate = backend_dir / ".venv" / "bin" / "python"

    if candidate.exists():
        return str(candidate)
    return sys.executable


def execute_backend_job(job_name: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    backend_dir = _backend_dir()
    python_executable = _backend_python(backend_dir)
    env = os.environ.copy()
    env["PYTHONPATH"] = str(backend_dir)
    env["PYTHONUNBUFFERED"] = "1"

    command = [
        python_executable,
        "-m",
        "app.worker_bridge.runner",
        job_name,
        "--payload",
        json.dumps(payload or {}),
    ]

    logger.info(
        "Executing backend bridge job",
        job_name=job_name,
        backend_dir=str(backend_dir),
        python_executable=python_executable,
    )
    completed = subprocess.run(
        command,
        cwd=str(backend_dir),
        env=env,
        capture_output=True,
        text=True,
        check=False,
        stdin=subprocess.DEVNULL,
        start_new_session=True,
    )

    stdout = (completed.stdout or "").strip()
    stderr = (completed.stderr or "").strip()

    if completed.returncode != 0:
        logger.error(
            "Backend bridge job failed",
            job_name=job_name,
            returncode=completed.returncode,
            stderr=stderr,
            stdout=stdout,
        )
        raise RuntimeError(stderr or stdout or f"Backend bridge job failed: {job_name}")

    if not stdout:
        return {"message": f"Backend bridge job completed: {job_name}"}

    try:
        return json.loads(stdout.splitlines()[-1])
    except json.JSONDecodeError:
        logger.warning(
            "Backend bridge job produced non-JSON output",
            job_name=job_name,
            stdout=stdout,
        )
        return {"message": stdout}
