"""
Shared test fixtures for backend integration tests.

Strategy
--------
- All endpoint tests mock the *service layer* via app.dependency_overrides so
  no real database or Redis connection is required.
- pytest_configure() injects required Settings env-vars before the FastAPI app
  module is first imported, so Pydantic BaseSettings validation passes.
- SimpleNamespace helper factories build fake ORM-like objects that satisfy
  Pydantic v2 `from_attributes=True` / `from_orm()` in each response schema.
"""

import os
import base64
from datetime import datetime
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

# ─── Inject env-vars before Settings / app are imported ──────────────────────


def pytest_configure(config):
    """Set mandatory env-vars so Pydantic Settings validation passes."""
    # 32-byte key (ASCII) — deterministic so AES-256-GCM round-trips work
    _raw_key = b"rosetta_test_key_32bytes_xxxx!!!!"  # exactly 32 bytes
    _b64_key = base64.b64encode(_raw_key).decode()

    os.environ.setdefault(
        "DATABASE_URL", "postgresql://test:test@localhost:5433/test_rosetta"
    )
    os.environ.setdefault("SECRET_KEY", "rosetta-unit-test-secret-key-minimum32!!")
    os.environ.setdefault("CREDENTIAL_ENCRYPTION_KEY", _b64_key)
    os.environ.setdefault("REDIS_URL", "redis://localhost:6379/0")
    os.environ.setdefault("APP_ENV", "test")
    os.environ.setdefault("WORKER_ENABLED", "false")


# ─── Late import of app (after env-vars are set) ─────────────────────────────

from app.main import app  # noqa: E402


# ─── Shared timestamp constant ───────────────────────────────────────────────

NOW = datetime(2025, 1, 1, 12, 0, 0)


# ─── ORM-object factory helpers ──────────────────────────────────────────────


def make_source_ns(**overrides) -> SimpleNamespace:
    """Build a SimpleNamespace that satisfies SourceResponse.from_orm()."""
    defaults: dict[str, Any] = dict(
        id=1,
        name="test-source",
        pg_host="localhost",
        pg_port=5432,
        pg_database="testdb",
        pg_username="replication_user",
        pg_password=None,
        publication_name="dbz_publication",
        replication_name="dbz_slot",
        is_publication_enabled=False,
        is_replication_enabled=False,
        last_check_replication_publication=None,
        total_tables=0,
        created_at=NOW,
        updated_at=NOW,
    )
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def make_destination_ns(**overrides) -> SimpleNamespace:
    """Build a SimpleNamespace that satisfies DestinationResponse.from_orm()."""
    defaults: dict[str, Any] = dict(
        id=1,
        name="test-destination",
        type="SNOWFLAKE",
        config={"account": "xy12345", "user": "ETL_USER"},
        total_tables=0,
        last_table_check_at=None,
        chain_client_id=None,
        # is_used_in_active_pipeline has default=False in DestinationResponse — omitted
        created_at=NOW,
        updated_at=NOW,
    )
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def make_pipeline_metadata_ns(**overrides) -> SimpleNamespace:
    """Build a SimpleNamespace for PipelineMetadataResponse."""
    defaults: dict[str, Any] = dict(
        id=1,
        pipeline_id=1,
        status="PAUSED",
        last_error=None,
        last_error_at=None,
        last_start_at=None,
        created_at=NOW,
        updated_at=NOW,
    )
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def make_pipeline_ns(**overrides) -> SimpleNamespace:
    """Build a SimpleNamespace that satisfies PipelineResponse.from_orm()."""
    defaults: dict[str, Any] = dict(
        id=1,
        name="test-pipeline",
        source_type="POSTGRES",
        source_id=1,
        chain_client_id=None,
        catalog_database_id=None,
        catalog_table_id=None,
        status="PAUSE",
        ready_refresh=False,
        last_refresh_at=None,
        source=make_source_ns(),
        chain_client=None,
        destinations=[],
        pipeline_metadata=make_pipeline_metadata_ns(),
        pipeline_progress=None,
        created_at=NOW,
        updated_at=NOW,
    )
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def make_tag_ns(**overrides) -> SimpleNamespace:
    """Build a SimpleNamespace that satisfies TagResponse.from_orm()."""
    defaults: dict[str, Any] = dict(
        id=1,
        tag="high-priority",
        created_at=NOW,
        updated_at=NOW,
    )
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def make_backfill_ns(**overrides) -> SimpleNamespace:
    """Build a SimpleNamespace that satisfies BackfillJobResponse.from_orm()."""
    defaults: dict[str, Any] = dict(
        id=1,
        pipeline_id=1,
        table_name="public.users",
        filter_sql=None,
        status="PENDING",
        total_records=0,
        processed_records=0,
        failed_records=0,
        last_pk_value=None,
        error_message=None,
        destination_id=None,
        started_at=None,
        completed_at=None,
        created_at=NOW,
        updated_at=NOW,
    )
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


# ─── Legacy / simple fixtures ────────────────────────────────────────────────


@pytest.fixture
def basic_client():
    """FastAPI test client WITHOUT lifespan (no real DB needed)."""
    return TestClient(app)


@pytest.fixture
def mock_db_session():
    """Mock SQLAlchemy Session for low-level tests."""
    session = MagicMock()
    session.commit = MagicMock()
    session.rollback = MagicMock()
    session.close = MagicMock()
    session.flush = MagicMock()
    session.refresh = MagicMock()
    return session
