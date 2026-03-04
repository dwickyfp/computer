"""
Compute test suite — shared configuration, environment injection, and fixtures.

Strategy
--------
* `pytest_configure()` runs before any import of compute modules, injecting
  required environment variables.
* Heavy JVM / Java dependencies (pydbzengine, jpype) are stubbed in sys.modules
  so tests can import event_handler and engine modules without a real JVM.
* Factories produce lightweight CDCRecord, Source, Destination, Pipeline, and
  PipelineDestination instances for use in unit tests.
"""

import os
import sys
import base64
import pytest
from unittest.mock import MagicMock


# ---------------------------------------------------------------------------
# Environment injection — must happen before any compute module is imported
# ---------------------------------------------------------------------------


def pytest_configure(config):
    """Inject required environment variables before app imports."""
    _raw_key = b"compute_test_key_32bytes_xxx!!!!"  # exactly 32 bytes
    _b64_key = base64.b64encode(_raw_key).decode()

    os.environ.setdefault("CREDENTIAL_ENCRYPTION_KEY", _b64_key)
    os.environ.setdefault(
        "CONFIG_DATABASE_URL", "postgresql://test:test@localhost:5433/test_rosetta"
    )
    os.environ.setdefault("REDIS_URL", "redis://localhost:6379/0")
    os.environ.setdefault("ROSETTA_TIMEZONE", "Asia/Jakarta")
    os.environ.setdefault("CHAIN_ENABLED", "true")
    os.environ.setdefault("PIPELINE_POOL_MAX_CONN", "5")

    # Stub heavy JVM/Java dependencies so importing engine/event_handler modules
    # does NOT attempt to start a JVM during test collection.
    _stub_jvm_deps()

    # Ensure compute/ is on sys.path for all tests
    compute_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if compute_root not in sys.path:
        sys.path.insert(0, compute_root)


def _stub_jvm_deps() -> None:
    """
    Insert lightweight MagicMock stubs for pydbzengine and jpype into
    sys.modules so any `import pydbzengine` inside compute code resolves
    to a mock rather than attempting to start the JVM.
    """
    for mod_name in (
        "pydbzengine",
        "jpype",
        "jpype.types",
        "jpype._jclass",
    ):
        if mod_name not in sys.modules:
            sys.modules[mod_name] = MagicMock()

    # ChangeEvent and BasePythonChangeHandler must be real enough that
    # CDCEventHandler can inherit from BasePythonChangeHandler.
    class _FakeBasePythonChangeHandler:
        pass

    mock_pdbz = sys.modules["pydbzengine"]
    mock_pdbz.BasePythonChangeHandler = _FakeBasePythonChangeHandler
    mock_pdbz.ChangeEvent = MagicMock
    mock_pdbz.DebeziumJsonEngine = MagicMock


# ---------------------------------------------------------------------------
# CDCRecord factory
# ---------------------------------------------------------------------------


def make_cdc_record(
    operation: str = "c",
    table_name: str = "public.orders",
    key: dict = None,
    value: dict = None,
    timestamp: int = 1700000000000,
):
    """Build a CDCRecord for testing."""
    from destinations.base import CDCRecord

    return CDCRecord(
        operation=operation,
        table_name=table_name,
        key=key or {"id": 1},
        value=value or {"id": 1, "name": "test", "amount": 100},
        timestamp=timestamp,
    )


# ---------------------------------------------------------------------------
# Source / Destination / Pipeline factories
# ---------------------------------------------------------------------------


def make_source(**overrides) -> dict:
    """Return a dict suitable for Source.from_dict()."""
    base = dict(
        id=1,
        name="test-source",
        pg_host="localhost",
        pg_port=5434,
        pg_database="sourcedb",
        pg_username="replication_user",
        pg_password="secret",
        publication_name="dbz_publication",
        replication_name="dbz_slot",
        is_publication_enabled=True,
        is_replication_enabled=True,
        total_tables=3,
    )
    base.update(overrides)
    return base


def make_destination(**overrides) -> dict:
    """Return a dict suitable for Destination.from_dict()."""
    base = dict(
        id=1,
        name="test-destination",
        type="POSTGRES",
        config={
            "host": "localhost",
            "port": 5435,
            "database": "targetdb",
            "user": "target_user",
            "password": "target_secret",
        },
    )
    base.update(overrides)
    return base


def make_pipeline(**overrides) -> object:
    """Build a minimal Pipeline-like SimpleNamespace for testing."""
    from types import SimpleNamespace

    base = SimpleNamespace(
        id=1,
        name="test-pipeline",
        source_id=1,
        status="START",
        source_type="POSTGRES",
        chain_client_id=None,
        source=None,
        destinations=[],
    )
    for k, v in overrides.items():
        setattr(base, k, v)
    return base


def make_pipeline_destination(dest_id: int = 1, table_syncs=None) -> object:
    """Build a minimal PipelineDestination-like SimpleNamespace."""
    from types import SimpleNamespace

    ts = table_syncs or []
    return SimpleNamespace(
        id=1,
        pipeline_id=1,
        destination_id=dest_id,
        table_syncs=ts,
    )


def make_table_sync(
    table_name: str = "orders",
    table_name_target: str = "orders",
    filter_sql: str = None,
    custom_sql: str = None,
) -> object:
    """Build a minimal PipelineDestinationTableSync-like SimpleNamespace."""
    from types import SimpleNamespace

    return SimpleNamespace(
        id=1,
        table_name=table_name,
        table_name_target=table_name_target,
        filter_sql=filter_sql,
        custom_sql=custom_sql,
    )


# ---------------------------------------------------------------------------
# Pytest fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def cdc_record():
    """Sample CDCRecord for tests."""
    return make_cdc_record()


@pytest.fixture
def sample_pipeline():
    """Pipeline SimpleNamespace for tests."""
    return make_pipeline()
