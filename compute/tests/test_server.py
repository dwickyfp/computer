"""
E2E tests for the FastAPI server endpoints in compute/server.py.

Tests:
  - GET /health                → 200 {"status": "healthy"}
  - GET /health/pool           → 200 (pool info)
  - GET /chain/health          → 200 / 503 based on CHAIN_ENABLED
  - GET /chain/tables          → 200 list
  - POST /chain/schema         → 200 (schema store)
  - POST /chain/ingest/{...}   → 200 / 413 / 429 / 503
"""

import sys
import os
import pytest
from unittest.mock import MagicMock, patch

# Ensure compute root is importable
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi.testclient import TestClient
from server import app, _rate_limit_windows

client = TestClient(app)


# ---------------------------------------------------------------------------
# /health
# ---------------------------------------------------------------------------


class TestHealthEndpoint:
    def test_health_returns_200(self):
        resp = client.get("/health")
        assert resp.status_code == 200

    def test_health_returns_healthy_status(self):
        resp = client.get("/health")
        assert resp.json()["status"] == "healthy"


# ---------------------------------------------------------------------------
# /health/pool
# ---------------------------------------------------------------------------


class TestPoolHealthEndpoint:
    def test_pool_health_returns_200(self):
        with patch("server.get_pool_health", return_value={"status": "ok", "total": 5}):
            resp = client.get("/health/pool")
        assert resp.status_code == 200

    def test_pool_health_returns_dict(self):
        with patch("server.get_pool_health", return_value={"status": "ok"}):
            data = client.get("/health/pool").json()
        assert isinstance(data, dict)


# ---------------------------------------------------------------------------
# /chain/health
# ---------------------------------------------------------------------------


class TestChainHealthEndpoint:
    def test_chain_health_enabled_returns_200(self):
        """When CHAIN_ENABLED=true chain/health returns 200."""
        resp = client.get("/chain/health")
        assert resp.status_code == 200

    def test_chain_health_disabled_returns_503(self):
        """When chain is disabled chain/health returns 503."""
        mock_cfg = MagicMock()
        mock_cfg.chain.enabled = False
        with patch("server.get_config", return_value=mock_cfg):
            resp = client.get("/chain/health")
        assert resp.status_code == 503

    def test_chain_health_disabled_message(self):
        mock_cfg = MagicMock()
        mock_cfg.chain.enabled = False
        with patch("server.get_config", return_value=mock_cfg):
            data = client.get("/chain/health").json()
        assert data["status"] == "disabled"


# ---------------------------------------------------------------------------
# POST /chain/ingest/{chain_id}/{table_name}
# ---------------------------------------------------------------------------


class TestChainIngestEndpoint:
    def _make_body(self):
        """Build a tiny valid Arrow IPC body for ingest tests."""
        import pyarrow as pa
        from io import BytesIO

        schema = pa.schema([pa.field("id", pa.int64()), pa.field("name", pa.string())])
        batch = pa.record_batch([[1, 2], ["a", "b"]], schema=schema)
        sink = BytesIO()
        writer = pa.ipc.new_stream(sink, schema)
        writer.write_batch(batch)
        writer.close()
        return sink.getvalue()

    def test_ingest_chains_disabled_returns_503(self):
        mock_cfg = MagicMock()
        mock_cfg.chain.enabled = False
        with patch("server.get_config", return_value=mock_cfg):
            resp = client.post(
                "/chain/ingest/mychain/orders",
                content=b"data",
                headers={"Content-Type": "application/octet-stream"},
            )
        assert resp.status_code == 503

    def test_ingest_payload_too_large_returns_413(self):
        """Bodies > _MAX_CHAIN_BODY_SIZE are rejected with 413."""
        big_body = b"x" * (50 * 1024 * 1024 + 1)
        mock_cfg = MagicMock()
        mock_cfg.chain.enabled = True

        with patch("server.get_config", return_value=mock_cfg):
            resp = client.post(
                "/chain/ingest/mychain/orders",
                content=big_body,
                headers={"Content-Type": "application/octet-stream"},
            )
        assert resp.status_code == 413

    def test_ingest_rate_limit_exceeded_returns_429(self):
        """Exceeding rate limit returns 429."""
        import time

        mock_cfg = MagicMock()
        mock_cfg.chain.enabled = True

        # Fill the sliding window beyond the limit
        chain_id = "rate_test_chain_unique"
        now = time.time()
        _rate_limit_windows[chain_id] = [now] * 200  # well above 60/min

        with patch("server.get_config", return_value=mock_cfg):
            resp = client.post(
                f"/chain/ingest/{chain_id}/orders",
                content=b"data",
                headers={"Content-Type": "application/octet-stream"},
            )

        # Clean up
        _rate_limit_windows.pop(chain_id, None)
        assert resp.status_code == 429

    def test_ingest_valid_arrow_data_returns_200(self):
        """Valid Arrow IPC payload is ingested successfully."""
        body = self._make_body()
        mock_mgr = MagicMock()
        mock_mgr.ingest_arrow_ipc.return_value = 2

        mock_cfg = MagicMock()
        mock_cfg.chain.enabled = True

        with (
            patch("server.get_config", return_value=mock_cfg),
            patch("server._get_ingest_manager", return_value=mock_mgr),
        ):
            resp = client.post(
                "/chain/ingest/mychain/orders",
                content=body,
                headers={"Content-Type": "application/octet-stream"},
            )
        assert resp.status_code == 200
        assert resp.json()["records_ingested"] == 2

    def test_ingest_invalid_arrow_data_returns_400(self):
        """Garbage payload yields 400."""
        mock_mgr = MagicMock()
        mock_mgr.ingest_arrow_ipc.side_effect = ValueError("Invalid Arrow IPC data")

        mock_cfg = MagicMock()
        mock_cfg.chain.enabled = True

        with (
            patch("server.get_config", return_value=mock_cfg),
            patch("server._get_ingest_manager", return_value=mock_mgr),
        ):
            resp = client.post(
                "/chain/ingest/mychain/orders",
                content=b"not-arrow-data",
                headers={"Content-Type": "application/octet-stream"},
            )
        assert resp.status_code == 400


# ---------------------------------------------------------------------------
# POST /chain/schema
# ---------------------------------------------------------------------------


class TestChainSchemaEndpoint:
    def test_schema_store_disabled_returns_503(self):
        mock_cfg = MagicMock()
        mock_cfg.chain.enabled = False
        with patch("server.get_config", return_value=mock_cfg):
            resp = client.post(
                "/chain/schema",
                json={"chain_client_id": "c1", "table_name": "orders", "schema": {}},
            )
        assert resp.status_code == 503

    def test_schema_store_success_returns_200(self):
        mock_mgr = MagicMock()
        mock_cfg = MagicMock()
        mock_cfg.chain.enabled = True

        with (
            patch("server.get_config", return_value=mock_cfg),
            patch("server._get_schema_manager", return_value=mock_mgr),
        ):
            resp = client.post(
                "/chain/schema",
                json={
                    "chain_client_id": "c1",
                    "table_name": "orders",
                    "schema": {"fields": []},
                },
            )
        assert resp.status_code == 200
