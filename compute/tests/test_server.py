"""
Focused tests for the compute FastAPI surface.
"""

import os
import sys
from unittest.mock import patch

from fastapi.testclient import TestClient

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.runtime_health import mark_worker, _workers
from server import app


client = TestClient(app)


def setup_function():
    _workers.clear()


class TestHealthEndpoint:
    def test_health_returns_healthy_with_worker_registry(self):
        mark_worker("api_server", "running", critical=True)
        mark_worker("pipeline_manager", "running", critical=True)

        resp = client.get("/health")

        assert resp.status_code == 200
        payload = resp.json()
        assert payload["status"] == "healthy"
        assert payload["workers"]["api_server"]["status"] == "running"

    def test_health_returns_503_when_critical_worker_failed(self):
        mark_worker("api_server", "running", critical=True)
        mark_worker(
            "pipeline_manager",
            "failed",
            critical=True,
            message="worker exited unexpectedly",
        )

        resp = client.get("/health")

        assert resp.status_code == 503
        payload = resp.json()
        assert payload["status"] == "unhealthy"
        assert payload["workers"]["pipeline_manager"]["message"] == "worker exited unexpectedly"


class TestPoolHealthEndpoint:
    def test_pool_health_returns_pool_payload(self):
        with patch("core.database.get_pool_health", return_value={"healthy": True, "size": 3}):
            resp = client.get("/health/pool")

        assert resp.status_code == 200
        assert resp.json() == {"healthy": True, "size": 3}
