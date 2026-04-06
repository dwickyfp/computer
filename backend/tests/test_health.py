"""
E2E tests for GET /api/v1/health endpoint.

The health check orchestrates 4 parallel async checks (DB, Redis, Compute, Worker).
Overall status is "healthy" only when BOTH database AND redis are up.
Worker check short-circuits to (False, {}) when WORKER_ENABLED=false (set in conftest).

Patch locations:
  check_database_health → app.api.v1.endpoints.health.check_database_health
  RedisClient           → app.infrastructure.redis.RedisClient
  httpx.AsyncClient     → app.api.v1.endpoints.health.httpx.AsyncClient
  cache globals         → new=None / new=0  (bypass 2-second response cache)
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from fastapi.testclient import TestClient

from app.main import app


@pytest.fixture
def client():
    return TestClient(app)


# =============================================================================
# Helpers — reusable context-manager factories
# =============================================================================


def _mock_redis_ok():
    mock_inst = MagicMock()
    mock_inst.ping.return_value = True
    return patch(
        "app.infrastructure.redis.RedisClient.get_instance", return_value=mock_inst
    )


def _mock_redis_fail():
    return patch(
        "app.infrastructure.redis.RedisClient.get_instance",
        side_effect=Exception("Redis unavailable"),
    )


def _mock_compute_ok():
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"status": "healthy"}
    mock_inst = AsyncMock()
    mock_inst.__aenter__.return_value = mock_inst
    mock_inst.get.return_value = mock_resp
    return patch(
        "app.api.v1.endpoints.health.httpx.AsyncClient", return_value=mock_inst
    )


def _mock_compute_fail():
    mock_inst = AsyncMock()
    mock_inst.__aenter__.return_value = mock_inst
    mock_inst.get.side_effect = Exception("connection refused")
    return patch(
        "app.api.v1.endpoints.health.httpx.AsyncClient", return_value=mock_inst
    )


# Disable in-memory cache for every test
_NO_CACHE = (
    patch("app.api.v1.endpoints.health._health_cache", new=None),
    patch("app.api.v1.endpoints.health._health_cache_time", new=0),
)


# =============================================================================
# TestHealthResponseShape
# =============================================================================


class TestHealthResponseShape:
    def test_returns_200_always(self, client):
        with (
            patch(
                "app.api.v1.endpoints.health.check_database_health", return_value=True
            ),
            _mock_redis_ok(),
            _mock_compute_fail(),
            patch("app.api.v1.endpoints.health._health_cache", new=None),
            patch("app.api.v1.endpoints.health._health_cache_time", new=0),
        ):
            resp = client.get("/api/v1/health")
        assert resp.status_code == 200

    def test_schema_has_required_keys(self, client):
        with (
            patch(
                "app.api.v1.endpoints.health.check_database_health", return_value=True
            ),
            _mock_redis_ok(),
            _mock_compute_fail(),
            patch("app.api.v1.endpoints.health._health_cache", new=None),
            patch("app.api.v1.endpoints.health._health_cache_time", new=0),
        ):
            data = client.get("/api/v1/health").json()
        for key in ("status", "version", "timestamp", "checks"):
            assert key in data, f"Missing key: {key}"

    def test_checks_dict_has_all_sub_checks(self, client):
        with (
            patch(
                "app.api.v1.endpoints.health.check_database_health", return_value=True
            ),
            _mock_redis_ok(),
            _mock_compute_fail(),
            patch("app.api.v1.endpoints.health._health_cache", new=None),
            patch("app.api.v1.endpoints.health._health_cache_time", new=0),
        ):
            data = client.get("/api/v1/health").json()
        for key in ("database", "redis", "compute", "worker"):
            assert key in data["checks"], f"Missing check key: {key}"

    def test_version_is_non_empty_string(self, client):
        with (
            patch(
                "app.api.v1.endpoints.health.check_database_health", return_value=True
            ),
            _mock_redis_ok(),
            _mock_compute_fail(),
            patch("app.api.v1.endpoints.health._health_cache", new=None),
            patch("app.api.v1.endpoints.health._health_cache_time", new=0),
        ):
            data = client.get("/api/v1/health").json()
        assert isinstance(data["version"], str) and len(data["version"]) > 0


# =============================================================================
# TestHealthStatus
# =============================================================================


class TestHealthStatus:
    def test_healthy_when_db_and_redis_ok(self, client):
        """status='healthy' requires BOTH database and redis to pass."""
        with (
            patch(
                "app.api.v1.endpoints.health.check_database_health", return_value=True
            ),
            _mock_redis_ok(),
            _mock_compute_fail(),
            patch("app.api.v1.endpoints.health._health_cache", new=None),
            patch("app.api.v1.endpoints.health._health_cache_time", new=0),
        ):
            data = client.get("/api/v1/health").json()
        assert data["status"] == "healthy"
        assert data["checks"]["database"] is True
        assert data["checks"]["redis"] is True

    def test_unhealthy_when_db_fails(self, client):
        with (
            patch(
                "app.api.v1.endpoints.health.check_database_health", return_value=False
            ),
            _mock_redis_ok(),
            _mock_compute_fail(),
            patch("app.api.v1.endpoints.health._health_cache", new=None),
            patch("app.api.v1.endpoints.health._health_cache_time", new=0),
        ):
            data = client.get("/api/v1/health").json()
        assert data["status"] == "unhealthy"
        assert data["checks"]["database"] is False

    def test_unhealthy_when_redis_fails(self, client):
        with (
            patch(
                "app.api.v1.endpoints.health.check_database_health", return_value=True
            ),
            _mock_redis_fail(),
            _mock_compute_fail(),
            patch("app.api.v1.endpoints.health._health_cache", new=None),
            patch("app.api.v1.endpoints.health._health_cache_time", new=0),
        ):
            data = client.get("/api/v1/health").json()
        assert data["status"] == "unhealthy"
        assert data["checks"]["redis"] is False

    def test_unhealthy_when_both_fail(self, client):
        with (
            patch(
                "app.api.v1.endpoints.health.check_database_health", return_value=False
            ),
            _mock_redis_fail(),
            _mock_compute_fail(),
            patch("app.api.v1.endpoints.health._health_cache", new=None),
            patch("app.api.v1.endpoints.health._health_cache_time", new=0),
        ):
            data = client.get("/api/v1/health").json()
        assert data["status"] == "unhealthy"
        assert data["checks"]["database"] is False
        assert data["checks"]["redis"] is False


# =============================================================================
# TestComputeCheck
# =============================================================================


class TestComputeCheck:
    def test_compute_true_when_returns_healthy(self, client):
        with (
            patch(
                "app.api.v1.endpoints.health.check_database_health", return_value=True
            ),
            _mock_redis_ok(),
            _mock_compute_ok(),
            patch("app.api.v1.endpoints.health._health_cache", new=None),
            patch("app.api.v1.endpoints.health._health_cache_time", new=0),
        ):
            data = client.get("/api/v1/health").json()
        assert data["checks"]["compute"] is True

    def test_compute_false_when_unreachable(self, client):
        with (
            patch(
                "app.api.v1.endpoints.health.check_database_health", return_value=True
            ),
            _mock_redis_ok(),
            _mock_compute_fail(),
            patch("app.api.v1.endpoints.health._health_cache", new=None),
            patch("app.api.v1.endpoints.health._health_cache_time", new=0),
        ):
            data = client.get("/api/v1/health").json()
        assert data["checks"]["compute"] is False

    def test_compute_false_when_status_unhealthy(self, client):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"status": "unhealthy"}
        mock_inst = AsyncMock()
        mock_inst.__aenter__.return_value = mock_inst
        mock_inst.get.return_value = mock_resp

        with (
            patch(
                "app.api.v1.endpoints.health.check_database_health", return_value=True
            ),
            _mock_redis_ok(),
            patch(
                "app.api.v1.endpoints.health.httpx.AsyncClient", return_value=mock_inst
            ),
            patch("app.api.v1.endpoints.health._health_cache", new=None),
            patch("app.api.v1.endpoints.health._health_cache_time", new=0),
        ):
            data = client.get("/api/v1/health").json()
        assert data["checks"]["compute"] is False

    def test_compute_false_when_returns_503(self, client):
        mock_resp = MagicMock()
        mock_resp.status_code = 503
        mock_resp.json.return_value = {"status": "healthy"}
        mock_inst = AsyncMock()
        mock_inst.__aenter__.return_value = mock_inst
        mock_inst.get.return_value = mock_resp

        with (
            patch(
                "app.api.v1.endpoints.health.check_database_health", return_value=True
            ),
            _mock_redis_ok(),
            patch(
                "app.api.v1.endpoints.health.httpx.AsyncClient", return_value=mock_inst
            ),
            patch("app.api.v1.endpoints.health._health_cache", new=None),
            patch("app.api.v1.endpoints.health._health_cache_time", new=0),
        ):
            data = client.get("/api/v1/health").json()
        assert data["checks"]["compute"] is False


# =============================================================================
# TestWorkerCheck
# =============================================================================


class TestWorkerCheck:
    def test_worker_false_when_worker_disabled(self, client):
        """WORKER_ENABLED=false (set in conftest) → worker check is always False."""
        with (
            patch(
                "app.api.v1.endpoints.health.check_database_health", return_value=True
            ),
            _mock_redis_ok(),
            _mock_compute_fail(),
            patch("app.api.v1.endpoints.health._health_cache", new=None),
            patch("app.api.v1.endpoints.health._health_cache_time", new=0),
        ):
            data = client.get("/api/v1/health").json()
        assert data["checks"]["worker"] is False
