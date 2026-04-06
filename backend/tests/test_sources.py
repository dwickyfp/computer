"""
E2E tests for /api/v1/sources endpoints.

Covers:
  - POST   /sources              (create)
  - GET    /sources              (list)
  - GET    /sources/{id}         (get by id)
  - GET    /sources/{id}/details (get details)
  - PUT    /sources/{id}         (update)
  - DELETE /sources/{id}         (delete)
  - POST   /sources/test_connection

All DB / service interactions are mocked via app.dependency_overrides so no
real database connection is required.
"""

import pytest
from unittest.mock import MagicMock
from fastapi.testclient import TestClient

from app.main import app
from app.api.deps import get_source_service, get_source_service_readonly
from app.core.exceptions import DuplicateEntityError, EntityNotFoundError

from tests.conftest import make_source_ns


# ─── Fixture: inject mock service ────────────────────────────────────────────


@pytest.fixture
def mock_svc():
    return MagicMock()


@pytest.fixture
def client(mock_svc):
    """TestClient with both source deps wired to the same mock service."""
    app.dependency_overrides[get_source_service] = lambda: mock_svc
    app.dependency_overrides[get_source_service_readonly] = lambda: mock_svc
    yield TestClient(app)
    app.dependency_overrides.clear()


# ─── Shared payload ───────────────────────────────────────────────────────────

VALID_PAYLOAD = {
    "name": "prod-postgres",
    "pg_host": "db.example.com",
    "pg_port": 5432,
    "pg_database": "myapp",
    "pg_username": "replication_user",
    "pg_password": "s3cur3pass",
    "publication_name": "dbz_publication",
    "replication_name": "dbz_slot",
}


# =============================================================================
# POST /sources
# =============================================================================


class TestCreateSource:
    def test_success_returns_201(self, client, mock_svc):
        mock_svc.create_source.return_value = make_source_ns(name="prod-postgres")
        resp = client.post("/api/v1/sources", json=VALID_PAYLOAD)
        assert resp.status_code == 201
        data = resp.json()
        assert data["name"] == "prod-postgres"
        assert data["id"] == 1
        # password must never appear in response
        assert "pg_password" not in data

    def test_name_lowercased(self, client, mock_svc):
        mock_svc.create_source.return_value = make_source_ns(name="prod-postgres")
        payload = {**VALID_PAYLOAD, "name": "PROD-POSTGRES"}
        resp = client.post("/api/v1/sources", json=payload)
        assert resp.status_code == 201
        # validator normalises to lowercase
        mock_svc.create_source.assert_called_once()
        call_arg = mock_svc.create_source.call_args[0][0]
        assert call_arg.name == "prod-postgres"

    def test_duplicate_returns_409(self, client, mock_svc):
        mock_svc.create_source.side_effect = DuplicateEntityError(
            "Source", "name", "prod-postgres"
        )
        resp = client.post("/api/v1/sources", json=VALID_PAYLOAD)
        assert resp.status_code == 409

    def test_invalid_name_with_spaces_returns_422(self, client, mock_svc):
        payload = {**VALID_PAYLOAD, "name": "invalid name"}
        resp = client.post("/api/v1/sources", json=payload)
        assert resp.status_code == 422

    def test_invalid_name_special_chars_returns_422(self, client, mock_svc):
        payload = {**VALID_PAYLOAD, "name": "source@prod"}
        resp = client.post("/api/v1/sources", json=payload)
        assert resp.status_code == 422

    def test_missing_pg_host_returns_422(self, client, mock_svc):
        payload = {k: v for k, v in VALID_PAYLOAD.items() if k != "pg_host"}
        resp = client.post("/api/v1/sources", json=payload)
        assert resp.status_code == 422

    def test_missing_publication_name_returns_422(self, client, mock_svc):
        payload = {k: v for k, v in VALID_PAYLOAD.items() if k != "publication_name"}
        resp = client.post("/api/v1/sources", json=payload)
        assert resp.status_code == 422

    def test_port_out_of_range_returns_422(self, client, mock_svc):
        payload = {**VALID_PAYLOAD, "pg_port": 99999}
        resp = client.post("/api/v1/sources", json=payload)
        assert resp.status_code == 422

    def test_port_zero_returns_422(self, client, mock_svc):
        payload = {**VALID_PAYLOAD, "pg_port": 0}
        resp = client.post("/api/v1/sources", json=payload)
        assert resp.status_code == 422

    def test_default_port_is_5432(self, client, mock_svc):
        mock_svc.create_source.return_value = make_source_ns()
        payload = {k: v for k, v in VALID_PAYLOAD.items() if k != "pg_port"}
        resp = client.post("/api/v1/sources", json=payload)
        assert resp.status_code == 201
        call_arg = mock_svc.create_source.call_args[0][0]
        assert call_arg.pg_port == 5432

    def test_publication_name_with_hyphen_returns_422(self, client, mock_svc):
        """Publication name must be alphanumeric + underscores only (no hyphens)."""
        payload = {**VALID_PAYLOAD, "publication_name": "pub-with-hyphen"}
        resp = client.post("/api/v1/sources", json=payload)
        assert resp.status_code == 422

    def test_service_called_once(self, client, mock_svc):
        mock_svc.create_source.return_value = make_source_ns()
        client.post("/api/v1/sources", json=VALID_PAYLOAD)
        mock_svc.create_source.assert_called_once()


# =============================================================================
# GET /sources
# =============================================================================


class TestListSources:
    def test_returns_200_with_empty_list(self, client, mock_svc):
        mock_svc.list_sources.return_value = []
        resp = client.get("/api/v1/sources")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_returns_list_of_sources(self, client, mock_svc):
        mock_svc.list_sources.return_value = [
            make_source_ns(id=1, name="source-a"),
            make_source_ns(id=2, name="source-b"),
        ]
        resp = client.get("/api/v1/sources")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 2
        assert data[0]["name"] == "source-a"
        assert data[1]["name"] == "source-b"

    def test_pagination_params_forwarded(self, client, mock_svc):
        mock_svc.list_sources.return_value = []
        client.get("/api/v1/sources?skip=10&limit=5")
        mock_svc.list_sources.assert_called_once_with(skip=10, limit=5)

    def test_default_pagination(self, client, mock_svc):
        mock_svc.list_sources.return_value = []
        client.get("/api/v1/sources")
        mock_svc.list_sources.assert_called_once_with(skip=0, limit=100)

    def test_invalid_skip_returns_422(self, client, mock_svc):
        resp = client.get("/api/v1/sources?skip=-1")
        assert resp.status_code == 422

    def test_invalid_limit_returns_422(self, client, mock_svc):
        resp = client.get("/api/v1/sources?limit=9999")
        assert resp.status_code == 422

    def test_password_excluded_from_list(self, client, mock_svc):
        mock_svc.list_sources.return_value = [
            make_source_ns(pg_password="should-be-hidden")
        ]
        resp = client.get("/api/v1/sources")
        assert "pg_password" not in resp.json()[0]


# =============================================================================
# GET /sources/{id}
# =============================================================================


class TestGetSource:
    def test_returns_200_when_found(self, client, mock_svc):
        mock_svc.get_source.return_value = make_source_ns(id=42, name="my-source")
        resp = client.get("/api/v1/sources/42")
        assert resp.status_code == 200
        assert resp.json()["id"] == 42
        assert resp.json()["name"] == "my-source"

    def test_returns_404_when_not_found(self, client, mock_svc):
        mock_svc.get_source.side_effect = EntityNotFoundError("Source", 999)
        resp = client.get("/api/v1/sources/999")
        assert resp.status_code == 404
        assert "999" in resp.json()["message"]

    def test_calls_service_with_correct_id(self, client, mock_svc):
        mock_svc.get_source.return_value = make_source_ns(id=7)
        client.get("/api/v1/sources/7")
        mock_svc.get_source.assert_called_once_with(7)

    def test_response_shape(self, client, mock_svc):
        mock_svc.get_source.return_value = make_source_ns()
        resp = client.get("/api/v1/sources/1")
        data = resp.json()
        expected_keys = {
            "id",
            "name",
            "pg_host",
            "pg_port",
            "pg_database",
            "pg_username",
            "publication_name",
            "replication_name",
            "is_publication_enabled",
            "is_replication_enabled",
            "total_tables",
            "created_at",
            "updated_at",
        }
        assert expected_keys.issubset(set(data.keys()))


# =============================================================================
# PUT /sources/{id}
# =============================================================================


class TestUpdateSource:
    def test_success_returns_200(self, client, mock_svc):
        mock_svc.update_source.return_value = make_source_ns(
            id=1, pg_host="new-host.example.com"
        )
        resp = client.put("/api/v1/sources/1", json={"pg_host": "new-host.example.com"})
        assert resp.status_code == 200
        assert resp.json()["pg_host"] == "new-host.example.com"

    def test_not_found_returns_404(self, client, mock_svc):
        mock_svc.update_source.side_effect = EntityNotFoundError("Source", 999)
        resp = client.put("/api/v1/sources/999", json={"pg_host": "x.y.z"})
        assert resp.status_code == 404

    def test_partial_update_allowed(self, client, mock_svc):
        """Update endpoint accepts any subset of fields (partial update)."""
        mock_svc.update_source.return_value = make_source_ns(pg_port=5433)
        resp = client.put("/api/v1/sources/1", json={"pg_port": 5433})
        assert resp.status_code == 200

    def test_invalid_name_update_returns_422(self, client, mock_svc):
        resp = client.put("/api/v1/sources/1", json={"name": "has spaces"})
        assert resp.status_code == 422

    def test_duplicate_name_update_returns_409(self, client, mock_svc):
        mock_svc.update_source.side_effect = DuplicateEntityError(
            "Source", "name", "existing-source"
        )
        resp = client.put("/api/v1/sources/1", json={"name": "existing-source"})
        assert resp.status_code == 409


# =============================================================================
# DELETE /sources/{id}
# =============================================================================


class TestDeleteSource:
    def test_success_returns_204(self, client, mock_svc):
        mock_svc.delete_source.return_value = None
        resp = client.delete("/api/v1/sources/1")
        assert resp.status_code == 204
        assert resp.content == b""

    def test_not_found_returns_404(self, client, mock_svc):
        mock_svc.delete_source.side_effect = EntityNotFoundError("Source", 999)
        resp = client.delete("/api/v1/sources/999")
        assert resp.status_code == 404

    def test_calls_service_with_correct_id(self, client, mock_svc):
        mock_svc.delete_source.return_value = None
        client.delete("/api/v1/sources/5")
        mock_svc.delete_source.assert_called_once_with(5)


# =============================================================================
# POST /sources/test_connection
# =============================================================================


class TestTestConnection:
    CONNECTION_PAYLOAD = {
        "pg_host": "db.prod.example.com",
        "pg_port": 5432,
        "pg_database": "myapp",
        "pg_username": "user",
        "pg_password": "pass",
    }

    def test_returns_true_when_connection_succeeds(self, client, mock_svc):
        mock_svc.test_connection_config.return_value = True
        resp = client.post(
            "/api/v1/sources/test_connection", json=self.CONNECTION_PAYLOAD
        )
        assert resp.status_code == 200
        assert resp.json() is True

    def test_returns_false_when_connection_fails(self, client, mock_svc):
        mock_svc.test_connection_config.return_value = False
        resp = client.post(
            "/api/v1/sources/test_connection", json=self.CONNECTION_PAYLOAD
        )
        assert resp.status_code == 200
        assert resp.json() is False

    def test_missing_host_returns_422(self, client, mock_svc):
        payload = {k: v for k, v in self.CONNECTION_PAYLOAD.items() if k != "pg_host"}
        resp = client.post("/api/v1/sources/test_connection", json=payload)
        assert resp.status_code == 422

    def test_missing_password_returns_422(self, client, mock_svc):
        payload = {
            k: v for k, v in self.CONNECTION_PAYLOAD.items() if k != "pg_password"
        }
        resp = client.post("/api/v1/sources/test_connection", json=payload)
        assert resp.status_code == 422
