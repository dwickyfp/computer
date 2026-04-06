"""
E2E tests for /api/v1/destinations endpoints.

Covers:
  - POST   /destinations                     (create)
  - GET    /destinations                     (list)
  - GET    /destinations/{id}                (get)
  - PUT    /destinations/{id}                (update)
  - DELETE /destinations/{id}               (delete)
  - POST   /destinations/test-connection     (test connection)
  - POST   /destinations/{id}/duplicate      (duplicate)
  - GET    /destinations/{id}/schema         (get schema)
  - GET    /destinations/{id}/tables         (get cached table list)
"""

import pytest
from unittest.mock import MagicMock
from fastapi.testclient import TestClient

from app.main import app
from app.api.deps import get_destination_service, get_destination_service_readonly
from app.core.exceptions import DuplicateEntityError, EntityNotFoundError

from tests.conftest import make_destination_ns


# ─── Fixture ─────────────────────────────────────────────────────────────────


@pytest.fixture
def mock_svc():
    return MagicMock()


@pytest.fixture
def client(mock_svc):
    app.dependency_overrides[get_destination_service] = lambda: mock_svc
    app.dependency_overrides[get_destination_service_readonly] = lambda: mock_svc
    yield TestClient(app)
    app.dependency_overrides.clear()


# ─── Shared payloads ─────────────────────────────────────────────────────────

VALID_PAYLOAD = {
    "name": "snowflake-prod",
    "type": "SNOWFLAKE",
    "config": {
        "account": "xy12345.us-east-1",
        "user": "ETL_USER",
        "database": "ANALYTICS",
        "schema": "RAW_DATA",
        "warehouse": "COMPUTE_WH",
    },
}


# =============================================================================
# POST /destinations
# =============================================================================


class TestCreateDestination:
    def test_success_returns_201(self, client, mock_svc):
        mock_svc.create_destination.return_value = make_destination_ns(
            name="snowflake-prod", type="SNOWFLAKE"
        )
        resp = client.post("/api/v1/destinations", json=VALID_PAYLOAD)
        assert resp.status_code == 201
        data = resp.json()
        assert data["name"] == "snowflake-prod"
        assert data["type"] == "SNOWFLAKE"

    def test_name_lowercased(self, client, mock_svc):
        mock_svc.create_destination.return_value = make_destination_ns(
            name="snowflake-prod"
        )
        payload = {**VALID_PAYLOAD, "name": "Snowflake-PROD"}
        resp = client.post("/api/v1/destinations", json=payload)
        assert resp.status_code == 201
        call_arg = mock_svc.create_destination.call_args[0][0]
        assert call_arg.name == "snowflake-prod"

    def test_type_uppercased(self, client, mock_svc):
        mock_svc.create_destination.return_value = make_destination_ns(type="SNOWFLAKE")
        payload = {**VALID_PAYLOAD, "type": "snowflake"}
        resp = client.post("/api/v1/destinations", json=payload)
        assert resp.status_code == 201
        call_arg = mock_svc.create_destination.call_args[0][0]
        assert call_arg.type == "SNOWFLAKE"

    def test_default_type_is_snowflake(self, client, mock_svc):
        mock_svc.create_destination.return_value = make_destination_ns()
        payload = {k: v for k, v in VALID_PAYLOAD.items() if k != "type"}
        resp = client.post("/api/v1/destinations", json=payload)
        assert resp.status_code == 201
        call_arg = mock_svc.create_destination.call_args[0][0]
        assert call_arg.type == "SNOWFLAKE"

    def test_duplicate_returns_409(self, client, mock_svc):
        mock_svc.create_destination.side_effect = DuplicateEntityError(
            "Destination", "name", "snowflake-prod"
        )
        resp = client.post("/api/v1/destinations", json=VALID_PAYLOAD)
        assert resp.status_code == 409

    def test_invalid_name_with_spaces_returns_422(self, client, mock_svc):
        payload = {**VALID_PAYLOAD, "name": "snowflake prod"}
        resp = client.post("/api/v1/destinations", json=payload)
        assert resp.status_code == 422

    def test_invalid_name_special_chars_returns_422(self, client, mock_svc):
        payload = {**VALID_PAYLOAD, "name": "dest@prod"}
        resp = client.post("/api/v1/destinations", json=payload)
        assert resp.status_code == 422

    def test_sensitive_config_keys_masked_in_response(self, client, mock_svc):
        """private_key_passphrase must never appear in the JSON response."""
        dest = make_destination_ns(
            config={
                "account": "xy12345",
                "user": "ETL_USER",
                "private_key_passphrase": "super-secret",
            }
        )
        mock_svc.create_destination.return_value = dest
        resp = client.post("/api/v1/destinations", json=VALID_PAYLOAD)
        assert resp.status_code == 201
        config = resp.json()["config"]
        assert "private_key_passphrase" not in config

    def test_empty_config_allowed(self, client, mock_svc):
        mock_svc.create_destination.return_value = make_destination_ns(config={})
        payload = {**VALID_PAYLOAD, "config": {}}
        resp = client.post("/api/v1/destinations", json=payload)
        assert resp.status_code == 201

    def test_postgres_type_accepted(self, client, mock_svc):
        mock_svc.create_destination.return_value = make_destination_ns(type="POSTGRES")
        payload = {**VALID_PAYLOAD, "type": "POSTGRES"}
        resp = client.post("/api/v1/destinations", json=payload)
        assert resp.status_code == 201


# =============================================================================
# GET /destinations
# =============================================================================


class TestListDestinations:
    def test_returns_empty_list(self, client, mock_svc):
        mock_svc.list_destinations.return_value = []
        resp = client.get("/api/v1/destinations")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_returns_destinations(self, client, mock_svc):
        mock_svc.list_destinations.return_value = [
            make_destination_ns(id=1, name="dest-a"),
            make_destination_ns(id=2, name="dest-b"),
        ]
        resp = client.get("/api/v1/destinations")
        assert resp.status_code == 200
        assert len(resp.json()) == 2
        names = [d["name"] for d in resp.json()]
        assert "dest-a" in names
        assert "dest-b" in names

    def test_pagination_params_forwarded(self, client, mock_svc):
        mock_svc.list_destinations.return_value = []
        client.get("/api/v1/destinations?skip=20&limit=10")
        mock_svc.list_destinations.assert_called_once_with(skip=20, limit=10)

    def test_invalid_limit_returns_422(self, client, mock_svc):
        resp = client.get("/api/v1/destinations?limit=0")
        assert resp.status_code == 422

    def test_sensitive_config_masked_in_list(self, client, mock_svc):
        mock_svc.list_destinations.return_value = [
            make_destination_ns(config={"password": "secret", "account": "xy12345"})
        ]
        resp = client.get("/api/v1/destinations")
        assert "password" not in resp.json()[0]["config"]


# =============================================================================
# GET /destinations/{id}
# =============================================================================


class TestGetDestination:
    def test_success(self, client, mock_svc):
        mock_svc.get_destination.return_value = make_destination_ns(
            id=5, name="dest-five"
        )
        resp = client.get("/api/v1/destinations/5")
        assert resp.status_code == 200
        assert resp.json()["id"] == 5

    def test_not_found_returns_404(self, client, mock_svc):
        mock_svc.get_destination.side_effect = EntityNotFoundError("Destination", 999)
        resp = client.get("/api/v1/destinations/999")
        assert resp.status_code == 404

    def test_response_contains_required_fields(self, client, mock_svc):
        mock_svc.get_destination.return_value = make_destination_ns()
        resp = client.get("/api/v1/destinations/1")
        data = resp.json()
        for field in ("id", "name", "type", "config", "created_at", "updated_at"):
            assert field in data, f"Field '{field}' missing from response"


# =============================================================================
# PUT /destinations/{id}
# =============================================================================


class TestUpdateDestination:
    def test_success_returns_200(self, client, mock_svc):
        mock_svc.update_destination.return_value = make_destination_ns(
            name="dest-updated"
        )
        resp = client.put("/api/v1/destinations/1", json={"name": "dest-updated"})
        assert resp.status_code == 200
        assert resp.json()["name"] == "dest-updated"

    def test_not_found_returns_404(self, client, mock_svc):
        mock_svc.update_destination.side_effect = EntityNotFoundError("Destination", 99)
        resp = client.put("/api/v1/destinations/99", json={"name": "new-name"})
        assert resp.status_code == 404

    def test_invalid_name_returns_422(self, client, mock_svc):
        resp = client.put("/api/v1/destinations/1", json={"name": "has space"})
        assert resp.status_code == 422

    def test_update_only_config(self, client, mock_svc):
        mock_svc.update_destination.return_value = make_destination_ns()
        resp = client.put(
            "/api/v1/destinations/1",
            json={"config": {"account": "newaccount"}},
        )
        assert resp.status_code == 200


# =============================================================================
# DELETE /destinations/{id}
# =============================================================================


class TestDeleteDestination:
    def test_success_returns_204(self, client, mock_svc):
        mock_svc.delete_destination.return_value = None
        resp = client.delete("/api/v1/destinations/1")
        assert resp.status_code == 204
        assert resp.content == b""

    def test_not_found_returns_404(self, client, mock_svc):
        mock_svc.delete_destination.side_effect = EntityNotFoundError("Destination", 99)
        resp = client.delete("/api/v1/destinations/99")
        assert resp.status_code == 404

    def test_calls_service_with_id(self, client, mock_svc):
        mock_svc.delete_destination.return_value = None
        client.delete("/api/v1/destinations/7")
        mock_svc.delete_destination.assert_called_once_with(7)


# =============================================================================
# POST /destinations/test-connection
# =============================================================================


class TestTestConnection:
    def test_success_returns_200(self, client, mock_svc):
        mock_svc.test_connection.return_value = None  # No exception = success
        resp = client.post("/api/v1/destinations/test-connection", json=VALID_PAYLOAD)
        assert resp.status_code == 200
        assert resp.json()["message"] == "Connection successful"

    def test_failure_returns_error_message(self, client, mock_svc):
        mock_svc.test_connection.side_effect = Exception("Connection refused")
        resp = client.post("/api/v1/destinations/test-connection", json=VALID_PAYLOAD)
        assert resp.status_code == 200
        data = resp.json()
        assert "Connection failed" in data["message"]
        assert data.get("error") is True

    def test_missing_name_returns_422(self, client, mock_svc):
        payload = {k: v for k, v in VALID_PAYLOAD.items() if k != "name"}
        resp = client.post("/api/v1/destinations/test-connection", json=payload)
        assert resp.status_code == 422


# =============================================================================
# POST /destinations/{id}/duplicate
# =============================================================================


class TestDuplicateDestination:
    def test_success_returns_201(self, client, mock_svc):
        mock_svc.duplicate_destination.return_value = make_destination_ns(
            id=2, name="test-destination-copy"
        )
        resp = client.post("/api/v1/destinations/1/duplicate")
        assert resp.status_code == 201
        assert resp.json()["id"] == 2

    def test_not_found_returns_404(self, client, mock_svc):
        mock_svc.duplicate_destination.side_effect = EntityNotFoundError(
            "Destination", 999
        )
        resp = client.post("/api/v1/destinations/999/duplicate")
        assert resp.status_code == 404


# =============================================================================
# GET /destinations/{id}/schema
# =============================================================================


class TestGetDestinationSchema:
    def test_success(self, client, mock_svc):
        mock_svc.fetch_schema.return_value = {"public.users": ["id", "email"]}
        resp = client.get("/api/v1/destinations/1/schema")
        assert resp.status_code == 200
        assert "public.users" in resp.json()

    def test_not_found_returns_404(self, client, mock_svc):
        mock_svc.fetch_schema.side_effect = EntityNotFoundError("Destination", 99)
        resp = client.get("/api/v1/destinations/99/schema")
        assert resp.status_code == 404

    def test_scope_tables_only_passed(self, client, mock_svc):
        mock_svc.fetch_schema.return_value = {}
        client.get("/api/v1/destinations/1/schema?scope=tables")
        mock_svc.fetch_schema.assert_called_once_with(
            1, table_name=None, only_tables=True
        )

    def test_scope_all_passed_by_default(self, client, mock_svc):
        mock_svc.fetch_schema.return_value = {}
        client.get("/api/v1/destinations/1/schema")
        mock_svc.fetch_schema.assert_called_once_with(
            1, table_name=None, only_tables=False
        )


# =============================================================================
# GET /destinations/{id}/tables
# =============================================================================


class TestGetDestinationTables:
    def test_success(self, client, mock_svc):
        mock_svc.get_table_list.return_value = {
            "tables": ["public.orders", "public.users"],
            "total_tables": 2,
            "last_table_check_at": None,
        }
        resp = client.get("/api/v1/destinations/1/tables")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total_tables"] == 2
