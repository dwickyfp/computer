"""
Integration tests for Schema Validation API endpoint (B2).

Tests the /validate-schema endpoint using mocked database and service.
"""

import pytest
from unittest.mock import MagicMock, patch
from fastapi.testclient import TestClient

from app.main import app
from app.api.deps import get_db


@pytest.fixture
def mock_db():
    return MagicMock()


@pytest.fixture
def client(mock_db):
    app.dependency_overrides[get_db] = lambda: mock_db
    yield TestClient(app)
    app.dependency_overrides.clear()


class TestValidateSchema:
    @patch("app.api.v1.endpoints.schema_validation.SchemaCompatibilityService")
    def test_validate_compatible(self, MockService, client):
        mock_svc = MockService.return_value
        mock_result = MagicMock()
        mock_result.to_dict.return_value = {
            "is_compatible": True,
            "errors": [],
            "warnings": [],
            "source_table": "users",
            "destination_table": "users",
        }
        mock_svc.validate_pipeline_schemas.return_value = mock_result

        resp = client.get(
            "/api/v1/schema/validate-schema",
            params={"source_id": 1, "table_name": "users", "destination_id": 2},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["is_compatible"] is True
        assert data["errors"] == []

    @patch("app.api.v1.endpoints.schema_validation.SchemaCompatibilityService")
    def test_validate_incompatible(self, MockService, client):
        mock_svc = MockService.return_value
        mock_result = MagicMock()
        mock_result.to_dict.return_value = {
            "is_compatible": False,
            "errors": ["Column 'email' missing in destination"],
            "warnings": ["Column 'age' type mismatch: int4 vs int8"],
            "source_table": "users",
            "destination_table": "users",
        }
        mock_svc.validate_pipeline_schemas.return_value = mock_result

        resp = client.get(
            "/api/v1/schema/validate-schema",
            params={"source_id": 1, "table_name": "users", "destination_id": 2},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["is_compatible"] is False
        assert len(data["errors"]) == 1
        assert len(data["warnings"]) == 1

    @patch("app.api.v1.endpoints.schema_validation.SchemaCompatibilityService")
    def test_validate_with_custom_target_table(self, MockService, client):
        mock_svc = MockService.return_value
        mock_result = MagicMock()
        mock_result.to_dict.return_value = {
            "is_compatible": True,
            "errors": [],
            "warnings": [],
            "source_table": "users",
            "destination_table": "dim_users",
        }
        mock_svc.validate_pipeline_schemas.return_value = mock_result

        resp = client.get(
            "/api/v1/schema/validate-schema",
            params={
                "source_id": 1,
                "table_name": "users",
                "destination_id": 2,
                "target_table": "dim_users",
            },
        )
        assert resp.status_code == 200
        mock_svc.validate_pipeline_schemas.assert_called_once_with(
            source_id=1,
            table_name="users",
            destination_id=2,
            target_table="dim_users",
        )

    @patch("app.api.v1.endpoints.schema_validation.SchemaCompatibilityService")
    def test_validate_service_error(self, MockService, client):
        mock_svc = MockService.return_value
        mock_svc.validate_pipeline_schemas.side_effect = Exception("Connection refused")

        resp = client.get(
            "/api/v1/schema/validate-schema",
            params={"source_id": 1, "table_name": "users", "destination_id": 2},
        )
        assert resp.status_code == 500

    def test_validate_missing_required_params(self, client):
        resp = client.get("/api/v1/schema/validate-schema")
        assert resp.status_code == 422

    def test_validate_missing_source_id(self, client):
        resp = client.get(
            "/api/v1/schema/validate-schema",
            params={"table_name": "users", "destination_id": 2},
        )
        assert resp.status_code == 422
