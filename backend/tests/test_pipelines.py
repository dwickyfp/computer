"""
E2E tests for /api/v1/pipelines endpoints.

Covers:
  - POST   /pipelines                              (create)
  - GET    /pipelines                              (list)
  - GET    /pipelines/{id}                         (get)
  - PUT    /pipelines/{id}                         (update)
  - DELETE /pipelines/{id}                         (delete)
  - POST   /pipelines/{id}/start
  - POST   /pipelines/{id}/pause
  - POST   /pipelines/{id}/refresh
  - GET    /pipelines/{id}/stats
  - POST   /pipelines/{id}/destinations            (add destination)
  - DELETE /pipelines/{id}/destinations/{dest_id}  (remove destination)
"""

import pytest
from unittest.mock import MagicMock
from fastapi.testclient import TestClient

from app.main import app
from app.api.deps import get_pipeline_service, get_pipeline_service_readonly
from app.core.exceptions import (
    DuplicateEntityError,
    EntityNotFoundError,
    PipelineOperationError,
)

from conftest import make_pipeline_ns, make_source_ns


# ─── Fixture ─────────────────────────────────────────────────────────────────


@pytest.fixture
def mock_svc():
    return MagicMock()


@pytest.fixture
def client(mock_svc):
    app.dependency_overrides[get_pipeline_service] = lambda: mock_svc
    app.dependency_overrides[get_pipeline_service_readonly] = lambda: mock_svc
    yield TestClient(app)
    app.dependency_overrides.clear()


# ─── Shared payloads ─────────────────────────────────────────────────────────

POSTGRES_PAYLOAD = {
    "name": "prod-to-snowflake",
    "source_id": 1,
}


# =============================================================================
# POST /pipelines
# =============================================================================


class TestCreatePipeline:
    def test_success_postgres_returns_201(self, client, mock_svc):
        mock_svc.create_pipeline.return_value = make_pipeline_ns(
            name="prod-to-snowflake", source_id=1
        )
        resp = client.post("/api/v1/pipelines", json=POSTGRES_PAYLOAD)
        assert resp.status_code == 201
        data = resp.json()
        assert data["name"] == "prod-to-snowflake"
        assert data["id"] == 1

    def test_name_lowercased(self, client, mock_svc):
        mock_svc.create_pipeline.return_value = make_pipeline_ns(
            name="prod-to-snowflake"
        )
        payload = {**POSTGRES_PAYLOAD, "name": "PROD-TO-SNOWFLAKE"}
        resp = client.post("/api/v1/pipelines", json=payload)
        assert resp.status_code == 201
        call_arg = mock_svc.create_pipeline.call_args[0][0]
        assert call_arg.name == "prod-to-snowflake"

    def test_duplicate_name_returns_409(self, client, mock_svc):
        mock_svc.create_pipeline.side_effect = DuplicateEntityError(
            "Pipeline", "name", "prod-to-snowflake"
        )
        resp = client.post("/api/v1/pipelines", json=POSTGRES_PAYLOAD)
        assert resp.status_code == 409

    def test_invalid_name_spaces_returns_422(self, client, mock_svc):
        payload = {**POSTGRES_PAYLOAD, "name": "invalid name"}
        resp = client.post("/api/v1/pipelines", json=payload)
        assert resp.status_code == 422

    def test_invalid_name_special_chars_returns_422(self, client, mock_svc):
        payload = {**POSTGRES_PAYLOAD, "name": "pipeline@prod"}
        resp = client.post("/api/v1/pipelines", json=payload)
        assert resp.status_code == 422

    def test_pipeline_requires_source_id(self, client, mock_svc):
        payload = {"name": "bad-pipeline"}
        resp = client.post("/api/v1/pipelines", json=payload)
        assert resp.status_code == 422
        body = resp.json()
        assert any("source_id" in str(err) for err in body.get("details", []))

    def test_response_includes_nested_source(self, client, mock_svc):
        mock_svc.create_pipeline.return_value = make_pipeline_ns(source=make_source_ns(id=1, name="my-source"))
        resp = client.post("/api/v1/pipelines", json=POSTGRES_PAYLOAD)
        assert resp.status_code == 201
        data = resp.json()
        assert data["source"]["name"] == "my-source"
        assert data["source"]["type"] == "POSTGRES"

    def test_response_includes_pipeline_metadata(self, client, mock_svc):
        mock_svc.create_pipeline.return_value = make_pipeline_ns()
        resp = client.post("/api/v1/pipelines", json=POSTGRES_PAYLOAD)
        assert resp.status_code == 201
        data = resp.json()
        assert data["pipeline_metadata"] is not None
        assert data["pipeline_metadata"]["status"] == "PAUSED"


# =============================================================================
# GET /pipelines
# =============================================================================


class TestListPipelines:
    def test_empty_list(self, client, mock_svc):
        mock_svc.list_pipelines.return_value = []
        resp = client.get("/api/v1/pipelines")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_returns_multiple_pipelines(self, client, mock_svc):
        mock_svc.list_pipelines.return_value = [
            make_pipeline_ns(id=1, name="pipeline-a"),
            make_pipeline_ns(id=2, name="pipeline-b"),
        ]
        resp = client.get("/api/v1/pipelines")
        assert resp.status_code == 200
        names = [p["name"] for p in resp.json()]
        assert "pipeline-a" in names
        assert "pipeline-b" in names

    def test_pagination_forwarded(self, client, mock_svc):
        mock_svc.list_pipelines.return_value = []
        client.get("/api/v1/pipelines?skip=5&limit=10")
        mock_svc.list_pipelines.assert_called_once_with(skip=5, limit=10)

    def test_invalid_skip_returns_422(self, client, mock_svc):
        resp = client.get("/api/v1/pipelines?skip=-1")
        assert resp.status_code == 422


# =============================================================================
# GET /pipelines/{id}
# =============================================================================


class TestGetPipeline:
    def test_success(self, client, mock_svc):
        mock_svc.get_pipeline.return_value = make_pipeline_ns(id=3, name="pipe-three")
        resp = client.get("/api/v1/pipelines/3")
        assert resp.status_code == 200
        assert resp.json()["id"] == 3

    def test_not_found_returns_404(self, client, mock_svc):
        mock_svc.get_pipeline.side_effect = EntityNotFoundError("Pipeline", 999)
        resp = client.get("/api/v1/pipelines/999")
        assert resp.status_code == 404

    def test_calls_service_with_correct_id(self, client, mock_svc):
        mock_svc.get_pipeline.return_value = make_pipeline_ns()
        client.get("/api/v1/pipelines/42")
        mock_svc.get_pipeline.assert_called_once_with(42)

    def test_response_shape(self, client, mock_svc):
        mock_svc.get_pipeline.return_value = make_pipeline_ns()
        resp = client.get("/api/v1/pipelines/1")
        data = resp.json()
        for field in (
            "id",
            "name",
            "status",
            "source_id",
            "destinations",
            "created_at",
        ):
            assert field in data


# =============================================================================
# PUT /pipelines/{id}
# =============================================================================


class TestUpdatePipeline:
    def test_success(self, client, mock_svc):
        mock_svc.update_pipeline.return_value = make_pipeline_ns(
            name="renamed-pipeline"
        )
        resp = client.put("/api/v1/pipelines/1", json={"name": "renamed-pipeline"})
        assert resp.status_code == 200

    def test_not_found_returns_404(self, client, mock_svc):
        mock_svc.update_pipeline.side_effect = EntityNotFoundError("Pipeline", 999)
        resp = client.put("/api/v1/pipelines/999", json={"name": "x"})
        assert resp.status_code == 404

    def test_invalid_name_returns_422(self, client, mock_svc):
        resp = client.put("/api/v1/pipelines/1", json={"name": "bad name!"})
        assert resp.status_code == 422


# =============================================================================
# DELETE /pipelines/{id}
# =============================================================================


class TestDeletePipeline:
    def test_success_returns_204(self, client, mock_svc):
        mock_svc.delete_pipeline.return_value = None
        resp = client.delete("/api/v1/pipelines/1")
        assert resp.status_code == 204

    def test_not_found_returns_404(self, client, mock_svc):
        mock_svc.delete_pipeline.side_effect = EntityNotFoundError("Pipeline", 99)
        resp = client.delete("/api/v1/pipelines/99")
        assert resp.status_code == 404

    def test_calls_service_with_correct_id(self, client, mock_svc):
        mock_svc.delete_pipeline.return_value = None
        client.delete("/api/v1/pipelines/8")
        mock_svc.delete_pipeline.assert_called_once_with(8)


# =============================================================================
# POST /pipelines/{id}/start
# =============================================================================


class TestStartPipeline:
    def test_success_returns_200(self, client, mock_svc):
        mock_svc.start_pipeline.return_value = make_pipeline_ns(status="START")
        resp = client.post("/api/v1/pipelines/1/start")
        assert resp.status_code == 200
        assert resp.json()["status"] == "START"

    def test_not_found_returns_404(self, client, mock_svc):
        mock_svc.start_pipeline.side_effect = EntityNotFoundError("Pipeline", 99)
        resp = client.post("/api/v1/pipelines/99/start")
        assert resp.status_code == 404

    def test_operation_error_returns_400(self, client, mock_svc):
        mock_svc.start_pipeline.side_effect = PipelineOperationError(
            1, "start", "already running"
        )
        resp = client.post("/api/v1/pipelines/1/start")
        assert resp.status_code == 400


# =============================================================================
# POST /pipelines/{id}/pause
# =============================================================================


class TestPausePipeline:
    def test_success_returns_200(self, client, mock_svc):
        mock_svc.pause_pipeline.return_value = make_pipeline_ns(status="PAUSE")
        resp = client.post("/api/v1/pipelines/1/pause")
        assert resp.status_code == 200
        assert resp.json()["status"] == "PAUSE"

    def test_not_found_returns_404(self, client, mock_svc):
        mock_svc.pause_pipeline.side_effect = EntityNotFoundError("Pipeline", 99)
        resp = client.post("/api/v1/pipelines/99/pause")
        assert resp.status_code == 404


# =============================================================================
# POST /pipelines/{id}/refresh
# =============================================================================


class TestRefreshPipeline:
    def test_success_returns_200(self, client, mock_svc):
        mock_svc.refresh_pipeline.return_value = make_pipeline_ns(status="REFRESH")
        resp = client.post("/api/v1/pipelines/1/refresh")
        assert resp.status_code == 200

    def test_not_found_returns_404(self, client, mock_svc):
        mock_svc.refresh_pipeline.side_effect = EntityNotFoundError("Pipeline", 99)
        resp = client.post("/api/v1/pipelines/99/refresh")
        assert resp.status_code == 404


# =============================================================================
# GET /pipelines/{id}/stats
# =============================================================================


class TestPipelineStats:
    def test_success_returns_200(self, client, mock_svc):
        mock_svc.get_pipeline_data_flow_stats.return_value = [
            {"table": "users", "records_today": 100},
            {"table": "orders", "records_today": 250},
        ]
        resp = client.get("/api/v1/pipelines/1/stats")
        assert resp.status_code == 200
        assert len(resp.json()) == 2

    def test_days_param_forwarded(self, client, mock_svc):
        mock_svc.get_pipeline_data_flow_stats.return_value = []
        client.get("/api/v1/pipelines/1/stats?days=14")
        mock_svc.get_pipeline_data_flow_stats.assert_called_once_with(1, 14)

    def test_invalid_days_too_large_returns_422(self, client, mock_svc):
        resp = client.get("/api/v1/pipelines/1/stats?days=100")
        assert resp.status_code == 422

    def test_not_found_returns_404(self, client, mock_svc):
        mock_svc.get_pipeline_data_flow_stats.side_effect = EntityNotFoundError(
            "Pipeline", 99
        )
        resp = client.get("/api/v1/pipelines/99/stats")
        assert resp.status_code == 404


# =============================================================================
# POST /pipelines/{id}/destinations  (add)
# =============================================================================


class TestAddPipelineDestination:
    def test_success_returns_201(self, client, mock_svc):
        mock_svc.add_pipeline_destination.return_value = make_pipeline_ns()
        resp = client.post("/api/v1/pipelines/1/destinations?destination_id=5")
        assert resp.status_code == 201

    def test_pipeline_not_found_returns_404(self, client, mock_svc):
        mock_svc.add_pipeline_destination.side_effect = EntityNotFoundError(
            "Pipeline", 99
        )
        resp = client.post("/api/v1/pipelines/99/destinations?destination_id=5")
        assert resp.status_code == 404

    def test_missing_destination_id_returns_422(self, client, mock_svc):
        resp = client.post("/api/v1/pipelines/1/destinations")
        assert resp.status_code == 422


# =============================================================================
# DELETE /pipelines/{id}/destinations/{dest_id}  (remove)
# =============================================================================


class TestRemovePipelineDestination:
    def test_success_returns_200(self, client, mock_svc):
        mock_svc.remove_pipeline_destination.return_value = make_pipeline_ns()
        resp = client.delete("/api/v1/pipelines/1/destinations/5")
        assert resp.status_code == 200

    def test_pipeline_not_found_returns_404(self, client, mock_svc):
        mock_svc.remove_pipeline_destination.side_effect = EntityNotFoundError(
            "Pipeline", 99
        )
        resp = client.delete("/api/v1/pipelines/99/destinations/5")
        assert resp.status_code == 404
