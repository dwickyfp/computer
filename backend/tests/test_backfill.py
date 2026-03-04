"""
E2E tests for backfill endpoints.

Routes:
  POST   /api/v1/pipelines/{id}/backfill    (create job)
  GET    /api/v1/pipelines/{id}/backfill    (list jobs)
  GET    /api/v1/backfill/{job_id}          (get job)
  POST   /api/v1/backfill/{job_id}/cancel   (cancel job)
  DELETE /api/v1/backfill/{job_id}          (delete job)

Backfill service returns Pydantic objects directly (no from_orm in endpoints).
"""

from datetime import datetime

import pytest
from unittest.mock import MagicMock
from fastapi.testclient import TestClient

from app.main import app
from app.api.deps import get_backfill_service
from app.core.exceptions import EntityNotFoundError
from app.domain.schemas.backfill import BackfillJobResponse, BackfillJobListResponse

NOW = datetime(2025, 1, 1, 12, 0, 0)


# ─── Helpers ─────────────────────────────────────────────────────────────────


def make_job(
    id: int = 1,
    pipeline_id: int = 1,
    source_id: int = 1,
    table_name: str = "public.users",
    status: str = "PENDING",
    **kw,
) -> BackfillJobResponse:
    return BackfillJobResponse(
        id=id,
        pipeline_id=pipeline_id,
        source_id=source_id,
        table_name=table_name,
        filter_sql=kw.get("filter_sql"),
        status=status,
        count_record=kw.get("count_record", 0),
        total_record=kw.get("total_record", 0),
        is_error=kw.get("is_error", False),
        error_message=kw.get("error_message"),
        created_at=NOW,
        updated_at=NOW,
    )


# ─── Fixture ─────────────────────────────────────────────────────────────────


@pytest.fixture
def mock_svc():
    return MagicMock()


@pytest.fixture
def client(mock_svc):
    app.dependency_overrides[get_backfill_service] = lambda: mock_svc
    yield TestClient(app)
    app.dependency_overrides.clear()


# =============================================================================
# POST /pipelines/{id}/backfill
# =============================================================================


class TestCreateBackfillJob:
    PAYLOAD = {"table_name": "public.users"}

    def test_success_returns_201(self, client, mock_svc):
        mock_svc.create_backfill_job.return_value = make_job()
        resp = client.post("/api/v1/pipelines/1/backfill", json=self.PAYLOAD)
        assert resp.status_code == 201
        data = resp.json()
        assert data["id"] == 1
        assert data["table_name"] == "public.users"
        assert data["status"] == "PENDING"

    def test_pipeline_not_found_returns_404(self, client, mock_svc):
        mock_svc.create_backfill_job.side_effect = EntityNotFoundError("Pipeline", 99)
        resp = client.post("/api/v1/pipelines/99/backfill", json=self.PAYLOAD)
        assert resp.status_code == 404

    def test_missing_table_name_returns_422(self, client, mock_svc):
        resp = client.post("/api/v1/pipelines/1/backfill", json={})
        assert resp.status_code == 422

    def test_empty_table_name_returns_422(self, client, mock_svc):
        resp = client.post("/api/v1/pipelines/1/backfill", json={"table_name": ""})
        assert resp.status_code == 422

    def test_with_filters_accepted(self, client, mock_svc):
        mock_svc.create_backfill_job.return_value = make_job(filter_sql="id > '100'")
        payload = {
            "table_name": "public.orders",
            "filters": [{"column": "id", "operator": ">", "value": "100"}],
        }
        resp = client.post("/api/v1/pipelines/1/backfill", json=payload)
        assert resp.status_code == 201

    def test_too_many_filters_returns_422(self, client, mock_svc):
        """Maximum 5 filters are allowed."""
        payload = {
            "table_name": "public.orders",
            "filters": [
                {"column": f"col{i}", "operator": "=", "value": f"val{i}"}
                for i in range(6)  # 6 filters — exceeds limit
            ],
        }
        resp = client.post("/api/v1/pipelines/1/backfill", json=payload)
        assert resp.status_code == 422

    def test_calls_service_with_pipeline_id(self, client, mock_svc):
        mock_svc.create_backfill_job.return_value = make_job(pipeline_id=5)
        client.post("/api/v1/pipelines/5/backfill", json=self.PAYLOAD)
        call_args = mock_svc.create_backfill_job.call_args[0]
        assert call_args[0] == 5  # pipeline_id

    def test_pipeline_id_zero_rejected(self, client, mock_svc):
        """Path parameter must be > 0."""
        resp = client.post("/api/v1/pipelines/0/backfill", json=self.PAYLOAD)
        assert resp.status_code == 422


# =============================================================================
# GET /pipelines/{id}/backfill
# =============================================================================


class TestListBackfillJobs:
    def test_success_returns_200(self, client, mock_svc):
        mock_svc.get_pipeline_backfill_jobs.return_value = BackfillJobListResponse(
            items=[make_job(id=1), make_job(id=2)], total=2
        )
        resp = client.get("/api/v1/pipelines/1/backfill")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 2
        assert len(data["items"]) == 2

    def test_empty_list(self, client, mock_svc):
        mock_svc.get_pipeline_backfill_jobs.return_value = BackfillJobListResponse(
            items=[], total=0
        )
        resp = client.get("/api/v1/pipelines/1/backfill")
        assert resp.status_code == 200
        assert resp.json()["total"] == 0

    def test_pagination_forwarded(self, client, mock_svc):
        mock_svc.get_pipeline_backfill_jobs.return_value = BackfillJobListResponse(
            items=[], total=0
        )
        client.get("/api/v1/pipelines/1/backfill?skip=10&limit=5")
        mock_svc.get_pipeline_backfill_jobs.assert_called_once_with(1, skip=10, limit=5)

    def test_pipeline_not_found_returns_404(self, client, mock_svc):
        mock_svc.get_pipeline_backfill_jobs.side_effect = EntityNotFoundError(
            "Pipeline", 99
        )
        resp = client.get("/api/v1/pipelines/99/backfill")
        assert resp.status_code == 404


# =============================================================================
# GET /backfill/{job_id}
# =============================================================================


class TestGetBackfillJob:
    def test_success(self, client, mock_svc):
        mock_svc.get_backfill_job.return_value = make_job(id=7, status="EXECUTING")
        resp = client.get("/api/v1/backfill/7")
        assert resp.status_code == 200
        assert resp.json()["id"] == 7
        assert resp.json()["status"] == "EXECUTING"

    def test_not_found_returns_404(self, client, mock_svc):
        mock_svc.get_backfill_job.side_effect = EntityNotFoundError("BackfillJob", 99)
        resp = client.get("/api/v1/backfill/99")
        assert resp.status_code == 404


# =============================================================================
# POST /backfill/{job_id}/cancel
# =============================================================================


class TestCancelBackfillJob:
    def test_success(self, client, mock_svc):
        mock_svc.cancel_backfill_job.return_value = make_job(id=3, status="CANCELLED")
        resp = client.post("/api/v1/backfill/3/cancel")
        assert resp.status_code == 200
        assert resp.json()["status"] == "CANCELLED"

    def test_not_found_returns_404(self, client, mock_svc):
        mock_svc.cancel_backfill_job.side_effect = EntityNotFoundError(
            "BackfillJob", 99
        )
        resp = client.post("/api/v1/backfill/99/cancel")
        assert resp.status_code == 404


# =============================================================================
# DELETE /backfill/{job_id}
# =============================================================================


class TestDeleteBackfillJob:
    def test_success_returns_204(self, client, mock_svc):
        mock_svc.delete_backfill_job.return_value = None
        resp = client.delete("/api/v1/backfill/1")
        assert resp.status_code == 204

    def test_not_found_returns_404(self, client, mock_svc):
        mock_svc.delete_backfill_job.side_effect = EntityNotFoundError(
            "BackfillJob", 99
        )
        resp = client.delete("/api/v1/backfill/99")
        assert resp.status_code == 404


# =============================================================================
# BackfillJobCreate.get_filter_sql() — unit test of business logic
# =============================================================================


class TestBackfillFilterSql:
    """Tests for the BackfillJobCreate.get_filter_sql() helper."""

    from app.domain.schemas.backfill import BackfillJobCreate, BackfillFilterCreate

    def _make_job(self, filters):
        from app.domain.schemas.backfill import BackfillJobCreate, BackfillFilterCreate

        return BackfillJobCreate(
            table_name="public.users",
            filters=[BackfillFilterCreate(**f) for f in filters],
        )

    def test_none_filters_returns_none(self):
        from app.domain.schemas.backfill import BackfillJobCreate

        job = BackfillJobCreate(table_name="public.orders")
        assert job.get_filter_sql() is None

    def test_single_equality_filter(self):
        job = self._make_job([{"column": "status", "operator": "=", "value": "active"}])
        sql = job.get_filter_sql()
        assert sql == "status = 'active'"

    def test_numeric_value_not_quoted(self):
        job = self._make_job([{"column": "age", "operator": ">", "value": "18"}])
        sql = job.get_filter_sql()
        assert sql == "age > 18"

    def test_like_operator_quoted(self):
        job = self._make_job([{"column": "name", "operator": "LIKE", "value": "Jo%"}])
        sql = job.get_filter_sql()
        assert "LIKE" in sql
        assert "'Jo%'" in sql

    def test_multiple_filters_semicolon_separated(self):
        job = self._make_job(
            [
                {"column": "status", "operator": "=", "value": "active"},
                {"column": "age", "operator": ">", "value": "18"},
            ]
        )
        sql = job.get_filter_sql()
        assert ";" in sql
        parts = sql.split(";")
        assert len(parts) == 2

    def test_in_operator(self):
        job = self._make_job([{"column": "id", "operator": "IN", "value": "1,2,3"}])
        sql = job.get_filter_sql()
        assert "IN" in sql
        assert "1" in sql and "2" in sql and "3" in sql
