"""
Integration tests for Flow Task Version & Watermark endpoints (D4 + D8).

Tests version listing, rollback, and watermark management.
Uses SimpleNamespace to satisfy Pydantic v1 from_orm() validation.
"""

import pytest
from types import SimpleNamespace
from unittest.mock import MagicMock
from datetime import datetime
from fastapi.testclient import TestClient

from app.main import app
from app.api.deps import get_flow_task_service
from app.core.exceptions import EntityNotFoundError

NOW = datetime(2025, 1, 1, 0, 0, 0)


# ─── Helpers ─────────────────────────────────────────────────────────────────

def make_version(id=1, flow_task_id=1, version=1, **kw):
    """Build a SimpleNamespace that satisfies FlowTaskGraphVersionResponse."""
    return SimpleNamespace(
        id=id,
        flow_task_id=flow_task_id,
        version=version,
        nodes_json=kw.get("nodes_json", []),
        edges_json=kw.get("edges_json", []),
        change_summary=kw.get("change_summary", f"Version {version}"),
        created_at=NOW,
    )


def make_watermark(id=1, flow_task_id=1, node_id="input_1", **kw):
    """Build a SimpleNamespace that satisfies FlowTaskWatermarkResponse."""
    return SimpleNamespace(
        id=id,
        flow_task_id=flow_task_id,
        node_id=node_id,
        watermark_column=kw.get("watermark_column", "updated_at"),
        last_watermark_value=kw.get("last_watermark_value"),
        watermark_type=kw.get("watermark_type", "TIMESTAMP"),
        last_run_at=kw.get("last_run_at"),
        record_count=kw.get("record_count", 0),
        created_at=NOW,
        updated_at=NOW,
    )


def make_graph(id=1, flow_task_id=1, **kw):
    """Build a SimpleNamespace that satisfies FlowTaskGraphResponse."""
    return SimpleNamespace(
        id=id,
        flow_task_id=flow_task_id,
        nodes_json=kw.get("nodes_json", []),
        edges_json=kw.get("edges_json", []),
        version=kw.get("version", 1),
        created_at=NOW,
        updated_at=NOW,
    )


@pytest.fixture
def mock_service():
    return MagicMock()


@pytest.fixture
def client(mock_service):
    app.dependency_overrides[get_flow_task_service] = lambda: mock_service
    yield TestClient(app)
    app.dependency_overrides.clear()


# ─── Graph Version Tests ────────────────────────────────────────────────────

class TestListGraphVersions:
    def test_list_success(self, client, mock_service):
        versions = [make_version(id=1, version=1), make_version(id=2, version=2)]
        mock_service.list_graph_versions.return_value = (versions, 2)

        resp = client.get("/api/v1/flow-tasks/1/versions")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 2
        assert len(data["items"]) == 2

    def test_list_with_pagination(self, client, mock_service):
        mock_service.list_graph_versions.return_value = ([], 0)
        resp = client.get("/api/v1/flow-tasks/1/versions?page=2&page_size=5")
        assert resp.status_code == 200
        call_kw = mock_service.list_graph_versions.call_args[1]
        assert call_kw["skip"] == 5
        assert call_kw["limit"] == 5

    def test_list_not_found(self, client, mock_service):
        mock_service.list_graph_versions.side_effect = EntityNotFoundError("FlowTask", 999)
        resp = client.get("/api/v1/flow-tasks/999/versions")
        assert resp.status_code == 404


class TestGetGraphVersion:
    def test_get_success(self, client, mock_service):
        mock_service.get_graph_version.return_value = make_version(version=3)
        resp = client.get("/api/v1/flow-tasks/1/versions/3")
        assert resp.status_code == 200
        assert resp.json()["version"] == 3

    def test_get_not_found(self, client, mock_service):
        mock_service.get_graph_version.side_effect = EntityNotFoundError("FlowTaskGraphVersion", 999)
        resp = client.get("/api/v1/flow-tasks/1/versions/999")
        assert resp.status_code == 404


class TestRollbackGraph:
    def test_rollback_success(self, client, mock_service):
        mock_service.rollback_graph.return_value = make_graph()
        resp = client.post("/api/v1/flow-tasks/1/rollback/2")
        assert resp.status_code == 200
        mock_service.rollback_graph.assert_called_once_with(1, 2)

    def test_rollback_not_found(self, client, mock_service):
        mock_service.rollback_graph.side_effect = EntityNotFoundError("FlowTask", 999)
        resp = client.post("/api/v1/flow-tasks/1/rollback/999")
        assert resp.status_code == 404

    def test_rollback_bad_request(self, client, mock_service):
        mock_service.rollback_graph.side_effect = ValueError("Cannot rollback to current")
        resp = client.post("/api/v1/flow-tasks/1/rollback/1")
        assert resp.status_code == 400


# ─── Watermark Tests ────────────────────────────────────────────────────────

class TestListWatermarks:
    def test_list_success(self, client, mock_service):
        wms = [make_watermark(id=1), make_watermark(id=2, node_id="input_2")]
        mock_service.get_watermarks.return_value = wms

        resp = client.get("/api/v1/flow-tasks/1/watermarks")
        assert resp.status_code == 200
        assert len(resp.json()) == 2

    def test_list_not_found(self, client, mock_service):
        mock_service.get_watermarks.side_effect = EntityNotFoundError("FlowTask", 999)
        resp = client.get("/api/v1/flow-tasks/999/watermarks")
        assert resp.status_code == 404


class TestSetWatermark:
    def test_set_success(self, client, mock_service):
        mock_service.set_watermark.return_value = make_watermark()

        resp = client.post(
            "/api/v1/flow-tasks/1/watermarks",
            json={
                "node_id": "input_1",
                "watermark_column": "updated_at",
                "watermark_type": "TIMESTAMP",
            },
        )
        assert resp.status_code == 201
        data = resp.json()
        assert data["node_id"] == "input_1"
        assert data["watermark_column"] == "updated_at"

    def test_set_not_found(self, client, mock_service):
        mock_service.set_watermark.side_effect = EntityNotFoundError("FlowTask", 1)
        resp = client.post(
            "/api/v1/flow-tasks/1/watermarks",
            json={
                "node_id": "input_1",
                "watermark_column": "updated_at",
                "watermark_type": "TIMESTAMP",
            },
        )
        assert resp.status_code == 404


class TestResetWatermark:
    def test_reset_success(self, client, mock_service):
        mock_service.reset_watermark.return_value = None
        resp = client.delete("/api/v1/flow-tasks/1/watermarks/input_1")
        assert resp.status_code == 204

    def test_reset_not_found(self, client, mock_service):
        mock_service.reset_watermark.side_effect = EntityNotFoundError("FlowTask", 999)
        resp = client.delete("/api/v1/flow-tasks/999/watermarks/input_1")
        assert resp.status_code == 404
