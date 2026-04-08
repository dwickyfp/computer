from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from app.api.deps import get_dlq_service, get_dlq_service_readonly
from app.main import app


@pytest.fixture
def mock_svc():
    return MagicMock()


@pytest.fixture
def client(mock_svc):
    app.dependency_overrides[get_dlq_service] = lambda: mock_svc
    app.dependency_overrides[get_dlq_service_readonly] = lambda: mock_svc
    yield TestClient(app)
    app.dependency_overrides.clear()


class TestListDLQQueues:
    def test_returns_queue_summary_shape(self, client, mock_svc):
        mock_svc.list_queues.return_value = {
            "items": [
                {
                    "pipeline_id": 1,
                    "pipeline_name": "analytics-pipeline",
                    "source_id": 1,
                    "source_name": "primary-db",
                    "destination_id": 2,
                    "destination_name": "snowflake",
                    "table_name": "public.users",
                    "table_name_target": "users_target",
                    "message_count": 8,
                    "oldest_failed_at": "2025-01-01T10:00:00Z",
                    "newest_failed_at": "2025-01-01T12:00:00Z",
                }
            ],
            "total_messages": 8,
            "total_queues": 1,
            "total_pipelines": 1,
            "total_destinations": 1,
        }

        response = client.get("/api/v1/dlq/queues")

        assert response.status_code == 200
        data = response.json()
        assert data["total_messages"] == 8
        assert data["items"][0]["pipeline_name"] == "analytics-pipeline"
        assert data["items"][0]["table_name_target"] == "users_target"

    def test_forwards_filters(self, client, mock_svc):
        mock_svc.list_queues.return_value = {
            "items": [],
            "total_messages": 0,
            "total_queues": 0,
            "total_pipelines": 0,
            "total_destinations": 0,
        }

        response = client.get(
            "/api/v1/dlq/queues",
            params={
                "pipeline_id": 7,
                "destination_id": 9,
                "search": "users",
                "include_empty": "true",
            },
        )

        assert response.status_code == 200
        mock_svc.list_queues.assert_called_once_with(
            pipeline_id=7,
            destination_id=9,
            search="users",
            include_empty=True,
        )


class TestListDLQMessages:
    def test_returns_messages_and_cursor(self, client, mock_svc):
        mock_svc.list_messages.return_value = {
            "items": [
                {
                    "message_id": "12-0",
                    "operation": "u",
                    "event_timestamp": "2025-01-01T12:00:00Z",
                    "first_failed_at": "2025-01-01T12:05:00Z",
                    "retry_count": 2,
                    "table_name": "public.users",
                    "table_name_target": "users_target",
                    "key": {"id": 1},
                    "value": {"id": 1, "name": "Alice"},
                    "schema": {"type": "struct"},
                    "table_sync_config": {"id": 10},
                }
            ],
            "next_before_id": "12-0",
            "total_count": 44,
        }

        response = client.get(
            "/api/v1/dlq/messages",
            params={
                "source_id": 1,
                "destination_id": 2,
                "table_name": "public.users",
                "before_id": "14-0",
                "limit": 25,
            },
        )

        assert response.status_code == 200
        data = response.json()
        assert data["total_count"] == 44
        assert data["next_before_id"] == "12-0"
        assert data["items"][0]["message_id"] == "12-0"
        mock_svc.list_messages.assert_called_once_with(
            source_id=1,
            destination_id=2,
            table_name="public.users",
            before_id="14-0",
            limit=25,
        )

    def test_missing_queue_identifier_returns_422(self, client, mock_svc):
        response = client.get(
            "/api/v1/dlq/messages",
            params={"destination_id": 2, "table_name": "public.users"},
        )

        assert response.status_code == 422


class TestDiscardActions:
    def test_discard_messages(self, client, mock_svc):
        mock_svc.discard_messages.return_value = {"discarded_count": 2}

        response = client.post(
            "/api/v1/dlq/messages/discard",
            json={
                "source_id": 1,
                "destination_id": 2,
                "table_name": "public.users",
                "message_ids": ["1-0", "2-0"],
            },
        )

        assert response.status_code == 200
        assert response.json() == {"discarded_count": 2}

    def test_empty_message_ids_returns_422(self, client, mock_svc):
        response = client.post(
            "/api/v1/dlq/messages/discard",
            json={
                "source_id": 1,
                "destination_id": 2,
                "table_name": "public.users",
                "message_ids": [],
            },
        )

        assert response.status_code == 422

    def test_missing_queue_identifier_in_queue_discard_returns_422(
        self,
        client,
        mock_svc,
    ):
        response = client.post(
            "/api/v1/dlq/queues/discard",
            json={"source_id": 1, "destination_id": 2},
        )

        assert response.status_code == 422

    def test_discard_pipeline(self, client, mock_svc):
        mock_svc.discard_pipeline.return_value = {
            "discarded_count": 9,
            "queues_cleared": 2,
        }

        response = client.post("/api/v1/dlq/pipelines/7/discard")

        assert response.status_code == 200
        assert response.json() == {"discarded_count": 9, "queues_cleared": 2}
        mock_svc.discard_pipeline.assert_called_once_with(7)
