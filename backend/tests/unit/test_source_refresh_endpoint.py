from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from app.api.deps import get_source_service, get_source_service_readonly
from app.main import app


@pytest.fixture
def mock_svc():
    return MagicMock()


@pytest.fixture
def client(mock_svc):
    app.dependency_overrides[get_source_service] = lambda: mock_svc
    app.dependency_overrides[get_source_service_readonly] = lambda: mock_svc
    yield TestClient(app)
    app.dependency_overrides.clear()


def test_refresh_source_returns_400_when_refresh_fails(client, mock_svc):
    mock_svc.refresh_source_metadata.side_effect = ValueError(
        "Failed to fetch Kafka metadata from 'kafka:9092'. Ensure the broker hostname is reachable from the backend service."
    )

    response = client.post("/api/v1/sources/2/refresh")

    assert response.status_code == 400
    assert "broker hostname is reachable" in response.json()["detail"]


def test_get_source_details_returns_400_when_details_fail(client, mock_svc):
    mock_svc.get_source_details.side_effect = ValueError(
        "Failed to fetch topics: Failed to fetch Kafka metadata from '192.168.200.83:9092'."
    )

    response = client.get("/api/v1/sources/2/details")

    assert response.status_code == 400
    assert "Failed to fetch Kafka metadata" in response.json()["detail"]
