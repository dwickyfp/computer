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


def test_get_kafka_topic_summaries_returns_service_data(client, mock_svc):
    mock_svc.get_kafka_topic_summaries.return_value = [
        {
            "topic_name": "orders",
            "full_topic_name": "salt.public.orders",
            "is_registered": True,
            "first_offset": 0,
            "next_offset": 42,
            "message_count": 42,
        }
    ]

    response = client.get("/api/v1/sources/2/topics/summary")

    assert response.status_code == 200
    assert response.json() == [
        {
            "topic_name": "orders",
            "full_topic_name": "salt.public.orders",
            "is_registered": True,
            "first_offset": 0,
            "next_offset": 42,
            "message_count": 42,
        }
    ]


def test_get_kafka_topic_summaries_returns_400_when_summary_fails(client, mock_svc):
    mock_svc.get_kafka_topic_summaries.side_effect = ValueError(
        "Failed to fetch Kafka metadata from 'kafka:9092'."
    )

    response = client.get("/api/v1/sources/2/topics/summary")

    assert response.status_code == 400
    assert "Failed to fetch Kafka metadata" in response.json()["detail"]


def test_get_kafka_topic_preview_returns_service_data(client, mock_svc):
    mock_svc.get_kafka_topic_preview.return_value = {
        "topic_name": "orders",
        "full_topic_name": "salt.public.orders",
        "page": 1,
        "page_size": 10,
        "total_messages": 1,
        "total_pages": 1,
        "messages": [
            {
                "partition": 0,
                "offset": 0,
                "timestamp": "2026-04-07T06:46:04.288664+00:00",
                "key_preview": '{"sale_id":61}',
                "value_preview": '{"sale_id":61}',
                "key": '{\n  "sale_id": 61\n}',
                "value": '{\n  "sale_id": 61\n}',
                "headers": None,
            }
        ],
    }

    response = client.get("/api/v1/sources/2/topics/orders/preview?page=1")

    assert response.status_code == 200
    assert response.json()["page_size"] == 10
    assert response.json()["messages"][0]["offset"] == 0


def test_get_kafka_topic_preview_returns_400_when_preview_fails(client, mock_svc):
    mock_svc.get_kafka_topic_preview.side_effect = ValueError(
        "Kafka topic 'salt.public.orders' was not found"
    )

    response = client.get("/api/v1/sources/2/topics/orders/preview?page=1")

    assert response.status_code == 400
    assert "was not found" in response.json()["detail"]
