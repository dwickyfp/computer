"""
E2E tests for /api/v1/tags endpoints.

Covers:
  - POST   /tags            (create tag)
  - GET    /tags            (list with pagination)
  - GET    /tags/search     (autocomplete search)
  - GET    /tags/{id}       (get by id)
  - GET    /tags/{id}/usage (get usage details)
  - DELETE /tags/{id}       (delete tag)
  - GET    /tags/smart-tags (grouped by alphabet)

Tag service returns Pydantic schema objects directly (no from_orm in endpoints).
"""

from datetime import datetime

import pytest
from unittest.mock import MagicMock
from fastapi.testclient import TestClient

from app.main import app
from app.api.deps import get_tag_service
from app.core.exceptions import DuplicateEntityError, EntityNotFoundError
from app.domain.schemas.tag import (
    TagResponse,
    TagListResponse,
    TagSuggestionResponse,
    SmartTagsResponse,
    AlphabetGroupedTags,
    TagWithUsageCount,
    TagUsageResponse,
)

NOW = datetime(2025, 1, 1, 12, 0, 0)


# ─── Helpers ─────────────────────────────────────────────────────────────────


def make_tag_response(id: int = 1, tag: str = "high-priority") -> TagResponse:
    return TagResponse(id=id, tag=tag, created_at=NOW, updated_at=NOW)


def make_tag_with_count(
    id: int = 1, tag: str = "analytics", count: int = 3
) -> TagWithUsageCount:
    return TagWithUsageCount(
        id=id, tag=tag, usage_count=count, created_at=NOW, updated_at=NOW
    )


# ─── Fixture ─────────────────────────────────────────────────────────────────


@pytest.fixture
def mock_svc():
    return MagicMock()


@pytest.fixture
def client(mock_svc):
    app.dependency_overrides[get_tag_service] = lambda: mock_svc
    yield TestClient(app)
    app.dependency_overrides.clear()


# =============================================================================
# POST /tags
# =============================================================================


class TestCreateTag:
    def test_success_returns_201(self, client, mock_svc):
        mock_svc.create_tag.return_value = make_tag_response(id=1, tag="high-priority")
        resp = client.post("/api/v1/tags", json={"tag": "high-priority"})
        assert resp.status_code == 201
        data = resp.json()
        assert data["id"] == 1
        assert data["tag"] == "high-priority"
        assert "created_at" in data

    def test_duplicate_returns_409(self, client, mock_svc):
        mock_svc.create_tag.side_effect = DuplicateEntityError(
            "Tag", "tag", "high-priority"
        )
        resp = client.post("/api/v1/tags", json={"tag": "high-priority"})
        assert resp.status_code == 409

    def test_empty_tag_returns_422(self, client, mock_svc):
        resp = client.post("/api/v1/tags", json={"tag": ""})
        assert resp.status_code == 422

    def test_special_chars_returns_422(self, client, mock_svc):
        """Tags may not contain characters other than alnum, -, _, space."""
        resp = client.post("/api/v1/tags", json={"tag": "tag@special!"})
        assert resp.status_code == 422

    def test_tag_with_hyphen_accepted(self, client, mock_svc):
        mock_svc.create_tag.return_value = make_tag_response(tag="customer-data")
        resp = client.post("/api/v1/tags", json={"tag": "customer-data"})
        assert resp.status_code == 201

    def test_tag_with_underscore_accepted(self, client, mock_svc):
        mock_svc.create_tag.return_value = make_tag_response(tag="customer_data")
        resp = client.post("/api/v1/tags", json={"tag": "customer_data"})
        assert resp.status_code == 201

    def test_tag_with_space_accepted(self, client, mock_svc):
        mock_svc.create_tag.return_value = make_tag_response(tag="customer data")
        resp = client.post("/api/v1/tags", json={"tag": "customer data"})
        assert resp.status_code == 201

    def test_tag_too_long_returns_422(self, client, mock_svc):
        long_tag = "a" * 151  # max_length=150
        resp = client.post("/api/v1/tags", json={"tag": long_tag})
        assert resp.status_code == 422

    def test_service_receives_stripped_tag(self, client, mock_svc):
        """Leading/trailing whitespace is stripped by the validator."""
        mock_svc.create_tag.return_value = make_tag_response(tag="analytics")
        resp = client.post("/api/v1/tags", json={"tag": "  analytics  "})
        # The validator strips whitespace before calling service
        assert resp.status_code == 201
        create_arg = mock_svc.create_tag.call_args[0][0]
        assert create_arg.tag == "analytics"


# =============================================================================
# GET /tags
# =============================================================================


class TestListTags:
    def test_returns_200_with_tags(self, client, mock_svc):
        mock_svc.get_all_tags.return_value = TagListResponse(
            tags=[
                make_tag_response(id=1, tag="analytics"),
                make_tag_response(id=2, tag="billing"),
            ],
            total=2,
        )
        resp = client.get("/api/v1/tags")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 2
        assert len(data["tags"]) == 2

    def test_returns_200_with_empty_list(self, client, mock_svc):
        mock_svc.get_all_tags.return_value = TagListResponse(tags=[], total=0)
        resp = client.get("/api/v1/tags")
        assert resp.status_code == 200
        assert resp.json()["total"] == 0

    def test_pagination_forwarded(self, client, mock_svc):
        mock_svc.get_all_tags.return_value = TagListResponse(tags=[], total=0)
        client.get("/api/v1/tags?skip=10&limit=20")
        mock_svc.get_all_tags.assert_called_once_with(skip=10, limit=20)

    def test_invalid_skip_returns_422(self, client, mock_svc):
        resp = client.get("/api/v1/tags?skip=-5")
        assert resp.status_code == 422


# =============================================================================
# GET /tags/search
# =============================================================================


class TestSearchTags:
    def test_returns_suggestions(self, client, mock_svc):
        mock_svc.search_tags.return_value = TagSuggestionResponse(
            suggestions=[make_tag_response(tag="analytics")]
        )
        resp = client.get("/api/v1/tags/search?q=anal")
        assert resp.status_code == 200
        data = resp.json()
        assert "suggestions" in data
        assert data["suggestions"][0]["tag"] == "analytics"

    def test_empty_query_returns_suggestions(self, client, mock_svc):
        mock_svc.search_tags.return_value = TagSuggestionResponse(suggestions=[])
        resp = client.get("/api/v1/tags/search")
        assert resp.status_code == 200

    def test_limit_param_forwarded(self, client, mock_svc):
        mock_svc.search_tags.return_value = TagSuggestionResponse(suggestions=[])
        client.get("/api/v1/tags/search?q=test&limit=5")
        mock_svc.search_tags.assert_called_once_with(query="test", limit=5)


# =============================================================================
# GET /tags/{id}
# =============================================================================


class TestGetTag:
    def test_success(self, client, mock_svc):
        mock_svc.get_tag.return_value = make_tag_response(id=7, tag="analytics")
        resp = client.get("/api/v1/tags/7")
        assert resp.status_code == 200
        assert resp.json()["id"] == 7
        assert resp.json()["tag"] == "analytics"

    def test_not_found_returns_404(self, client, mock_svc):
        mock_svc.get_tag.side_effect = EntityNotFoundError("Tag", 999)
        resp = client.get("/api/v1/tags/999")
        assert resp.status_code == 404

    def test_calls_service_with_correct_id(self, client, mock_svc):
        mock_svc.get_tag.return_value = make_tag_response()
        client.get("/api/v1/tags/42")
        mock_svc.get_tag.assert_called_once_with(42)


# =============================================================================
# DELETE /tags/{id}
# =============================================================================


class TestDeleteTag:
    def test_success_returns_204(self, client, mock_svc):
        mock_svc.delete_tag.return_value = None
        resp = client.delete("/api/v1/tags/1")
        assert resp.status_code == 204
        assert resp.content == b""

    def test_not_found_returns_404(self, client, mock_svc):
        mock_svc.delete_tag.side_effect = EntityNotFoundError("Tag", 999)
        resp = client.delete("/api/v1/tags/999")
        assert resp.status_code == 404

    def test_calls_service_with_correct_id(self, client, mock_svc):
        mock_svc.delete_tag.return_value = None
        client.delete("/api/v1/tags/9")
        mock_svc.delete_tag.assert_called_once_with(9)


# =============================================================================
# GET /tags/smart-tags
# =============================================================================


class TestSmartTags:
    def test_returns_grouped_tags(self, client, mock_svc):
        mock_svc.get_smart_tags.return_value = SmartTagsResponse(
            groups=[
                AlphabetGroupedTags(
                    letter="A",
                    tags=[make_tag_with_count(tag="analytics", count=5)],
                    count=1,
                )
            ],
            total_tags=1,
        )
        resp = client.get("/api/v1/tags/smart-tags")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total_tags"] == 1
        assert len(data["groups"]) == 1
        assert data["groups"][0]["letter"] == "A"

    def test_pipeline_filter_forwarded(self, client, mock_svc):
        mock_svc.get_smart_tags.return_value = SmartTagsResponse(
            groups=[], total_tags=0
        )
        client.get("/api/v1/tags/smart-tags?pipeline_id=3")
        mock_svc.get_smart_tags.assert_called_once_with(
            pipeline_id=3, destination_id=None, source_id=None
        )
