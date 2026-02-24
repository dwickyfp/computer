"""
Shared test fixtures for backend integration tests.
Uses FastAPI TestClient with mock database sessions.
"""

import pytest
from unittest.mock import MagicMock, patch
from fastapi.testclient import TestClient
from app.main import app


@pytest.fixture
def client():
    """FastAPI test client."""
    return TestClient(app)


@pytest.fixture
def mock_db_session():
    """Mock SQLAlchemy Session."""
    session = MagicMock()
    session.commit = MagicMock()
    session.rollback = MagicMock()
    session.close = MagicMock()
    session.flush = MagicMock()
    session.refresh = MagicMock()
    return session
