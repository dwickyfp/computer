from core.runtime_health import mark_worker, _workers
from fastapi.testclient import TestClient
from server import app


client = TestClient(app)


def setup_function():
    _workers.clear()


def test_health_check_endpoint():
    mark_worker("api_server", "running", critical=True)
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "healthy"
    assert "workers" in response.json()
