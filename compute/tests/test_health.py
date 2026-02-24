from fastapi.testclient import TestClient
from server import app

client = TestClient(app)

def test_health_check_endpoint():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "healthy"}

if __name__ == "__main__":
    test_health_check_endpoint()
    print("Health check endpoint test passed!")
