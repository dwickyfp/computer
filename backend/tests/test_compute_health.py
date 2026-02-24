from fastapi.testclient import TestClient
from unittest.mock import patch, AsyncMock
from app.main import app

client = TestClient(app)

@patch("app.main.httpx.AsyncClient")
def test_health_check_compute_healthy(mock_client):
    # Mock compute node response
    mock_response = AsyncMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"status": "healthy"}

    # Mock context manager
    mock_ac_instance = AsyncMock()
    mock_ac_instance.get.return_value = mock_response
    mock_ac_instance.__aenter__.return_value = mock_ac_instance
    
    mock_client.return_value = mock_ac_instance

    # Mock database check
    with patch("app.main.check_database_health", new_callable=AsyncMock) as mock_db:
        mock_db.return_value = True
        
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["checks"]["compute"] is True
        assert data["status"] == "healthy"
        
        # Verify URL
        mock_ac_instance.get.assert_called_with("http://localhost:8001/health")


@patch("app.main.httpx.AsyncClient")
def test_health_check_compute_unhealthy(mock_client):
    # Mock compute node failure
    mock_ac_instance = AsyncMock()
    mock_ac_instance.get.side_effect = Exception("Connection refused")
    mock_ac_instance.__aenter__.return_value = mock_ac_instance
    
    mock_client.return_value = mock_ac_instance

    # Mock database check
    with patch("app.main.check_database_health", new_callable=AsyncMock) as mock_db:
        mock_db.return_value = True
        
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["checks"]["compute"] is False
        # Overall status currently only depends on DB in my implementation, 
        # but the check should be False
