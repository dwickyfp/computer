
import sys
import unittest
from unittest.mock import MagicMock, patch
from datetime import datetime

# Adjust path to find compute module
import os
sys.path.append(os.path.join(os.getcwd()))

from core.notification import NotificationLogRepository, NotificationLogCreate
from core.database import DatabaseSession

# Mock database connection
class MockCursor:
    def __init__(self):
        self.data = {}
        self.last_query = ""
        self.fetchone_result = None
    
    def execute(self, query, params=None):
        self.last_query = query
        self.params = params
        print(f"Executed: {query} | Params: {params}")
        
    def fetchone(self):
        return self.fetchone_result
        
    def close(self):
        pass
    
    def __enter__(self):
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

class MockConnection:
    def __init__(self):
        self.cursor_mock = MockCursor()
        
    def cursor(self):
        return self.cursor_mock
        
    def commit(self):
        print("Commit called")
        
    def rollback(self):
        print("Rollback called")
        
    def close(self):
        pass

class TestNotificationLog(unittest.TestCase):
    
    @patch('core.notification.get_db_connection')
    @patch('core.notification.return_db_connection')
    def test_upsert_new_notification(self, mock_return_conn, mock_get_conn):
        mock_conn = MockConnection()
        mock_get_conn.return_value = mock_conn
        
        # Simulate no existing notification
        mock_conn.cursor_mock.fetchone_result = None
        
        repo = NotificationLogRepository()
        data = NotificationLogCreate(
            key_notification="test_key",
            title="Test Title",
            message="Test Message",
            type="ERROR"
        )
        
        # Mock fetchone to return ID after insert
        def side_effect():
            if "SELECT id, iteration_check" in mock_conn.cursor_mock.last_query:
                return None
            if "INSERT INTO" in mock_conn.cursor_mock.last_query:
                return (123,) # New ID
            return None
            
        mock_conn.cursor_mock.fetchone = side_effect
        
        result_id = repo.upsert_notification_by_key(data)
        
        self.assertEqual(result_id, 123)
        self.assertTrue("INSERT INTO notification_log" in mock_conn.cursor_mock.last_query)
        self.assertTrue("iteration_check" in mock_conn.cursor_mock.last_query)
        
    @patch('core.notification.get_db_connection')
    @patch('core.notification.return_db_connection')
    def test_update_existing_notification(self, mock_return_conn, mock_get_conn):
        mock_conn = MockConnection()
        mock_get_conn.return_value = mock_conn
        
        # Simulate existing notification with iteration 1
        # ID=1, Iteration=1
        
        repo = NotificationLogRepository()
        data = NotificationLogCreate(
            key_notification="test_key",
            title="Test Title",
            message="Test Message Update",
            type="ERROR"
        )
        
        def side_effect():
            if "SELECT id, iteration_check" in mock_conn.cursor_mock.last_query:
                 return (1, 1) # ID=1, Iteration=1
            return None
        
        mock_conn.cursor_mock.fetchone = side_effect
        
        result_id = repo.upsert_notification_by_key(data)
        
        self.assertEqual(result_id, 1)
        self.assertTrue("UPDATE notification_log" in mock_conn.cursor_mock.last_query)
        # Check that iteration is incremented (passed as param)
        # Params: message, title, type, iteration+1 (2), time, id
        self.assertEqual(mock_conn.cursor_mock.params[3], 2)

if __name__ == '__main__':
    unittest.main()
