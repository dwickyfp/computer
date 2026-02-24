
import asyncio
from unittest.mock import MagicMock, patch
from datetime import datetime

# Hack to mock modules if not running in full environment
import sys
from typing import Any

# Mock app modules
import contextlib
sys.modules['app.core.config'] = MagicMock()
sys.modules['app.core.logging'] = MagicMock()
sys.modules['app.domain.repositories.source'] = MagicMock()

# Mock database module and context manager
mock_db_module = MagicMock()
sys.modules['app.core.database'] = mock_db_module

@contextlib.contextmanager
def mock_ctx():
    yield MagicMock()
    
mock_db_module.get_session_context = mock_ctx

# Now import the service (will use mocks)
# We need to ensure models are importable. 
# This script is best run in the environment where dependencies are installed.
# But for now, let's try to mock the DB interactions and Service logic flow.

from app.domain.services.schema_monitor import SchemaMonitorService
from app.domain.models.source import Source
from app.domain.models.table_metadata import TableMetadata
from app.domain.models.history_schema_evolution import HistorySchemaEvolution

async def test_schema_monitor():
    print("Starting manual verification of Schema Monitor...")
    
    # 1. Setup Mocks
    mock_db = MagicMock()
    mock_source_repo = MagicMock()
    
    # Mock Source
    source = Source(
        id=1,
        name="test_source",
        pg_host="localhost",
        pg_port=5432, 
        pg_database="test_db",
        pg_username="user",
        pg_password="password",
        publication_name="test_pub",
        is_publication_enabled=True
    )
    
    # Mock Repository returning source
    # We patch SourceRepository within the service module scope if needed, 
    # but here we are calling internal methods or mocking the whole flow.
    # Let's mock the internal methods of the service to unit test the logic.
    
    service = SchemaMonitorService()
    
    # --- Test Case 1: Sync Table List ---
    print("\nTest Case 1: Sync Table List")
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    
    # Mock publication tables: tbl_users (new), tbl_orders (existing)
    mock_cursor.fetchall.return_value = [('public', 'tbl_users'), ('public', 'tbl_orders')]
    
    # Mock existing in DB: tbl_orders, tbl_legacy (to be removed)
    existing_t1 = TableMetadata(id=1, source_id=1, table_name="tbl_orders")
    existing_t2 = TableMetadata(id=2, source_id=1, table_name="tbl_legacy")
    
    # Setup DB query result
    mock_db.query.return_value.filter.return_value.all.return_value = [existing_t1, existing_t2]
    
    try:
        await service.sync_table_list(source, mock_conn, mock_db)
        print("Sync Table List: OK")
        # Verify add: tbl_users
        mock_db.add.assert_called()
        added_arg = mock_db.add.call_args[0][0]
        assert added_arg.table_name == "tbl_users"
        print(" - Added new table: Verified")
        
        # Verify delete: tbl_legacy
        mock_db.delete.assert_called()
        deleted_arg = mock_db.delete.call_args[0][0]
        assert deleted_arg.table_name == "tbl_legacy"
        print(" - Removed old table: Verified")
        
    except Exception as e:
        print(f"FAILED Sync Table List: {e}")

    # --- Test Case 2: Fetch and Compare Schema (Change Detected) ---
    print("\nTest Case 2: Detect Schema Change")
    
    # Table to test
    table_meta = TableMetadata(
        id=1, 
        source_id=1, 
        table_name="tbl_orders", 
        schema_table={"id": {"column_name": "id", "data_type": "INT"}}
    )
    
    # Mock fetch_table_schema result (New column 'status')
    new_schema_list = [
        {"column_name": "id", "data_type": "INT"},
        {"column_name": "status", "data_type": "VARCHAR"}
    ]
    
    # Patch the fetch method to avoid real DB call
    with patch.object(service, 'fetch_table_schema', return_value=new_schema_list):
        # Mock History Count
        mock_db.query.return_value.filter.return_value.count.return_value = 0
        
        await service.fetch_and_compare_schema(source, table_meta, mock_conn, mock_db)
        
        # Verify
        assert table_meta.is_changes_schema == True
        print(" - Flag is_changes_schema set to True: Verified")
        
        # Verify history created
        mock_db.add.assert_called() # Should be called for history
        # Check the last add call
        history_arg = mock_db.add.call_args[0][0]
        if isinstance(history_arg, HistorySchemaEvolution):
            assert history_arg.changes_type == "NEW COLUMN"
            print(" - History record created with NEW COLUMN: Verified")
        else:
             # It might have been the previous add from sync test if we reused mock_db
             # Reuse of mock_db implies we should check call_args_list
             found = False
             for call in mock_db.add.call_args_list:
                 if isinstance(call[0][0], HistorySchemaEvolution):
                     assert call[0][0].changes_type == "NEW COLUMN"
                     found = True
             if found:
                 print(" - History record created with NEW COLUMN: Verified")
             else:
                 print(" - History record NOT found")

    print("\nVerification Complete.")

if __name__ == "__main__":
    asyncio.run(test_schema_monitor())
