"""
Example: Using WAL Monitor API with Upsert Pattern

Demonstrates how to use the WAL monitor API endpoints to maintain
real-time WAL status with automatic upsert (1 source = 1 row).
"""

import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo
import httpx


BASE_URL = "http://localhost:8000/api/v1"


async def example_wal_monitor_workflow():
    """
    Demonstrate complete WAL monitor workflow with upsert pattern.
    """
    async with httpx.AsyncClient() as client:
        source_id = 1  # Example source ID

        print("=" * 60)
        print("WAL Monitor Upsert Pattern Example")
        print("=" * 60)

        # ===================================================================
        # 1. FIRST UPSERT - Creates new record
        # ===================================================================
        print("\n1. First Upsert (INSERT) - Creating new WAL monitor record")
        print("-" * 60)

        create_data = {
            "source_id": source_id,
            "wal_lsn": "0/1A2B3C4D",
            "wal_position": 440401997,
            "last_wal_received": datetime.now(ZoneInfo('Asia/Jakarta'))().isoformat(),
            "replication_slot_name": "slot_replication_1",
            "replication_lag_bytes": 1024,
            "status": "ACTIVE",
        }

        response = await client.post(
            f"{BASE_URL}/wal-monitor/sources/{source_id}",
            json=create_data,
        )

        if response.status_code == 200:
            monitor = response.json()
            print(f"✅ Monitor created successfully!")
            print(f"   Monitor ID: {monitor['id']}")
            print(f"   Source ID: {monitor['source_id']}")
            print(f"   WAL LSN: {monitor['wal_lsn']}")
            print(f"   Status: {monitor['status']}")
            print(f"   Created At: {monitor['created_at']}")
            initial_id = monitor["id"]
        else:
            print(f"❌ Failed: {response.status_code} - {response.text}")
            return

        # ===================================================================
        # 2. SECOND UPSERT - Updates existing record (same source_id)
        # ===================================================================
        print("\n2. Second Upsert (UPDATE) - Updating existing record")
        print("-" * 60)

        # Simulate WAL progress
        update_data = {
            "source_id": source_id,
            "wal_lsn": "0/1A2B3C5E",  # Advanced LSN
            "wal_position": 440402014,  # Advanced position
            "last_wal_received": datetime.now(ZoneInfo('Asia/Jakarta'))().isoformat(),
            "replication_slot_name": "slot_replication_1",
            "replication_lag_bytes": 512,  # Reduced lag
            "status": "ACTIVE",
        }

        response = await client.post(
            f"{BASE_URL}/wal-monitor/sources/{source_id}",
            json=update_data,
        )

        if response.status_code == 200:
            monitor = response.json()
            print(f"✅ Monitor updated successfully!")
            print(f"   Monitor ID: {monitor['id']} (same as before: {initial_id})")
            print(f"   Source ID: {monitor['source_id']}")
            print(f"   WAL LSN: {monitor['wal_lsn']} (updated)")
            print(f"   Lag: {monitor['replication_lag_bytes']} bytes (reduced)")
            print(f"   Status: {monitor['status']}")
            print(f"   Updated At: {monitor['updated_at']}")
        else:
            print(f"❌ Failed: {response.status_code} - {response.text}")
            return

        # ===================================================================
        # 3. GET - Retrieve current monitor state
        # ===================================================================
        print("\n3. Get WAL Monitor by Source")
        print("-" * 60)

        response = await client.get(f"{BASE_URL}/wal-monitor/sources/{source_id}")

        if response.status_code == 200:
            monitor = response.json()
            print(f"✅ Retrieved monitor successfully!")
            print(f"   Monitor ID: {monitor['id']}")
            print(f"   WAL LSN: {monitor['wal_lsn']}")
            print(f"   Lag: {monitor['replication_lag_bytes']} bytes")
            print(f"   Status: {monitor['status']}")
        else:
            print(f"❌ Failed: {response.status_code} - {response.text}")

        # ===================================================================
        # 4. UPDATE STATUS - Quick status update
        # ===================================================================
        print("\n4. Update Monitor Status to ERROR")
        print("-" * 60)

        status_data = {
            "status": "ERROR",
            "error_message": "Connection timeout to source database",
        }

        response = await client.patch(
            f"{BASE_URL}/wal-monitor/sources/{source_id}/status",
            json=status_data,
        )

        if response.status_code == 200:
            monitor = response.json()
            print(f"✅ Status updated successfully!")
            print(f"   Status: {monitor['status']}")
            print(f"   Error: {monitor['error_message']}")
        else:
            print(f"❌ Failed: {response.status_code} - {response.text}")

        # ===================================================================
        # 5. LIST ALL - View all monitors
        # ===================================================================
        print("\n5. List All WAL Monitors")
        print("-" * 60)

        response = await client.get(f"{BASE_URL}/wal-monitor/")

        if response.status_code == 200:
            data = response.json()
            monitors = data["monitors"]
            total = data["total"]
            print(f"✅ Retrieved {total} monitor(s)")
            for mon in monitors:
                print(
                    f"   - Source {mon['source_id']}: {mon['status']} (LSN: {mon['wal_lsn']})"
                )
        else:
            print(f"❌ Failed: {response.status_code} - {response.text}")

        # ===================================================================
        # 6. DELETE - Remove monitor record
        # ===================================================================
        print("\n6. Delete WAL Monitor")
        print("-" * 60)

        response = await client.delete(f"{BASE_URL}/wal-monitor/sources/{source_id}")

        if response.status_code == 204:
            print(f"✅ Monitor deleted successfully!")
        else:
            print(f"❌ Failed: {response.status_code} - {response.text}")

        print("\n" + "=" * 60)
        print("Workflow completed!")
        print("=" * 60)


async def example_multiple_sources():
    """
    Demonstrate upsert pattern with multiple sources.
    Shows that each source maintains exactly 1 row.
    """
    async with httpx.AsyncClient() as client:
        print("\n" + "=" * 60)
        print("Multiple Sources Example (1 source = 1 row)")
        print("=" * 60)

        sources = [1, 2, 3]

        # Upsert monitors for multiple sources
        for source_id in sources:
            print(f"\nUpserting monitor for source {source_id}...")

            data = {
                "source_id": source_id,
                "wal_lsn": f"0/{source_id}A2B3C4D",
                "wal_position": 440401997 + (source_id * 1000),
                "last_wal_received": datetime.now(ZoneInfo('Asia/Jakarta'))().isoformat(),
                "status": "ACTIVE",
            }

            response = await client.post(
                f"{BASE_URL}/wal-monitor/sources/{source_id}",
                json=data,
            )

            if response.status_code == 200:
                monitor = response.json()
                print(f"  ✅ Source {source_id}: Monitor ID {monitor['id']}")

        # List all monitors
        print("\nListing all monitors...")
        response = await client.get(f"{BASE_URL}/wal-monitor/")

        if response.status_code == 200:
            data = response.json()
            print(f"✅ Total monitors: {data['total']}")
            for mon in data["monitors"]:
                print(
                    f"   - Monitor ID {mon['id']}: Source {mon['source_id']} ({mon['status']})"
                )

        # Update one of them (should maintain same Monitor ID)
        source_to_update = 2
        print(f"\nUpdating monitor for source {source_to_update} again...")

        update_data = {
            "source_id": source_to_update,
            "wal_lsn": "0/UPDATED123",
            "wal_position": 999999999,
            "status": "ACTIVE",
        }

        response = await client.post(
            f"{BASE_URL}/wal-monitor/sources/{source_to_update}",
            json=update_data,
        )

        if response.status_code == 200:
            monitor = response.json()
            print(
                f"  ✅ Source {source_to_update}: Still same Monitor ID {monitor['id']}"
            )
            print(f"     Updated WAL LSN: {monitor['wal_lsn']}")


if __name__ == "__main__":
    print(
        """
    WAL Monitor API - Upsert Pattern Examples
    
    This demonstrates the upsert mechanism where:
    - 1 source_id = 1 monitor record
    - First call creates (INSERT)
    - Subsequent calls update (UPDATE)
    - Database constraint ensures uniqueness
    
    Make sure the backend server is running on http://localhost:8000
    """
    )

    # Run the examples
    asyncio.run(example_wal_monitor_workflow())
    asyncio.run(example_multiple_sources())
