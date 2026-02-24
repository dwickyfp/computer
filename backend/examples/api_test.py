#!/usr/bin/env python3
"""
API Testing Examples for Rosetta ETL Platform.

This script demonstrates how to interact with the Rosetta API programmatically.
Can be used for testing, integration, or as a reference for API usage.
"""

import asyncio
from typing import Dict, Any

import httpx


class RosettaAPIClient:
    """Client for interacting with Rosetta ETL Platform API."""

    def __init__(self, base_url: str = "http://localhost:8000"):
        """
        Initialize API client.

        Args:
            base_url: Base URL of the API
        """
        self.base_url = base_url
        self.api_prefix = "/api/v1"

    def _url(self, path: str) -> str:
        """Construct full URL."""
        return f"{self.base_url}{self.api_prefix}{path}"

    async def health_check(self) -> Dict[str, Any]:
        """Check API health."""
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{self.base_url}/health")
            response.raise_for_status()
            return response.json()

    # Source Operations

    async def create_source(self, source_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new source."""
        async with httpx.AsyncClient() as client:
            response = await client.post(self._url("/sources"), json=source_data)
            response.raise_for_status()
            return response.json()

    async def list_sources(self, skip: int = 0, limit: int = 100) -> list:
        """List all sources."""
        async with httpx.AsyncClient() as client:
            response = await client.get(
                self._url("/sources"), params={"skip": skip, "limit": limit}
            )
            response.raise_for_status()
            return response.json()

    async def get_source(self, source_id: int) -> Dict[str, Any]:
        """Get source by ID."""
        async with httpx.AsyncClient() as client:
            response = await client.get(self._url(f"/sources/{source_id}"))
            response.raise_for_status()
            return response.json()

    async def update_source(
        self, source_id: int, update_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Update a source."""
        async with httpx.AsyncClient() as client:
            response = await client.put(
                self._url(f"/sources/{source_id}"), json=update_data
            )
            response.raise_for_status()
            return response.json()

    async def delete_source(self, source_id: int) -> None:
        """Delete a source."""
        async with httpx.AsyncClient() as client:
            response = await client.delete(self._url(f"/sources/{source_id}"))
            response.raise_for_status()

    # Destination Operations

    async def create_destination(
        self, destination_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Create a new destination."""
        async with httpx.AsyncClient() as client:
            response = await client.post(
                self._url("/destinations"), json=destination_data
            )
            response.raise_for_status()
            return response.json()

    async def list_destinations(self, skip: int = 0, limit: int = 100) -> list:
        """List all destinations."""
        async with httpx.AsyncClient() as client:
            response = await client.get(
                self._url("/destinations"), params={"skip": skip, "limit": limit}
            )
            response.raise_for_status()
            return response.json()

    async def get_destination(self, destination_id: int) -> Dict[str, Any]:
        """Get destination by ID."""
        async with httpx.AsyncClient() as client:
            response = await client.get(self._url(f"/destinations/{destination_id}"))
            response.raise_for_status()
            return response.json()

    async def update_destination(
        self, destination_id: int, update_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Update a destination."""
        async with httpx.AsyncClient() as client:
            response = await client.put(
                self._url(f"/destinations/{destination_id}"), json=update_data
            )
            response.raise_for_status()
            return response.json()

    async def delete_destination(self, destination_id: int) -> None:
        """Delete a destination."""
        async with httpx.AsyncClient() as client:
            response = await client.delete(self._url(f"/destinations/{destination_id}"))
            response.raise_for_status()

    # Pipeline Operations

    async def create_pipeline(self, pipeline_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new pipeline."""
        async with httpx.AsyncClient() as client:
            response = await client.post(self._url("/pipelines"), json=pipeline_data)
            response.raise_for_status()
            return response.json()

    async def list_pipelines(self, skip: int = 0, limit: int = 100) -> list:
        """List all pipelines."""
        async with httpx.AsyncClient() as client:
            response = await client.get(
                self._url("/pipelines"), params={"skip": skip, "limit": limit}
            )
            response.raise_for_status()
            return response.json()

    async def get_pipeline(self, pipeline_id: int) -> Dict[str, Any]:
        """Get pipeline by ID."""
        async with httpx.AsyncClient() as client:
            response = await client.get(self._url(f"/pipelines/{pipeline_id}"))
            response.raise_for_status()
            return response.json()

    async def update_pipeline(
        self, pipeline_id: int, update_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Update a pipeline."""
        async with httpx.AsyncClient() as client:
            response = await client.put(
                self._url(f"/pipelines/{pipeline_id}"), json=update_data
            )
            response.raise_for_status()
            return response.json()

    async def delete_pipeline(self, pipeline_id: int) -> None:
        """Delete a pipeline."""
        async with httpx.AsyncClient() as client:
            response = await client.delete(self._url(f"/pipelines/{pipeline_id}"))
            response.raise_for_status()

    async def start_pipeline(self, pipeline_id: int) -> Dict[str, Any]:
        """Start a pipeline."""
        async with httpx.AsyncClient() as client:
            response = await client.post(self._url(f"/pipelines/{pipeline_id}/start"))
            response.raise_for_status()
            return response.json()

    async def pause_pipeline(self, pipeline_id: int) -> Dict[str, Any]:
        """Pause a pipeline."""
        async with httpx.AsyncClient() as client:
            response = await client.post(self._url(f"/pipelines/{pipeline_id}/pause"))
            response.raise_for_status()
            return response.json()

    async def refresh_pipeline(self, pipeline_id: int) -> Dict[str, Any]:
        """Refresh a pipeline."""
        async with httpx.AsyncClient() as client:
            response = await client.post(self._url(f"/pipelines/{pipeline_id}/refresh"))
            response.raise_for_status()
            return response.json()

    # Metrics Operations

    async def get_wal_metrics(
        self, source_id: int | None = None, limit: int = 100
    ) -> list:
        """Get WAL metrics."""
        params = {"limit": limit}
        if source_id is not None:
            params["source_id"] = source_id

        async with httpx.AsyncClient() as client:
            response = await client.get(self._url("/metrics/wal"), params=params)
            response.raise_for_status()
            return response.json()


async def example_workflow():
    """
    Example workflow demonstrating complete API usage.

    This creates a source, destination, pipeline, and queries metrics.
    """
    client = RosettaAPIClient()

    print("=" * 60)
    print("Rosetta ETL Platform - API Example Workflow")
    print("=" * 60)

    # 1. Health Check
    print("\n1. Checking API health...")
    health = await client.health_check()
    print(f"   Status: {health['status']}")
    print(f"   Version: {health['version']}")

    # 2. Create Source
    print("\n2. Creating a PostgreSQL source...")
    source_data = {
        "name": "example-postgres",
        "pg_host": "localhost",
        "pg_port": 5432,
        "pg_database": "example_db",
        "pg_username": "postgres",
        "pg_password": "postgres",
        "publication_name": "example_publication",
        "replication_name": "test_slot_1",
    }

    try:
        source = await client.create_source(source_data)
        source_id = source["id"]
        print(f"   ✓ Source created with ID: {source_id}")
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 409:
            print("   ⚠ Source already exists, fetching existing...")
            sources = await client.list_sources()
            source = next(s for s in sources if s["name"] == "example-postgres")
            source_id = source["id"]
        else:
            raise

    # 3. Create Destination
    print("\n3. Creating a Snowflake destination...")
    destination_data = {
        "name": "example-snowflake",
        "snowflake_account": "xy12345",
        "snowflake_user": "ETL_USER",
        "snowflake_database": "ANALYTICS",
        "snowflake_schema": "RAW",
        "snowflake_role": "SYSADMIN",
        "snowflake_host": "xy12345.snowflakecomputing.com",
    }

    try:
        destination = await client.create_destination(destination_data)
        destination_id = destination["id"]
        print(f"   ✓ Destination created with ID: {destination_id}")
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 409:
            print("   ⚠ Destination already exists, fetching existing...")
            destinations = await client.list_destinations()
            destination = next(
                d for d in destinations if d["name"] == "example-snowflake"
            )
            destination_id = destination["id"]
        else:
            raise

    # 4. Create Pipeline
    print("\n4. Creating an ETL pipeline...")
    pipeline_data = {
        "name": "example-pipeline",
        "source_id": source_id,
        "destination_id": destination_id,
        "status": "START",
    }

    try:
        pipeline = await client.create_pipeline(pipeline_data)
        pipeline_id = pipeline["id"]
        print(f"   ✓ Pipeline created with ID: {pipeline_id}")
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 409:
            print("   ⚠ Pipeline already exists, fetching existing...")
            pipelines = await client.list_pipelines()
            pipeline = next(p for p in pipelines if p["name"] == "example-pipeline")
            pipeline_id = pipeline["id"]
        else:
            raise

    # 5. Get Pipeline Details
    print("\n5. Fetching pipeline details...")
    pipeline_details = await client.get_pipeline(pipeline_id)
    print(f"   Pipeline: {pipeline_details['name']}")
    print(f"   Status: {pipeline_details['status']}")
    print(f"   Source: {pipeline_details['source']['name']}")
    print(f"   Destination: {pipeline_details['destination']['name']}")

    # 6. Pipeline Operations
    print("\n6. Testing pipeline operations...")

    print("   - Pausing pipeline...")
    await client.pause_pipeline(pipeline_id)
    print("     ✓ Pipeline paused")

    print("   - Starting pipeline...")
    await client.start_pipeline(pipeline_id)
    print("     ✓ Pipeline started")

    # 7. Query WAL Metrics
    print("\n7. Querying WAL metrics...")
    metrics = await client.get_wal_metrics(source_id=source_id, limit=5)
    if metrics:
        print(f"   Found {len(metrics)} metrics:")
        for metric in metrics[:3]:
            print(f"     - {metric['size_mb']:.2f} MB at {metric['recorded_at']}")
    else:
        print("   No metrics available yet (WAL monitoring may not have run)")

    # 8. List All Resources
    print("\n8. Listing all resources...")
    sources = await client.list_sources()
    destinations = await client.list_destinations()
    pipelines = await client.list_pipelines()
    print(f"   Total Sources: {len(sources)}")
    print(f"   Total Destinations: {len(destinations)}")
    print(f"   Total Pipelines: {len(pipelines)}")

    print("\n" + "=" * 60)
    print("Example workflow completed successfully!")
    print("=" * 60)


async def cleanup_example_resources():
    """
    Cleanup example resources created by the workflow.

    WARNING: This will delete the example pipeline, source, and destination.
    """
    client = RosettaAPIClient()

    print("\n" + "=" * 60)
    print("Cleaning up example resources...")
    print("=" * 60)

    try:
        # Find and delete example pipeline
        pipelines = await client.list_pipelines()
        example_pipeline = next(
            (p for p in pipelines if p["name"] == "example-pipeline"), None
        )
        if example_pipeline:
            await client.delete_pipeline(example_pipeline["id"])
            print("✓ Deleted example pipeline")

        # Find and delete example source
        sources = await client.list_sources()
        example_source = next(
            (s for s in sources if s["name"] == "example-postgres"), None
        )
        if example_source:
            await client.delete_source(example_source["id"])
            print("✓ Deleted example source")

        # Find and delete example destination
        destinations = await client.list_destinations()
        example_destination = next(
            (d for d in destinations if d["name"] == "example-snowflake"), None
        )
        if example_destination:
            await client.delete_destination(example_destination["id"])
            print("✓ Deleted example destination")

        print("\nCleanup completed successfully!")

    except Exception as e:
        print(f"\n⚠ Error during cleanup: {e}")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "cleanup":
        asyncio.run(cleanup_example_resources())
    else:
        asyncio.run(example_workflow())
