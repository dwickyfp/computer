"""
Worker client for submitting Celery tasks from the backend.

Provides a clean interface to dispatch tasks to the worker service
and check their status.
"""

import time
from typing import Any, Optional

from celery import Celery
from celery.result import AsyncResult

from app.core.config import get_settings
from app.core.logging import get_logger

logger = get_logger(__name__)


class WorkerClient:
    """
    Client for interacting with the Celery worker service.

    Manages task submission, status polling, and result retrieval.
    """

    _instance: Optional["WorkerClient"] = None
    _celery_app: Optional[Celery] = None
    _health_cache: Optional[dict[str, Any]] = None
    _health_cache_time: float = 0
    _health_cache_ttl: float = 2.0  # Reduced to 2 seconds for faster error recovery

    @classmethod
    def get_instance(cls) -> "WorkerClient":
        """Get or create singleton WorkerClient."""
        if cls._instance is None:
            cls._instance = cls()
            cls._instance._initialize()
        return cls._instance

    @classmethod
    def reset_instance(cls) -> None:
        """Reset the singleton instance to force re-initialization."""
        if cls._instance is not None and cls._instance._celery_app is not None:
            try:
                cls._instance._celery_app.close()
            except Exception:
                pass
        cls._instance = None
        cls._celery_app = None
        logger.info("WorkerClient instance reset")

    def _initialize(self) -> None:
        """Initialize Celery app connection (as producer only)."""
        settings = get_settings()
        # Use Redis db 1 for broker (same as worker)
        broker_url = getattr(settings, "celery_broker_url", None)
        if not broker_url:
            # Derive from redis_url by changing db number
            base = settings.redis_url.rsplit("/", 1)[0]
            broker_url = f"{base}/1"

        result_backend = getattr(settings, "celery_result_backend", None)
        if not result_backend:
            # Use Redis db 2 for result backend (non-blocking)
            base = settings.redis_url.rsplit("/", 1)[0]
            result_backend = f"{base}/2"

        self._celery_app = Celery(
            "rosetta_backend_producer",
            broker=broker_url,
            backend=result_backend,
        )

        self._celery_app.conf.update(
            task_serializer="json",
            accept_content=["json"],
            result_serializer="json",
            result_extended=True,
            task_track_started=True,
            # Disable persistent connection retry to avoid log spam
            broker_connection_retry_on_startup=False,
            broker_connection_retry=False,
            # Result backend settings - don't retry aggressively
            redis_retry_on_timeout=False,
            result_backend_transport_options={
                "retry_policy": {"max_retries": 0},
            },
        )

        logger.info(f"WorkerClient initialized with broker: {broker_url}")

    def _send_task_with_retry(
        self,
        task_name: str,
        args: list,
        queue: str,
    ) -> Any:
        """
        Send a task with automatic retry on connection errors.

        If the Celery app is in a bad state (e.g., after Redis reconnection failure),
        reset the instance and retry once.
        """
        try:
            return self._celery_app.send_task(task_name, args=args, queue=queue)
        except Exception as e:
            error_msg = str(e).lower()
            # Check if this is a recoverable connection error
            if "retry limit exceeded" in error_msg or "connection" in error_msg:
                logger.warning(f"Celery connection error, resetting client: {e}")
                # Reset the singleton to get a fresh connection
                WorkerClient.reset_instance()
                # Re-initialize
                self._initialize()
                # Retry once
                return self._celery_app.send_task(task_name, args=args, queue=queue)
            raise

    def submit_preview_task(
        self,
        sql: str | None,
        source_id: int,
        destination_id: int,
        table_name: str,
        filter_sql: str | None = None,
    ) -> str:
        """
        Submit a preview task to the worker.

        Args:
            sql: Optional custom SQL query
            source_id: Source database ID
            destination_id: Destination database ID
            table_name: Table name to preview
            filter_sql: Optional filter SQL

        Returns:
            Task ID string for polling

        Raises:
            ConnectionError: If Redis/worker is not available
        """
        try:
            result = self._send_task_with_retry(
                "worker.preview.execute",
                args=[sql, source_id, destination_id, table_name, filter_sql],
                queue="preview",
            )

            logger.info(
                f"Preview task submitted: {result.id}",
                extra={
                    "task_id": result.id,
                    "source_id": source_id,
                    "table_name": table_name,
                },
            )

            return result.id
        except Exception as e:
            logger.error(f"Failed to submit preview task: {e}")
            raise ConnectionError(f"Worker unavailable: {e}") from e

    def submit_flow_task_execute(
        self,
        flow_task_id: int,
        run_history_id: int,
        graph_json: dict,
    ) -> str:
        """
        Submit a flow task execution to the Celery worker.

        Args:
            flow_task_id: Flow task ID
            run_history_id: Run history record ID (pre-created)
            graph_json: Dict with {nodes, edges} for the DuckDB compiler

        Returns:
            Celery task ID string
        """
        try:
            result = self._send_task_with_retry(
                "worker.flow_task.execute",
                args=[flow_task_id, run_history_id, graph_json],
                queue="default",
            )
            logger.info(
                f"FlowTask execute task submitted: {result.id}",
                extra={"task_id": result.id, "flow_task_id": flow_task_id},
            )
            return result.id
        except Exception as e:
            logger.error(f"Failed to submit flow task execute: {e}")
            raise ConnectionError(f"Worker unavailable: {e}") from e

    def submit_flow_task_preview(
        self,
        flow_task_id: int,
        node_id: str,
        graph_snapshot: dict,
        limit: int = 500,
    ) -> str:
        """
        Submit a node preview task to the Celery worker.

        Args:
            flow_task_id: Flow task ID (for logging context)
            node_id: Target node ID to preview up to
            graph_snapshot: Current unsaved graph {nodes, edges}
            limit: Row limit for the preview query

        Returns:
            Celery task ID string
        """
        try:
            result = self._send_task_with_retry(
                "worker.flow_task.preview",
                args=[flow_task_id, node_id, graph_snapshot, limit],
                queue="preview",
            )
            logger.info(
                f"FlowTask preview task submitted: {result.id}",
                extra={
                    "task_id": result.id,
                    "flow_task_id": flow_task_id,
                    "node_id": node_id,
                },
            )
            return result.id
        except Exception as e:
            logger.error(f"Failed to submit flow task preview: {e}")
            raise ConnectionError(f"Worker unavailable: {e}") from e

    def submit_destination_table_list_task(
        self,
        destination_id: int,
    ) -> str:
        """
        Submit a task to fetch and persist the table list for a destination.

        Args:
            destination_id: Destination database ID.

        Returns:
            Celery task ID string.
        """
        try:
            result = self._send_task_with_retry(
                "worker.destination_table_list.fetch",
                args=[destination_id],
                queue="default",
            )
            logger.info(
                f"Destination table list task submitted: {result.id}",
                extra={"task_id": result.id, "destination_id": destination_id},
            )
            return result.id
        except Exception as e:
            logger.error(f"Failed to submit destination table list task: {e}")
            raise ConnectionError(f"Worker unavailable: {e}") from e

    def submit_lineage_task(
        self,
        table_sync_id: int,
        custom_sql: str | None,
        source_table: str,
        source_columns: list[str] | None = None,
    ) -> str:
        """
        Submit a lineage generation task to the worker.

        Args:
            table_sync_id: ID of the table sync config
            custom_sql: Custom SQL to parse
            source_table: Source table name
            source_columns: List of source column names

        Returns:
            Task ID string
        """
        try:
            result = self._send_task_with_retry(
                "worker.lineage.generate",
                args=[table_sync_id, custom_sql, source_table, source_columns],
                queue="default",
            )
            logger.info(
                f"Lineage task submitted: {result.id}",
                extra={
                    "task_id": result.id,
                    "table_sync_id": table_sync_id,
                },
            )
            return result.id
        except Exception as e:
            logger.error(f"Failed to submit lineage task: {e}")
            raise ConnectionError(f"Worker unavailable: {e}") from e

    def get_task_status(self, task_id: str) -> dict[str, Any]:
        """
        Get the status of a submitted task.

        Args:
            task_id: Celery task ID

        Returns:
            Dict with state, result (if complete), and metadata
        """
        try:
            result = AsyncResult(task_id, app=self._celery_app)

            response: dict[str, Any] = {
                "task_id": task_id,
                "state": result.state,
            }

            if result.state == "PENDING":
                response["status"] = "queued"
            elif result.state == "STARTED" or result.state == "PROGRESS":
                response["status"] = "running"
                if result.info and isinstance(result.info, dict):
                    response["meta"] = result.info
            elif result.state == "SUCCESS":
                response["status"] = "completed"
                response["result"] = result.result
            elif result.state == "FAILURE":
                response["status"] = "failed"
                response["error"] = (
                    str(result.result) if result.result else "Unknown error"
                )
            elif result.state == "REVOKED":
                response["status"] = "cancelled"
            else:
                response["status"] = result.state.lower()

            return response
        except Exception as e:
            logger.debug(f"Failed to get task status for {task_id}: {e}")
            return {
                "task_id": task_id,
                "state": "UNKNOWN",
                "status": "error",
                "error": f"Worker unavailable: {e}",
            }

    def cancel_task(self, task_id: str) -> bool:
        """
        Cancel a running or pending task.

        Args:
            task_id: Celery task ID

        Returns:
            True if revoke signal was sent
        """
        try:
            self._celery_app.control.revoke(task_id, terminate=True, signal="SIGTERM")
            logger.info(f"Task cancelled: {task_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to cancel task {task_id}: {e}")
            return False

    def get_worker_health(self) -> dict[str, Any]:
        """
        Check if Celery workers are alive and get stats.

        Uses 5-second cache to avoid repeated expensive Celery inspector calls.

        Returns:
            Dict with healthy (bool), active_workers (int),
            active_tasks (int), queues info.
        """
        # Return cached result if fresh
        now = time.time()
        if (
            self._health_cache
            and (now - self._health_cache_time) < self._health_cache_ttl
        ):
            return self._health_cache

        try:
            # Reduced timeout to 1.5 seconds for faster response
            inspector = self._celery_app.control.inspect(timeout=1.5)
            ping_result = inspector.ping()

            if not ping_result:
                return {"healthy": False, "active_workers": 0, "active_tasks": 0}

            active_workers = len(ping_result)

            # Count active tasks
            active_tasks = 0
            active = inspector.active()
            if active:
                for worker_tasks in active.values():
                    active_tasks += len(worker_tasks)

            # Get reserved (queued) tasks
            reserved_tasks = 0
            reserved = inspector.reserved()
            if reserved:
                for worker_tasks in reserved.values():
                    reserved_tasks += len(worker_tasks)

            result = {
                "healthy": active_workers > 0,
                "active_workers": active_workers,
                "active_tasks": active_tasks,
                "reserved_tasks": reserved_tasks,
            }

            # Cache the result
            self._health_cache = result
            self._health_cache_time = now
            return result
        except Exception as e:
            logger.warning(f"Worker health check failed: {e}")
            error_result = {
                "healthy": False,
                "active_workers": 0,
                "active_tasks": 0,
                "error": str(e),
            }
            # Don't cache errors - allow retry on next call
            # Only set cache if it was a connection issue to prevent hammering
            if "Connection refused" in str(e) or "timed out" in str(e).lower():
                self._health_cache = error_result
                self._health_cache_time = now
            return error_result


def get_worker_client() -> WorkerClient:
    """Get the singleton WorkerClient instance."""
    return WorkerClient.get_instance()


def get_node_schema_from_worker(
    node_id: str,
    nodes: list,
    edges: list,
    worker_base_url: str = "http://0.0.0.0:8002",
) -> list[dict]:
    """
    Call the worker's synchronous POST /schema endpoint to resolve the output
    column schema for a node by executing LIMIT 0 through the DuckDB CTE chain.

    Returns a list of {"column_name": str, "data_type": str} dicts,
    or an empty list if the worker is unreachable or the graph is incomplete.
    """
    import httpx

    payload = {"node_id": node_id, "nodes": nodes, "edges": edges}
    try:
        with httpx.Client(timeout=30.0) as client:
            resp = client.post(f"{worker_base_url}/schema", json=payload)
            resp.raise_for_status()
            return resp.json().get("columns", [])
    except Exception as e:
        logger.warning(f"Worker /schema call failed (node={node_id}): {e}")
        return []
