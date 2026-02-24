"""
Worker main entry point.

Usage:
    # Start worker
    celery -A main worker --loglevel=info -Q preview,default

    # Start with specific concurrency
    celery -A main worker --loglevel=info -Q preview -c 4

    # Monitor with Flower (optional)
    celery -A main flower --port=5555
"""

from app.celery_app import celery_app  # noqa: F401

# Import tasks to register them (all task modules, not just preview)
from app.tasks.preview.task import execute_preview_task  # noqa: F401
from app.tasks.lineage.task import generate_lineage_task  # noqa: F401
from app.tasks.flow_task.task import execute_flow_task_task, preview_flow_task_node_task  # noqa: F401
from app.tasks.destination_table_list.task import fetch_destination_table_list_task  # noqa: F401
from app.tasks.linked_task.task import execute_linked_task_task  # noqa: F401


if __name__ == "__main__":
    celery_app.start()
