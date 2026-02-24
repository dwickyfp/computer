"""
Repositories for Linked Task entities.
"""

from typing import List, Optional

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.domain.models.linked_task import (
    LinkedTask,
    LinkedTaskEdge,
    LinkedTaskRunHistory,
    LinkedTaskRunStepLog,
    LinkedTaskStep,
)


class LinkedTaskRepository:
    def __init__(self, db: Session):
        self.db = db

    def list(self, page: int = 1, page_size: int = 20) -> tuple[List[LinkedTask], int]:
        offset = (page - 1) * page_size
        total = self.db.scalar(select(func.count()).select_from(LinkedTask))
        items = self.db.scalars(
            select(LinkedTask).order_by(LinkedTask.created_at.desc()).offset(offset).limit(page_size)
        ).all()
        return list(items), total or 0

    def get(self, linked_task_id: int) -> Optional[LinkedTask]:
        return self.db.get(LinkedTask, linked_task_id)

    def create(self, name: str, description: Optional[str] = None) -> LinkedTask:
        task = LinkedTask(name=name, description=description)
        self.db.add(task)
        return task

    def update(self, linked_task_id: int, **kwargs) -> Optional[LinkedTask]:
        task = self.get(linked_task_id)
        if not task:
            return None
        for key, value in kwargs.items():
            if value is not None:
                setattr(task, key, value)
        return task

    def delete(self, linked_task_id: int) -> bool:
        task = self.get(linked_task_id)
        if not task:
            return False
        self.db.delete(task)
        return True


class LinkedTaskGraphRepository:
    """Manages steps and edges — the 'graph' of a linked task."""

    def __init__(self, db: Session):
        self.db = db

    def get_steps(self, linked_task_id: int) -> List[LinkedTaskStep]:
        return list(
            self.db.scalars(
                select(LinkedTaskStep).where(
                    LinkedTaskStep.linked_task_id == linked_task_id
                )
            ).all()
        )

    def get_edges(self, linked_task_id: int) -> List[LinkedTaskEdge]:
        return list(
            self.db.scalars(
                select(LinkedTaskEdge).where(
                    LinkedTaskEdge.linked_task_id == linked_task_id
                )
            ).all()
        )

    def replace_graph(
        self,
        linked_task_id: int,
        steps_data: list[dict],
        edges_data: list[dict],
    ) -> tuple[List[LinkedTaskStep], List[LinkedTaskEdge]]:
        """
        Replace all steps and edges for a linked task.

        steps_data: list of dicts with keys: flow_task_id, pos_x, pos_y
        edges_data: list of dicts with keys: source_step_id, target_step_id, condition
        """
        # Delete existing steps (cascades to edges usually, but we clear explicitly if needed)
        # Note: We rely on ON DELETE CASCADE for edges if configured, otherwise we must delete edges first.
        # Let's delete edges first to be safe.
        existing_edges = self.db.scalars(
            select(LinkedTaskEdge).where(LinkedTaskEdge.linked_task_id == linked_task_id)
        ).all()
        for edge in existing_edges:
            self.db.delete(edge)

        existing_steps = self.db.scalars(
            select(LinkedTaskStep).where(LinkedTaskStep.linked_task_id == linked_task_id)
        ).all()
        for step in existing_steps:
            self.db.delete(step)
        self.db.flush()

        # Insert new steps and build ID map
        new_steps = []
        id_map = {}  # frontend_id (int/str) -> new_db_id (int)

        for sd in steps_data:
            step = LinkedTaskStep(
                linked_task_id=linked_task_id,
                flow_task_id=sd["flow_task_id"],
                pos_x=sd.get("pos_x", 0.0),
                pos_y=sd.get("pos_y", 0.0),
            )
            self.db.add(step)
            self.db.flush()  # We flush inside loop to get ID immediately
            new_steps.append(step)
            
            # Map the provided ID (temp or old) to the new real ID
            if "id" in sd and sd["id"] is not None:
                id_map[sd["id"]] = step.id

        # Insert edges using the ID map
        new_edges = []
        for ed in edges_data:
            source_ref = ed["source_step_id"]
            target_ref = ed["target_step_id"]

            real_source_id = id_map.get(source_ref)
            real_target_id = id_map.get(target_ref)

            if real_source_id and real_target_id:
                edge = LinkedTaskEdge(
                    linked_task_id=linked_task_id,
                    source_step_id=real_source_id,
                    target_step_id=real_target_id,
                    condition=ed.get("condition", "ON_SUCCESS"),
                )
                self.db.add(edge)
                new_edges.append(edge)
            else:
                # This might happen if frontend sends an edge to a step that wasn't in steps list
                # We log or ignore. For now, we skip.
                pass

        return new_steps, new_edges


class LinkedTaskRunHistoryRepository:
    def __init__(self, db: Session):
        self.db = db

    def create(self, linked_task_id: int, trigger_type: str = "MANUAL") -> LinkedTaskRunHistory:
        run = LinkedTaskRunHistory(
            linked_task_id=linked_task_id,
            trigger_type=trigger_type,
            status="RUNNING",
        )
        self.db.add(run)
        return run

    def get(self, run_id: int) -> Optional[LinkedTaskRunHistory]:
        return self.db.get(LinkedTaskRunHistory, run_id)

    def list(
        self, linked_task_id: int, page: int = 1, page_size: int = 20
    ) -> tuple[List[LinkedTaskRunHistory], int]:
        offset = (page - 1) * page_size
        total = self.db.scalar(
            select(func.count())
            .select_from(LinkedTaskRunHistory)
            .where(LinkedTaskRunHistory.linked_task_id == linked_task_id)
        )
        items = self.db.scalars(
            select(LinkedTaskRunHistory)
            .where(LinkedTaskRunHistory.linked_task_id == linked_task_id)
            .order_by(LinkedTaskRunHistory.started_at.desc())
            .offset(offset)
            .limit(page_size)
        ).all()
        return list(items), total or 0

    def create_step_log(
        self,
        run_history_id: int,
        linked_task_id: int,
        step_id: int,
        flow_task_id: int,
    ) -> LinkedTaskRunStepLog:
        log = LinkedTaskRunStepLog(
            run_history_id=run_history_id,
            linked_task_id=linked_task_id,
            step_id=step_id,
            flow_task_id=flow_task_id,
            status="PENDING",
        )
        self.db.add(log)
        return log
