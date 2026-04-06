"""
Debezium change event handler.

Parses Debezium JSON change events into normalized ``CDCRecord`` batches and
delegates routing to ``RecordRouter``.
"""

import json
import logging
import threading
import time as _time
from typing import Optional

from pydbzengine import BasePythonChangeHandler, ChangeEvent

from core.dlq_manager import DLQManager
from core.models import Pipeline
from core.record_router import RecordRouter
from destinations.base import BaseDestination, CDCRecord

logger = logging.getLogger(__name__)


class CDCEventHandler(BasePythonChangeHandler):
    """Convert Debezium events into normalized CDC records."""

    def __init__(
        self,
        pipeline: Pipeline,
        destinations: dict[int, BaseDestination],
        dlq_manager: Optional[DLQManager] = None,
        shutdown_event: Optional[threading.Event] = None,
    ):
        self._pipeline = pipeline
        self._shutdown_event = shutdown_event or threading.Event()
        self._logger = logging.getLogger(f"{__name__}.{pipeline.name}")
        self._router = RecordRouter(
            pipeline=pipeline,
            destinations=destinations,
            dlq_manager=dlq_manager,
            shutdown_event=self._shutdown_event,
        )

    def _parse_destination_to_table_name(self, destination: str) -> str:
        dest_str = str(destination) if destination is not None else ""
        if not dest_str:
            return ""

        parts = dest_str.split(".")
        if len(parts) < 3:
            return ""
        return parts[-1]

    def _parse_record(self, record: ChangeEvent) -> Optional[CDCRecord]:
        try:
            destination = record.destination()
            key_data = record.key()
            value_data = record.value()

            key_obj = json.loads(str(key_data)) if key_data is not None else {}
            value_obj = json.loads(str(value_data)) if value_data is not None else {}

            payload = value_obj.get("payload", {})
            op = payload.get("op")

            if isinstance(key_obj, dict) and "payload" in key_obj:
                key = key_obj["payload"]
            else:
                key = key_obj

            if op in ("c", "u", "r"):
                value = payload.get("after", {})
            elif op == "d":
                value = payload.get("before", {})
            elif op == "m":
                return None
            else:
                value = payload if payload else {}

            table_name = self._parse_destination_to_table_name(destination)
            if not table_name:
                return None

            ts_ms = payload.get("ts_ms")
            if ts_ms is None:
                ts_ms = int(_time.time() * 1000)

            return CDCRecord(
                operation=op or "u",
                table_name=table_name,
                key=key if isinstance(key, dict) else {},
                value=value if isinstance(value, dict) else {},
                schema=value_obj.get("schema"),
                timestamp=ts_ms,
            )
        except Exception as exc:
            self._logger.error("Failed to parse record: %s", exc, exc_info=True)
            return None

    def handleJsonBatch(self, records: list[ChangeEvent]) -> None:
        records_by_table: dict[str, list[CDCRecord]] = {}
        for record in records:
            cdc_record = self._parse_record(record)
            if cdc_record is None:
                continue
            records_by_table.setdefault(cdc_record.table_name, []).append(cdc_record)

        self._router.route_batches(records_by_table)

