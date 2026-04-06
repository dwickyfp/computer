"""
PostgreSQL source runner.
"""

import logging
from threading import Event
from typing import Optional

from pydbzengine import DebeziumJsonEngine

from core.event_handler import CDCEventHandler
from core.record_router import RecordRouter
from sources.postgresql import PostgreSQLSource
from sources.runner_base import BaseSourceRunner

logger = logging.getLogger(__name__)


class PostgresSourceRunner(BaseSourceRunner):
    """Run PostgreSQL CDC via Debezium."""

    def __init__(
        self,
        source: PostgreSQLSource,
        offset_file: str,
        shutdown_event: Event,
    ):
        self._source = source
        self._offset_file = offset_file
        self._shutdown_event = shutdown_event
        self._engine: Optional[DebeziumJsonEngine] = None

    def validate(self, pipeline_name: str, table_include_list: list[str]) -> None:
        is_valid, error_msg = self._source.validate_replication_setup(pipeline_name)
        if not is_valid:
            raise ValueError(error_msg)

    def run(
        self,
        pipeline_name: str,
        table_include_list: list[str],
        router: RecordRouter,
        stop_event: Event,
    ) -> None:
        handler = CDCEventHandler(
            pipeline=router._pipeline,
            destinations=router._destinations,
            dlq_manager=router._dlq_manager,
            shutdown_event=self._shutdown_event,
        )
        props = self._source.build_debezium_props(
            pipeline_name=pipeline_name,
            table_include_list=table_include_list,
            offset_file=self._offset_file,
        )
        self._engine = DebeziumJsonEngine(properties=props, handler=handler)
        self._engine.run()

    def stop(self) -> None:
        if self._engine is not None:
            try:
                self._engine.stop()
            except Exception as exc:
                logger.warning("Failed to stop Debezium engine cleanly: %s", exc)

