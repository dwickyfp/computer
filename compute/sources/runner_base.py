"""
Base source runner interface.
"""

from abc import ABC, abstractmethod
from threading import Event

from core.record_router import RecordRouter


class BaseSourceRunner(ABC):
    """Common runtime interface for all source transports."""

    @abstractmethod
    def validate(self, pipeline_name: str, table_include_list: list[str]) -> None:
        """Validate the source before starting the pipeline."""

    @abstractmethod
    def run(
        self,
        pipeline_name: str,
        table_include_list: list[str],
        router: RecordRouter,
        stop_event: Event,
    ) -> None:
        """Run the source until stopped."""

    @abstractmethod
    def stop(self) -> None:
        """Stop the source runner."""

