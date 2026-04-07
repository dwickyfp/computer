"""
Source Detail schemas.

Separated from source.py to avoid circular imports with wal_monitor.py
"""

from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field

from app.domain.schemas.source import SourceResponse
from app.domain.schemas.wal_monitor import WALMonitorResponse


class SourceTableInfo(BaseModel):
    """
    Schema for table information in source details.
    """

    id: int
    table_name: str
    version: int = Field(default=1, description="Table schema version")
    schema_definition: Optional[List[dict]] = Field(default=None, alias="schema_table")
    first_offset: Optional[int] = None
    next_offset: Optional[int] = None
    message_count: int = 0

    model_config = ConfigDict(from_attributes=True)


class KafkaTopicSummary(BaseModel):
    topic_name: str
    full_topic_name: str
    is_registered: bool
    first_offset: Optional[int] = None
    next_offset: Optional[int] = None
    message_count: int = 0


class KafkaTopicPreviewMessage(BaseModel):
    partition: int
    offset: int
    timestamp: Optional[str] = None
    key_preview: Optional[str] = None
    value_preview: Optional[str] = None
    key: Optional[str] = None
    value: Optional[str] = None
    headers: Optional[str] = None


class KafkaTopicPreviewResponse(BaseModel):
    topic_name: str
    full_topic_name: str
    page: int
    page_size: int
    total_messages: int
    total_pages: int
    messages: List[KafkaTopicPreviewMessage] = []


class TableSchemaDiff(BaseModel):
    """
    Schema for schema differences/evolution.
    """

    new_columns: List[str] = []
    dropped_columns: List[dict] = []
    type_changes: dict = {}  # col_name -> {old_type: str, new_type: str}


class TableSchemaResponse(BaseModel):
    """
    Response schema for table schema with evolution info.
    """

    columns: List[dict]
    diff: Optional[TableSchemaDiff] = None


class SourceDetailResponse(BaseModel):
    """
    Schema for detailed source response.

    Includes source info, source-specific runtime metrics, and table list.
    """

    source: SourceResponse
    wal_monitor: Optional[WALMonitorResponse] = None
    runtime: dict = Field(default_factory=dict)
    tables: List[SourceTableInfo] = []
    destinations: List[str] = []

    model_config = ConfigDict(from_attributes=True)
