from datetime import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock

from app.domain.services.pipeline import PipelineService


def _make_query(result):
    query = MagicMock()
    query.filter_by.return_value = query
    if isinstance(result, list):
        query.all.return_value = result
    else:
        query.first.return_value = result
    return query


def test_get_destination_tables_includes_kafka_topics_without_schema(monkeypatch):
    db = MagicMock()
    db.query.side_effect = [
        _make_query(SimpleNamespace(id=77)),
        _make_query([]),
    ]

    service = PipelineService(db)
    service.repository = MagicMock()
    service.repository.get_by_id_with_relations.return_value = SimpleNamespace(
        id=12,
        source_id=5,
    )

    table_meta = SimpleNamespace(
        table_name="tbl_rosetta_sales",
        schema_table={},
    )

    fake_repo = MagicMock()
    fake_repo.get_by_source_id.return_value = [table_meta]
    monkeypatch.setattr("app.domain.services.pipeline.TableMetadataRepository", lambda _: fake_repo)

    tables = service.get_destination_tables(12, 77)

    assert len(tables) == 1
    assert tables[0]["table_name"] == "tbl_rosetta_sales"
    assert tables[0]["columns"] == []
    assert tables[0]["sync_configs"] == []


def test_get_destination_tables_keeps_sync_configs_for_tables_without_schema(monkeypatch):
    now = datetime(2026, 4, 6, 15, 0, 0)
    db = MagicMock()
    sync = SimpleNamespace(
        id=9,
        pipeline_destination_id=77,
        table_name="tbl_rosetta_sales",
        table_name_target="tbl_rosetta_sales",
        custom_sql=None,
        filter_sql=None,
        primary_key_column_target=None,
        is_exists_table_landing=False,
        is_exists_stream=False,
        is_exists_task=False,
        is_exists_table_destination=False,
        is_error=False,
        error_message=None,
        lineage_metadata=None,
        lineage_status="PENDING",
        lineage_error=None,
        lineage_generated_at=None,
        catalog_database_name=None,
        created_at=now,
        updated_at=now,
    )
    db.query.side_effect = [
        _make_query(SimpleNamespace(id=77)),
        _make_query([sync]),
    ]

    service = PipelineService(db)
    service.repository = MagicMock()
    service.repository.get_by_id_with_relations.return_value = SimpleNamespace(
        id=12,
        source_id=5,
    )

    table_meta = SimpleNamespace(
        table_name="tbl_rosetta_sales",
        schema_table={},
    )
    fake_repo = MagicMock()
    fake_repo.get_by_source_id.return_value = [table_meta]
    monkeypatch.setattr("app.domain.services.pipeline.TableMetadataRepository", lambda _: fake_repo)

    tables = service.get_destination_tables(12, 77)

    assert len(tables) == 1
    assert len(tables[0]["sync_configs"]) == 1
    assert tables[0]["sync_configs"][0]["id"] == 9
