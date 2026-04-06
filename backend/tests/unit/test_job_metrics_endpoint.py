from datetime import datetime, timezone
from types import SimpleNamespace

from app.api.v1.endpoints.job_metrics import serialize_job_metrics


def test_serialize_job_metrics_hides_retired_rows_and_sorts_latest_first():
    metrics = [
        SimpleNamespace(
            key_job_scheduler="client_database_sync",
            last_run_at=datetime(2026, 4, 6, 4, 36, 41, tzinfo=timezone.utc),
            created_at=datetime(2026, 4, 6, 3, 36, 38, tzinfo=timezone.utc),
            updated_at=datetime(2026, 4, 6, 4, 36, 41, tzinfo=timezone.utc),
        ),
        SimpleNamespace(
            key_job_scheduler="destination_table_list_refresh",
            last_run_at=datetime(2026, 4, 6, 10, 3, 30, tzinfo=timezone.utc),
            created_at=datetime(2026, 4, 6, 4, 9, 14, tzinfo=timezone.utc),
            updated_at=datetime(2026, 4, 6, 10, 3, 30, tzinfo=timezone.utc),
        ),
        SimpleNamespace(
            key_job_scheduler="system_metric_collection",
            last_run_at=datetime(2026, 4, 6, 10, 3, 39, tzinfo=timezone.utc),
            created_at=datetime(2026, 4, 6, 3, 35, 53, tzinfo=timezone.utc),
            updated_at=datetime(2026, 4, 6, 10, 3, 39, tzinfo=timezone.utc),
        ),
    ]

    result = serialize_job_metrics(metrics)

    assert [item["key_job_scheduler"] for item in result] == [
        "system_metric_collection",
        "destination_table_list_refresh",
    ]
