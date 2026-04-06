"""
Focused tests for backfill key extraction and filter handling.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.backfill_manager import BackfillManager


def test_extract_keys_uses_detected_composite_key_columns():
    manager = BackfillManager()

    key = manager._extract_keys(
        {"tenant_id": 10, "order_id": 99, "status": "paid"},
        ["tenant_id", "order_id"],
    )

    assert key == {"tenant_id": 10, "order_id": 99}


def test_extract_keys_falls_back_to_id_only_when_present():
    manager = BackfillManager()

    key = manager._extract_keys({"id": 7, "status": "paid"})

    assert key == {"id": 7}


def test_backfill_where_clause_uses_shared_json_v2_compiler():
    manager = BackfillManager()

    clause = manager._build_backfill_where_clause(
        '{"version":2,"groups":[{"conditions":[{"column":"status","operator":"IN","value":"paid,shipped"}],"intraLogic":"AND"}],"interLogic":[]}'
    )

    assert clause == '"status" IN (\'paid\', \'shipped\')'


def test_backfill_where_clause_rejects_invalid_filter():
    manager = BackfillManager()

    with pytest.raises(ValueError):
        manager._build_backfill_where_clause(
            '{"version":2,"groups":[{"conditions":[{"column":"status;DROP","operator":"=","value":"paid"}],"intraLogic":"AND"}],"interLogic":[]}'
        )
