"""
Unit tests for the shared JSON v2 filter compiler.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.filter_sql import build_where_clause_from_filter_sql


def test_build_where_clause_supports_json_v2_groups():
    filter_sql = (
        '{"version":2,"groups":[{"conditions":[{"column":"status","operator":"=","value":"paid"},'
        '{"column":"amount","operator":">=","value":"100"}],"intraLogic":"AND"}],'
        '"interLogic":[]}'
    )

    clause = build_where_clause_from_filter_sql(filter_sql)

    assert clause == '("status" = \'paid\' AND "amount" >= 100)'


def test_build_where_clause_rejects_invalid_identifier():
    filter_sql = (
        '{"version":2,"groups":[{"conditions":[{"column":"status;DROP TABLE users","operator":"=","value":"paid"}],'
        '"intraLogic":"AND"}],"interLogic":[]}'
    )

    with pytest.raises(ValueError):
        build_where_clause_from_filter_sql(filter_sql)


def test_build_where_clause_rejects_invalid_operator():
    filter_sql = (
        '{"version":2,"groups":[{"conditions":[{"column":"amount","operator":"@@","value":"100"}],'
        '"intraLogic":"AND"}],"interLogic":[]}'
    )

    with pytest.raises(ValueError):
        build_where_clause_from_filter_sql(filter_sql)
