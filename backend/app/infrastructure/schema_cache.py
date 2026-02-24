"""
Redis-backed server-side cache for node output column schemas.

Cache key:  schema:{flow_task_id}:{node_id}:{fingerprint_hex}
TTL:        86400 s (24 h) — naturally invalidated by fingerprint changes

Fingerprint logic mirrors the frontend's `getSchemaFingerprint()` in
`useNodeSchema.ts`.  Only schema-affecting fields are included so that
editing e.g. a filter value (which doesn't change output columns) does
NOT bust the cache.

Schema-affecting fields per node type
--------------------------------------
input      : source_id, destination_id, table_name, schema_name, columns
clean      : select_columns, drop_columns, renames, calculations
             (filter_expr / filter_rows / drop_nulls do NOT affect column shape)
aggregate  : group_by, aggregations[function, column, alias]
join       : join_type, left_keys, right_keys, output_columns
pivot      : pivot_type, pivot_column, value_column, pivot_values
union      : (no schema-affecting config — depends on inputs only)
output     : (no schema-affecting config)
"""

import hashlib
import json
import logging
from functools import lru_cache
from typing import Any

import redis as redis_lib

logger = logging.getLogger(__name__)

CACHE_TTL = 86_400  # 24 hours
CACHE_KEY_PREFIX = "schema"

# ──────────────────────────────────────────────────────────────────────────────
# Redis client (lazily initialised, singleton)
# ──────────────────────────────────────────────────────────────────────────────

_redis_client: redis_lib.Redis | None = None


def _get_redis(redis_url: str) -> redis_lib.Redis:
    global _redis_client
    if _redis_client is None:
        _redis_client = redis_lib.from_url(
            redis_url,
            decode_responses=True,
            socket_connect_timeout=2,
            socket_timeout=2,
        )
    return _redis_client


# ──────────────────────────────────────────────────────────────────────────────
# Fingerprint helpers
# ──────────────────────────────────────────────────────────────────────────────

def _node_fingerprint(node: dict) -> dict:
    """
    Extract only schema-affecting fields from a node dict.
    Mirrors frontend getSchemaFingerprint().
    """
    node_type = node.get("type", "")
    data: dict = node.get("data", {})

    if node_type == "input":
        return {
            "source_id": data.get("source_id"),
            "destination_id": data.get("destination_id"),
            "table_name": data.get("table_name"),
            "schema_name": data.get("schema_name"),
            "columns": data.get("columns"),
        }

    if node_type == "clean":
        return {
            "select_columns": data.get("select_columns"),
            "drop_columns": data.get("drop_columns"),
            "renames": data.get("renames"),
            "calculations": data.get("calculations"),
        }

    if node_type == "aggregate":
        aggs = data.get("aggregations") or []
        return {
            "group_by": data.get("group_by"),
            "aggregations": [
                {
                    "function": a.get("function") or a.get("func"),
                    "column": a.get("column"),
                    "alias": a.get("alias"),
                }
                for a in aggs
            ],
        }

    if node_type == "join":
        return {
            "join_type": data.get("join_type"),
            "left_keys": data.get("left_keys"),
            "right_keys": data.get("right_keys"),
            "output_columns": data.get("output_columns"),
        }

    if node_type == "pivot":
        return {
            "pivot_type": data.get("pivot_type"),
            "pivot_column": data.get("pivot_column"),
            "value_column": data.get("value_column"),
            "pivot_values": data.get("pivot_values"),
        }

    # union, output — schema is purely determined by topology
    return {}


def _graph_fingerprint(
    node_id: str,
    nodes: list[dict],
    edges: list[dict],
) -> str:
    """
    Return a stable hex digest representing the schema-relevant parts of the
    graph for a given target node.
    """
    node_fps = [
        {"id": n["id"], "type": n.get("type", ""), "fp": _node_fingerprint(n)}
        for n in nodes
    ]
    edge_fps = sorted(
        f"{e.get('source', e.get('sourceHandle', ''))}->{e.get('target', e.get('targetHandle', ''))}"
        for e in edges
    )
    payload = json.dumps(
        {"target": node_id, "nodes": node_fps, "edges": edge_fps},
        sort_keys=True,
        default=str,
    )
    return hashlib.sha256(payload.encode()).hexdigest()[:24]


# ──────────────────────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────────────────────

def get_or_fetch_schema(
    *,
    flow_task_id: int,
    node_id: str,
    nodes: list[dict],
    edges: list[dict],
    redis_url: str,
    fetcher,  # callable() -> list[dict]
) -> list[dict]:
    """
    Return cached column schema if available; otherwise call *fetcher*,
    store the result in Redis, and return it.

    Parameters
    ----------
    fetcher : zero-argument callable that returns list[{"column_name", "data_type"}]
    """
    fingerprint = _graph_fingerprint(node_id, nodes, edges)
    cache_key = f"{CACHE_KEY_PREFIX}:{flow_task_id}:{node_id}:{fingerprint}"

    try:
        r = _get_redis(redis_url)
        cached = r.get(cache_key)
        if cached is not None:
            logger.debug("Schema cache HIT  key=%s", cache_key)
            return json.loads(cached)
        logger.debug("Schema cache MISS key=%s", cache_key)
    except Exception as exc:
        logger.warning("Redis schema cache read failed (%s) — bypassing cache", exc)
        return fetcher()

    # Cache miss — fetch from worker
    result = fetcher()

    # Only store non-empty results (empty means worker couldn't compute yet)
    if result:
        try:
            r.set(cache_key, json.dumps(result), ex=CACHE_TTL)
            logger.debug("Schema cache SET  key=%s  ttl=%ds", cache_key, CACHE_TTL)
        except Exception as exc:
            logger.warning("Redis schema cache write failed (%s) — result not cached", exc)

    return result


def invalidate_schema_cache(
    *,
    flow_task_id: int,
    redis_url: str,
) -> int:
    """
    Delete all cached schemas for a specific flow task (e.g. when the graph
    is saved/replaced).  Returns the number of keys deleted.
    """
    pattern = f"{CACHE_KEY_PREFIX}:{flow_task_id}:*"
    try:
        r = _get_redis(redis_url)
        keys = r.keys(pattern)
        if keys:
            deleted = r.delete(*keys)
            logger.info("Schema cache invalidated %d keys for flow_task_id=%d", deleted, flow_task_id)
            return deleted
    except Exception as exc:
        logger.warning("Redis schema cache invalidation failed (%s)", exc)
    return 0
