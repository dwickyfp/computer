/**
 * useNodeSchema — resolve the output column schema for a node by running
 * the CTE chain up to it (LIMIT 0) inside the worker's DuckDB instance.
 *
 * Because the execution goes through DuckDB, the returned columns correctly
 * reflect transformations — e.g. an Aggregate node returns its group-by
 * columns + aggregation aliases, not the raw source columns.
 *
 * ## Cache strategy
 * The query key is built from a *schema fingerprint* — only the fields that
 * affect the node's output column shape are included. Fields that do NOT
 * change the schema (filter_expr, filter_rows, drop_nulls, deduplicate, label,
 * write_mode, upsert_keys, …) are intentionally excluded. This means:
 *
 *   - Editing a filter row  → fingerprint unchanged → instant cached result
 *   - Changing select_columns / group_by / aggregations → fingerprint changes
 *     → single fresh DuckDB fetch
 *   - Adding/removing an edge → topology changes → re-fetch
 *
 * staleTime: Infinity so the cache is never auto-expired; it is only
 * invalidated when the fingerprint changes via user actions in the graph.
 *
 * Usage:
 *   const { columns, isLoading } = useNodeSchema(flowTaskId, nodeId)
 */

import { useQuery } from '@tanstack/react-query'
import { useFlowTaskStore } from '../store/flow-task-store'
import { flowTasksRepo } from '@/repo/flow-tasks'
import type { FlowNode, FlowEdge, ColumnInfo } from '@/repo/flow-tasks'

export interface UseNodeSchemaResult {
    columns: ColumnInfo[]
    isLoading: boolean
    isError: boolean
}

// ─── Schema fingerprint ────────────────────────────────────────────────────────
//
// Returns a minimal plain object containing ONLY the fields that determine the
// output column shape of a node. Changes to anything outside this set (filters,
// labels, write modes, etc.) will NOT bust the cache.

function getSchemaFingerprint(node: FlowNode): Record<string, unknown> {
    const d = node.data
    switch (node.type) {
        case 'input':
            return {
                source_id: d.source_id ?? null,
                destination_id: d.destination_id ?? null,
                table_name: d.table_name ?? null,
                schema_name: d.schema_name ?? null,
                // explicit column projection changes output shape
                columns: d.columns ?? null,
            }
        case 'clean':
            return {
                // only structural transforms change output columns
                select_columns: d.select_columns ?? [],
                drop_columns: (d as Record<string, unknown>).drop_columns ?? [],
                renames: d.rename_columns ?? {},
                calculations: (d as Record<string, unknown>).calculations ?? [],
            }
        case 'aggregate':
            return {
                group_by: d.group_by ?? [],
                // alias + function + column all affect output column names/types
                aggregations: (d.aggregations ?? []).map((a) => ({
                    function: a.function,
                    column: a.column,
                    alias: a.alias,
                })),
            }
        case 'join':
            return {
                join_type: d.join_type ?? 'INNER',
                left_keys: d.join_conditions?.map((c) => c.left_col) ?? [],
                right_keys: d.join_conditions?.map((c) => c.right_col) ?? [],
                output_columns: (d as Record<string, unknown>).output_columns ?? [],
            }
        case 'union':
            return {
                // UNION vs UNION ALL doesn't change column shape, but distinct does
                distinct: d.union_all === false,
            }
        case 'pivot':
            return {
                pivot_type: d.pivot_type ?? 'PIVOT',
                pivot_column: d.pivot_column ?? null,
                value_column: d.value_column ?? null,
                pivot_values: d.pivot_values ?? [],
            }
        case 'output':
        case 'new_rows':
        default:
            return {}
    }
}

// ─── Helper: Get Ancestors ─────────────────────────────────────────────────────

function getAncestors(targetId: string, edges: FlowEdge[]): Set<string> {
    const ancestors = new Set<string>()
    const queue = [targetId]
    ancestors.add(targetId)

    while (queue.length > 0) {
        const current = queue.shift()!
        // Find edges where target is current (incoming edges)
        const incoming = edges.filter((e) => e.target === current)
        for (const edge of incoming) {
            if (!ancestors.has(edge.source)) {
                ancestors.add(edge.source)
                queue.push(edge.source)
            }
        }
    }
    return ancestors
}

// ─── Hook ─────────────────────────────────────────────────────────────────────

/**
 * Fetch the DuckDB-derived output schema for a node.
 *
 * @param flowTaskId  ID of the persisted flow task (for the backend route).
 * @param nodeId      Target node whose output schema we want.
 */
export function useNodeSchema(
    flowTaskId: number | null | undefined,
    nodeId: string | null | undefined,
): UseNodeSchemaResult {
    const { nodes, edges } = useFlowTaskStore()

    // Build graph snapshot (full, sent to worker for DuckDB execution)
    const graphSnapshot = {
        node_id: nodeId ?? '',
        nodes: (nodes ?? []).map((n) => ({
            id: n.id,
            type: n.type,
            position: n.position,
            data: n.data,
        })),
        edges: (edges ?? []).map((e) => ({
            id: e.id,
            source: e.source,
            target: e.target,
            sourceHandle: (typeof (e as Record<string, unknown>).sourceHandle === 'string'
                ? (e as Record<string, unknown>).sourceHandle
                : null) as string | null,
            targetHandle: (typeof (e as Record<string, unknown>).targetHandle === 'string'
                ? (e as Record<string, unknown>).targetHandle
                : null) as string | null,
        })),
    }

    // Only enable when at least one input node has a configured source — otherwise
    // the worker cannot ATTACH to any DB and the schema call will always fail.
    const hasConfiguredInput = (nodes ?? []).some(
        (n) => n.type === 'input' && (n.data.source_id || n.data.destination_id),
    )

    const enabled =
        !!flowTaskId &&
        !!nodeId &&
        (nodes ?? []).length > 0 &&
        hasConfiguredInput

    // ── Cache key ──────────────────────────────────────────────────────────────
    // Optimization: The output schema of `nodeId` depends ONLY on `nodeId` itself
    // and its upstream ancestors. Changes to downstream nodes or disjoint branches
    // should NOT invalidate the cache.
    
    // 1. Find all ancestor nodes
    const ancestorIds = nodeId ? getAncestors(nodeId, edges) : new Set<string>()

    // 2. Filter nodes/edges for the fingerprint
    const relevantNodes = (nodes ?? []).filter((n) => ancestorIds.has(n.id))
    const relevantEdges = (edges ?? []).filter((e) => ancestorIds.has(e.source) && ancestorIds.has(e.target))

    const fingerprintKey = JSON.stringify({
        nodes: relevantNodes.map((n) => ({
            id: n.id,
            type: n.type,
            fp: getSchemaFingerprint(n),
        })),
        edges: relevantEdges.map((e) => `${e.source}->${e.target}`),
    })

    const { data, isLoading, isError } = useQuery({
        queryKey: ['node-schema', flowTaskId, nodeId, fingerprintKey],
        queryFn: () => flowTasksRepo.getNodeSchema(flowTaskId!, graphSnapshot),
        enabled,
        // Never auto-expire — schema only changes when fingerprint changes
        staleTime: Infinity,
        // Keep unused cache entries for 10 minutes before GC
        gcTime: 10 * 60_000,
        // Don't retry on failure — if the worker is unreachable one retry is enough
        retry: 1,
        retryDelay: 2_000,
        select: (res) => res.data.columns,
    })

    return {
        columns: data ?? [],
        isLoading: enabled ? isLoading : false,
        isError,
    }
}
