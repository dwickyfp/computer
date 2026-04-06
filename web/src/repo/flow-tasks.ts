import type { Edge } from '@xyflow/react'
import { api } from './client'

// ─── Enums ──────────────────────────────────────────────────────────────────

export type FlowTaskStatus = 'IDLE' | 'RUNNING' | 'SUCCESS' | 'FAILED'
export type FlowTaskTriggerType = 'MANUAL' | 'SCHEDULED'
export type FlowTaskRunStatus = 'RUNNING' | 'SUCCESS' | 'FAILED' | 'CANCELLED'
export type FlowTaskNodeStatus =
  | 'PENDING'
  | 'RUNNING'
  | 'SUCCESS'
  | 'FAILED'
  | 'SKIPPED'
export type WriteMode = 'APPEND' | 'UPSERT' | 'REPLACE'

// ─── Graph node / edge types ─────────────────────────────────────────────────

export type FlowNodeType =
  | 'input'
  | 'clean'
  | 'aggregate'
  | 'join'
  | 'union'
  | 'pivot'
  | 'new_rows'
  | 'sql'
  | 'output'
  | 'note'

export interface NodePosition {
  x: number
  y: number
}

/** Data payload carried inside each ReactFlow node */
export interface FlowNodeData {
  label?: string
  // input node
  source_type?: 'POSTGRES' | 'SNOWFLAKE'
  source_id?: number
  destination_id?: number
  schema_name?: string
  table_name?: string
  alias?: string
  sample_limit?: number // for input node preview limit
  filter_sql?: string // raw WHERE clause for input node filtering
  filter_rows?: Array<{ col: string; op: string; val: string }> // UI filter builder rows (input node)
  // clean node
  drop_nulls?: boolean
  deduplicate?: boolean
  rename_columns?: Record<string, string>
  cast_columns?: Array<{ column: string; target_type: string }> // column type casting
  expressions?: Array<{ expr: string; alias: string }> // SQL function expressions (COALESCE, etc.)
  filter_expr?: string
  select_columns?: string[]
  // aggregate node
  group_by?: string[]
  aggregations?: Array<{ function: string; column: string; alias: string }>
  // join node
  join_type?: string
  left_input?: string
  right_input?: string
  join_conditions?: Array<{ left_col: string; right_col: string }>
  // union node
  union_all?: boolean
  input_ids?: string[]
  // pivot node
  pivot_type?: 'PIVOT' | 'UNPIVOT'
  pivot_column?: string
  pivot_values?: string[]
  value_column?: string
  value_columns?: string[]
  name_column?: string
  include_columns?: string[]
  // new_rows node
  new_rows?: Array<Record<string, unknown>>
  column_defs?: Array<{ name: string; type: string }>
  // output node
  write_mode?: WriteMode
  upsert_keys?: string[]
  // sql node
  sql_expression?: string
  // note node
  note_content?: string
  // watermark (incremental execution)
  watermark_column?: string
  watermark_value?: string
  // generic
  [key: string]: unknown
}

export interface FlowNode {
  id: string
  type: FlowNodeType
  position: NodePosition
  data: FlowNodeData
  width?: number
  height?: number
}

// FlowEdge is a ReactFlow Edge — gives us animated, style, markerEnd etc. for free
export type FlowEdge = Edge

export interface FlowGraph {
  nodes: FlowNode[]
  edges: FlowEdge[]
}

// ─── Flow Task CRUD ──────────────────────────────────────────────────────────

export interface FlowTask {
  id: number
  name: string
  description: string | null
  status: FlowTaskStatus
  trigger_type: FlowTaskTriggerType
  last_run_at: string | null
  last_run_status: FlowTaskRunStatus | null
  last_run_record_count: number | null
  created_at: string
  updated_at: string
}

export interface FlowTaskCreate {
  name: string
  description?: string
  trigger_type?: FlowTaskTriggerType
}

export interface FlowTaskUpdate {
  name?: string
  description?: string
  trigger_type?: FlowTaskTriggerType
}

export interface FlowTaskListResponse {
  items: FlowTask[]
  total: number
  page: number
  page_size: number
}

// ─── Graph ───────────────────────────────────────────────────────────────────

export interface FlowTaskGraphResponse {
  id: number
  flow_task_id: number
  nodes_json: FlowNode[]
  edges_json: FlowEdge[]
  version: number
  created_at: string
  updated_at: string
}

// ─── Graph Versioning (D4) ───────────────────────────────────────────────────

export interface FlowTaskGraphVersion {
  id: number
  flow_task_id: number
  version: number
  change_summary: string | null
  nodes_json: FlowNode[]
  edges_json: FlowEdge[]
  created_at: string
}

export interface FlowTaskGraphVersionListResponse {
  items: FlowTaskGraphVersion[]
  total: number
  page: number
  page_size: number
}

// ─── Watermarks (D8) ────────────────────────────────────────────────────────

export interface FlowTaskWatermark {
  id: number
  flow_task_id: number
  node_id: string
  watermark_column: string
  watermark_value: string | null
  updated_at: string
}

export interface FlowTaskWatermarkConfig {
  node_id: string
  watermark_column: string
}

// ─── Run History ─────────────────────────────────────────────────────────────

export interface FlowTaskRunNodeLog {
  id: number
  node_id: string
  node_type: string
  node_label: string | null
  row_count_in: number
  row_count_out: number
  duration_ms: number | null
  status: FlowTaskNodeStatus
  error_message: string | null
  created_at: string
}

export interface FlowTaskRunHistory {
  id: number
  flow_task_id: number
  trigger_type: FlowTaskTriggerType
  status: FlowTaskRunStatus
  celery_task_id: string | null
  started_at: string
  finished_at: string | null
  error_message: string | null
  total_input_records: number
  total_output_records: number
  run_metadata: Record<string, unknown> | null
  node_logs: FlowTaskRunNodeLog[]
  created_at: string
}

export interface FlowTaskRunHistoryListResponse {
  items: FlowTaskRunHistory[]
  total: number
  page: number
  page_size: number
}

// ─── Preview ─────────────────────────────────────────────────────────────────

export interface NodePreviewRequest {
  node_id: string
  nodes: FlowNode[]
  edges: FlowEdge[]
  limit?: number
  include_profiling?: boolean
}

export interface NodePreviewTaskResponse {
  task_id: string
  status: string
  message?: string
}

export interface NodePreviewResult {
  columns: string[]
  column_types: Record<string, string>
  rows: unknown[][]
  row_count: number
  elapsed_ms: number
  profile?: ColumnProfile[]
}

export interface ColumnProfile {
  column: string
  type: string
  total_count: number
  null_count: number
  null_percent: number
  distinct_count: number | null
  distinct_percent?: number | null
  min?: unknown
  max?: unknown
  mean?: number
  std_dev?: number
  median?: number
  top_values?: Array<{ value: unknown; count: number; percent: number }>
}

// ─── Task Status ─────────────────────────────────────────────────────────────

export interface TaskStatusResponse {
  task_id: string
  state: string
  status: string
  result: unknown | null
  meta: Record<string, unknown> | null
  error: string | null
}

// ─── Column Schema ────────────────────────────────────────────────────────────

export interface ColumnInfo {
  column_name: string
  data_type: string
}

export interface NodeColumnsResponse {
  columns: ColumnInfo[]
}

// ─── Trigger ─────────────────────────────────────────────────────────────────

export interface FlowTaskTriggerResponse {
  message: string
  run_id: number
  celery_task_id: string
  status: string
}

// ─── Repository ──────────────────────────────────────────────────────────────

export const flowTasksRepo = {
  // CRUD
  async list(page = 1, pageSize = 20) {
    const { data } = await api.get<FlowTaskListResponse>('/flow-tasks', {
      params: { page, page_size: pageSize },
    })
    return data
  },

  async get(id: number) {
    const { data } = await api.get<FlowTask>(`/flow-tasks/${id}`)
    return data
  },

  async create(payload: FlowTaskCreate) {
    const { data } = await api.post<FlowTask>('/flow-tasks', payload)
    return data
  },

  async update(id: number, payload: FlowTaskUpdate) {
    const { data } = await api.put<FlowTask>(`/flow-tasks/${id}`, payload)
    return data
  },

  async remove(id: number) {
    const { data } = await api.delete<{ message: string }>(`/flow-tasks/${id}`)
    return data
  },

  async duplicate(id: number) {
    const { data } = await api.post<FlowTask>(`/flow-tasks/${id}/duplicate`)
    return data
  },

  // Graph
  async getGraph(id: number) {
    const { data } = await api.get<FlowTaskGraphResponse>(
      `/flow-tasks/${id}/graph`
    )
    return data
  },

  async saveGraph(id: number, graph: FlowGraph) {
    const { data } = await api.post<FlowTaskGraphResponse>(
      `/flow-tasks/${id}/graph`,
      graph
    )
    return data
  },

  // Run
  async run(id: number) {
    const { data } = await api.post<FlowTaskTriggerResponse>(
      `/flow-tasks/${id}/run`
    )
    return data
  },

  async cancelRun(id: number) {
    const { data } = await api.post<{ status: string; message: string }>(
      `/flow-tasks/${id}/cancel`
    )
    return data
  },

  // Preview
  async previewNode(id: number, payload: NodePreviewRequest) {
    const { data } = await api.post<NodePreviewTaskResponse>(
      `/flow-tasks/${id}/preview`,
      payload
    )
    return data
  },

  // Task status polling
  async getTaskStatus(celeryTaskId: string) {
    const { data } = await api.get<TaskStatusResponse>(
      `/flow-tasks/task-status/${celeryTaskId}`
    )
    return data
  },

  // Run history
  async getRuns(id: number, page = 1, pageSize = 20) {
    const { data } = await api.get<FlowTaskRunHistoryListResponse>(
      `/flow-tasks/${id}/runs`,
      {
        params: { page, page_size: pageSize },
      }
    )
    return data
  },

  async getRunDetail(runId: number) {
    const { data } = await api.get<FlowTaskRunHistory>(
      `/flow-tasks/runs/${runId}`
    )
    return data
  },

  // Node schema — resolved via DuckDB LIMIT 0 in the worker
  // Sends the live graph snapshot; returns column names + DuckDB type strings
  // that reflect the *actual* output of the node (including transforms, aggregates, etc.)
  async getNodeSchema(flowTaskId: number, payload: NodePreviewRequest) {
    const { data } = await api.post<NodeColumnsResponse>(
      `/flow-tasks/${flowTaskId}/node-schema`,
      payload
    )
    return data
  },

  // ─── Versioning (D4) ────────────────────────────────────────────────

  async listVersions(id: number, page = 1, pageSize = 20) {
    const { data } = await api.get<FlowTaskGraphVersionListResponse>(
      `/flow-tasks/${id}/versions`,
      { params: { page, page_size: pageSize } }
    )
    return data
  },

  async getVersion(id: number, version: number) {
    const { data } = await api.get<FlowTaskGraphVersion>(
      `/flow-tasks/${id}/versions/${version}`
    )
    return data
  },

  async rollbackToVersion(id: number, version: number) {
    const { data } = await api.post<FlowTaskGraphResponse>(
      `/flow-tasks/${id}/rollback/${version}`
    )
    return data
  },

  // ─── Watermarks (D8) ────────────────────────────────────────────────

  async getWatermarks(id: number) {
    const { data } = await api.get<FlowTaskWatermark[]>(
      `/flow-tasks/${id}/watermarks`
    )
    return data
  },

  async setWatermark(id: number, config: FlowTaskWatermarkConfig) {
    const { data } = await api.post<FlowTaskWatermark>(
      `/flow-tasks/${id}/watermarks`,
      config
    )
    return data
  },

  async resetWatermark(id: number, nodeId: string) {
    const { data } = await api.delete<{ message: string }>(
      `/flow-tasks/${id}/watermarks/${nodeId}`
    )
    return data
  },
}
