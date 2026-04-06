import { api } from './client'

// ─── Enums ──────────────────────────────────────────────────────────────────

export type LinkedTaskStatus = 'IDLE' | 'RUNNING' | 'SUCCESS' | 'FAILED'
export type LinkedTaskRunStatus = 'RUNNING' | 'SUCCESS' | 'FAILED' | 'CANCELLED'
export type LinkedTaskStepStatus =
  | 'PENDING'
  | 'RUNNING'
  | 'SUCCESS'
  | 'FAILED'
  | 'SKIPPED'
export type EdgeCondition = 'ON_SUCCESS' | 'ALWAYS'

// ─── Data types ──────────────────────────────────────────────────────────────

export interface FlowTaskRef {
  id: number
  name: string
  status: string
}

export interface LinkedTaskStep {
  id: number
  linked_task_id: number
  flow_task_id: number
  pos_x: number
  pos_y: number
  flow_task?: FlowTaskRef
  created_at: string
  updated_at: string
}

export interface LinkedTaskEdge {
  id: number
  linked_task_id: number
  source_step_id: number
  target_step_id: number
  condition: EdgeCondition
  created_at: string
  updated_at: string
}

export interface LinkedTask {
  id: number
  name: string
  description: string | null
  status: LinkedTaskStatus
  last_run_at: string | null
  last_run_status: LinkedTaskRunStatus | null
  created_at: string
  updated_at: string
}

export interface LinkedTaskDetail extends LinkedTask {
  steps: LinkedTaskStep[]
  edges: LinkedTaskEdge[]
}

export interface LinkedTaskListResponse {
  items: LinkedTask[]
  total: number
  page: number
  page_size: number
}

// ─── Graph save ──────────────────────────────────────────────────────────────

export interface LinkedTaskStepSave {
  id?: number | string
  flow_task_id: number
  pos_x: number
  pos_y: number
}

export interface LinkedTaskEdgeSave {
  source_step_id: number | string
  target_step_id: number | string
  condition: EdgeCondition
}

export interface LinkedTaskGraphSave {
  steps: LinkedTaskStepSave[]
  edges: LinkedTaskEdgeSave[]
}

// ─── Run history ─────────────────────────────────────────────────────────────

export interface LinkedTaskRunStepLog {
  id: number
  run_history_id: number
  step_id: number
  flow_task_id: number
  flow_task_run_history_id: number | null
  status: LinkedTaskStepStatus
  started_at: string | null
  finished_at: string | null
  error_message: string | null
  created_at: string
}

export interface LinkedTaskRunHistory {
  id: number
  linked_task_id: number
  trigger_type: string
  status: LinkedTaskRunStatus
  celery_task_id: string | null
  started_at: string
  finished_at: string | null
  error_message: string | null
  step_logs: LinkedTaskRunStepLog[]
  created_at: string
}

export interface LinkedTaskRunHistoryListResponse {
  items: LinkedTaskRunHistory[]
  total: number
  page: number
  page_size: number
}

// ─── Trigger ─────────────────────────────────────────────────────────────────

export interface LinkedTaskTriggerResponse {
  message: string
  run_id: number
  celery_task_id: string
  status: string
}

// ─── Repository ──────────────────────────────────────────────────────────────

export const linkedTasksRepo = {
  async list(page = 1, pageSize = 20) {
    const { data } = await api.get<LinkedTaskListResponse>('/linked-tasks', {
      params: { page, page_size: pageSize },
    })
    return data
  },

  async get(id: number) {
    const { data } = await api.get<LinkedTaskDetail>(`/linked-tasks/${id}`)
    return data
  },

  async create(payload: { name: string; description?: string }) {
    const { data } = await api.post<LinkedTask>('/linked-tasks', payload)
    return data
  },

  async update(id: number, payload: { name?: string; description?: string }) {
    const { data } = await api.put<LinkedTask>(`/linked-tasks/${id}`, payload)
    return data
  },

  async remove(id: number) {
    const { data } = await api.delete<void>(`/linked-tasks/${id}`)
    return data
  },

  async saveGraph(id: number, graph: LinkedTaskGraphSave) {
    const { data } = await api.post<LinkedTaskDetail>(
      `/linked-tasks/${id}/graph`,
      graph
    )
    return data
  },

  async trigger(id: number) {
    const { data } = await api.post<LinkedTaskTriggerResponse>(
      `/linked-tasks/${id}/run`
    )
    return data
  },

  async getRuns(id: number, page = 1, pageSize = 20) {
    const { data } = await api.get<LinkedTaskRunHistoryListResponse>(
      `/linked-tasks/${id}/runs`,
      {
        params: { page, page_size: pageSize },
      }
    )
    return data
  },

  async cancelRun(linkedTaskId: number, runId: number) {
    const { data } = await api.post<LinkedTaskRunHistory>(
      `/linked-tasks/${linkedTaskId}/runs/${runId}/cancel`
    )
    return data
  },
}
