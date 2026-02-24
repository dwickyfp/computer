import { api } from './client'

// ─── Types ───────────────────────────────────────────────────────────────────

export type ScheduleTaskType = 'FLOW_TASK' | 'LINKED_TASK'
export type ScheduleStatus = 'ACTIVE' | 'PAUSED'
export type ScheduleRunStatus = 'RUNNING' | 'SUCCESS' | 'FAILED'

export interface ScheduleRunHistory {
  id: number
  schedule_id: number
  task_type: ScheduleTaskType
  task_id: number
  triggered_at: string
  completed_at: string | null
  duration_ms: number | null
  status: ScheduleRunStatus
  message: string | null
}

export interface Schedule {
  id: number
  name: string
  description: string | null
  task_type: ScheduleTaskType
  task_id: number
  cron_expression: string
  status: ScheduleStatus
  last_run_at: string | null
  next_run_at: string | null
  created_at: string
  updated_at: string
  run_history: ScheduleRunHistory[]
}

export interface ScheduleListItem extends Omit<Schedule, 'run_history'> {
  run_history: ScheduleRunHistory[]
}

export interface ScheduleCreate {
  name: string
  description?: string | null
  task_type: ScheduleTaskType
  task_id: number
  cron_expression: string
  status?: ScheduleStatus
}

export type ScheduleUpdate = Partial<ScheduleCreate>

export interface ScheduleHistoryPage {
  items: ScheduleRunHistory[]
  total: number
  skip: number
  limit: number
}

// ─── Repo ─────────────────────────────────────────────────────────────────────

export const schedulesRepo = {
  getAll: async (params?: {
    skip?: number
    limit?: number
  }): Promise<ScheduleListItem[]> => {
    const { data } = await api.get<ScheduleListItem[]>('/schedules', {
      params: { skip: params?.skip ?? 0, limit: params?.limit ?? 100 },
    })
    return data
  },

  getById: async (id: number): Promise<Schedule> => {
    const { data } = await api.get<Schedule>(`/schedules/${id}`)
    return data
  },

  create: async (payload: ScheduleCreate): Promise<Schedule> => {
    const { data } = await api.post<Schedule>('/schedules', payload)
    return data
  },

  update: async (id: number, payload: ScheduleUpdate): Promise<Schedule> => {
    const { data } = await api.put<Schedule>(`/schedules/${id}`, payload)
    return data
  },

  delete: async (id: number): Promise<void> => {
    await api.delete(`/schedules/${id}`)
  },

  pause: async (id: number): Promise<ScheduleListItem> => {
    const { data } = await api.post<ScheduleListItem>(`/schedules/${id}/pause`)
    return data
  },

  resume: async (id: number): Promise<ScheduleListItem> => {
    const { data } = await api.post<ScheduleListItem>(`/schedules/${id}/resume`)
    return data
  },

  getHistory: async (
    id: number,
    params?: { skip?: number; limit?: number }
  ): Promise<ScheduleHistoryPage> => {
    const { data } = await api.get<ScheduleHistoryPage>(
      `/schedules/${id}/history`,
      {
        params: { skip: params?.skip ?? 0, limit: params?.limit ?? 50 },
      }
    )
    return data
  },
}
