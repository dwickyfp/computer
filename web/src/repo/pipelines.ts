import { type AxiosResponse } from 'axios'
import { api } from './client'

export interface Pipeline {
  id: number
  name: string
  source_id: number
  destination_id: number
  status: 'START' | 'PAUSE' | 'REFRESH'
  ready_refresh?: boolean
  last_refresh_at?: string
  pipeline_metadata?: {
    status: 'RUNNING' | 'PAUSED' | 'ERROR'
    last_error?: string
    last_start_at?: string
  }
  pipeline_progress?: {
    progress: number
    step?: string
    status: 'PENDING' | 'IN_PROGRESS' | 'COMPLETED' | 'FAILED'
    details?: string
  }
  source?: {
    id: number
    name: string
    is_publication_enabled?: boolean
    is_replication_enabled?: boolean
  }
  destinations?: {
    id: number
    destination: {
      id: number
      name: string
      type: string
    }
    is_error?: boolean
    error_message?: string | null
    last_error_at?: string | null
    table_syncs?: TableSyncConfig[]
  }[]
}

export interface CreatePipelineRequest {
  name: string
  source_id: number
  status?: string
}

export interface AddPipelineDestinationRequest {
  destination_id: number
}

export interface PipelineListResponse {
  pipelines: Pipeline[]
  total: number
}

export const pipelinesRepo = {
  getAll: async (): Promise<PipelineListResponse> => {
    const response: AxiosResponse<Pipeline[]> = await api.get('/pipelines')
    return {
      pipelines: response.data,
      total: response.data.length,
    }
  },
  create: async (data: CreatePipelineRequest): Promise<Pipeline> => {
    const response: AxiosResponse<Pipeline> = await api.post('/pipelines', data)
    return response.data
  },
  delete: async (id: number): Promise<void> => {
    await api.delete(`/pipelines/${id}`)
  },
  start: async (id: number): Promise<Pipeline> => {
    const response: AxiosResponse<Pipeline> = await api.post(
      `/pipelines/${id}/start`
    )
    return response.data
  },
  pause: async (id: number): Promise<Pipeline> => {
    const response: AxiosResponse<Pipeline> = await api.post(
      `/pipelines/${id}/pause`
    )
    return response.data
  },
  get: async (id: number): Promise<Pipeline> => {
    const response: AxiosResponse<Pipeline> = await api.get(`/pipelines/${id}`)
    return response.data
  },
  refresh: async (id: number): Promise<Pipeline> => {
    const response: AxiosResponse<Pipeline> = await api.post(
      `/pipelines/${id}/refresh`
    )
    return response.data
  },
  getStats: async (id: number, days: number = 7): Promise<PipelineStats[]> => {
    const response: AxiosResponse<PipelineStats[]> = await api.get(
      `/pipelines/${id}/stats`,
      { params: { days } }
    )
    return response.data
  },
  addDestination: async (
    id: number,
    destinationId: number
  ): Promise<Pipeline> => {
    const response: AxiosResponse<Pipeline> = await api.post(
      `/pipelines/${id}/destinations`,
      null,
      {
        params: { destination_id: destinationId },
      }
    )
    return response.data
  },
  removeDestination: async (
    id: number,
    destinationId: number
  ): Promise<Pipeline> => {
    const response: AxiosResponse<Pipeline> = await api.delete(
      `/pipelines/${id}/destinations/${destinationId}`
    )
    return response.data
  },
}

export interface PipelineStats {
  pipeline_destination_id: number | null
  pipeline_destination_table_sync_id?: number | null
  table_name: string
  target_table_name?: string
  destination_name?: string
  daily_stats: {
    date: string
    count: number
  }[]
  recent_stats: {
    timestamp: string
    count: number
  }[]
}

// Table Sync Types
export interface ColumnSchema {
  column_name: string
  data_type?: string
  real_data_type?: string
  is_nullable: boolean | string
  is_primary_key: boolean
  has_default?: boolean
  default_value?: string | null
  numeric_scale?: number | null
  numeric_precision?: number | null
}

export interface TableSyncConfig {
  id: number
  pipeline_destination_id: number
  table_name: string
  table_name_target: string
  custom_sql: string | null
  filter_sql: string | null
  primary_key_column_target: string | null

  is_exists_table_landing: boolean
  is_exists_stream: boolean
  is_exists_task: boolean
  is_exists_table_destination: boolean

  is_error: boolean
  error_message: string | null
  created_at: string
  updated_at: string
}

export interface TableWithSyncInfo {
  table_name: string
  columns: ColumnSchema[]
  sync_configs: TableSyncConfig[]
  is_exists_table_landing: boolean
  is_exists_stream: boolean
  is_exists_task: boolean
  is_exists_table_destination: boolean
}

export interface TableSyncRequest {
  id?: number | null
  table_name: string
  table_name_target?: string | null
  custom_sql?: string | null
  filter_sql?: string | null
  primary_key_column_target?: string | null
  enabled?: boolean
}

export interface TableValidationResponse {
  valid: boolean
  exists: boolean
  message: string | null
}

export const tableSyncRepo = {
  getDestinationTables: async (
    pipelineId: number,
    pipelineDestinationId: number
  ): Promise<TableWithSyncInfo[]> => {
    const response: AxiosResponse<TableWithSyncInfo[]> = await api.get(
      `/pipelines/${pipelineId}/destinations/${pipelineDestinationId}/tables`
    )
    return response.data
  },

  saveTableSync: async (
    pipelineId: number,
    pipelineDestinationId: number,
    config: TableSyncRequest
  ): Promise<TableSyncConfig> => {
    const response: AxiosResponse<TableSyncConfig> = await api.post(
      `/pipelines/${pipelineId}/destinations/${pipelineDestinationId}/tables`,
      config
    )
    return response.data
  },

  deleteTableSync: async (
    pipelineId: number,
    pipelineDestinationId: number,
    tableName: string
  ): Promise<void> => {
    await api.delete(
      `/pipelines/${pipelineId}/destinations/${pipelineDestinationId}/tables/${tableName}`
    )
  },

  deleteTableSyncById: async (
    pipelineId: number,
    pipelineDestinationId: number,
    syncConfigId: number
  ): Promise<void> => {
    await api.delete(
      `/pipelines/${pipelineId}/destinations/${pipelineDestinationId}/table-syncs/${syncConfigId}`
    )
  },

  initSnowflakeTable: async (
    pipelineId: number,
    pipelineDestinationId: number,
    tableName: string
  ): Promise<{ status: string; message: string }> => {
    const response: AxiosResponse<{ status: string; message: string }> =
      await api.post(
        `/pipelines/${pipelineId}/destinations/${pipelineDestinationId}/tables/${tableName}/init`
      )
    return response.data
  },

  validateTargetTable: async (
    pipelineId: number,
    pipelineDestinationId: number,
    tableName: string
  ): Promise<TableValidationResponse> => {
    const response: AxiosResponse<TableValidationResponse> = await api.post(
      `/pipelines/${pipelineId}/destinations/${pipelineDestinationId}/tables/validate`,
      { table_name: tableName }
    )
    return response.data
  },
}
