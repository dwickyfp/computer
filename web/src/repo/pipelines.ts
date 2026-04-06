import { api } from './client'

export interface Pipeline {
  id: number
  name: string
  source_id: number | null
  source_type?: string
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
    type?: string
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
  catalog_database_name: string | null
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

export interface TableSyncLineageMetadata {
  version: number
  source_tables: { table: string; type: string }[]
  source_columns: string[]
  output_columns: string[]
  column_lineage: Record<string, { sources: string[]; transform: string }>
  referenced_tables: string[]
  parsed_at: string
  error?: string
}

export interface TableSyncDetails {
  id: number
  pipeline: { id: number; name: string; status: string }
  source: { id: number; name: string; database: string }
  destination: { id: number; name: string; type: string }
  table_name: string
  table_name_target: string
  custom_sql: string | null
  filter_sql: string | null
  primary_key_column_target: string | null
  tags: string[]
  record_count: number
  is_error: boolean
  error_message: string | null
  lineage_metadata: TableSyncLineageMetadata | null
  lineage_status: string
  lineage_error: string | null
  lineage_generated_at: string | null
  created_at: string
  updated_at: string
}

export interface PipelinePreviewRequest {
  destination_id: number
  filter_sql?: string | null
  source_id: number
  sql?: string
  table_name: string
}

export interface PipelinePreviewData {
  column_types: string[]
  columns: string[]
  data: Record<string, unknown>[]
  error?: string
}

export interface PipelinePreviewTaskResponse {
  task_id: string
}

export interface PipelinePreviewStatusResponse {
  error?: string | null
  result?: PipelinePreviewData
  state: string
}

export interface TaskDispatchResponse {
  message: string
  task_id: string | null
}

export type PipelinePreviewResponse =
  | PipelinePreviewData
  | PipelinePreviewTaskResponse

function normalizePipeline(data: Pipeline): Pipeline {
  const sourceType = data.source?.type || data.source_type || 'POSTGRES'
  return {
    ...data,
    source_type: sourceType,
  }
}

export const pipelinesRepo = {
  addDestination: async (
    id: number,
    destinationId: number
  ): Promise<Pipeline> => {
    const response = await api.post<Pipeline>(
      `/pipelines/${id}/destinations`,
      undefined,
      {
        params: { destination_id: destinationId },
      }
    )
    return normalizePipeline(response.data)
  },

  create: async (data: CreatePipelineRequest): Promise<Pipeline> => {
    const response = await api.post<Pipeline>('/pipelines', data)
    return normalizePipeline(response.data)
  },

  delete: async (id: number): Promise<void> => {
    await api.delete(`/pipelines/${id}`)
  },

  generateLineage: async (
    pipelineId: number,
    destId: number,
    syncId: number
  ): Promise<void> => {
    await api.post(
      `/pipelines/${pipelineId}/destinations/${destId}/tables/${syncId}/lineage/generate`
    )
  },

  get: async (id: number): Promise<Pipeline> => {
    const response = await api.get<Pipeline>(`/pipelines/${id}`)
    return normalizePipeline(response.data)
  },

  getAll: async (): Promise<PipelineListResponse> => {
    const response = await api.get<Pipeline[]>('/pipelines')
    const pipelines = response.data.map(normalizePipeline)
    return {
      pipelines,
      total: pipelines.length,
    }
  },

  getPreviewStatus: async (
    pipelineId: number,
    taskId: string
  ): Promise<PipelinePreviewStatusResponse> => {
    const response = await api.get<PipelinePreviewStatusResponse>(
      `/pipelines/${pipelineId}/preview/${taskId}`
    )
    return response.data
  },

  getStats: async (id: number, days = 7): Promise<PipelineStats[]> => {
    const response = await api.get<PipelineStats[]>(`/pipelines/${id}/stats`, {
      params: { days },
    })
    return response.data
  },

  getTableSyncDetail: async (
    pipelineId: number,
    destId: number,
    syncId: number
  ): Promise<TableSyncDetails> => {
    const response = await api.get<TableSyncDetails>(
      `/pipelines/${pipelineId}/destinations/${destId}/tables/${syncId}`
    )
    return response.data
  },

  pause: async (id: number): Promise<Pipeline> => {
    const response = await api.post<Pipeline>(`/pipelines/${id}/pause`)
    return normalizePipeline(response.data)
  },

  refresh: async (id: number): Promise<TaskDispatchResponse> => {
    const response = await api.post<TaskDispatchResponse>(
      `/pipelines/${id}/refresh`
    )
    return response.data
  },

  removeDestination: async (
    id: number,
    destinationId: number
  ): Promise<Pipeline> => {
    const response = await api.delete<Pipeline>(
      `/pipelines/${id}/destinations/${destinationId}`
    )
    return normalizePipeline(response.data)
  },

  rename: async (id: number, name: string): Promise<Pipeline> => {
    const response = await api.put<Pipeline>(`/pipelines/${id}`, { name })
    return normalizePipeline(response.data)
  },

  start: async (id: number): Promise<Pipeline> => {
    const response = await api.post<Pipeline>(`/pipelines/${id}/start`)
    return normalizePipeline(response.data)
  },

  startPreview: async (
    pipelineId: number,
    payload: PipelinePreviewRequest
  ): Promise<PipelinePreviewResponse> => {
    const response = await api.post<PipelinePreviewResponse>(
      `/pipelines/${pipelineId}/preview`,
      payload
    )
    return response.data
  },
}

export const tableSyncRepo = {
  deleteTableSync: async (
    pipelineId: number,
    pipelineDestinationId: number,
    syncId: number
  ): Promise<void> => {
    await api.delete(
      `/pipelines/${pipelineId}/destinations/${pipelineDestinationId}/tables/${syncId}`
    )
  },

  getDestinationTables: async (
    pipelineId: number,
    pipelineDestinationId: number
  ): Promise<TableWithSyncInfo[]> => {
    const response = await api.get<TableWithSyncInfo[]>(
      `/pipelines/${pipelineId}/destinations/${pipelineDestinationId}/tables`
    )
    return response.data
  },

  saveTableSync: async (
    pipelineId: number,
    pipelineDestinationId: number,
    config: TableSyncRequest
  ): Promise<TableSyncConfig> => {
    const response = await api.post<TableSyncConfig>(
      `/pipelines/${pipelineId}/destinations/${pipelineDestinationId}/tables`,
      config
    )
    return response.data
  },

  validateTargetTable: async (
    pipelineId: number,
    pipelineDestinationId: number,
    tableName: string
  ): Promise<TableValidationResponse> => {
    const response = await api.post<TableValidationResponse>(
      `/pipelines/${pipelineId}/destinations/${pipelineDestinationId}/tables/validate`,
      { table_name: tableName }
    )
    return response.data
  },
}
