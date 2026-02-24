import { api } from './client'

export interface BackfillFilter {
  column: string
  operator: string
  value: string
}

export interface BackfillJobCreate {
  table_name: string
  filters?: BackfillFilter[]
}

export interface BackfillJob {
  id: number
  pipeline_id: number
  source_id: number
  table_name: string
  filter_sql: string | null
  status: 'PENDING' | 'EXECUTING' | 'COMPLETED' | 'FAILED' | 'CANCELLED'
  count_record: number
  total_record: number
  is_error: boolean
  error_message: string | null
  created_at: string
  updated_at: string
}

export interface BackfillJobListResponse {
  total: number
  items: BackfillJob[]
}

export const backfillApi = {
  // Create a new backfill job
  createJob: async (
    pipelineId: number,
    data: BackfillJobCreate
  ): Promise<BackfillJob> => {
    const response = await api.post<BackfillJob>(
      `/pipelines/${pipelineId}/backfill`,
      data
    )
    return response.data
  },

  // List backfill jobs for a pipeline
  listJobs: async (
    pipelineId: number,
    skip: number = 0,
    limit: number = 100
  ): Promise<BackfillJobListResponse> => {
    const response = await api.get<BackfillJobListResponse>(
      `/pipelines/${pipelineId}/backfill`,
      {
        params: { skip, limit },
      }
    )
    return response.data
  },

  // Get a specific backfill job
  getJob: async (jobId: number): Promise<BackfillJob> => {
    const response = await api.get<BackfillJob>(`/backfill/${jobId}`)
    return response.data
  },

  // Cancel a backfill job
  cancelJob: async (jobId: number): Promise<BackfillJob> => {
    const response = await api.post<BackfillJob>(`/backfill/${jobId}/cancel`)
    return response.data
  },

  // Delete a backfill job
  deleteJob: async (jobId: number): Promise<void> => {
    await api.delete(`/backfill/${jobId}`)
  },
}
