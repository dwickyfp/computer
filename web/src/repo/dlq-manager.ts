import { api } from './client'

export interface DLQQueueIdentifier {
  destination_id: number
  source_id: number
  table_name: string
}

export interface DLQQueueSummary extends DLQQueueIdentifier {
  destination_name: string | null
  message_count: number
  newest_failed_at: string | null
  oldest_failed_at: string | null
  pipeline_id: number | null
  pipeline_name: string | null
  source_name: string | null
  table_name_target: string | null
}

export interface DLQQueueListResponse {
  items: DLQQueueSummary[]
  total_destinations: number
  total_messages: number
  total_pipelines: number
  total_queues: number
}

export interface DLQMessage {
  event_timestamp: string | null
  first_failed_at: string | null
  key: Record<string, unknown> | null
  message_id: string
  operation: string | null
  retry_count: number
  schema: Record<string, unknown> | null
  table_name: string
  table_name_target: string | null
  table_sync_config: Record<string, unknown> | null
  value: Record<string, unknown> | null
}

export interface DLQMessagesResponse {
  items: DLQMessage[]
  next_before_id: string | null
  total_count: number
}

export interface DLQDiscardMessagesRequest extends DLQQueueIdentifier {
  message_ids: string[]
}

export interface DLQDiscardResponse {
  discarded_count: number
}

export interface DLQPipelineDiscardResponse extends DLQDiscardResponse {
  queues_cleared: number
}

export interface DLQQueueQueryParams {
  destination_id?: number
  include_empty?: boolean
  pipeline_id?: number
  search?: string
}

export interface DLQMessagesQueryParams extends DLQQueueIdentifier {
  before_id?: string | null
  limit?: number
}

export const dlqManagerRepo = {
  discardMessages: async (
    payload: DLQDiscardMessagesRequest
  ): Promise<DLQDiscardResponse> => {
    const response = await api.post<DLQDiscardResponse>(
      '/dlq/messages/discard',
      payload
    )
    return response.data
  },

  discardPipeline: async (
    pipelineId: number
  ): Promise<DLQPipelineDiscardResponse> => {
    const response = await api.post<DLQPipelineDiscardResponse>(
      `/dlq/pipelines/${pipelineId}/discard`
    )
    return response.data
  },

  discardQueue: async (
    payload: DLQQueueIdentifier
  ): Promise<DLQDiscardResponse> => {
    const response = await api.post<DLQDiscardResponse>(
      '/dlq/queues/discard',
      payload
    )
    return response.data
  },

  getMessages: async (
    params: DLQMessagesQueryParams
  ): Promise<DLQMessagesResponse> => {
    const response = await api.get<DLQMessagesResponse>('/dlq/messages', {
      params,
    })
    return response.data
  },

  getQueues: async (
    params: DLQQueueQueryParams = {}
  ): Promise<DLQQueueListResponse> => {
    const response = await api.get<DLQQueueListResponse>('/dlq/queues', {
      params,
    })
    return response.data
  },
}
