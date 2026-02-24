import { type AxiosResponse } from 'axios'
import { api } from './client'

export interface Tag {
  id: number
  tag: string
  created_at: string
  updated_at: string
}

export interface TagListResponse {
  tags: Tag[]
  total: number
}

export interface TagSuggestionResponse {
  suggestions: Tag[]
}

export interface TableSyncTagsResponse {
  table_sync_id: number
  tags: Tag[]
  total: number
}

export interface TableSyncTagAssociation {
  id: number
  pipelines_destination_table_sync_id: number
  tag_id: number
  tag_item: Tag
  created_at: string
  updated_at: string
}

export interface CreateTagRequest {
  tag: string
}

export interface AddTagToTableSyncRequest {
  tag: string
}

export interface TagWithUsageCount extends Tag {
  usage_count: number
}

export interface AlphabetGroupedTags {
  letter: string
  tags: TagWithUsageCount[]
  count: number
}

export interface SmartTagsResponse {
  groups: AlphabetGroupedTags[]
  total_tags: number
}

export interface SmartTagsFilterParams {
  pipeline_id?: number
  destination_id?: number
  source_id?: number
}

export interface DestinationUsage {
  destination_id: number
  destination_name: string
  tables: string[]
}

export interface PipelineUsage {
  pipeline_id: number
  pipeline_name: string
  destinations: DestinationUsage[]
}

export interface TagUsageResponse {
  tag: string
  usage: PipelineUsage[]
}

export interface TagRelationNode {
  id: number
  tag: string
  usage_count: number
}

export interface TagRelationEdge {
  source: number
  target: number
  shared_tables: number
}

export interface TagRelationsResponse {
  nodes: TagRelationNode[]
  edges: TagRelationEdge[]
}

export const tagsRepo = {
  // Tag CRUD operations
  getAll: async (skip: number = 0, limit: number = 100): Promise<TagListResponse> => {
    const response: AxiosResponse<TagListResponse> = await api.get('/tags', {
      params: { skip, limit },
    })
    return response.data
  },

  create: async (data: CreateTagRequest): Promise<Tag> => {
    const response: AxiosResponse<Tag> = await api.post('/tags', data)
    return response.data
  },

  get: async (id: number): Promise<Tag> => {
    const response: AxiosResponse<Tag> = await api.get(`/tags/${id}`)
    return response.data
  },

  delete: async (id: number): Promise<void> => {
    await api.delete(`/tags/${id}`)
  },

  search: async (query: string, limit: number = 10): Promise<TagSuggestionResponse> => {
    const response: AxiosResponse<TagSuggestionResponse> = await api.get('/tags/search', {
      params: { q: query, limit },
    })
    return response.data
  },

  getSmartTags: async (params?: SmartTagsFilterParams): Promise<SmartTagsResponse> => {
    const response: AxiosResponse<SmartTagsResponse> = await api.get('/tags/smart-tags', {
      params,
    })
    return response.data
  },

  getUsage: async (id: number): Promise<TagUsageResponse> => {
    const response: AxiosResponse<TagUsageResponse> = await api.get(`/tags/${id}/usage`)
    return response.data
  },

  getRelations: async (): Promise<TagRelationsResponse> => {
    const response: AxiosResponse<TagRelationsResponse> = await api.get('/tags/relations')
    return response.data
  },

  // Table Sync Tag Association operations
  getTableSyncTags: async (tableSyncId: number): Promise<TableSyncTagsResponse> => {
    const response: AxiosResponse<TableSyncTagsResponse> = await api.get(
      `/tags/table-sync/${tableSyncId}`
    )
    return response.data
  },

  addTagToTableSync: async (
    tableSyncId: number,
    data: AddTagToTableSyncRequest
  ): Promise<TableSyncTagAssociation> => {
    const response: AxiosResponse<TableSyncTagAssociation> = await api.post(
      `/tags/table-sync/${tableSyncId}`,
      data
    )
    return response.data
  },

  removeTagFromTableSync: async (tableSyncId: number, tagId: number): Promise<void> => {
    await api.delete(`/tags/table-sync/${tableSyncId}/tags/${tagId}`)
  },

  getTableSyncsByTag: async (tagId: number): Promise<number[]> => {
    const response: AxiosResponse<number[]> = await api.get(`/tags/tag/${tagId}/table-syncs`)
    return response.data
  },
}
