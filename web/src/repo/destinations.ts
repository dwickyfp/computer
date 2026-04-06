import { api } from './client'

export interface Destination {
  id: number
  name: string
  type: string
  config: Record<string, unknown>
  created_at: string
  updated_at: string
  is_used_in_active_pipeline?: boolean
  total_tables?: number
  last_table_check_at?: string | null
}

export interface DestinationTableList {
  tables: string[]
  total_tables: number
  last_table_check_at: string | null
}

export interface DestinationSchemaLookupParams {
  scope?: 'tables'
  table?: string
}

export type DestinationSchemaResponse = Record<string, string[]>

export interface DestinationCreate {
  name: string
  type: string
  config?: Record<string, unknown>
}

export type DestinationUpdate = Partial<DestinationCreate>

export interface DestinationListResponse {
  destinations: Destination[]
  total: number
}

export const destinationsRepo = {
  getAll: async () => {
    const { data } = await api.get<Destination[]>('/destinations', {
      headers: {
        'Cache-Control': 'no-cache',
        Pragma: 'no-cache',
        Expires: '0',
      },
    })
    return {
      destinations: data,
      total: data.length,
    }
  },
  create: async (destination: DestinationCreate) => {
    const { data } = await api.post<Destination>('/destinations', destination)
    return data
  },
  update: async (id: number, destination: DestinationUpdate) => {
    const { data } = await api.put<Destination>(
      `/destinations/${id}`,
      destination
    )
    return data
  },
  delete: async (id: number) => {
    await api.delete(`/destinations/${id}`)
  },
  get: async (id: number) => {
    const { data } = await api.get<Destination>(`/destinations/${id}`)
    return data
  },
  testConnection: async (destination: DestinationCreate) => {
    const { data } = await api.post<{ message: string; error?: boolean }>(
      '/destinations/test-connection',
      destination
    )
    return data
  },
  duplicate: async (id: number) => {
    const { data } = await api.post<Destination>(
      `/destinations/${id}/duplicate`
    )
    return data
  },
  getTableList: async (id: number) => {
    const { data } = await api.get<DestinationTableList>(
      `/destinations/${id}/tables`
    )
    return data
  },
  getSchema: async (id: number, params: DestinationSchemaLookupParams) => {
    const { data } = await api.get<DestinationSchemaResponse>(
      `/destinations/${id}/schema`,
      { params }
    )
    return data
  },
  refreshTableList: async (id: number) => {
    const { data } = await api.post<{
      message: string
      task_id: string | null
    }>(`/destinations/${id}/tables/refresh`)
    return data
  },
}
