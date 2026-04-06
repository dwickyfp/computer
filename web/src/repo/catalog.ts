import { api } from './client'

export interface CatalogDatabase {
  id: number
  name: string
  description: string | null
  created_at: string
}

export interface CatalogDatabaseUpdate {
  name?: string
  description?: string
}

export interface CatalogTable {
  id: number
  database_id: number
  table_name: string
  schema_json: Record<string, any>
  stream_name: string
  source_chain_id: string | null
  status: string
  last_health_check_at: string | null
  created_at: string
}

export const catalogRepo = {
  getDatabases: async () => {
    const { data } = await api.get<CatalogDatabase[]>('/catalog/databases')
    return data
  },

  createDatabase: async (payload: { name: string; description?: string }) => {
    const { data } = await api.post<CatalogDatabase>(
      '/catalog/databases',
      payload
    )
    return data
  },

  deleteDatabase: async (id: number) => {
    await api.delete(`/catalog/databases/${id}`)
  },

  updateDatabase: async (id: number, data: CatalogDatabaseUpdate) => {
    const { data: result } = await api.put<CatalogDatabase>(
      `/catalog/databases/${id}`,
      data
    )
    return result
  },

  getTables: async (dbId: number) => {
    const { data } = await api.get<CatalogTable[]>(
      `/catalog/databases/${dbId}/tables`
    )
    return data
  },

  getTable: async (tableId: number) => {
    const { data } = await api.get<CatalogTable>(`/catalog/tables/${tableId}`)
    return data
  },

  deleteTable: async (tableId: number) => {
    await api.delete(`/catalog/tables/${tableId}`)
  },
}
