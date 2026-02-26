import { api } from './client'

// ─── Chain Key ───────────────────────────────────────────────────────────────

export interface ChainKey {
  chain_key_masked: string
  is_active: boolean
  created_at: string | null
}

export interface ChainKeyGenerated {
  chain_key: string
  message: string
}

export interface ChainKeyGenerate {
  is_active?: boolean
}

// ─── Chain Client ────────────────────────────────────────────────────────────

export interface ChainTable {
  id: number
  chain_client_id: number | null
  table_name: string
  table_schema: Record<string, unknown>
  record_count: number
  last_synced_at: string | null
  created_at: string
  updated_at: string
}

export interface ChainDatabase {
  id: number
  chain_client_id: number | null
  name: string
  created_at: string
  updated_at: string
}

export interface ChainClient {
  id: number
  name: string
  url: string
  port: number
  is_active: boolean
  source_chain_id: string | null
  description: string | null
  last_connected_at: string | null
  tables: ChainTable[]
  databases: ChainDatabase[]
  created_at: string
  updated_at: string
}

export interface ChainClientCreate {
  name: string
  url: string
  port?: number
  chain_key: string
  source_chain_id?: string | null
  description?: string
  is_active?: boolean
}

export interface ChainClientUpdate extends Partial<ChainClientCreate> {}

export interface ChainClientTestResult {
  success: boolean
  message: string
  latency_ms: number | null
}

// ─── Repo Object ─────────────────────────────────────────────────────────────

export const chainRepo = {
  // Chain Key
  getKey: async () => {
    const { data } = await api.get<ChainKey>('/chain/key')
    return data
  },

  revealKey: async () => {
    const { data } = await api.get<{ chain_key: string | null }>(
      '/chain/key/reveal'
    )
    return data
  },

  generateKey: async (payload?: ChainKeyGenerate) => {
    const { data } = await api.post<ChainKeyGenerated>(
      '/chain/generate-key',
      payload ?? {}
    )
    return data
  },

  toggleActive: async (is_active: boolean) => {
    const { data } = await api.patch<ChainKey>('/chain/toggle-active', {
      is_active,
    })
    return data
  },

  // Chain Clients
  getClients: async () => {
    const { data } = await api.get<ChainClient[]>('/chain/clients')
    return data
  },

  getClient: async (id: number) => {
    const { data } = await api.get<ChainClient>(`/chain/clients/${id}`)
    return data
  },

  createClient: async (client: ChainClientCreate) => {
    const { data } = await api.post<ChainClient>('/chain/clients', client)
    return data
  },

  updateClient: async (id: number, client: ChainClientUpdate) => {
    const { data } = await api.put<ChainClient>(`/chain/clients/${id}`, client)
    return data
  },

  deleteClient: async (id: number) => {
    await api.delete(`/chain/clients/${id}`)
  },

  testClient: async (id: number) => {
    const { data } = await api.post<ChainClientTestResult>(
      `/chain/clients/${id}/test`
    )
    return data
  },

  getClientTables: async (id: number) => {
    const { data } = await api.get<ChainTable[]>(`/chain/clients/${id}/tables`)
    return data
  },

  syncClientTables: async (id: number) => {
    const { data } = await api.post<ChainTable[]>(
      `/chain/clients/${id}/sync-tables`
    )
    return data
  },

  syncDestinations: async () => {
    const { data } = await api.post<{ created: number; message: string }>(
      '/chain/clients/sync-destinations'
    )
    return data
  },

  registerCatalogTable: async (clientId: number, payload: any) => {
    const { data } = await api.post(
      `/chain/clients/${clientId}/catalog/register`,
      payload
    )
    return data
  },

  getClientDatabases: async (id: number) => {
    const { data } = await api.get<ChainDatabase[]>(
      `/chain/clients/${id}/catalog/databases`
    )
    return data
  },

  syncClientDatabases: async (id: number) => {
    const { data } = await api.post<ChainDatabase[]>(
      `/chain/clients/${id}/catalog/databases/sync`
    )
    return data
  },
}
