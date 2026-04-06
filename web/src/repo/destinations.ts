import { api } from './client'

export type DestinationType = 'SNOWFLAKE' | 'POSTGRES' | 'KAFKA'

export interface DestinationConfig {
  account?: string
  user?: string
  database?: string
  schema?: string
  landing_database?: string
  landing_schema?: string
  role?: string
  warehouse?: string
  host?: string
  port?: number
  password?: string
  bootstrap_servers?: string
  topic_prefix?: string
  security_protocol?: string
  sasl_mechanism?: string
  sasl_username?: string
  sasl_password?: string
  format?: string
}

export interface Destination {
  id: number
  name: string
  type: DestinationType
  config: DestinationConfig
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

function normalizeDestinationType(value: unknown): DestinationType {
  const normalized = String(value || 'SNOWFLAKE').toUpperCase()

  if (normalized === 'POSTGRES' || normalized === 'KAFKA') {
    return normalized
  }

  return 'SNOWFLAKE'
}

function normalizeKafkaFormat(value: unknown): string {
  const normalized = String(value || 'PLAIN_JSON').toUpperCase()
  return normalized === 'DEBEZIUM_JSON' ? normalized : 'PLAIN_JSON'
}

function normalizeDestination(data: Destination): Destination {
  const type = normalizeDestinationType(data.type)
  const config: DestinationConfig = { ...(data.config || {}) }
  const parsedPort =
    typeof config.port === 'number' ? config.port : Number(config.port)

  return {
    ...data,
    type,
    config: {
      ...config,
      port: Number.isFinite(parsedPort) ? parsedPort : 5432,
      format:
        type === 'KAFKA' ? normalizeKafkaFormat(config.format) : config.format,
    },
    total_tables: data.total_tables ?? 0,
    last_table_check_at: data.last_table_check_at ?? null,
  }
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
    const destinations = data.map(normalizeDestination)
    return {
      destinations,
      total: destinations.length,
    }
  },
  create: async (destination: DestinationCreate) => {
    const { data } = await api.post<Destination>('/destinations', destination)
    return normalizeDestination(data)
  },
  update: async (id: number, destination: DestinationUpdate) => {
    const { data } = await api.put<Destination>(
      `/destinations/${id}`,
      destination
    )
    return normalizeDestination(data)
  },
  delete: async (id: number) => {
    await api.delete(`/destinations/${id}`)
  },
  get: async (id: number) => {
    const { data } = await api.get<Destination>(`/destinations/${id}`)
    return normalizeDestination(data)
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
    return normalizeDestination(data)
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
