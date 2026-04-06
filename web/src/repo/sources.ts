import { api } from './client'

export type SourceType = 'POSTGRES' | 'KAFKA'

export interface SourceConfig {
  host?: string
  port?: number
  database?: string
  username?: string
  password?: string
  publication_name?: string
  replication_name?: string
  bootstrap_servers?: string
  topic_prefix?: string
  group_id?: string
  security_protocol?: string
  sasl_mechanism?: string
  sasl_username?: string
  sasl_password?: string
  ssl_ca_location?: string
  ssl_certificate_location?: string
  ssl_key_location?: string
  auto_offset_reset?: string
  format?: string
}

export interface Source {
  id: number
  name: string
  type: SourceType
  config: SourceConfig
  is_publication_enabled: boolean
  is_replication_enabled: boolean
  last_check_replication_publication: string | null
  total_tables: number
  created_at: string
  updated_at: string
  pg_host?: string
  pg_port?: number
  pg_database?: string
  pg_username?: string
  publication_name?: string
  replication_name?: string
  bootstrap_servers?: string
  topic_prefix?: string
  group_id?: string
  auto_offset_reset?: string
  format?: string
}

export interface SourceCreate {
  name: string
  type: SourceType
  config: SourceConfig
}

export type SourceUpdate = Partial<SourceCreate>

export interface SourceListResponse {
  sources: Source[]
  total: number
}

export interface SourceTableInfo {
  id: number
  table_name: string
  version: number
  schema_table?: SchemaColumn[]
}

export interface SchemaColumn {
  column_name: string
  is_nullable: string
  real_data_type: string
  data_type?: string
  is_primary_key: boolean
  has_default: boolean
  default_value: string | null
}

export interface TableSchemaDiff {
  new_columns: string[]
  dropped_columns: SchemaColumn[]
  type_changes: Record<string, { old_type: string; new_type: string }>
}

export interface TableSchemaResponse {
  columns: SchemaColumn[]
  diff?: TableSchemaDiff
}

export interface SourceSchemaLookupParams {
  scope?: 'tables'
  table?: string
}

export interface TaskDispatchResponse {
  message: string
  task_id: string | null
}

export type SourceSchemaLookupResponse = Record<string, string[]>

export interface WALMonitorResponse {
  wal_lsn: string | null
  wal_position: number | null
  last_wal_received: string | null
  last_transaction_time: string | null
  replication_slot_name: string | null
  replication_lag_bytes: number | null
  total_wal_size: string | null
  status: string
  error_message: string | null
  id: number
  created_at: string
  updated_at: string
}

export interface SourceDetailResponse {
  source: SourceResponse
  wal_monitor: WALMonitorResponse | null
  runtime?: Record<string, unknown>
  tables: SourceTableInfo[]
  destinations: string[]
}

export type SourceResponse = Source

export interface Preset {
  id: number
  source_id: number
  name: string
  table_names: string[]
  created_at: string
  updated_at: string
}

export interface PresetCreate {
  name: string
  table_names: string[]
}

export type PresetResponse = Preset

function normalizeSourceType(value: unknown): SourceType {
  return String(value || 'POSTGRES').toUpperCase() === 'KAFKA'
    ? 'KAFKA'
    : 'POSTGRES'
}

function normalizeKafkaFormat(value: unknown): string {
  void value
  return 'PLAIN_JSON'
}

function normalizeSource(data: Source): Source {
  const type = normalizeSourceType(data.type)
  const config: SourceConfig = { ...(data.config || {}) }

  return {
    ...data,
    type,
    config,
    pg_host: config.host ?? data.pg_host ?? '',
    pg_port: config.port ?? data.pg_port ?? 5432,
    pg_database: config.database ?? data.pg_database ?? '',
    pg_username: config.username ?? data.pg_username ?? '',
    publication_name: config.publication_name ?? data.publication_name ?? '',
    replication_name: config.replication_name ?? data.replication_name ?? '',
    bootstrap_servers: config.bootstrap_servers ?? data.bootstrap_servers ?? '',
    topic_prefix: config.topic_prefix ?? data.topic_prefix ?? '',
    group_id: config.group_id ?? data.group_id ?? '',
    auto_offset_reset:
      config.auto_offset_reset ?? data.auto_offset_reset ?? 'earliest',
    format: normalizeKafkaFormat(config.format ?? data.format),
  }
}

export const sourcesRepo = {
  getAll: async () => {
    const { data } = await api.get<Source[]>('/sources')
    const sources = data.map(normalizeSource)
    return {
      sources,
      total: sources.length,
    }
  },
  create: async (source: SourceCreate) => {
    const { data } = await api.post<Source>('/sources', source)
    return normalizeSource(data)
  },
  update: async (id: number, source: SourceUpdate) => {
    const { data } = await api.put<Source>(`/sources/${id}`, source)
    return normalizeSource(data)
  },
  delete: async (id: number) => {
    await api.delete(`/sources/${id}`)
  },
  testConnection: async (config: SourceCreate) => {
    const { data } = await api.post<boolean>('/sources/test_connection', config)
    return data
  },
  getDetails: async (id: number) => {
    const { data } = await api.get<SourceDetailResponse>(
      `/sources/${id}/details`
    )
    return {
      ...data,
      source: normalizeSource(data.source),
    }
  },
  getTableSchema: async (tableId: number, version: number) => {
    const { data } = await api.get<TableSchemaResponse>(
      `/sources/tables/${tableId}/schema`,
      {
        params: { version },
      }
    )
    return data
  },
  getSchema: async (sourceId: number, params: SourceSchemaLookupParams) => {
    const { data } = await api.get<SourceSchemaLookupResponse>(
      `/sources/${sourceId}/schema`,
      { params }
    )
    return data
  },
  registerTable: async (sourceId: number, tableName: string) => {
    await api.post(`/sources/${sourceId}/tables/register`, {
      table_name: tableName,
    })
  },
  unregisterTable: async (sourceId: number, tableName: string) => {
    await api.delete(`/sources/${sourceId}/tables/${tableName}`)
  },
  refreshSource: async (sourceId: number): Promise<TaskDispatchResponse> => {
    const { data } = await api.post<TaskDispatchResponse>(
      `/sources/${sourceId}/refresh`
    )
    return data
  },
  createPublication: async (sourceId: number, tables: string[]) => {
    await api.post(`/sources/${sourceId}/publication`, { tables })
  },
  dropPublication: async (sourceId: number) => {
    await api.delete(`/sources/${sourceId}/publication`)
  },
  createReplication: async (sourceId: number) => {
    await api.post(`/sources/${sourceId}/replication`)
  },
  dropReplication: async (sourceId: number) => {
    await api.delete(`/sources/${sourceId}/replication`)
  },
  getAvailableTables: async (sourceId: number, refresh = false) => {
    const { data } = await api.get<string[]>(
      `/sources/${sourceId}/available_tables`,
      {
        params: { refresh },
      }
    )
    return data
  },
  createPreset: async (sourceId: number, preset: PresetCreate) => {
    const { data } = await api.post<PresetResponse>(
      `/sources/${sourceId}/presets`,
      preset
    )
    return data
  },
  getPresets: async (sourceId: number) => {
    const { data } = await api.get<PresetResponse[]>(
      `/sources/${sourceId}/presets`
    )
    return data
  },
  deletePreset: async (presetId: number) => {
    await api.delete(`/sources/presets/${presetId}`)
  },
  updatePreset: async (presetId: number, preset: PresetCreate) => {
    const { data } = await api.put<PresetResponse>(
      `/sources/presets/${presetId}`,
      preset
    )
    return data
  },
  duplicate: async (sourceId: number) => {
    const { data } = await api.post<Source>(`/sources/${sourceId}/duplicate`)
    return normalizeSource(data)
  },
}
