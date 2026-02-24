import { api } from './client'

export interface Source {
    id: number
    name: string
    pg_host: string
    pg_port: number
    pg_database: string
    pg_username: string
    publication_name: string
    replication_name: string
    is_publication_enabled: boolean
    is_replication_enabled: boolean
    last_check_replication_publication: string | null
    total_tables: number
    created_at: string
    updated_at: string
}

export interface SourceCreate {
    name: string
    pg_host: string
    pg_port: number
    pg_database: string
    pg_username: string
    pg_password?: string
    publication_name: string
    replication_name: string
}

export interface SourceUpdate extends Partial<SourceCreate> { }

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
    data_type?: string // Fallback for legacy data
    is_primary_key: boolean
    has_default: boolean
    default_value: string | null
}

export interface TableSchemaDiff {
    new_columns: string[]
    dropped_columns: SchemaColumn[]
    type_changes: Record<string, { old_type: string, new_type: string }>
}

export interface TableSchemaResponse {
    columns: SchemaColumn[]
    diff?: TableSchemaDiff
}

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
    tables: SourceTableInfo[]
    destinations: string[]
}

// Ensure SourceResponse is compatible or defined if not already perfect
// Ensure SourceResponse is compatible or defined if not already perfect
export interface SourceResponse extends Source { }

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

export interface PresetResponse extends Preset { }

export const sourcesRepo = {
    getAll: async () => {
        const { data } = await api.get<Source[]>('/sources')
        return {
            sources: data,
            total: data.length
        }
    },
    create: async (source: SourceCreate) => {
        const { data } = await api.post<Source>('/sources', source)
        return data
    },
    update: async (id: number, source: SourceUpdate) => {
        const { data } = await api.put<Source>(`/sources/${id}`, source)
        return data
    },
    delete: async (id: number) => {
        await api.delete(`/sources/${id}`)
    },
    testConnection: async (config: SourceCreate) => {
        // We use SourceCreate as it contains all necessary fields for connection test
        // Ensure required fields for test are present if using a partial type elsewhere
        const { data } = await api.post<boolean>('/sources/test_connection', config)
        return data
    },
    getDetails: async (id: number) => {
        const { data } = await api.get<SourceDetailResponse>(`/sources/${id}/details`)
        return data
    },
    getTableSchema: async (tableId: number, version: number) => {
        const { data } = await api.get<TableSchemaResponse>(`/sources/tables/${tableId}/schema`, {
            params: { version }
        })
        return data
    },
    registerTable: async (sourceId: number, tableName: string) => {
        await api.post(`/sources/${sourceId}/tables/register`, { table_name: tableName })
    },
    unregisterTable: async (sourceId: number, tableName: string) => {
        await api.delete(`/sources/${sourceId}/tables/${tableName}`)
    },
    refreshSource: async (sourceId: number) => {
        await api.post(`/sources/${sourceId}/refresh`)
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
    getAvailableTables: async (sourceId: number, refresh: boolean = false) => {
        const { data } = await api.get<string[]>(`/sources/${sourceId}/available_tables`, {
            params: { refresh }
        })
        return data
    },
    createPreset: async (sourceId: number, preset: PresetCreate) => {
        const { data } = await api.post<PresetResponse>(`/sources/${sourceId}/presets`, preset)
        return data
    },
    getPresets: async (sourceId: number) => {
        const { data } = await api.get<PresetResponse[]>(`/sources/${sourceId}/presets`)
        return data
    },
    deletePreset: async (presetId: number) => {
        await api.delete(`/sources/presets/${presetId}`)
    },
    updatePreset: async (presetId: number, preset: PresetCreate) => {
        const { data } = await api.put<PresetResponse>(`/sources/presets/${presetId}`, preset)
        return data
    },
    duplicate: async (sourceId: number) => {
        const { data } = await api.post<Source>(`/sources/${sourceId}/duplicate`)
        return data
    }
}
