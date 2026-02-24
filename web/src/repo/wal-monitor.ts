import { api } from './client'

export interface WALMonitor {
    id: number
    source_id: number
    source?: {
        name: string
        [key: string]: any
    }
    wal_lsn: string | null
    wal_position: number | null
    last_wal_received: string | null
    last_transaction_time: string | null
    replication_slot_name: string | null
    replication_lag_bytes: number | null
    total_wal_size: string | null
    wal_size_bytes: number | null
    wal_threshold_status: 'OK' | 'WARNING' | 'ERROR' | null
    status: 'ACTIVE' | 'IDLE' | 'ERROR'
    error_message: string | null
    updated_at: string
}

export interface WALMonitorListResponse {
    monitors: WALMonitor[]
    total: number
}

export const walMonitorRepo = {
    getAll: async () => {
        const { data } = await api.get<WALMonitorListResponse>('/wal-monitor/')
        return data
    },
}
