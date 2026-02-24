import { api } from './client'

export interface SystemMetric {
    id: number
    cpu_usage: number | null
    total_memory: number | null
    used_memory: number | null
    total_swap: number | null
    used_swap: number | null
    recorded_at: string
    memory_usage_percent: number | null
    swap_usage_percent: number | null
}

export const systemMetricsRepo = {
    getLatest: async () => {
        const { data } = await api.get<SystemMetric>('/system-metrics/latest')
        return data
    },
    getHistory: async (limit = 100) => {
        const { data } = await api.get<SystemMetric[]>('/system-metrics/history', {
            params: { limit },
        })
        return data
    },
}
