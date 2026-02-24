import { api } from './client'

export interface WALThresholdsConfig {
    warning: number  // in MB
    error: number    // in MB
    enable_webhook: boolean
    webhook_url: string
    notification_iteration: number
    enable_telegram: boolean
    telegram_bot_token: string
    telegram_chat_id: string
}

export interface BatchConfiguration {
    max_batch_size: number
    max_queue_size: number
}

export const configurationRepo = {
    getWALThresholds: async () => {
        const { data } = await api.get<WALThresholdsConfig>('/configuration/wal-thresholds')
        return data
    },

    updateWALThresholds: async (config: WALThresholdsConfig) => {
        const { data } = await api.put<WALThresholdsConfig>('/configuration/wal-thresholds', config)
        return data
    },

    testNotification: async (webhook_url?: string, telegram_bot_token?: string, telegram_chat_id?: string) => {
        const { data } = await api.post('/configuration/wal-thresholds/test', { 
            webhook_url,
            telegram_bot_token,
            telegram_chat_id
        })
        return data
    },

    getBatchConfiguration: async () => {
        const { data } = await api.get<BatchConfiguration>('/configuration/batch')
        return data
    },

    updateBatchConfiguration: async (config: BatchConfiguration) => {
        const { data } = await api.put<BatchConfiguration>('/configuration/batch', config)
        return data
    },
}
