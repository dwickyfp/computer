import { api } from './client'

export interface NotificationLog {
    id: number
    key_notification: string
    title: string
    message: string
    type: string
    is_read: boolean
    is_deleted: boolean
    iteration_check: number
    is_sent: boolean
    created_at: string
    updated_at: string
}

export const notificationRepo = {
    getAll: async (params?: { skip?: number; limit?: number; is_read?: boolean }) => {
        const { data } = await api.get<NotificationLog[]>('/notifications/', { params })
        return data
    },

    markAsRead: async (id: number) => {
        const { data } = await api.post<NotificationLog>(`/notifications/${id}/read`)
        return data
    },

    markAllAsRead: async () => {
        const { data } = await api.post<{ message: string; count: number }>('/notifications/read-all')
        return data
    },

    delete: async (id: number) => {
        const { data } = await api.delete<NotificationLog>(`/notifications/${id}`)
        return data
    },

    deleteAll: async () => {
        const { data } = await api.delete<{ message: string; count: number }>('/notifications/clear-all')
        return data
    },
}
