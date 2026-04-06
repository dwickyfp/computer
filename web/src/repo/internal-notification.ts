import { api } from './client'

export interface InternalNotificationConfig {
  id: number
  name: string
  is_enabled: boolean
  base_url: string
  requester: string
  menu_code: string
  company_group_id: number
  mail_from_code: string
  mail_to: string // comma-separated emails
  subject: string
  created_at: string
  updated_at: string
}

export interface InternalNotificationConfigCreate {
  name: string
  is_enabled: boolean
  base_url: string
  requester: string
  menu_code: string
  company_group_id: number
  mail_from_code: string
  mail_to: string
  subject: string
}

export type InternalNotificationConfigUpdate =
  Partial<InternalNotificationConfigCreate>

export interface InternalNotificationGlobalStatus {
  is_active: boolean
}

export const internalNotificationRepo = {
  getAll: async () => {
    const { data } = await api.get<InternalNotificationConfig[]>(
      '/internal-notifications/'
    )
    return data
  },

  getGlobalStatus: async () => {
    const { data } = await api.get<InternalNotificationGlobalStatus>(
      '/internal-notifications/global-status'
    )
    return data
  },

  setGlobalToggle: async (is_active: boolean) => {
    const { data } = await api.patch<InternalNotificationGlobalStatus>(
      '/internal-notifications/global-toggle',
      { is_active }
    )
    return data
  },

  create: async (payload: InternalNotificationConfigCreate) => {
    const { data } = await api.post<InternalNotificationConfig>(
      '/internal-notifications/',
      payload
    )
    return data
  },

  update: async (id: number, payload: InternalNotificationConfigUpdate) => {
    const { data } = await api.put<InternalNotificationConfig>(
      `/internal-notifications/${id}`,
      payload
    )
    return data
  },

  delete: async (id: number) => {
    await api.delete(`/internal-notifications/${id}`)
  },

  toggleEnabled: async (id: number, is_active: boolean) => {
    const { data } = await api.patch<InternalNotificationConfig>(
      `/internal-notifications/${id}/toggle`,
      { is_active }
    )
    return data
  },

  test: async (id: number) => {
    const { data } = await api.post<{ message: string }>(
      `/internal-notifications/${id}/test`
    )
    return data
  },
}
