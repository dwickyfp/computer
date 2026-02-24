import { api } from './client'

export interface DashboardSummary {
  pipelines: {
    [status: string]: number
    total: number
  }
  data_flow: {
    today: number
    yesterday: number
  }
  credits: {
    month_total: number
  }
  backfills: {
    PENDING: number
    EXECUTING: number
    COMPLETED: number
    FAILED: number
    CANCELLED: number
    total: number
  }
}

export interface FlowChartData {
  total_today: number
  total_yesterday: number
  history: {
    date: string
    [key: string]: string | number // Allow dynamic pipeline keys
  }[]
  pipelines: string[]
}

export interface CreditChartData {
  current_month_total: number
  history: {
    date: string
    [key: string]: string | number
  }[]
  destinations: string[]
}

export interface DashboardSourceHealth {
  ACTIVE: number
  IDLE: number
  ERROR: number
  total: number
  [key: string]: number
}

export interface ReplicationLagData {
  history: {
    date: string
    [key: string]: string | number
  }[]
  sources: string[]
}

export interface TopTableData {
  table_name: string
  record_count: number
}

export interface ActivityFeedItem {
  timestamp: string
  message: string
  source: string
  type: string // 'ERROR' | 'STATUS'
}

export const dashboardRepo = {
  getSummary: async (): Promise<DashboardSummary> => {
    const response = await api.get<DashboardSummary>('/dashboard/summary')
    return response.data
  },

  getFlowChart: async (days: number = 7): Promise<FlowChartData> => {
    const response = await api.get<FlowChartData>(`/dashboard/flow-chart?days=${days}`)
    return response.data
  },

  getCreditChart: async (days: number = 30): Promise<CreditChartData> => {
    const response = await api.get<CreditChartData>(`/dashboard/credit-chart?days=${days}`)
    return response.data
  },

  getSourceHealth: async (): Promise<DashboardSourceHealth> => {
    const response = await api.get<DashboardSourceHealth>('/dashboard/health/sources')
    return response.data
  },

  getReplicationLag: async (days: number = 1): Promise<ReplicationLagData> => {
    const response = await api.get<ReplicationLagData>(`/dashboard/charts/replication-lag?days=${days}`)
    return response.data
  },

  getTopTables: async (limit: number = 5): Promise<TopTableData[]> => {
    const response = await api.get<TopTableData[]>(`/dashboard/charts/top-tables?limit=${limit}`)
    return response.data
  },

  getActivityFeed: async (limit: number = 20): Promise<ActivityFeedItem[]> => {
    const response = await api.get<ActivityFeedItem[]>(`/dashboard/activity-feed?limit=${limit}`)
    return response.data
  },
}
