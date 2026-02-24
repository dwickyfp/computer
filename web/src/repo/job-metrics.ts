import { api } from './client'

export interface JobMetric {
  key_job_scheduler: string
  last_run_at: string
  created_at: string
  updated_at: string
}

export const jobMetricsRepo = {
  getAll: async () => {
    const { data } = await api.get<JobMetric[]>('/job-metrics')
    return data
  },
}
