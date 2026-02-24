import { useQuery } from '@tanstack/react-query'
import { DashboardPanel } from './dashboard-panel'
// Badge import removed as it is not used in the updated code
import { jobMetricsRepo, type JobMetric } from '@/repo/job-metrics'
import { formatDistanceToNow } from 'date-fns'
import { Activity } from 'lucide-react'
import { useRefreshInterval } from '../context/refresh-interval-context'
import { ScrollArea } from '@/components/ui/scroll-area'

export function JobStatusCard() {
  const { refreshInterval } = useRefreshInterval()
  const { data: metrics, isLoading } = useQuery({
    queryKey: ['job-metrics'],
    queryFn: jobMetricsRepo.getAll,
    refetchInterval: refreshInterval,
  })

  // Define expected intervals for health check (in seconds)
  const jobThresholds: Record<string, number> = {
    'wal_monitor': 60,
    'replication_monitor': 60,
    'schema_monitor': 60,
    'system_metric_collection': 10,
    'table_list_refresh': 600,
    'credit_monitor': 21600, // 6 hours
  }

  const getStatus = (key: string, lastRun: string) => {
    const threshold = jobThresholds[key] || 60
    const diffSeconds = (new Date().getTime() - new Date(lastRun).getTime()) / 1000

    // Allow 2x threshold + buffer before warning
    if (diffSeconds < threshold * 3) return 'healthy'
    return 'delayed'
  }

  const getJobDisplayName = (key: string) => {
    // Convert snake_case to Title Case
    return key.split('_').map(word => word.charAt(0).toUpperCase() + word.slice(1)).join(' ')
  }

  return (
    <DashboardPanel
      title="Job Status"
      headerAction={<Activity className="h-4 w-4 text-muted-foreground" />}
      className='h-full'
      noPadding
    >
      <ScrollArea className='h-full px-4 py-4'>
        {isLoading ? (
          <div className="text-xs text-muted-foreground">Loading...</div>
        ) : !metrics || metrics.length === 0 ? (
          <div className="text-xs text-muted-foreground">No job history found.</div>
        ) : (
          <div className="space-y-1">
            {metrics.map((metric: JobMetric) => {
              const status = getStatus(metric.key_job_scheduler, metric.last_run_at)
              return (
                <div key={metric.key_job_scheduler} className="flex items-center justify-between py-1 border-b border-border/40 last:border-0">
                  <div className="flex flex-col">
                    <span className="text-xs font-medium">{getJobDisplayName(metric.key_job_scheduler)}</span>
                    <span className="text-[10px] text-muted-foreground">
                      {formatDistanceToNow(new Date(metric.last_run_at), { addSuffix: true })}
                    </span>
                  </div>
                  <div className={`h-2 w-2 rounded-full ${status === 'healthy' ? 'bg-emerald-500' : 'bg-rose-500'}`} />
                </div>
              )
            })}
          </div>
        )}
      </ScrollArea>
    </DashboardPanel>
  )
}
