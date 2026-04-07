import { useQuery } from '@tanstack/react-query'
import { memo } from 'react'
import { formatDistanceToNow } from 'date-fns'
import { Activity, Loader2 } from 'lucide-react'
import { ScrollArea } from '@/components/ui/scroll-area'
import { jobMetricsRepo, type JobMetric } from '@/repo/job-metrics'
import { useRefreshInterval } from '../context/refresh-interval-context'
import { getDashboardPollingQueryOptions } from '../query-defaults'
import { DashboardPanel } from './dashboard-panel'

const jobThresholds: Record<string, number> = {
  wal_monitor: 60,
  replication_monitor: 60,
  schema_monitor: 60,
  table_list_refresh: 300,
  destination_table_list_refresh: 1800,
  system_metric_collection: 15,
  notification_sender: 30,
  worker_health_check: 10,
  pipeline_refresh_check: 10,
  credit_monitor: 3600,
}

function getStatus(key: string, lastRun: string) {
  const threshold = jobThresholds[key] || 60
  const diffSeconds =
    (new Date().getTime() - new Date(lastRun).getTime()) / 1000

  if (diffSeconds < threshold * 3) return 'healthy'
  return 'delayed'
}

function getJobDisplayName(key: string) {
  return key
    .split('_')
    .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
    .join(' ')
}

export const JobStatusCard = memo(function JobStatusCard() {
  const { refreshInterval } = useRefreshInterval()
  const { data: metrics, isLoading } = useQuery({
    queryKey: ['job-metrics'],
    queryFn: jobMetricsRepo.getAll,
    ...getDashboardPollingQueryOptions(refreshInterval),
    notifyOnChangeProps: ['data', 'isLoading'],
  })

  return (
    <DashboardPanel
      title='Job status'
      description='Scheduler recency for recurring platform jobs.'
      headerSlot={
        <div className='dashboard-chip rounded-full p-2.5'>
          <Activity className='h-4 w-4 text-muted-foreground' />
        </div>
      }
      noPadding
      variant='dense'
    >
      <ScrollArea className='h-full'>
        <div className='flex flex-col gap-3 px-5 pb-5 pt-4 sm:px-6 sm:pb-6 sm:pt-5'>
          {isLoading && (
            <div className='dashboard-inset flex items-center gap-3 rounded-[22px] px-4 py-4 text-sm text-muted-foreground'>
              <Loader2 className='h-4 w-4 animate-spin' />
              Loading scheduled job health...
            </div>
          )}

          {!isLoading && (!metrics || metrics.length === 0) && (
            <div className='dashboard-inset flex min-h-[220px] items-center justify-center rounded-[24px] px-6 py-8 text-center text-sm text-muted-foreground'>
              No job history has been recorded yet.
            </div>
          )}

          {metrics?.map((metric: JobMetric) => {
            const status = getStatus(metric.key_job_scheduler, metric.last_run_at)

            return (
              <div
                key={metric.key_job_scheduler}
                className='dashboard-row flex items-center justify-between rounded-[20px] px-4 py-3'
              >
                <div className='min-w-0'>
                  <p className='truncate text-sm font-medium text-foreground'>
                    {getJobDisplayName(metric.key_job_scheduler)}
                  </p>
                  <p className='text-xs text-muted-foreground'>
                    Last run{' '}
                    {formatDistanceToNow(new Date(metric.last_run_at), {
                      addSuffix: true,
                    })}
                  </p>
                </div>

                <div
                  className={
                    status === 'healthy'
                      ? 'rounded-full border border-emerald-500/20 bg-emerald-500/10 px-3 py-1 text-[11px] font-medium text-emerald-300'
                      : 'rounded-full border border-rose-500/20 bg-rose-500/10 px-3 py-1 text-[11px] font-medium text-rose-300'
                  }
                >
                  {status === 'healthy' ? 'Healthy' : 'Delayed'}
                </div>
              </div>
            )
          })}
        </div>
      </ScrollArea>
    </DashboardPanel>
  )
})
