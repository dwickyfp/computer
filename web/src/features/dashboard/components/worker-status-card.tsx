import { useQuery } from '@tanstack/react-query'
import { memo } from 'react'
import {
  CircleCheck,
  CircleDashed,
  CircleX,
  Cog,
  Loader2,
} from 'lucide-react'
import {
  dashboardRepo,
  type WorkerStatusResponse,
} from '@/repo/dashboard'
import { cn } from '@/lib/utils'
import { useRefreshInterval } from '../context/refresh-interval-context'
import { getDashboardPollingQueryOptions } from '../query-defaults'
import { DashboardPanel } from './dashboard-panel'

export const WorkerStatusCard = memo(function WorkerStatusCard() {
  const { refreshInterval } = useRefreshInterval()
  const { data, isLoading, isError } = useQuery<WorkerStatusResponse>({
    queryKey: ['worker-status'],
    queryFn: dashboardRepo.getWorkerStatus,
    ...getDashboardPollingQueryOptions(refreshInterval),
    notifyOnChangeProps: ['data', 'isError', 'isLoading'],
  })

  return (
    <DashboardPanel
      title='Worker status'
      description='Celery health and queue load.'
      headerSlot={
        <div className='dashboard-chip rounded-full p-2.5'>
          <Cog className='h-4 w-4 text-muted-foreground' />
        </div>
      }
      variant='dense'
    >
      {isLoading ? (
        <div className='dashboard-inset flex items-center gap-3 rounded-[22px] px-4 py-4 text-sm text-muted-foreground'>
          <Loader2 className='h-4 w-4 animate-spin' />
          Refreshing worker health...
        </div>
      ) : isError || !data ? (
        <div className='dashboard-inset flex min-h-[220px] flex-col items-center justify-center gap-2 rounded-[24px] px-6 py-8 text-center'>
          <CircleX className='h-8 w-8 text-rose-300' />
          <div className='space-y-1'>
            <p className='text-sm font-medium text-foreground'>
              Worker health unavailable
            </p>
            <p className='text-sm text-muted-foreground'>
              The worker status endpoint could not be reached.
            </p>
          </div>
        </div>
      ) : !data.enabled ? (
        <div className='space-y-3'>
          <div className='dashboard-inset flex items-center gap-3 rounded-[22px] px-4 py-4'>
            <div className='flex h-10 w-10 items-center justify-center rounded-full bg-white/[0.04] text-muted-foreground ring-1 ring-inset ring-white/5'>
              <CircleDashed className='h-4 w-4' />
            </div>
            <div>
              <p className='text-sm font-medium text-foreground'>
                Worker service disabled
              </p>
              <p className='text-xs text-muted-foreground'>
                Enable the background worker to unlock queue processing.
              </p>
            </div>
          </div>

          <div className='dashboard-inset dashboard-inset-strong rounded-[22px] px-4 py-3 text-sm text-muted-foreground'>
            Set <code className='rounded bg-white/[0.04] px-1.5 py-0.5 text-xs'>WORKER_ENABLED=true</code> to activate the service.
          </div>
        </div>
      ) : (
        <div className='space-y-3'>
          <div className='dashboard-row flex items-center justify-between rounded-[22px] px-4 py-4'>
            <div className='flex items-center gap-3'>
              <div
                className={cn(
                  'flex h-10 w-10 items-center justify-center rounded-full ring-1 ring-inset ring-white/5',
                  data.healthy
                    ? 'bg-emerald-500/12 text-emerald-300'
                    : 'bg-rose-500/14 text-rose-300'
                )}
              >
                {data.healthy ? (
                  <CircleCheck className='h-4 w-4' />
                ) : (
                  <CircleX className='h-4 w-4' />
                )}
              </div>
              <div>
                <p className='text-sm font-medium text-foreground'>
                  Celery worker
                </p>
                <p className='text-xs text-muted-foreground'>
                  {data.healthy ? 'Healthy and processing tasks' : 'Needs attention'}
                </p>
              </div>
            </div>

            <div
              className={cn(
                'rounded-full border px-3 py-1 text-[11px] font-medium',
                data.healthy
                  ? 'border-emerald-500/20 bg-emerald-500/10 text-emerald-300'
                  : 'border-rose-500/20 bg-rose-500/10 text-rose-300'
              )}
            >
              {data.healthy ? 'Healthy' : 'Degraded'}
            </div>
          </div>

          <div className='grid grid-cols-3 gap-3'>
            <div className='dashboard-inset rounded-[20px] px-4 py-3 text-center'>
              <p className='text-xs text-muted-foreground'>Workers</p>
              <p className='mt-2 font-mono text-2xl font-semibold'>
                {data.active_workers}
              </p>
            </div>
            <div className='dashboard-inset rounded-[20px] px-4 py-3 text-center'>
              <p className='text-xs text-muted-foreground'>Running</p>
              <p className='mt-2 font-mono text-2xl font-semibold'>
                {data.active_tasks}
              </p>
            </div>
            <div className='dashboard-inset rounded-[20px] px-4 py-3 text-center'>
              <p className='text-xs text-muted-foreground'>Queued</p>
              <p className='mt-2 font-mono text-2xl font-semibold'>
                {data.reserved_tasks}
              </p>
            </div>
          </div>

          {data.error && (
            <div className='dashboard-inset rounded-[20px] border-rose-500/20 bg-rose-500/10 px-4 py-3 text-sm text-rose-200'>
              {data.error}
            </div>
          )}
        </div>
      )}
    </DashboardPanel>
  )
})
