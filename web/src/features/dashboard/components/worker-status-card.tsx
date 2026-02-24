import { useQuery } from '@tanstack/react-query'
import { DashboardPanel } from './dashboard-panel'
import { Cog, CircleCheck, CircleX, CircleDashed, Loader2 } from 'lucide-react'
import { api } from '@/repo/client'
import { useRefreshInterval } from '../context/refresh-interval-context'

interface WorkerStatus {
  enabled: boolean
  healthy: boolean
  active_workers: number
  active_tasks: number
  reserved_tasks: number
  error?: string
}

const getWorkerStatus = async (): Promise<WorkerStatus> => {
  const { data } = await api.get<WorkerStatus>('/health/worker')
  return data
}

export function WorkerStatusCard() {
  const { refreshInterval } = useRefreshInterval()
  const { data, isLoading, isError } = useQuery({
    queryKey: ['worker-status'],
    queryFn: getWorkerStatus,
    refetchInterval: refreshInterval,
  })

  const StatusDot = ({ healthy }: { healthy: boolean }) => (
    <div
      className={`h-2.5 w-2.5 rounded-sm ${healthy ? 'bg-emerald-500' : 'bg-rose-500'}`}
    />
  )

  return (
    <DashboardPanel
      title='Worker Status'
      headerAction={<Cog className='h-4 w-4 text-muted-foreground' />}
      className='h-full'
    >
      {isLoading ? (
        <div className='flex items-center gap-2 text-xs text-muted-foreground'>
          <Loader2 className='h-3 w-3 animate-spin' />
          Loading worker status...
        </div>
      ) : isError || !data ? (
        <div className='text-xs text-rose-500'>Failed to fetch worker status</div>
      ) : !data.enabled ? (
        <div className='space-y-3'>
          <div className='flex items-center gap-2'>
            <CircleDashed className='h-4 w-4 text-muted-foreground' />
            <span className='text-sm font-medium text-muted-foreground'>
              Disabled
            </span>
          </div>
          <p className='text-xs text-muted-foreground'>
            Worker service is not enabled. Set{' '}
            <code className='rounded bg-muted px-1 py-0.5 text-[10px]'>
              WORKER_ENABLED=true
            </code>{' '}
            to activate.
          </p>
        </div>
      ) : (
        <div className='space-y-2'>
          {/* Overall Status */}
          <div className='flex items-center justify-between rounded bg-muted/20 p-2 transition-colors hover:bg-muted/40'>
            <div className='flex items-center space-x-2'>
              {data.healthy ? (
                <CircleCheck className='h-3.5 w-3.5 text-emerald-500' />
              ) : (
                <CircleX className='h-3.5 w-3.5 text-rose-500' />
              )}
              <span className='text-xs font-medium'>Celery Worker</span>
            </div>
            <StatusDot healthy={data.healthy} />
          </div>

          {/* Stats */}
          <div className='grid grid-cols-3 gap-2'>
            <div className='rounded bg-muted/20 p-2 text-center'>
              <div className='text-lg font-bold font-mono leading-none'>
                {data.active_workers}
              </div>
              <div className='mt-1 text-[10px] text-muted-foreground'>
                Workers
              </div>
            </div>
            <div className='rounded bg-muted/20 p-2 text-center'>
              <div className='text-lg font-bold font-mono leading-none'>
                {data.active_tasks}
              </div>
              <div className='mt-1 text-[10px] text-muted-foreground'>
                Running
              </div>
            </div>
            <div className='rounded bg-muted/20 p-2 text-center'>
              <div className='text-lg font-bold font-mono leading-none'>
                {data.reserved_tasks}
              </div>
              <div className='mt-1 text-[10px] text-muted-foreground'>
                Queued
              </div>
            </div>
          </div>

          {/* Error message if any */}
          {data.error && (
            <div className='rounded border border-rose-500/20 bg-rose-500/5 px-2 py-1.5'>
              <p className='text-[10px] text-rose-400 line-clamp-2'>
                {data.error}
              </p>
            </div>
          )}
        </div>
      )}
    </DashboardPanel>
  )
}
