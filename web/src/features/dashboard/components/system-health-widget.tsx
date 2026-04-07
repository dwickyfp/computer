import { useQuery } from '@tanstack/react-query'
import {
  Activity,
  Cpu,
  Database,
  Loader2,
  RadioTower,
  Server,
} from 'lucide-react'
import {
  dashboardRepo,
  type SystemHealthResponse,
} from '@/repo/dashboard'
import { cn } from '@/lib/utils'
import { useRefreshInterval } from '../context/refresh-interval-context'
import { DashboardPanel } from './dashboard-panel'

const healthChecks = [
  { key: 'database', label: 'Postgres core', icon: Database },
  { key: 'redis', label: 'Redis cache', icon: Server },
  { key: 'wal_monitor', label: 'WAL monitor', icon: RadioTower },
  { key: 'compute', label: 'Compute node', icon: Cpu },
  { key: 'worker', label: 'Worker', icon: Activity },
] as const

export function SystemHealthWidget() {
  const { refreshInterval } = useRefreshInterval()
  const { data, isLoading, isError } = useQuery<SystemHealthResponse>({
    queryKey: ['system-health'],
    queryFn: dashboardRepo.getSystemHealth,
    refetchInterval: refreshInterval,
  })

  return (
    <DashboardPanel
      title='System status'
      description='Service readiness across the platform.'
      headerSlot={
        <div className='dashboard-chip rounded-full p-2.5'>
          <Activity className='h-4 w-4 text-muted-foreground' />
        </div>
      }
      variant='dense'
    >
      {isLoading ? (
        <div className='dashboard-inset flex items-center gap-3 rounded-[22px] px-4 py-4 text-sm text-muted-foreground'>
          <Loader2 className='h-4 w-4 animate-spin' />
          Checking platform services...
        </div>
      ) : isError || !data ? (
        <div className='dashboard-inset flex min-h-[220px] flex-col items-center justify-center gap-2 rounded-[24px] px-6 py-8 text-center'>
          <Activity className='h-8 w-8 text-rose-300' />
          <div className='space-y-1'>
            <p className='text-sm font-medium text-foreground'>
              Status unavailable
            </p>
            <p className='text-sm text-muted-foreground'>
              The health endpoint did not return the latest service signals.
            </p>
          </div>
        </div>
      ) : (
        <div className='space-y-3'>
          {healthChecks.map(({ key, label, icon: Icon }) => {
            const healthy = data.checks[key]

            return (
              <div
                key={key}
                className='dashboard-row flex items-center justify-between rounded-[20px] px-4 py-3'
              >
                <div className='flex items-center gap-3'>
                  <div
                    className={cn(
                      'flex h-9 w-9 items-center justify-center rounded-full ring-1 ring-inset ring-white/5',
                      healthy
                        ? 'bg-emerald-500/12 text-emerald-300'
                        : 'bg-rose-500/14 text-rose-300'
                    )}
                  >
                    <Icon className='h-4 w-4' />
                  </div>
                  <div>
                    <p className='text-sm font-medium text-foreground'>{label}</p>
                    <p className='text-xs text-muted-foreground'>
                      {healthy ? 'Healthy and responding' : 'Needs attention'}
                    </p>
                  </div>
                </div>

                <div
                  className={cn(
                    'rounded-full border px-3 py-1 text-[11px] font-medium',
                    healthy
                      ? 'border-emerald-500/20 bg-emerald-500/10 text-emerald-300'
                      : 'border-rose-500/20 bg-rose-500/10 text-rose-300'
                  )}
                >
                  {healthy ? 'Healthy' : 'Degraded'}
                </div>
              </div>
            )
          })}

          <div className='dashboard-inset dashboard-inset-strong flex items-center justify-between rounded-[20px] px-4 py-3 text-xs text-muted-foreground'>
            <span>v{data.version}</span>
            <span>{new Date(data.timestamp).toLocaleTimeString()}</span>
          </div>
        </div>
      )}
    </DashboardPanel>
  )
}
