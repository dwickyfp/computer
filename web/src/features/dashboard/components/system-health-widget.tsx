import { useQuery } from '@tanstack/react-query'
import { memo } from 'react'
import {
  Activity,
  Cpu,
  Database,
  Loader2,
  RadioTower,
  Server,
} from 'lucide-react'
import { cn } from '@/lib/utils'
import {
  dashboardRepo,
  type SystemHealthResponse,
} from '@/repo/dashboard'
import { useRefreshInterval } from '../context/refresh-interval-context'
import { getDashboardPollingQueryOptions } from '../query-defaults'
import { DashboardPanel } from './dashboard-panel'

const healthChecks = [
  { key: 'database', label: 'Postgres core', icon: Database },
  { key: 'redis', label: 'Redis cache', icon: Server },
  { key: 'wal_monitor', label: 'WAL monitor', icon: RadioTower },
  { key: 'compute', label: 'Compute node', icon: Cpu },
  { key: 'worker', label: 'Worker', icon: Activity },
] as const

function ReadinessRing({ percentage, score }: { percentage: number; score: number }) {
  const size = 170
  const strokeWidth = 12
  const radius = (size - strokeWidth) / 2
  const circumference = radius * 2 * Math.PI
  const offset = circumference - (percentage / 100) * circumference

  return (
    <div className='relative flex items-center justify-center'>
      <svg
        width={size}
        height={size}
        viewBox={`0 0 ${size} ${size}`}
        className='-rotate-90 transform'
      >
        <circle
          cx={size / 2}
          cy={size / 2}
          r={radius}
          stroke='var(--dashboard-ring-track)'
          strokeWidth={strokeWidth}
          fill='transparent'
        />
        <circle
          cx={size / 2}
          cy={size / 2}
          r={radius}
          stroke='url(#system-health-ring)'
          strokeWidth={strokeWidth}
          fill='transparent'
          strokeDasharray={circumference}
          strokeDashoffset={offset}
          strokeLinecap='round'
        />
        <defs>
          <linearGradient id='system-health-ring' x1='0%' x2='100%' y1='0%' y2='100%'>
            <stop offset='0%' stopColor='rgba(45, 148, 255, 0.95)' />
            <stop offset='100%' stopColor='rgba(16, 185, 129, 0.95)' />
          </linearGradient>
        </defs>
      </svg>

      <div className='absolute inset-0 flex flex-col items-center justify-center'>
        <span className='dashboard-text-muted text-xs uppercase tracking-[0.24em]'>
          Score
        </span>
        <span className='dashboard-text-strong mt-2 font-mono text-5xl font-semibold tracking-[-0.05em]'>
          {score.toFixed(1)}
        </span>
        <span className='dashboard-text mt-1 text-sm'>of 10</span>
      </div>
    </div>
  )
}

export const SystemHealthWidget = memo(function SystemHealthWidget() {
  const { refreshInterval } = useRefreshInterval()
  const { data, isLoading, isError } = useQuery<SystemHealthResponse>({
    queryKey: ['system-health'],
    queryFn: dashboardRepo.getSystemHealth,
    ...getDashboardPollingQueryOptions(refreshInterval),
    notifyOnChangeProps: ['data', 'isError', 'isLoading'],
  })

  if (isLoading) {
    return (
      <DashboardPanel
        title='System status'
        description='Platform readiness derived from the core health checks.'
        headerSlot={
          <div className='dashboard-chip dashboard-text rounded-full px-3 py-2 text-xs'>
            <Loader2 className='h-3.5 w-3.5 animate-spin text-sky-600 dark:text-sky-300' />
            Checking
          </div>
        }
        className='min-h-[280px]'
        variant='dense'
      >
        <div className='dashboard-inset dashboard-text flex min-h-[220px] items-center gap-3 rounded-[24px] px-4 py-4 text-sm'>
          <Loader2 className='h-4 w-4 animate-spin' />
          Checking platform services...
        </div>
      </DashboardPanel>
    )
  }

  if (isError || !data) {
    return (
      <DashboardPanel
        title='System status'
        description='Platform readiness derived from the core health checks.'
        headerSlot={
          <div className='dashboard-chip rounded-full px-3 py-2 text-xs text-rose-700 dark:text-rose-200'>
            <Activity className='h-3.5 w-3.5 text-rose-700 dark:text-rose-300' />
            Unavailable
          </div>
        }
        className='min-h-[280px]'
        variant='dense'
      >
        <div className='dashboard-inset flex min-h-[220px] flex-col items-center justify-center gap-2 rounded-[24px] px-6 py-8 text-center'>
          <Activity className='h-8 w-8 text-rose-700 dark:text-rose-300' />
          <div className='space-y-1'>
            <p className='dashboard-text-strong text-sm font-medium'>
              Status unavailable
            </p>
            <p className='dashboard-text text-sm'>
              The health endpoint did not return the latest service signals.
            </p>
          </div>
        </div>
      </DashboardPanel>
    )
  }

  const healthyCount = healthChecks.filter(({ key }) => data.checks[key]).length
  const totalChecks = healthChecks.length
  const readinessPercentage = (healthyCount / totalChecks) * 100
  const readinessScore = (healthyCount / totalChecks) * 10
  const statusLabel =
    readinessPercentage >= 100
      ? 'Nominal'
      : readinessPercentage >= 80
        ? 'Stable'
        : 'Needs attention'

  return (
    <DashboardPanel
      title='System status'
      description='Platform readiness derived from the core health checks.'
      headerSlot={
        <div
          className={cn(
            'dashboard-chip rounded-full px-3 py-2 text-xs',
            readinessPercentage >= 80
              ? 'text-emerald-700 dark:text-emerald-200'
              : 'text-amber-700 dark:text-amber-200'
          )}
        >
          <span
            className={cn(
              'h-2 w-2 rounded-full',
              readinessPercentage >= 80 ? 'bg-emerald-400' : 'bg-amber-400'
            )}
          />
          {statusLabel}
        </div>
      }
      className='min-h-[280px]'
      contentClassName='gap-4'
      variant='dense'
    >
      <div className='grid gap-4 sm:grid-cols-[0.88fr_1.12fr]'>
        <div className='dashboard-inset dashboard-inset-strong flex min-h-[240px] flex-col items-center justify-center rounded-[24px] px-4 py-5 text-center'>
          <ReadinessRing
            percentage={readinessPercentage}
            score={readinessScore}
          />
          <p className='dashboard-text mt-4 text-sm leading-6'>
            {healthyCount} of {totalChecks} platform services are reporting
            healthy status.
          </p>
        </div>

        <div className='space-y-3'>
          <div className='dashboard-inset rounded-[22px] px-4 py-4'>
            <div className='flex items-center justify-between gap-3'>
              <div>
                <p className='dashboard-text-muted text-[11px] uppercase tracking-[0.22em]'>
                  Snapshot
                </p>
                <p className='dashboard-text mt-2 text-sm'>
                  Version {data.version} · updated{' '}
                  {new Date(data.timestamp).toLocaleTimeString()}
                </p>
              </div>
              <div className='dashboard-status-neutral rounded-full border px-3 py-1 text-[11px] font-medium uppercase tracking-[0.16em]'>
                {statusLabel}
              </div>
            </div>
          </div>

          <div className='space-y-2.5'>
            {healthChecks.map(({ key, label, icon: Icon }) => {
              const healthy = data.checks[key]

              return (
                <div
                  key={key}
                  className='dashboard-row flex items-center justify-between rounded-[18px] px-3.5 py-3'
                >
                  <div className='flex items-center gap-3'>
                    <div
                      className={cn(
                        'flex h-9 w-9 items-center justify-center rounded-full ring-1 ring-inset ring-white/5',
                        healthy
                          ? 'bg-emerald-500/12 text-emerald-700 dark:text-emerald-300'
                          : 'bg-rose-500/14 text-rose-700 dark:text-rose-300'
                      )}
                    >
                      <Icon className='h-4 w-4' />
                    </div>
                    <div>
                      <p className='dashboard-text-strong text-sm font-medium'>
                        {label}
                      </p>
                      <p className='dashboard-text-muted text-xs'>
                        {healthy ? 'Healthy and responding' : 'Needs intervention'}
                      </p>
                    </div>
                  </div>

                  <div
                    className={cn(
                      'rounded-full border px-3 py-1 text-[11px] font-medium uppercase tracking-[0.16em]',
                      healthy
                        ? 'border-emerald-500/18 bg-emerald-500/10 text-emerald-700 dark:text-emerald-200'
                        : 'border-rose-500/18 bg-rose-500/10 text-rose-700 dark:text-rose-200'
                    )}
                  >
                    {healthy ? 'Healthy' : 'Alert'}
                  </div>
                </div>
              )
            })}
          </div>
        </div>
      </div>
    </DashboardPanel>
  )
})
