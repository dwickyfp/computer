import { useQuery } from '@tanstack/react-query'
import { memo } from 'react'
import {
  AlertOctagon,
  CheckCircle2,
  Database,
  PauseCircle,
} from 'lucide-react'
import { cn } from '@/lib/utils'
import { dashboardRepo } from '@/repo/dashboard'
import { useRefreshInterval } from '../context/refresh-interval-context'
import { getDashboardPollingQueryOptions } from '../query-defaults'
import { DashboardPanel } from './dashboard-panel'

interface SourceHealthCardProps {
  className?: string
  href?: string
}

const sourceStates = [
  {
    key: 'ACTIVE',
    label: 'Active',
    icon: CheckCircle2,
    iconClassName: 'bg-emerald-500/12 text-emerald-700 dark:text-emerald-300',
    textClassName: 'text-emerald-700 dark:text-emerald-200',
  },
  {
    key: 'IDLE',
    label: 'Idle',
    icon: PauseCircle,
    iconClassName: 'bg-amber-500/14 text-amber-700 dark:text-amber-300',
    textClassName: 'text-amber-700 dark:text-amber-200',
  },
  {
    key: 'ERROR',
    label: 'Error',
    icon: AlertOctagon,
    iconClassName: 'bg-rose-500/14 text-rose-700 dark:text-rose-300',
    textClassName: 'text-rose-700 dark:text-rose-200',
  },
] as const

export const SourceHealthCard = memo(function SourceHealthCard({
  className,
  href,
}: SourceHealthCardProps) {
  const { refreshInterval } = useRefreshInterval()
  const { data: health, isLoading } = useQuery({
    queryKey: ['dashboard', 'source-health'],
    queryFn: dashboardRepo.getSourceHealth,
    ...getDashboardPollingQueryOptions(refreshInterval),
    notifyOnChangeProps: ['data', 'isLoading'],
  })

  const totalSources = health?.total ?? 0
  const activeSources = health?.ACTIVE ?? 0
  const idleSources = health?.IDLE ?? 0
  const errorSources = health?.ERROR ?? 0
  const coverage = totalSources > 0 ? (activeSources / totalSources) * 100 : 0

  return (
    <DashboardPanel
      title='Source health'
      description='Connection readiness across capture sources.'
      headerSlot={
        <div className='dashboard-chip dashboard-text rounded-full px-3 py-2 text-xs'>
          <Database className='h-3.5 w-3.5 text-sky-600 dark:text-sky-300' />
          {isLoading ? 'Syncing' : `${totalSources} tracked`}
        </div>
      }
      className={cn('min-h-[280px]', className)}
      contentClassName='gap-4'
      href={href}
      interactive={Boolean(href)}
      variant='dense'
    >
      <div className='grid gap-4 sm:grid-cols-[0.9fr_1.1fr]'>
        <div className='dashboard-inset dashboard-inset-strong rounded-[24px] px-4 py-4'>
          <p className='dashboard-text-muted text-[11px] font-medium uppercase tracking-[0.22em]'>
            Healthy coverage
          </p>
          <div className='mt-4 flex items-end gap-2'>
            <p className='dashboard-text-strong font-mono text-5xl font-semibold tracking-[-0.05em]'>
              {isLoading ? '--' : Math.round(coverage)}
            </p>
            <span className='dashboard-text pb-1 text-sm'>%</span>
          </div>
          <p className='dashboard-text mt-3 text-sm leading-6'>
            {isLoading
              ? 'Refreshing source readiness...'
              : totalSources > 0
                ? `${activeSources} of ${totalSources} sources are actively replicating.`
                : 'Source telemetry will appear once connections begin reporting.'}
          </p>
        </div>

        <div className='dashboard-inset rounded-[24px] px-4 py-4'>
          <div className='flex items-center justify-between gap-3'>
            <div>
              <p className='dashboard-text-muted text-[11px] font-medium uppercase tracking-[0.22em]'>
                Status mix
              </p>
              <p className='dashboard-text mt-2 text-sm'>
                Distribution across active, idle, and error states.
              </p>
            </div>
            <div className='dashboard-text-strong font-mono text-2xl font-semibold'>
              {isLoading ? '...' : totalSources.toLocaleString()}
            </div>
          </div>

          <div
            className='mt-4 flex h-3 overflow-hidden rounded-full'
            style={{ backgroundColor: 'var(--dashboard-progress-track)' }}
          >
            {[
              {
                key: 'ACTIVE',
                value: activeSources,
                className: 'bg-emerald-400',
              },
              {
                key: 'IDLE',
                value: idleSources,
                className: 'bg-amber-400',
              },
              {
                key: 'ERROR',
                value: errorSources,
                className: 'bg-rose-400',
              },
            ].map(({ key, value, className: barClassName }) => (
              <div
                key={key}
                className={cn('h-full', barClassName)}
                style={{
                  width:
                    totalSources > 0 ? `${(value / totalSources) * 100}%` : '0%',
                }}
              />
            ))}
          </div>

          <div className='mt-5 space-y-2.5'>
            {sourceStates.map(
              ({ key, label, icon: Icon, iconClassName, textClassName }) => (
                <div
                  key={key}
                  className='dashboard-row flex items-center justify-between rounded-[18px] px-3.5 py-3'
                >
                  <div className='flex items-center gap-3'>
                    <div
                      className={cn(
                        'flex h-9 w-9 items-center justify-center rounded-full ring-1 ring-inset ring-white/5',
                        iconClassName
                      )}
                    >
                      <Icon className='h-4 w-4' />
                    </div>
                    <p className='dashboard-text-strong text-sm font-medium'>
                      {label}
                    </p>
                  </div>

                  <div className={cn('font-mono text-lg font-semibold', textClassName)}>
                    {isLoading ? '...' : (health?.[key] ?? 0).toLocaleString()}
                  </div>
                </div>
              )
            )}
          </div>
        </div>
      </div>
    </DashboardPanel>
  )
})
