import { useQuery } from '@tanstack/react-query'
import {
  AlertOctagon,
  CheckCircle2,
  Database,
  PauseCircle,
} from 'lucide-react'
import { dashboardRepo } from '@/repo/dashboard'
import { cn } from '@/lib/utils'
import { useRefreshInterval } from '../context/refresh-interval-context'
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
    iconClassName: 'bg-emerald-500/12 text-emerald-300',
  },
  {
    key: 'IDLE',
    label: 'Idle',
    icon: PauseCircle,
    iconClassName: 'bg-amber-500/14 text-amber-300',
  },
  {
    key: 'ERROR',
    label: 'Error',
    icon: AlertOctagon,
    iconClassName: 'bg-rose-500/14 text-rose-300',
  },
] as const

export function SourceHealthCard({ className, href }: SourceHealthCardProps) {
  const { refreshInterval } = useRefreshInterval()
  const { data: health, isLoading } = useQuery({
    queryKey: ['dashboard', 'source-health'],
    queryFn: dashboardRepo.getSourceHealth,
    refetchInterval: refreshInterval,
  })

  const totalSources = health?.total ?? 0

  return (
    <DashboardPanel
      title='Source health'
      description='Connection readiness across sources.'
      headerSlot={
        <div className='dashboard-chip rounded-full p-2.5'>
          <Database className='h-4 w-4 text-muted-foreground' />
        </div>
      }
      className={cn('min-h-[220px]', className)}
      contentClassName='gap-4 px-5 pb-5 pt-4 sm:px-6 sm:pb-6 sm:pt-5'
      href={href}
      interactive={Boolean(href)}
      variant='dense'
    >
      <div className='grid flex-1 grid-cols-3 gap-3'>
        {sourceStates.map(({ key, label, icon: Icon, iconClassName }) => (
          <div
            key={key}
            className='dashboard-inset rounded-[22px] p-3.5 sm:p-4'
          >
            <div
              className={cn(
                'flex h-10 w-10 items-center justify-center rounded-full ring-1 ring-inset ring-white/5',
                iconClassName
              )}
            >
              <Icon className='h-4 w-4' />
            </div>
            <div className='mt-4 space-y-1.5'>
              <div className='font-mono text-2xl font-semibold tracking-tight sm:text-3xl'>
                {isLoading ? '...' : (health?.[key] ?? 0).toLocaleString()}
              </div>
              <p className='text-sm text-muted-foreground'>{label}</p>
            </div>
          </div>
        ))}
      </div>

      <div className='dashboard-inset dashboard-inset-strong flex items-center justify-between rounded-[22px] px-4 py-3'>
        <div>
          <p className='text-xs font-medium uppercase tracking-[0.24em] text-muted-foreground/70'>
            Coverage
          </p>
          <p className='mt-1 text-sm text-foreground'>
            {isLoading
              ? 'Syncing source health...'
              : totalSources > 0
                ? `${totalSources} sources tracked`
                : 'No sources are reporting yet'}
          </p>
        </div>
        <div className='rounded-full border border-white/10 bg-white/[0.03] px-3 py-1 text-xs font-medium text-muted-foreground'>
          {isLoading ? 'Loading' : `${health?.ACTIVE ?? 0} healthy`}
        </div>
      </div>
    </DashboardPanel>
  )
}
