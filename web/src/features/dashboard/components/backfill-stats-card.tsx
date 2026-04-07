import {
  AlertCircle,
  CheckCircle2,
  Clock,
  DatabaseBackup,
  Loader2,
  XCircle,
} from 'lucide-react'
import { Progress } from '@/components/ui/progress'
import { type DashboardSummary } from '@/repo/dashboard'
import { cn } from '@/lib/utils'
import { DashboardPanel } from './dashboard-panel'

interface BackfillStatsCardProps {
  className?: string
  data?: DashboardSummary['backfills']
}

const statusRows = [
  {
    key: 'EXECUTING',
    label: 'Running',
    icon: Loader2,
    colorClassName: 'text-sky-300 bg-sky-500/12',
  },
  {
    key: 'PENDING',
    label: 'Pending',
    icon: Clock,
    colorClassName: 'text-amber-300 bg-amber-500/14',
  },
  {
    key: 'COMPLETED',
    label: 'Completed',
    icon: CheckCircle2,
    colorClassName: 'text-emerald-300 bg-emerald-500/12',
  },
  {
    key: 'FAILED',
    label: 'Failed',
    icon: XCircle,
    colorClassName: 'text-rose-300 bg-rose-500/14',
  },
  {
    key: 'CANCELLED',
    label: 'Cancelled',
    icon: AlertCircle,
    colorClassName: 'text-muted-foreground bg-white/[0.04]',
  },
] as const

export function BackfillStatsCard({
  className,
  data,
}: BackfillStatsCardProps) {
  const active = (data?.PENDING ?? 0) + (data?.EXECUTING ?? 0)
  const completed = data?.COMPLETED ?? 0
  const failed = data?.FAILED ?? 0
  const total = data?.total ?? 0
  const finishedCount = completed + failed
  const successRate = finishedCount > 0 ? (completed / finishedCount) * 100 : 0

  return (
    <DashboardPanel
      title='Backfill operations'
      description='Global backfill job status across the platform.'
      headerSlot={
        <div className='dashboard-chip rounded-full p-2.5'>
          <DatabaseBackup className='h-4 w-4 text-muted-foreground' />
        </div>
      }
      className={className}
      contentClassName='gap-4'
      variant='dense'
    >
      <div className='grid gap-3 sm:grid-cols-2'>
        <div className='dashboard-inset rounded-[22px] px-4 py-3.5'>
          <p className='text-xs text-muted-foreground'>Active jobs</p>
          <p className='mt-2 font-mono text-4xl font-semibold tracking-tight'>
            {active.toLocaleString()}
          </p>
        </div>
        <div className='dashboard-inset rounded-[22px] px-4 py-3.5'>
          <p className='text-xs text-muted-foreground'>Total backfills</p>
          <p className='mt-2 font-mono text-4xl font-semibold tracking-tight'>
            {total.toLocaleString()}
          </p>
        </div>
      </div>

      <div className='grid gap-2'>
        {statusRows.map(({ key, label, icon: Icon, colorClassName }) => (
          <div
            key={key}
            className='dashboard-row flex items-center justify-between rounded-[20px] px-4 py-3'
          >
            <div className='flex items-center gap-3'>
              <div
                className={cn(
                  'flex h-9 w-9 items-center justify-center rounded-full ring-1 ring-inset ring-white/5',
                  colorClassName
                )}
              >
                <Icon
                  className={cn(
                    'h-4 w-4',
                    key === 'EXECUTING' && 'animate-spin'
                  )}
                />
              </div>
              <div>
                <p className='text-sm font-medium text-foreground'>{label}</p>
                <p className='text-xs text-muted-foreground'>
                  {key === 'FAILED'
                    ? 'Jobs requiring intervention'
                    : `Currently ${label.toLowerCase()} jobs`}
                </p>
              </div>
            </div>

            <span
              className={cn(
                'font-mono text-lg font-semibold',
                key === 'FAILED' && 'text-rose-300'
              )}
            >
              {(data?.[key] ?? 0).toLocaleString()}
            </span>
          </div>
        ))}
      </div>

      <div className='dashboard-inset dashboard-inset-strong rounded-[22px] p-4'>
        <div className='flex items-center justify-between gap-3'>
          <div>
            <p className='text-xs text-muted-foreground'>Successful completions</p>
            <p className='mt-1 text-sm text-foreground'>
              Based on completed versus failed backfill runs.
            </p>
          </div>
          <span
            className={cn(
              'font-mono text-lg font-semibold',
              successRate >= 90
                ? 'text-emerald-300'
                : successRate >= 70
                  ? 'text-amber-300'
                  : 'text-rose-300'
            )}
          >
            {successRate.toFixed(1)}%
          </span>
        </div>
        <Progress value={successRate} className='mt-4 h-2.5 bg-white/6' />
      </div>
    </DashboardPanel>
  )
}
