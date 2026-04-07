import { useQuery } from '@tanstack/react-query'
import { memo } from 'react'
import { Cpu, Loader2, Zap } from 'lucide-react'
import { systemMetricsRepo } from '@/repo/system-metrics'
import { cn, formatBytes } from '@/lib/utils'
import { useRefreshInterval } from '../context/refresh-interval-context'
import { getDashboardPollingQueryOptions } from '../query-defaults'
import { DashboardPanel } from './dashboard-panel'

function CircularProgress({
  value,
  color,
  size = 64,
  strokeWidth = 6,
}: {
  value: number
  color: string
  size?: number
  strokeWidth?: number
}) {
  const radius = (size - strokeWidth) / 2
  const circumference = radius * 2 * Math.PI
  const offset = circumference - (value / 100) * circumference

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
          stroke='rgba(148, 163, 184, 0.18)'
          strokeWidth={strokeWidth}
          fill='transparent'
        />
        <circle
          cx={size / 2}
          cy={size / 2}
          r={radius}
          stroke='currentColor'
          strokeWidth={strokeWidth}
          fill='transparent'
          strokeDasharray={circumference}
          strokeDashoffset={offset}
          strokeLinecap='round'
          className={cn('transition-all duration-500 ease-out', color)}
        />
      </svg>
      <div className='absolute inset-0 flex items-center justify-center font-mono text-xs font-semibold text-foreground'>
        {Math.round(value)}%
      </div>
    </div>
  )
}

function MetricItem({
  label,
  value,
  color,
  icon: Icon,
  details,
}: {
  label: string
  value: number
  color: string
  icon: typeof Cpu
  details: React.ReactNode
}) {
  return (
    <div className='dashboard-row flex items-center justify-between rounded-[22px] px-4 py-4'>
      <div className='flex items-center gap-3'>
        <div
          className={cn(
            'flex h-10 w-10 items-center justify-center rounded-full ring-1 ring-inset ring-white/5',
            color.replace('text-', 'bg-').replace('300', '500/12'),
            color.includes('rose') && 'animate-pulse'
          )}
        >
          <Icon className={cn('h-4 w-4', color)} />
        </div>
        <div className='space-y-1'>
          <p className='text-sm font-medium text-foreground'>{label}</p>
          <div className='text-xs text-muted-foreground'>{details}</div>
        </div>
      </div>

      <CircularProgress value={value} color={color} />
    </div>
  )
}

function getLoadColor(percentage: number) {
  if (percentage < 50) return 'text-emerald-300'
  if (percentage < 80) return 'text-amber-300'
  return 'text-rose-300'
}

export const SystemLoadCard = memo(function SystemLoadCard() {
  const { refreshInterval } = useRefreshInterval()
  const { data: metrics, isLoading } = useQuery({
    queryKey: ['system-metrics', 'latest'],
    queryFn: systemMetricsRepo.getLatest,
    ...getDashboardPollingQueryOptions(refreshInterval),
    notifyOnChangeProps: ['data', 'isLoading'],
    select: (metrics) => ({
      cpuUsage: metrics.cpu_usage ?? 0,
      memoryUsage: metrics.memory_usage_percent ?? 0,
      totalMemory: metrics.total_memory ?? 0,
      usedMemory: metrics.used_memory ?? 0,
    }),
  })

  const cpuUsage = metrics?.cpuUsage ?? 0
  const memoryUsage = metrics?.memoryUsage ?? 0

  return (
    <DashboardPanel
      title='System load'
      description='Core resource utilization.'
      headerSlot={
        <div className='dashboard-chip rounded-full px-3 py-2 text-xs text-muted-foreground'>
          <span className='h-2 w-2 rounded-full bg-emerald-400' />
          Live
        </div>
      }
      variant='dense'
    >
      <div className='space-y-3'>
        {isLoading && (
          <div className='dashboard-inset flex items-center gap-3 rounded-[22px] px-4 py-4 text-sm text-muted-foreground'>
            <Loader2 className='h-4 w-4 animate-spin' />
            Refreshing resource metrics...
          </div>
        )}

        <MetricItem
          label='CPU usage'
          value={cpuUsage}
          color={getLoadColor(cpuUsage)}
          icon={Cpu}
          details={<span>Processor activity across the host process.</span>}
        />

        <MetricItem
          label='Memory'
          value={memoryUsage}
          color={getLoadColor(memoryUsage)}
          icon={Zap}
          details={
            <div className='font-mono'>
              {formatBytes(metrics?.usedMemory ?? 0)} /{' '}
              {formatBytes(metrics?.totalMemory ?? 0)}
            </div>
          }
        />
      </div>
    </DashboardPanel>
  )
})
