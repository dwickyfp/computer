import { DashboardPanel } from './dashboard-panel'
import { cn, formatBytes } from '@/lib/utils'
import { Cpu, Zap } from 'lucide-react'
import { systemMetricsRepo } from '@/repo/system-metrics'
import { useQuery } from '@tanstack/react-query'
import { useRefreshInterval } from '../context/refresh-interval-context'

function CircularProgress({
  value,
  color,
  size = 60,
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
        className='transform -rotate-90 transition-all duration-300 ease-in-out'
      >
        <circle
          cx={size / 2}
          cy={size / 2}
          r={radius}
          stroke='currentColor'
          strokeWidth={strokeWidth}
          fill='transparent'
          className='text-white/10'
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
      <div className='absolute inset-0 flex items-center justify-center text-xs font-bold'>
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
  icon: any
  details?: React.ReactNode
}) {
  return (
    <div className='flex items-center justify-between p-1 py-0 rounded-lg hover:bg-white/5 transition-colors'>
      <div className='flex items-center gap-3'>
        <div className={cn('p-2 rounded-full bg-opacity-10', color.replace('text-', 'bg-'),
          (color.includes('rose') || color.includes('red')) && "animate-pulse"
        )}>
          <Icon className={cn('w-4 h-4', color)} />
        </div>
        <div className='flex flex-col'>
          <span className='text-sm font-medium'>{label}</span>
          {details && <span className='text-xs text-muted-foreground'>{details}</span>}
        </div>
      </div>
      <div className='flex items-center gap-3'>
        <CircularProgress value={value} color={color} size={48} strokeWidth={4} />
      </div>
    </div>
  )
}

function getLoadColor(percentage: number) {
  if (percentage < 50) return 'text-emerald-500'
  if (percentage < 80) return 'text-amber-500'
  return 'text-rose-500'
}

export function SystemLoadCard() {
  const { refreshInterval } = useRefreshInterval()
  const { data: metrics } = useQuery({
    queryKey: ['system-metrics', 'latest'],
    queryFn: systemMetricsRepo.getLatest,
    refetchInterval: refreshInterval,
  })

  // Defaults if loading
  const cpuParams = metrics
    ? { val: metrics.cpu_usage || 0, color: getLoadColor(metrics.cpu_usage || 0) }
    : { val: 0, color: 'text-muted-foreground' }

  const memParams = metrics
    ? { val: metrics.memory_usage_percent || 0, color: getLoadColor(metrics.memory_usage_percent || 0) }
    : { val: 0, color: 'text-muted-foreground' }

  return (
    <DashboardPanel
      title="System Load"
      headerAction={
        <div className='flex items-center space-x-1'>
          <div className='w-2 h-2 rounded-full bg-emerald-500 animate-pulse' />
          <span className='text-[10px] text-muted-foreground font-medium uppercase tracking-wider'>Live</span>
        </div>
      }
      className='overflow-hidden h-full'
    >
      <div className='grid gap-2'>
        <MetricItem
          label="CPU Usage"
          value={cpuParams.val}
          color={cpuParams.color}
          icon={Cpu}
          details={
            <span className="text-[10px] opacity-70">
              Process Load
            </span>
          }
        />
        <MetricItem
          label="Memory"
          value={memParams.val}
          color={memParams.color}
          icon={Zap} // Using Zap as a generic power/active icon
          details={
            <div className='flex gap-1 font-mono text-[10px]'>
              <span>{formatBytes(metrics?.used_memory || 0)}</span>
              <span className='opacity-50'>/</span>
              <span>{formatBytes(metrics?.total_memory || 0)}</span>
            </div>
          }
        />
      </div>
    </DashboardPanel>
  )
}
