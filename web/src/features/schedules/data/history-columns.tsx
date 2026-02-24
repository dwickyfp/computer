import { formatDistanceToNow, format } from 'date-fns'
import { type ColumnDef } from '@tanstack/react-table'
import {
  type ScheduleRunHistory,
} from '@/repo/schedules'
import { AlertTriangle } from 'lucide-react'
import { cn } from '@/lib/utils'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip'
import { Badge } from '@/components/ui/badge'

// ─── Helper ────────────────────────────────────────────────────────────────

// Minimalist status indicator
function StatusCell({ status }: { status: string }) {
  if (status === 'RUNNING') {
    return (
      <div className="flex items-center gap-2">
        <span className="relative flex h-2 w-2">
          <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-blue-400 opacity-75"></span>
          <span className="relative inline-flex rounded-full h-2 w-2 bg-blue-500"></span>
        </span>
        <span className="text-xs font-medium text-blue-600 dark:text-blue-400">Running</span>
      </div>
    )
  }
  if (status === 'SUCCESS') {
    return (
      <div className="flex items-center gap-2">
        <div className="h-1.5 w-1.5 rounded-full bg-emerald-500" />
        <span className="text-xs font-medium text-muted-foreground">Success</span>
      </div>
    )
  }
  if (status === 'FAILED') {
    return (
      <Badge variant="destructive" className="h-5 px-1.5 gap-1 text-[10px] font-semibold uppercase tracking-wider">
        Failed
      </Badge>
    )
  }
  return <span className="text-muted-foreground">—</span>
}

function DurationCell({ ms }: { ms: number | null }) {
  if (ms === null) return <span className="text-muted-foreground/30 text-[10px]">—</span>

  let formatted = ''
  let unit = ''

  if (ms < 1000) {
    formatted = `${ms}`
    unit = 'ms'
  } else if (ms < 60_000) {
    formatted = `${(ms / 1000).toFixed(1)}`
    unit = 's'
  } else {
    const m = Math.floor(ms / 60_000)
    const s = Math.floor((ms % 60_000) / 1000)
    formatted = `${m}m ${s}s`
    unit = ''
  }

  return (
    <div className='flex items-baseline justify-end gap-0.5 font-mono text-xs'>
      <span className={cn("font-medium", ms > 60000 ? "text-amber-600 dark:text-amber-500" : "text-foreground")}>
        {formatted}
      </span>
      {unit && <span className="text-[10px] text-muted-foreground">{unit}</span>}
    </div>
  )
}

// ─── Columns ─────────────────────────────────────────────────────────────────

export const historyColumns: ColumnDef<ScheduleRunHistory>[] = [
  {
    accessorKey: 'status',
    header: () => (
      <div className="ml-1 text-xs font-semibold uppercase tracking-wider text-muted-foreground/70">Status</div>
    ),
    cell: ({ row }) => <StatusCell status={row.original.status} />,
    meta: { className: 'w-[120px]' },
  },
  {
    accessorKey: 'triggered_at',
    header: () => (
      <div className="text-xs font-semibold uppercase tracking-wider text-muted-foreground/70">Triggered</div>
    ),
    cell: ({ row }) => {
      const date = new Date(row.original.triggered_at)
      return (
        <TooltipProvider>
          <Tooltip delayDuration={300}>
            <TooltipTrigger asChild>
              <div className="flex flex-col cursor-default">
                <span className='text-xs font-medium text-foreground'>
                  {formatDistanceToNow(date, { addSuffix: true })}
                </span>
              </div>
            </TooltipTrigger>
            <TooltipContent side="bottom" className="text-xs font-mono">
              {date.toLocaleString()}
            </TooltipContent>
          </Tooltip>
        </TooltipProvider>
      )
    },
    meta: { className: 'w-[140px]' },
  },
  {
    accessorKey: 'running_at', // Virtual accessor, we'll use triggered_at in cell
    header: () => (
      <div className="text-xs font-semibold uppercase tracking-wider text-muted-foreground/70">Running</div>
    ),
    cell: ({ row }) => {
      const date = new Date(row.original.triggered_at)
      return (
        <span className='text-xs font-medium text-foreground font-mono'>
          {format(date, 'dd-MM-yyyy HH:mm:ss')}
        </span>
      )
    },
    meta: { className: 'w-[160px]' },
  },
  {
    accessorKey: 'duration_ms',
    header: () => (
      <div className="w-full text-right text-xs font-semibold uppercase tracking-wider text-muted-foreground/70">Duration</div>
    ),
    cell: ({ row }) => <DurationCell ms={row.original.duration_ms} />,
    meta: { className: 'w-[100px]' },
  },
  {
    accessorKey: 'message',
    header: () => (
      <div className="ml-4 text-xs font-semibold uppercase tracking-wider text-muted-foreground/70">Message</div>
    ),
    cell: ({ row }) => {
      const msg = row.original.message
      if (!msg) return <span className='text-xs text-muted-foreground/30 pl-4'>—</span>

      return (
        <TooltipProvider>
          <Tooltip delayDuration={0}>
            <TooltipTrigger asChild>
              <div className='flex items-center gap-2 max-w-[350px] cursor-default pl-4 group'>
                {row.original.status === 'FAILED' && (
                  <AlertTriangle className="h-3.5 w-3.5 text-destructive shrink-0" />
                )}
                <span className={cn(
                  'truncate text-xs transition-colors',
                  row.original.status === 'FAILED' ? 'text-destructive font-medium' : 'text-muted-foreground group-hover:text-foreground'
                )}>
                  {msg}
                </span>
              </div>
            </TooltipTrigger>
            <TooltipContent side="bottom" align="start" className="max-w-[400px] break-words text-xs bg-foreground text-background">
              {msg}
            </TooltipContent>
          </Tooltip>
        </TooltipProvider>
      )
    },
  },
]
