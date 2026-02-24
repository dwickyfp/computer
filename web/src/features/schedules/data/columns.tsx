import { formatDistanceToNow } from 'date-fns'
import { type ColumnDef } from '@tanstack/react-table'
import { type ScheduleListItem } from '@/repo/schedules'
import cronstrue from 'cronstrue'
import { Calendar, Pause, Play } from 'lucide-react'
import { DataTableColumnHeader } from '@/components/data-table'
import { SchedulesRowActions } from '../components/schedules-row-actions'

// ─── Helper components ───────────────────────────────────────────────────────

function StatusBadge({ status }: { status: 'ACTIVE' | 'PAUSED' }) {
  if (status === 'ACTIVE') {
    return (
      <span className='inline-flex items-center gap-1.5 rounded-full border border-emerald-600 bg-emerald-600 px-2.5 py-0.5 text-xs font-semibold text-white shadow-sm dark:border-emerald-500 dark:bg-emerald-500'>
        <Play className='h-3 w-3' />
        ACTIVE
      </span>
    )
  }
  return (
    <span className='inline-flex items-center gap-1.5 rounded-full border border-border bg-muted px-2.5 py-0.5 text-xs font-semibold text-muted-foreground shadow-sm'>
      <Pause className='h-3 w-3' />
      PAUSED
    </span>
  )
}

function TaskTypeBadge({ type }: { type: 'FLOW_TASK' | 'LINKED_TASK' }) {
  const label = type === 'FLOW_TASK' ? 'Flow Task' : 'Linked Task'
  const cls =
    type === 'FLOW_TASK'
      ? 'bg-blue-100 text-blue-800 border-blue-200 dark:bg-blue-900/30 dark:text-blue-300 dark:border-blue-800'
      : 'bg-violet-100 text-violet-800 border-violet-200 dark:bg-violet-900/30 dark:text-violet-300 dark:border-violet-800'
  return (
    <span
      className={`inline-flex items-center gap-1 rounded border px-2 py-0.5 text-xs font-medium ${cls}`}
    >
      {label}
    </span>
  )
}

function CronDisplay({ expr }: { expr: string }) {
  let human = expr
  try {
    human = cronstrue.toString(expr, { verbose: false })
  } catch {
    // fall back to raw expression
  }
  return (
    <div className='flex flex-col gap-0.5'>
      <code className='font-mono text-xs text-foreground'>{expr}</code>
      <span className='text-[11px] leading-tight text-muted-foreground'>
        {human}
      </span>
    </div>
  )
}

// ─── Column definitions ───────────────────────────────────────────────────────

export const schedulesColumns: ColumnDef<ScheduleListItem>[] = [
  {
    accessorKey: 'name',
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title='Name' />
    ),
    cell: ({ row }) => (
      <div className='flex items-center gap-2'>
        <Calendar className='h-4 w-4 shrink-0 text-muted-foreground' />
        <span className='max-w-[180px] truncate font-medium'>
          {row.original.name}
        </span>
      </div>
    ),
    meta: { className: 'min-w-[160px]' },
  },
  {
    accessorKey: 'task_type',
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title='Type' />
    ),
    cell: ({ row }) => <TaskTypeBadge type={row.original.task_type} />,
    meta: { className: 'w-[120px]' },
  },
  {
    accessorKey: 'cron_expression',
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title='Schedule' />
    ),
    cell: ({ row }) => <CronDisplay expr={row.original.cron_expression} />,
    meta: { className: 'min-w-[220px]' },
  },
  {
    accessorKey: 'status',
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title='Status' />
    ),
    cell: ({ row }) => <StatusBadge status={row.original.status} />,
    meta: { className: 'w-[110px]' },
  },
  {
    accessorKey: 'last_run_at',
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title='Last Run' />
    ),
    cell: ({ row }) => {
      const v = row.original.last_run_at
      if (!v)
        return <span className='text-xs text-muted-foreground'>Never</span>
      return (
        <span className='text-xs text-muted-foreground'>
          {formatDistanceToNow(new Date(v), { addSuffix: true })}
        </span>
      )
    },
    meta: { className: 'w-[130px]' },
  },
  {
    accessorKey: 'next_run_at',
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title='Next Run' />
    ),
    cell: ({ row }) => {
      const v = row.original.next_run_at
      if (!v) return <span className='text-xs text-muted-foreground'>—</span>
      return (
        <span className='text-xs text-muted-foreground'>
          {formatDistanceToNow(new Date(v), { addSuffix: true })}
        </span>
      )
    },
    meta: { className: 'w-[130px]' },
  },
  {
    id: 'actions',
    cell: ({ row }) => <SchedulesRowActions schedule={row.original} />,
    meta: { className: 'w-[60px]' },
  },
]
