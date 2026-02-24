import { type ColumnDef } from '@tanstack/react-table'
import { DataTableColumnHeader } from '@/components/data-table'
import { type FlowTask, type FlowTaskStatus, flowTasksRepo } from '@/repo/flow-tasks'
import { FlowTasksRowActions } from './flow-tasks-row-actions'
import { Link } from '@tanstack/react-router'
import { GitBranch, Loader2, Play, Square } from 'lucide-react'
import { Badge } from '@/components/ui/badge'
import { formatDistanceToNow } from 'date-fns'
import { Button } from '@/components/ui/button'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { toast } from 'sonner'
import { useState } from 'react'

function StatusBadge({ status }: { status: FlowTaskStatus }) {
    const variants: Record<FlowTaskStatus, string> = {
        IDLE: 'bg-muted text-muted-foreground border-border',
        RUNNING: 'bg-blue-600 text-white border-blue-600 dark:bg-blue-500 dark:border-blue-500',
        SUCCESS: 'bg-emerald-600 text-white border-emerald-600 dark:bg-emerald-500 dark:border-emerald-500',
        FAILED: 'bg-rose-600 text-white border-rose-600 dark:bg-rose-500 dark:border-rose-500',
    }
    return (
        <span className={`inline-flex items-center gap-1.5 px-2.5 py-0.5 rounded-full text-xs font-semibold border shadow-sm ${variants[status]}`}>
            {status === 'RUNNING' && <Loader2 className="h-3 w-3 animate-spin" />}
            {status}
        </span>
    )
}

function RunButton({ flowTask }: { flowTask: FlowTask }) {
    const queryClient = useQueryClient()
    const [running, setRunning] = useState(false)

    const runMutation = useMutation({
        mutationFn: (id: number) => flowTasksRepo.run(id),
        onMutate: () => setRunning(true),
        onSuccess: () => {
            setRunning(false)
            toast.success('Flow task triggered')
            setTimeout(() => queryClient.invalidateQueries({ queryKey: ['flow-tasks'] }), 300)
        },
        onError: () => {
            setRunning(false)
            toast.error('Failed to trigger flow task')
        },
    })

    const cancelMutation = useMutation({
        mutationFn: (id: number) => flowTasksRepo.cancelRun(id),
        onSuccess: () => {
            toast.info('Run cancelled')
            setTimeout(() => queryClient.invalidateQueries({ queryKey: ['flow-tasks'] }), 300)
        },
        onError: () => toast.error('Failed to cancel run'),
    })

    const isRunning = flowTask.status === 'RUNNING' || running

    if (isRunning) {
        return (
            <Button
                size="sm"
                variant="destructive"
                className="h-7 px-2.5 gap-1.5 text-xs font-medium"
                disabled={cancelMutation.isPending}
                onClick={() => cancelMutation.mutate(flowTask.id)}
            >
                {cancelMutation.isPending
                    ? <Loader2 className="h-3 w-3 animate-spin" />
                    : <Square className="h-3 w-3" />}
                Cancel
            </Button>
        )
    }

    return (
        <Button
            size="sm"
            variant="outline"
            className="h-7 px-2.5 gap-1.5 text-xs font-medium"
            disabled={runMutation.isPending}
            onClick={() => runMutation.mutate(flowTask.id)}
        >
            {runMutation.isPending
                ? <Loader2 className="h-3 w-3 animate-spin" />
                : <Play className="h-3 w-3" />}
            Run
        </Button>
    )
}

export const flowTasksColumns: ColumnDef<FlowTask>[] = [
    {
        accessorKey: 'name',
        header: ({ column }) => (
            <DataTableColumnHeader column={column} title='Flow Task' />
        ),
        cell: ({ row }) => (
            <div className="flex items-start gap-3 pl-2">
                <div className="h-9 w-9 mt-0.5 rounded-lg border bg-background/50 flex items-center justify-center shrink-0 shadow-sm">
                    <GitBranch className="h-4 w-4 text-muted-foreground" />
                </div>
                <div className="flex flex-col gap-0.5">
                    <Link
                        to="/flow-tasks/$flowTaskId"
                        params={{ flowTaskId: String(row.original.id) }}
                        className="font-medium hover:underline text-sm text-foreground block decoration-primary/50 underline-offset-4"
                    >
                        {row.getValue('name')}
                    </Link>
                    {row.original.description ? (
                        <p className="text-xs text-muted-foreground line-clamp-1 max-w-[280px]">
                            {row.original.description}
                        </p>
                    ) : (
                        <span className="text-xs text-muted-foreground/50 italic">No description provided</span>
                    )}
                </div>
            </div>
        ),
        meta: { title: 'Flow Task' },
    },
    {
        accessorKey: 'status',
        header: ({ column }) => (
            <DataTableColumnHeader column={column} title='Status' />
        ),
        cell: ({ row }) => <StatusBadge status={row.getValue('status')} />,
        meta: { title: 'Status' },
    },
    {
        accessorKey: 'trigger_type',
        header: ({ column }) => (
            <DataTableColumnHeader column={column} title='Trigger' />
        ),
        cell: ({ row }) => (
            <Badge variant="secondary" className="text-[10px] font-mono capitalize px-2 py-0.5 bg-muted/50 text-muted-foreground font-medium border-0 tracking-wider">
                {(row.getValue('trigger_type') as string)?.toLowerCase() || 'manual'}
            </Badge>
        ),
        meta: { title: 'Trigger' },
    },
    {
        accessorKey: 'last_run_at',
        header: ({ column }) => (
            <DataTableColumnHeader column={column} title='Last Execution' />
        ),
        cell: ({ row }) => {
             const lastRunAt = row.getValue('last_run_at') as string | null
             return (
                <div className="flex flex-col gap-0.5">
                    <span className="text-sm font-medium">
                        {lastRunAt
                            ? formatDistanceToNow(new Date(lastRunAt), {
                                    addSuffix: true,
                                })
                            : 'Never executed'}
                    </span>
                    {lastRunAt && (
                        <span className="text-xs text-muted-foreground font-mono">
                            {row.original.last_run_record_count != null
                                    ? `${row.original.last_run_record_count.toLocaleString()} records`
                                    : '—'}
                        </span>
                    )}
                </div>
            )
        },
        meta: { title: 'Last Execution' },
    },
    {
        id: 'run',
        header: () => <div className="text-center font-semibold text-xs text-muted-foreground uppercase tracking-wider">Run</div>,
        cell: ({ row }) => (
            <div className='flex justify-center'>
               <RunButton flowTask={row.original} />
            </div>
        ),
        enableSorting: false,
        enableHiding: false,
        meta: { title: 'Run' },
    },
    {
        id: 'actions',
        cell: ({ row }) => <FlowTasksRowActions row={row} />,
        meta: { title: 'Actions' },
    },
]
