import { type ColumnDef } from '@tanstack/react-table'
import { DataTableColumnHeader } from '@/components/data-table'
import { type LinkedTask, type LinkedTaskStatus, linkedTasksRepo } from '@/repo/linked-tasks'
import { LinkedTasksRowActions } from './linked-tasks-row-actions'
import { Link } from '@tanstack/react-router'
import { Link2, Loader2, Play } from 'lucide-react'
import { formatDistanceToNow } from 'date-fns'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { toast } from 'sonner'
import { useState } from 'react'

function StatusBadge({ status }: { status: LinkedTaskStatus }) {
    const variants: Record<LinkedTaskStatus, string> = {
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

function RunButton({ linkedTask }: { linkedTask: LinkedTask }) {
    const queryClient = useQueryClient()
    const [running, setRunning] = useState(false)

    const runMutation = useMutation({
        mutationFn: (id: number) => linkedTasksRepo.trigger(id),
        onMutate: () => setRunning(true),
        onSuccess: () => {
            setRunning(false)
            toast.success('Linked task triggered')
            setTimeout(() => queryClient.invalidateQueries({ queryKey: ['linked-tasks'] }), 300)
        },
        onError: () => {
            setRunning(false)
            toast.error('Failed to trigger linked task')
        },
    })

    return (
        <Button
            size="sm"
            variant="outline"
            className="h-7 px-2.5 gap-1.5 text-xs font-medium"
            disabled={linkedTask.status === 'RUNNING' || running}
            onClick={() => runMutation.mutate(linkedTask.id)}
        >
            {(linkedTask.status === 'RUNNING' || running)
                ? <Loader2 className="h-3 w-3 animate-spin" />
                : <Play className="h-3 w-3" />}
            Run
        </Button>
    )
}

export const linkedTasksColumns: ColumnDef<LinkedTask>[] = [
    {
        accessorKey: 'name',
        header: ({ column }) => (
            <DataTableColumnHeader column={column} title='Linked Task' />
        ),
        cell: ({ row }) => (
            <div className="flex items-start gap-3 pl-2">
                <div className="h-9 w-9 mt-0.5 rounded-lg border bg-background/50 flex items-center justify-center shrink-0 shadow-sm">
                    <Link2 className="h-4 w-4 text-muted-foreground" />
                </div>
                <div className="flex flex-col gap-0.5">
                    <Link
                        to={'/linked-tasks/$linkedTaskId' as any}
                        params={{ linkedTaskId: String(row.original.id) } as any}
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
        meta: { title: 'Linked Task' },
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
        accessorKey: 'last_run_at',
        header: ({ column }) => (
            <DataTableColumnHeader column={column} title='Last Execution' />
        ),
        cell: ({ row }) => {
            const lastRunAt = row.getValue('last_run_at') as string | null
            const lastRunStatus = row.original.last_run_status
            return (
                <div className="flex flex-col gap-0.5">
                    <span className="text-sm font-medium">
                        {lastRunAt
                            ? formatDistanceToNow(new Date(lastRunAt), { addSuffix: true })
                            : 'Never executed'}
                    </span>
                    {lastRunStatus && (
                        <Badge variant="secondary" className="w-fit text-[10px]">
                            {lastRunStatus}
                        </Badge>
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
                <RunButton linkedTask={row.original} />
            </div>
        ),
        enableSorting: false,
        enableHiding: false,
        meta: { title: 'Run' },
    },
    {
        id: 'actions',
        cell: ({ row }) => <LinkedTasksRowActions row={row} />,
        meta: { title: 'Actions' },
    },
]
