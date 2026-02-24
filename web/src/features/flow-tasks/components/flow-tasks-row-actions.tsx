import { DotsHorizontalIcon } from '@radix-ui/react-icons'
import { type Row } from '@tanstack/react-table'
import { Trash2, Copy, Pencil, GitBranch, ExternalLink } from 'lucide-react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { toast } from 'sonner'
import { Link } from '@tanstack/react-router'

import { flowTasksRepo, type FlowTask } from '@/repo/flow-tasks'
import { Button } from '@/components/ui/button'
import {
    DropdownMenu,
    DropdownMenuContent,
    DropdownMenuItem,
    DropdownMenuSeparator,
    DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu'
import { useFlowTasks } from './flow-tasks-provider'

interface DataTableRowActionsProps {
    row: Row<FlowTask>
}

export function FlowTasksRowActions({ row }: DataTableRowActionsProps) {
    const flowTask = row.original
    const { setOpen, setCurrentRow } = useFlowTasks()
    const queryClient = useQueryClient()

    const duplicateMutation = useMutation({
        mutationFn: (id: number) => flowTasksRepo.duplicate(id),
        onSuccess: () => {
            toast.success('Flow task duplicated')
            queryClient.invalidateQueries({ queryKey: ['flow-tasks'] })
        },
        onError: () => toast.error('Failed to duplicate flow task'),
    })

    return (
        <DropdownMenu modal={false}>
            <DropdownMenuTrigger asChild>
                <Button
                    variant='ghost'
                    className='flex h-8 w-8 p-0 data-[state=open]:bg-muted'
                >
                    <DotsHorizontalIcon className='h-4 w-4' />
                    <span className='sr-only'>Open menu</span>
                </Button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align='end' className='w-48'>
                <DropdownMenuItem asChild>
                    <Link
                        to="/flow-tasks/$flowTaskId"
                        params={{ flowTaskId: String(flowTask.id) }}
                        className="flex items-center gap-2 cursor-pointer"
                    >
                        <ExternalLink className="h-4 w-4 text-muted-foreground" />
                        View Details
                    </Link>
                </DropdownMenuItem>
                <DropdownMenuItem asChild>
                    <Link
                        to="/flow-tasks/$flowTaskId/flow"
                        params={{ flowTaskId: String(flowTask.id) }}
                        className="flex items-center gap-2 cursor-pointer"
                    >
                        <GitBranch className="h-4 w-4 text-muted-foreground" />
                        Flow Editor
                    </Link>
                </DropdownMenuItem>
                <DropdownMenuSeparator />
                <DropdownMenuItem
                    onClick={() => {
                        setCurrentRow(flowTask)
                        setOpen('update')
                    }}
                    className="flex items-center gap-2 cursor-pointer"
                >
                    <Pencil className="h-4 w-4 text-muted-foreground" />
                    Edit Settings
                </DropdownMenuItem>
                <DropdownMenuItem
                    onClick={() => duplicateMutation.mutate(flowTask.id)}
                    className="flex items-center gap-2 cursor-pointer"
                >
                    <Copy className="h-4 w-4 text-muted-foreground" />
                    Duplicate
                </DropdownMenuItem>
                <DropdownMenuSeparator />
                <DropdownMenuItem
                    onClick={() => {
                        setCurrentRow(flowTask)
                        setOpen('delete')
                    }}
                    className="text-destructive focus:text-destructive flex items-center gap-2 cursor-pointer"
                >
                    <Trash2 className="h-4 w-4" />
                    Delete
                </DropdownMenuItem>
            </DropdownMenuContent>
        </DropdownMenu>
    )
}
