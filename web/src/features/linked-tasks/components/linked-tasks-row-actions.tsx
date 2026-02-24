import { DotsHorizontalIcon } from '@radix-ui/react-icons'
import { type Row } from '@tanstack/react-table'
import { Trash2, Pencil, ExternalLink } from 'lucide-react'
import { Link } from '@tanstack/react-router'

import { type LinkedTask } from '@/repo/linked-tasks'
import { Button } from '@/components/ui/button'
import {
    DropdownMenu,
    DropdownMenuContent,
    DropdownMenuItem,
    DropdownMenuSeparator,
    DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu'
import { useLinkedTasks } from './linked-tasks-provider'

interface DataTableRowActionsProps {
    row: Row<LinkedTask>
}

export function LinkedTasksRowActions({ row }: DataTableRowActionsProps) {
    const linkedTask = row.original
    const { setOpen, setCurrentRow } = useLinkedTasks()

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
                        to={'/linked-tasks/$linkedTaskId' as any}
                        params={{ linkedTaskId: String(linkedTask.id) } as any}
                        className="flex items-center gap-2 cursor-pointer"
                    >
                        <ExternalLink className="h-4 w-4 text-muted-foreground" />
                        Open Editor
                    </Link>
                </DropdownMenuItem>
                <DropdownMenuSeparator />
                <DropdownMenuItem
                    onClick={() => {
                        setCurrentRow(linkedTask)
                        setOpen('update')
                    }}
                    className="flex items-center gap-2 cursor-pointer"
                >
                    <Pencil className="h-4 w-4 text-muted-foreground" />
                    Edit Settings
                </DropdownMenuItem>
                <DropdownMenuSeparator />
                <DropdownMenuItem
                    onClick={() => {
                        setCurrentRow(linkedTask)
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
