import { DotsHorizontalIcon } from '@radix-ui/react-icons'
import { type Row } from '@tanstack/react-table'
import { Trash2, Copy } from 'lucide-react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { toast } from 'sonner'
import { sourcesRepo } from '@/repo/sources'
import { Button } from '@/components/ui/button'
import {
    DropdownMenu,
    DropdownMenuContent,
    DropdownMenuItem,
    DropdownMenuSeparator,
    DropdownMenuShortcut,
    DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu'
import { sourceSchema } from '../data/schema'
import { useSources } from './sources-provider'

type DataTableRowActionsProps<TData> = {
    row: Row<TData>
}

export function SourcesRowActions<TData>({
    row,
}: DataTableRowActionsProps<TData>) {
    const source = sourceSchema.parse(row.original)

    const { setOpen, setCurrentRow } = useSources()
    const queryClient = useQueryClient()

    const duplicateMutation = useMutation({
        mutationFn: sourcesRepo.duplicate,
        onSuccess: async () => {
            await queryClient.invalidateQueries({ queryKey: ['sources'] })
            toast.success('Source duplicated successfully')
        },
        onError: () => {
            toast.error('Failed to duplicate source')
        }
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
            <DropdownMenuContent align='end' className='w-[160px]'>
                <DropdownMenuItem
                    onClick={() => {
                        setCurrentRow(source)
                        setOpen('update')
                    }}
                >
                    Edit
                </DropdownMenuItem>
                <DropdownMenuItem
                    onClick={() => {
                        duplicateMutation.mutate(source.id)
                    }}
                >
                    Duplicate
                    <DropdownMenuShortcut>
                        <Copy size={16} />
                    </DropdownMenuShortcut>
                </DropdownMenuItem>
                <DropdownMenuSeparator />
                <DropdownMenuItem
                    onClick={() => {
                        setCurrentRow(source)
                        setOpen('delete')
                    }}
                >
                    Delete
                    <DropdownMenuShortcut>
                        <Trash2 size={16} />
                    </DropdownMenuShortcut>
                </DropdownMenuItem>
            </DropdownMenuContent>
        </DropdownMenu>
    )
}
