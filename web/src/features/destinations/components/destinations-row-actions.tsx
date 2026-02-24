import { DotsHorizontalIcon } from '@radix-ui/react-icons'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { useNavigate } from '@tanstack/react-router'
import { type Row } from '@tanstack/react-table'
import { destinationsRepo } from '@/repo/destinations'
import { Trash2, Lock, Copy, Info } from 'lucide-react'
import { toast } from 'sonner'
import { Button } from '@/components/ui/button'
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuShortcut,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu'
import { destinationSchema } from '../data/schema'
import { useDestinations } from './destinations-provider'

type DataTableRowActionsProps<TData> = {
  row: Row<TData>
}

export function DestinationsRowActions<TData>({
  row,
}: DataTableRowActionsProps<TData>) {
  const destination = destinationSchema.parse(row.original)
  const navigate = useNavigate()

  const { setOpen, setCurrentRow } = useDestinations()
  const queryClient = useQueryClient()

  const duplicateMutation = useMutation({
    mutationFn: destinationsRepo.duplicate,
    onSuccess: async () => {
      toast.success('Destination duplicated successfully')
      // Wait for DB transaction to commit before refetching
      await new Promise((resolve) => setTimeout(resolve, 300))
      await queryClient.invalidateQueries({
        queryKey: ['destinations'],
        refetchType: 'active',
      })
    },
    onError: () => {
      toast.error('Failed to duplicate destination')
    },
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
      <DropdownMenuContent align='end' className='w-40'>
        {destination.type === 'SNOWFLAKE' && (
          <DropdownMenuItem
            onClick={() =>
              navigate({
                to: '/destinations/$destinationId',
                params: { destinationId: destination.id },
              })
            }
          >
            Detail
            <DropdownMenuShortcut>
              <Info size={16} />
            </DropdownMenuShortcut>
          </DropdownMenuItem>
        )}
        <DropdownMenuItem
          onClick={() => {
            setCurrentRow(destination)
            setOpen('update')
          }}
          disabled={destination.is_used_in_active_pipeline}
        >
          Edit
          {destination.is_used_in_active_pipeline && (
            <DropdownMenuShortcut>
              <Lock size={16} />
            </DropdownMenuShortcut>
          )}
        </DropdownMenuItem>
        <DropdownMenuItem
          onClick={() => {
            duplicateMutation.mutate(destination.id)
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
            setCurrentRow(destination)
            setOpen('delete')
          }}
          disabled={destination.is_used_in_active_pipeline}
        >
          Delete
          <DropdownMenuShortcut>
            {destination.is_used_in_active_pipeline ? (
              <Lock size={16} />
            ) : (
              <Trash2 size={16} />
            )}
          </DropdownMenuShortcut>
        </DropdownMenuItem>
      </DropdownMenuContent>
    </DropdownMenu>
  )
}
