import { DotsHorizontalIcon } from '@radix-ui/react-icons'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { type Row } from '@tanstack/react-table'
import { chainRepo } from '@/repo/chains'
import { Pencil, Trash2, Zap, RefreshCw } from 'lucide-react'
import { toast } from 'sonner'
import { Button } from '@/components/ui/button'
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu'
import { type ChainClient, chainClientSchema } from '../data/schema'
import { useChain } from './chain-provider'

interface ChainClientRowActionsProps {
  row: Row<ChainClient>
}

export function ChainClientRowActions({ row }: ChainClientRowActionsProps) {
  const client = chainClientSchema.parse(row.original)
  const { setOpen, setCurrentRow } = useChain()
  const queryClient = useQueryClient()

  const syncMutation = useMutation({
    mutationFn: () => chainRepo.syncClientDatabases(client.id),
    onSuccess: () => {
      toast.success(`Refreshed databases for ${client.name}`)
      queryClient.invalidateQueries({ queryKey: ['chain-clients'] })
    },
    onError: (err: any) => {
      toast.error(
        `Failed to refresh databases: ${err.message || 'Unknown error'}`
      )
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
      <DropdownMenuContent align='end' className='w-auto min-w-[180px]'>
        <DropdownMenuItem
          onClick={() => {
            setCurrentRow(client)
            setOpen('test')
          }}
          className='whitespace-nowrap'
        >
          <Zap className='mr-2 h-4 w-4' />
          Test Connection
        </DropdownMenuItem>
        <DropdownMenuSeparator />
        <DropdownMenuItem
          onClick={(e) => {
            e.preventDefault()
            syncMutation.mutate()
          }}
          disabled={syncMutation.isPending}
          className='whitespace-nowrap'
        >
          <RefreshCw
            className={`mr-2 h-4 w-4 ${syncMutation.isPending ? 'animate-spin' : ''}`}
          />
          Refresh Databases
        </DropdownMenuItem>
        <DropdownMenuSeparator />
        <DropdownMenuItem
          onClick={() => {
            setCurrentRow(client)
            setOpen('update')
          }}
        >
          <Pencil className='mr-2 h-4 w-4' />
          Edit
        </DropdownMenuItem>
        <DropdownMenuSeparator />
        <DropdownMenuItem
          className='text-destructive focus:text-destructive'
          onClick={() => {
            setCurrentRow(client)
            setOpen('delete')
          }}
        >
          <Trash2 className='mr-2 h-4 w-4' />
          Delete
        </DropdownMenuItem>
      </DropdownMenuContent>
    </DropdownMenu>
  )
}
