import { Plus, RefreshCw } from 'lucide-react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { toast } from 'sonner'
import { Button } from '@/components/ui/button'
import { chainRepo, type ChainClient } from '@/repo/chains'
import { useChain } from './chain-provider'

interface ChainPrimaryButtonsProps {
  clients?: ChainClient[]
}

export function ChainPrimaryButtons({ clients = [] }: ChainPrimaryButtonsProps) {
  const { setOpen } = useChain()
  const queryClient = useQueryClient()

  const refreshAllMutation = useMutation({
    mutationFn: async () => {
      // For each client, fetch remote databases and compare+upsert in local DB
      const results = await Promise.allSettled(
        clients.map((client) => chainRepo.syncClientDatabases(client.id))
      )
      const failed = results.filter((r) => r.status === 'rejected').length
      return { total: clients.length, failed }
    },
    onSuccess: ({ total, failed }) => {
      queryClient.invalidateQueries({ queryKey: ['chain-clients'] })
      if (failed === 0) {
        toast.success(`Refreshed databases for ${total} client(s)`)
      } else {
        toast.warning(
          `Refreshed ${total - failed} of ${total} client(s). ${failed} failed.`
        )
      }
    },
    onError: (err: any) => {
      toast.error(`Refresh failed: ${err.message || 'Unknown error'}`)
    },
  })

  return (
    <div className='flex items-center gap-2'>
      <Button
        variant='outline'
        onClick={() => refreshAllMutation.mutate()}
        disabled={refreshAllMutation.isPending || clients.length === 0}
      >
        <RefreshCw
          className={`mr-2 h-4 w-4 ${refreshAllMutation.isPending ? 'animate-spin' : ''}`}
        />
        Refresh All
      </Button>
      <Button onClick={() => setOpen('create')}>
        Add Client <Plus className='ml-2 h-4 w-4' />
      </Button>
    </div>
  )
}
