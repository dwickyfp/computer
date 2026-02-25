import { useMutation, useQueryClient } from '@tanstack/react-query'
import { chainRepo } from '@/repo/chains'
import { toast } from 'sonner'
import { ConfirmDialog } from '@/components/confirm-dialog'
import { ChainClientMutateDrawer } from './chain-client-mutate-drawer'
import { useChain } from './chain-provider'

export function ChainDialogs() {
  const { open, setOpen, currentRow, setCurrentRow } = useChain()
  const queryClient = useQueryClient()

  const deleteMutation = useMutation({
    mutationFn: chainRepo.deleteClient,
    onSuccess: async () => {
      setOpen(null)
      setTimeout(() => setCurrentRow(null), 500)
      toast.success('Chain client deleted')
      await new Promise((r) => setTimeout(r, 300))
      await queryClient.invalidateQueries({ queryKey: ['chain-clients'] })
    },
    onError: () => toast.error('Failed to delete chain client'),
  })

  const testMutation = useMutation({
    mutationFn: chainRepo.testClient,
    onSuccess: (result) => {
      if (result.success) {
        toast.success(
          `Connection successful (${result.latency_ms?.toFixed(0)}ms)`
        )
      } else {
        toast.error(`Connection failed: ${result.message}`)
      }
      setOpen(null)
    },
    onError: () => {
      toast.error('Connection test failed')
      setOpen(null)
    },
  })

  return (
    <>
      {/* Create drawer */}
      <ChainClientMutateDrawer
        key='chain-client-create'
        open={open === 'create'}
        onOpenChange={() => setOpen('create')}
      />

      {/* Update drawer (requires currentRow) */}
      {currentRow && (
        <>
          <ChainClientMutateDrawer
            key={`chain-client-update-${currentRow.id}`}
            open={open === 'update'}
            onOpenChange={() => setOpen('update')}
            currentRow={currentRow}
          />

          {/* Delete confirm */}
          <ConfirmDialog
            key='chain-client-delete'
            destructive
            open={open === 'delete'}
            onOpenChange={() => setOpen('delete')}
            handleConfirm={() => deleteMutation.mutate(currentRow.id)}
            isLoading={deleteMutation.isPending}
            title={`Delete "${currentRow.name}"?`}
            desc={`This will permanently remove the chain client "${currentRow.name}".
                            Any pipelines using this client will stop working.`}
            confirmText='Delete'
          />

          {/* Test connection confirm */}
          <ConfirmDialog
            key='chain-client-test'
            open={open === 'test'}
            onOpenChange={() => setOpen('test')}
            handleConfirm={() => testMutation.mutate(currentRow.id)}
            isLoading={testMutation.isPending}
            title={`Test "${currentRow.name}"?`}
            desc={`This will test the connection to ${currentRow.url}${currentRow.port ? `:${currentRow.port}` : ''}.`}
            confirmText='Test Connection'
          />
        </>
      )}
    </>
  )
}
