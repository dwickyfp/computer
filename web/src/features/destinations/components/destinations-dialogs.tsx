import { useMutation, useQueryClient } from '@tanstack/react-query'
import { destinationsRepo } from '@/repo/destinations'
import { toast } from 'sonner'
import { ConfirmDialog } from '@/components/confirm-dialog'
import { DestinationsMutateDrawer } from './destinations-mutate-drawer'
import { DestinationTableListModal } from './destination-table-list-modal'
import { useDestinations } from './destinations-provider'

export function DestinationsDialogs() {
  const { open, setOpen, currentRow, setCurrentRow } = useDestinations()
  const queryClient = useQueryClient()

  const deleteMutation = useMutation({
    mutationFn: destinationsRepo.delete,
    onSuccess: async () => {
      setOpen(null)
      setTimeout(() => {
        setCurrentRow(null)
      }, 500)
      toast.success('Destination deleted successfully')
      // Wait for DB transaction to commit before refetching
      await new Promise((resolve) => setTimeout(resolve, 300))
      await queryClient.invalidateQueries({
        queryKey: ['destinations'],
        refetchType: 'active',
      })
    },
    onError: (error) => {
      toast.error('Failed to delete destination')
      console.error(error)
    },
  })

  return (
    <>
      <DestinationsMutateDrawer
        key='destination-create'
        open={open === 'create'}
        onOpenChange={() => setOpen('create')}
      />

      {currentRow && (
        <>
          <DestinationsMutateDrawer
            key={`destination-update-${currentRow.id}`}
            open={open === 'update'}
            onOpenChange={(isOpen) => {
              if (!isOpen) {
                setOpen(null)
                setTimeout(() => setCurrentRow(null), 500)
              }
            }}
            currentRow={currentRow}
          />

          <DestinationTableListModal
            key={`destination-table-list-${currentRow.id}`}
            destination={currentRow}
            open={open === 'table-list'}
            onOpenChange={(isOpen) => {
              if (!isOpen) {
                setOpen(null)
                setTimeout(() => setCurrentRow(null), 500)
              }
            }}
          />

          <ConfirmDialog
            key='destination-delete'
            destructive
            open={open === 'delete'}
            onOpenChange={() => {
              setOpen('delete')
              setTimeout(() => {
                setCurrentRow(null)
              }, 500)
            }}
            handleConfirm={() => {
              deleteMutation.mutate(currentRow.id)
            }}
            className='max-w-md'
            title={`Delete this destination: ${currentRow.name} ?`}
            desc={
              <>
                You are about to delete the destination{' '}
                <strong>{currentRow.name}</strong>. <br />
                This action cannot be undone.
              </>
            }
            confirmText='Delete'
          />
        </>
      )}
    </>
  )
}
