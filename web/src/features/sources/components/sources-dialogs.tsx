import { useMutation, useQueryClient } from '@tanstack/react-query'
import { sourcesRepo } from '@/repo/sources'
import { toast } from 'sonner'
import { ConfirmDialog } from '@/components/confirm-dialog'
import { SourcesMutateDrawer } from './sources-mutate-drawer'
import { useSources } from './sources-provider'

export function SourcesDialogs() {
  const { open, setOpen, currentRow, setCurrentRow } = useSources()
  const queryClient = useQueryClient()

  const deleteMutation = useMutation({
    mutationFn: sourcesRepo.delete,
    onSuccess: async () => {
      setOpen(null)
      setTimeout(() => {
        setCurrentRow(null)
      }, 500)
      toast.success('Source deleted successfully')
      // Wait 300ms for backend transaction to commit before refetching
      await new Promise((resolve) => setTimeout(resolve, 300))
      await queryClient.invalidateQueries({ queryKey: ['sources'] })
    },
    onError: (error) => {
      toast.error('Failed to delete source')
      console.error(error)
    },
  })

  return (
    <>
      <SourcesMutateDrawer
        key='source-create'
        open={open === 'create'}
        onOpenChange={() => setOpen('create')}
      />

      {currentRow && (
        <>
          <SourcesMutateDrawer
            key={`source-update-${currentRow.id}`}
            open={open === 'update'}
            onOpenChange={(isOpen) => {
              if (!isOpen) {
                setOpen(null)
                setTimeout(() => setCurrentRow(null), 500)
              }
            }}
            currentRow={currentRow}
          />

          <ConfirmDialog
            key='source-delete'
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
            title={`Delete this source: ${currentRow.name} ?`}
            desc={
              <>
                You are about to delete the source{' '}
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
