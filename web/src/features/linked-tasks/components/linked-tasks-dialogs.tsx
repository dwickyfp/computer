import { useMutation, useQueryClient } from '@tanstack/react-query'
import { linkedTasksRepo } from '@/repo/linked-tasks'
import { toast } from 'sonner'
import { ConfirmDialog } from '@/components/confirm-dialog'
import { useLinkedTasks } from './linked-tasks-provider'
import { LinkedTaskDialog } from './linked-task-dialog'

export function LinkedTasksDialogs() {
    const { open, setOpen, currentRow, setCurrentRow } = useLinkedTasks()
    const queryClient = useQueryClient()

    const deleteMutation = useMutation({
        mutationFn: linkedTasksRepo.remove,
        onSuccess: async () => {
            setOpen(null)
            setTimeout(() => {
                setCurrentRow(null)
            }, 500)
            toast.success('Linked task deleted successfully')
            await Promise.all([
                new Promise((resolve) => setTimeout(resolve, 300)),
                queryClient.invalidateQueries({ queryKey: ['linked-tasks'] })
            ])
        },
        onError: (error) => {
            toast.error('Failed to delete linked task')
            console.error(error)
        },
    })

    return (
        <>
            <LinkedTaskDialog
                key='linked-task-create'
                open={open === 'create'}
                onClose={() => setOpen(null)}
                onSaved={() => queryClient.invalidateQueries({ queryKey: ['linked-tasks'] })}
            />

            {currentRow && (
                <>
                    <LinkedTaskDialog
                        key={`linked-task-update-${currentRow.id}`}
                        open={open === 'update'}
                        onClose={() => {
                            setOpen(null)
                            setTimeout(() => setCurrentRow(null), 500)
                        }}
                        onSaved={() => queryClient.invalidateQueries({ queryKey: ['linked-tasks'] })}
                        existing={currentRow}
                    />

                    <ConfirmDialog
                        key='linked-task-delete'
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
                        title={`Delete this linked task: ${currentRow.name} ?`}
                        desc={
                            <>
                                You are about to delete the linked task{' '}
                                <strong>{currentRow.name}</strong>. <br />
                                This action cannot be undone and will permanently remove the linked task and all associated run history.
                            </>
                        }
                        confirmText='Delete'
                    />
                </>
            )}
        </>
    )
}
