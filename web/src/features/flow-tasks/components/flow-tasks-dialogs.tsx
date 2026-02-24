import { useMutation, useQueryClient } from '@tanstack/react-query'
import { flowTasksRepo } from '@/repo/flow-tasks'
import { toast } from 'sonner'
import { ConfirmDialog } from '@/components/confirm-dialog'
import { useFlowTasks } from './flow-tasks-provider'
import { FlowTaskDialog } from './flow-task-dialog'

export function FlowTasksDialogs() {
    const { open, setOpen, currentRow, setCurrentRow } = useFlowTasks()
    const queryClient = useQueryClient()

    const deleteMutation = useMutation({
        mutationFn: flowTasksRepo.remove,
        onSuccess: async () => {
            setOpen(null)
            setTimeout(() => {
                setCurrentRow(null)
            }, 500)
            toast.success('Flow task deleted successfully')
            await Promise.all([
                new Promise((resolve) => setTimeout(resolve, 300)),
                queryClient.invalidateQueries({ queryKey: ['flow-tasks'] })
            ])
        },
        onError: (error) => {
            toast.error('Failed to delete flow task')
            console.error(error)
        },
    })

    return (
        <>
            <FlowTaskDialog
                key='flow-task-create'
                open={open === 'create'}
                onClose={() => setOpen(null)}
                onSaved={() => queryClient.invalidateQueries({ queryKey: ['flow-tasks'] })}
            />

            {currentRow && (
                <>
                    <FlowTaskDialog
                        key={`flow-task-update-${currentRow.id}`}
                        open={open === 'update'}
                        onClose={() => {
                            setOpen(null)
                            setTimeout(() => setCurrentRow(null), 500)
                        }}
                        onSaved={() => queryClient.invalidateQueries({ queryKey: ['flow-tasks'] })}
                        existing={currentRow}
                    />

                    <ConfirmDialog
                        key='flow-task-delete'
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
                        title={`Delete this flow task: ${currentRow.name} ?`}
                        desc={
                            <>
                                You are about to delete the flow task{' '}
                                <strong>{currentRow.name}</strong>. <br />
                                This action cannot be undone and will permanently remove the flow task and all associated run history.
                            </>
                        }
                        confirmText='Delete'
                    />
                </>
            )}
        </>
    )
}
