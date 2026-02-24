import { useMutation, useQueryClient } from '@tanstack/react-query'
import { schedulesRepo } from '@/repo/schedules'
import { toast } from 'sonner'
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from '@/components/ui/alert-dialog'
import { useSchedules } from './schedules-provider'

export function SchedulesDialogs() {
  const { open, setOpen, currentRow, setCurrentRow } = useSchedules()
  const queryClient = useQueryClient()

  function closeAll() {
    setOpen(null)
    setTimeout(() => setCurrentRow(null), 300)
  }

  // ── Delete ─────────────────────────────────────────────────────────────

  const deleteMutation = useMutation({
    mutationFn: (id: number) => schedulesRepo.delete(id),
    onSuccess: async () => {
      closeAll()
      toast.success('Schedule deleted')
      await new Promise((resolve) => setTimeout(resolve, 300))
      queryClient.invalidateQueries({
        queryKey: ['schedules'],
        refetchType: 'active',
      })
    },
    onError: () => toast.error('Failed to delete schedule'),
  })

  // ── Pause ──────────────────────────────────────────────────────────────

  const pauseMutation = useMutation({
    mutationFn: (id: number) => schedulesRepo.pause(id),
    onSuccess: async () => {
      closeAll()
      toast.success('Schedule paused')
      await new Promise((resolve) => setTimeout(resolve, 300))
      queryClient.invalidateQueries({
        queryKey: ['schedules'],
        refetchType: 'active',
      })
    },
    onError: () => toast.error('Failed to pause schedule'),
  })

  // ── Resume ─────────────────────────────────────────────────────────────

  const resumeMutation = useMutation({
    mutationFn: (id: number) => schedulesRepo.resume(id),
    onSuccess: async () => {
      closeAll()
      toast.success('Schedule resumed')
      await new Promise((resolve) => setTimeout(resolve, 300))
      queryClient.invalidateQueries({
        queryKey: ['schedules'],
        refetchType: 'active',
      })
    },
    onError: () => toast.error('Failed to resume schedule'),
  })

  return (
    <>
      {/* Delete */}
      <AlertDialog
        open={open === 'delete'}
        onOpenChange={(v) => !v && closeAll()}
      >
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete Schedule</AlertDialogTitle>
            <AlertDialogDescription>
              Are you sure you want to delete{' '}
              <span className='font-semibold'>{currentRow?.name}</span>? This
              will permanently remove the schedule and all its run history.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction
              className='text-destructive-foreground bg-destructive hover:bg-destructive/90'
              onClick={() => currentRow && deleteMutation.mutate(currentRow.id)}
              disabled={deleteMutation.isPending}
            >
              {deleteMutation.isPending ? 'Deleting…' : 'Delete'}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      {/* Pause */}
      <AlertDialog
        open={open === 'pause'}
        onOpenChange={(v) => !v && closeAll()}
      >
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Pause Schedule</AlertDialogTitle>
            <AlertDialogDescription>
              Pausing <span className='font-semibold'>{currentRow?.name}</span>{' '}
              will stop future cron executions. You can resume it at any time.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction
              onClick={() => currentRow && pauseMutation.mutate(currentRow.id)}
              disabled={pauseMutation.isPending}
            >
              {pauseMutation.isPending ? 'Pausing…' : 'Pause'}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      {/* Resume */}
      <AlertDialog
        open={open === 'resume'}
        onOpenChange={(v) => !v && closeAll()}
      >
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Resume Schedule</AlertDialogTitle>
            <AlertDialogDescription>
              Resuming <span className='font-semibold'>{currentRow?.name}</span>{' '}
              will re-register it in the scheduler on the current cron
              expression.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction
              onClick={() => currentRow && resumeMutation.mutate(currentRow.id)}
              disabled={resumeMutation.isPending}
            >
              {resumeMutation.isPending ? 'Resuming…' : 'Resume'}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </>
  )
}
