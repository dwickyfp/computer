/**
 * VersionHistoryDialog — lists graph versions for a flow task
 * and allows rollback to a previous version.
 */

import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
} from '@/components/ui/dialog'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { ScrollArea } from '@/components/ui/scroll-area'
import { ConfirmDialog } from '@/components/confirm-dialog'
import { flowTasksRepo, type FlowTaskGraphVersion } from '@/repo/flow-tasks'
import { toast } from 'sonner'
import { formatDistanceToNow } from 'date-fns'
import { RotateCcw, GitBranch } from 'lucide-react'

interface VersionHistoryDialogProps {
  open: boolean
  onOpenChange: (open: boolean) => void
  flowTaskId: number
}

export function VersionHistoryDialog({
  open,
  onOpenChange,
  flowTaskId,
}: VersionHistoryDialogProps) {
  const queryClient = useQueryClient()
  const [rollbackTarget, setRollbackTarget] =
    useState<FlowTaskGraphVersion | null>(null)

  const { data } = useQuery({
    queryKey: ['flow-task-versions', flowTaskId],
    queryFn: () => flowTasksRepo.listVersions(flowTaskId),
    enabled: open,
  })

  const rollbackMutation = useMutation({
    mutationFn: (version: number) =>
      flowTasksRepo.rollbackToVersion(flowTaskId, version),
    onSuccess: async () => {
      toast.success('Graph rolled back successfully')
      setRollbackTarget(null)
      onOpenChange(false)
      await new Promise((r) => setTimeout(r, 300))
      queryClient.invalidateQueries({
        queryKey: ['flow-task-graph', flowTaskId],
      })
      queryClient.invalidateQueries({
        queryKey: ['flow-task-versions', flowTaskId],
      })
    },
    onError: (err: Error) =>
      toast.error(err.message || 'Failed to rollback'),
  })

  const versions = data?.data?.items ?? []

  return (
    <>
      <Dialog open={open} onOpenChange={onOpenChange}>
        <DialogContent className='sm:max-w-lg'>
          <DialogHeader>
            <DialogTitle className='flex items-center gap-2'>
              <GitBranch className='h-4 w-4' />
              Version History
            </DialogTitle>
            <DialogDescription>
              Each save creates a snapshot. Rollback restores the graph to a
              previous version.
            </DialogDescription>
          </DialogHeader>
          <ScrollArea className='max-h-[400px]'>
            {versions.length === 0 ? (
              <p className='text-muted-foreground py-8 text-center text-sm'>
                No versions saved yet.
              </p>
            ) : (
              <div className='space-y-2'>
                {versions.map((ver, idx) => (
                  <div
                    key={ver.id}
                    className='flex items-center justify-between rounded-lg border p-3'
                  >
                    <div className='space-y-1'>
                      <div className='flex items-center gap-2'>
                        <Badge variant='outline'>v{ver.version}</Badge>
                        {idx === 0 && (
                          <Badge variant='secondary'>Latest</Badge>
                        )}
                      </div>
                      {ver.change_summary && (
                        <p className='text-xs text-muted-foreground'>
                          {ver.change_summary}
                        </p>
                      )}
                      <p className='text-[10px] text-muted-foreground'>
                        {ver.nodes_json.length} nodes ·{' '}
                        {ver.edges_json.length} edges ·{' '}
                        {formatDistanceToNow(new Date(ver.created_at), {
                          addSuffix: true,
                        })}
                      </p>
                    </div>
                    {idx > 0 && (
                      <Button
                        size='sm'
                        variant='outline'
                        onClick={() => setRollbackTarget(ver)}
                      >
                        <RotateCcw className='mr-1.5 h-3.5 w-3.5' />
                        Rollback
                      </Button>
                    )}
                  </div>
                ))}
              </div>
            )}
          </ScrollArea>
        </DialogContent>
      </Dialog>

      {rollbackTarget && (
        <ConfirmDialog
          open={!!rollbackTarget}
          onOpenChange={(v) => {
            if (!v) setRollbackTarget(null)
          }}
          title='Rollback Graph'
          desc={`Restore the graph to version ${rollbackTarget.version}? The current graph will be saved as a new version first.`}
          confirmText='Rollback'
          handleConfirm={() =>
            rollbackMutation.mutate(rollbackTarget.version)
          }
          isLoading={rollbackMutation.isPending}
        />
      )}
    </>
  )
}
