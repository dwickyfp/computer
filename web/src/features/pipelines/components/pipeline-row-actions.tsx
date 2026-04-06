import { useState } from 'react'
import { useQueryClient, useMutation } from '@tanstack/react-query'
import { type Row } from '@tanstack/react-table'
import { type Pipeline, pipelinesRepo } from '@/repo/pipelines'
import { MoreHorizontal, Lock, Pencil } from 'lucide-react'
import { toast } from 'sonner'
import { getApiErrorMessage } from '@/lib/handle-server-error'
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
import { Button } from '@/components/ui/button'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuShortcut,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'

interface DataTableRowActionsProps<TData> {
  row: Row<TData>
}

export function PipelineRowActions<TData>({
  row,
}: DataTableRowActionsProps<TData>) {
  const pipeline = row.original as Pipeline
  const queryClient = useQueryClient()
  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false)
  const [renameDialogOpen, setRenameDialogOpen] = useState(false)
  const [newName, setNewName] = useState('')

  const { mutate: deleteMutate, isPending: isDeleting } = useMutation({
    mutationFn: pipelinesRepo.delete,
    onSuccess: () => {
      toast.success('Pipeline deleted')
      setDeleteDialogOpen(false)

      // Manually remove from cache to ensure immediate UI update and avoid race conditions
      queryClient.setQueryData(['pipelines'], (old: any) => {
        if (!old) return old
        return {
          ...old,
          pipelines: old.pipelines.filter(
            (p: Pipeline) => p.id !== pipeline.id
          ),
          total: Math.max(0, old.total - 1),
        }
      })

      // We do NOT invalidate queries immediately here because the backend might still return the deleted item
      // due to eventual consistency or race conditions.
      // The manual cache update above is sufficient for the UI.
      // queryClient.invalidateQueries({ queryKey: ['pipelines'] })
    },
  })

  const { mutate: refreshMutate } = useMutation({
    mutationFn: pipelinesRepo.refresh,
    onSuccess: () => {
      setTimeout(() => {
        queryClient.invalidateQueries({ queryKey: ['pipelines'] })
      }, 3000)
      toast.success('Pipeline refresh queued')
    },
    onError: () => {
      toast.error('Failed to refresh pipeline')
    },
  })

  const { mutate: renameMutate, isPending: isRenaming } = useMutation({
    mutationFn: (name: string) => pipelinesRepo.rename(pipeline.id, name),
    onSuccess: async () => {
      setRenameDialogOpen(false)
      toast.success('Pipeline renamed')
      await new Promise((r) => setTimeout(r, 300))
      queryClient.invalidateQueries({ queryKey: ['pipelines'] })
    },
    onError: (error) => {
      toast.error(getApiErrorMessage(error, 'Failed to rename pipeline'))
    },
  })

  const handleRenameOpen = () => {
    setNewName(pipeline.name)
    setRenameDialogOpen(true)
  }

  return (
    <>
      <DropdownMenu>
        <DropdownMenuTrigger asChild>
          <Button
            variant='ghost'
            className='flex h-8 w-8 p-0 data-[state=open]:bg-muted'
          >
            <MoreHorizontal className='h-4 w-4' />
            <span className='sr-only'>Open menu</span>
          </Button>
        </DropdownMenuTrigger>
        <DropdownMenuContent align='end' className='w-[160px]'>
          <DropdownMenuItem onClick={handleRenameOpen}>
            <Pencil className='mr-2 h-3.5 w-3.5' />
            Rename
          </DropdownMenuItem>
          <DropdownMenuItem
            onClick={() => refreshMutate(pipeline.id)}
            disabled={pipeline.status === 'PAUSE'}
            className='flex items-center justify-between'
          >
            <span>Refresh</span>
            {pipeline.status === 'PAUSE' && (
              <Lock className='h-3 w-3 text-red-500' />
            )}
          </DropdownMenuItem>
          <DropdownMenuSeparator />
          <DropdownMenuItem onClick={() => setDeleteDialogOpen(true)}>
            Delete
            <DropdownMenuShortcut>⌘⌫</DropdownMenuShortcut>
          </DropdownMenuItem>
        </DropdownMenuContent>
      </DropdownMenu>

      {/* Rename Dialog */}
      <Dialog open={renameDialogOpen} onOpenChange={setRenameDialogOpen}>
        <DialogContent className='sm:max-w-[400px]'>
          <DialogHeader>
            <DialogTitle>Rename Pipeline</DialogTitle>
            <DialogDescription>
              Enter a new name for the pipeline. Only lowercase letters,
              numbers, hyphens, and underscores are allowed.
            </DialogDescription>
          </DialogHeader>
          <div className='space-y-2 py-2'>
            <Label htmlFor='pipeline-name'>Name</Label>
            <Input
              id='pipeline-name'
              value={newName}
              onChange={(e) => setNewName(e.target.value.toLowerCase())}
              placeholder={pipeline.name}
              onKeyDown={(e) => {
                if (
                  e.key === 'Enter' &&
                  newName.trim() &&
                  newName !== pipeline.name
                ) {
                  renameMutate(newName.trim())
                }
              }}
            />
          </div>
          <DialogFooter>
            <Button
              variant='outline'
              onClick={() => setRenameDialogOpen(false)}
            >
              Cancel
            </Button>
            <Button
              onClick={() => renameMutate(newName.trim())}
              disabled={
                isRenaming ||
                !newName.trim() ||
                newName.trim() === pipeline.name
              }
            >
              {isRenaming ? 'Renaming...' : 'Rename'}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Delete Confirmation Modal */}
      <AlertDialog open={deleteDialogOpen} onOpenChange={setDeleteDialogOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete Pipeline</AlertDialogTitle>
            <AlertDialogDescription>
              Are you sure you want to delete{' '}
              <span className='font-medium text-foreground'>
                {pipeline.name}
              </span>{' '}
              ? This will remove all associated data and cannot be undone.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction
              onClick={(e) => {
                e.preventDefault()
                deleteMutate(pipeline.id)
              }}
              disabled={isDeleting}
              className='bg-destructive text-white hover:bg-destructive/90'
            >
              {isDeleting ? 'Deleting...' : 'Delete'}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </>
  )
}
