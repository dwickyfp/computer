import { MoreHorizontal, Lock } from 'lucide-react'
import { type Row } from '@tanstack/react-table'
import { Button } from '@/components/ui/button'
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuShortcut,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu'
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
import { type Pipeline, pipelinesRepo } from '@/repo/pipelines'
import { useQueryClient, useMutation } from '@tanstack/react-query'
import { toast } from 'sonner'
import { useState } from 'react'



interface DataTableRowActionsProps<TData> {
  row: Row<TData>
}

export function PipelineRowActions<TData>({ row }: DataTableRowActionsProps<TData>) {
  const pipeline = row.original as Pipeline
  const queryClient = useQueryClient()
  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false)

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
          pipelines: old.pipelines.filter((p: Pipeline) => p.id !== pipeline.id),
          total: Math.max(0, old.total - 1)
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
      queryClient.invalidateQueries({ queryKey: ['pipelines'] })
      toast.success('Pipeline refreshed')
    },
    onError: () => {
      toast.error('Failed to refresh pipeline')
    }
  })

  return (
    <>
      <DropdownMenu>
        <DropdownMenuTrigger asChild>
          <Button variant='ghost' className='flex h-8 w-8 p-0 data-[state=open]:bg-muted'>
            <MoreHorizontal className='h-4 w-4' />
            <span className='sr-only'>Open menu</span>
          </Button>
        </DropdownMenuTrigger>
        <DropdownMenuContent align='end' className='w-[160px]'>

          <DropdownMenuItem 
            onClick={() => refreshMutate(pipeline.id)}
            disabled={pipeline.status === 'PAUSE'}
            className="flex items-center justify-between"
          >
            <span>Refresh</span>
            {pipeline.status === 'PAUSE' && (
              <Lock className="h-3 w-3 text-red-500" />
            )}
          </DropdownMenuItem>
          <DropdownMenuSeparator />
          <DropdownMenuItem onClick={() => setDeleteDialogOpen(true)}>
            Delete
            <DropdownMenuShortcut>⌘⌫</DropdownMenuShortcut>
          </DropdownMenuItem>
        </DropdownMenuContent>
      </DropdownMenu>

      {/* Delete Confirmation Modal */}
      <AlertDialog open={deleteDialogOpen} onOpenChange={setDeleteDialogOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete Pipeline</AlertDialogTitle>
            <AlertDialogDescription>
              Are you sure you want to delete {' '}
              <span className="font-medium text-foreground">
                {pipeline.name}
              </span>
              {' '}? This will remove all associated data and cannot be undone.
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
              className="bg-destructive text-white hover:bg-destructive/90"
            >
              {isDeleting ? 'Deleting...' : 'Delete'}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </>
  )
}

