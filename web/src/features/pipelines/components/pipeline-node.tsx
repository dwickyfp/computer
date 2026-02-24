import { useMutation, useQueryClient } from '@tanstack/react-query'
import { pipelinesRepo } from '@/repo/pipelines'
import { Handle, Position, type NodeProps, type Node } from '@xyflow/react'
import { Database, Server, Trash, Info } from 'lucide-react'
import { toast } from 'sonner'
import { cn } from '@/lib/utils'
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
  AlertDialogTrigger,
} from '@/components/ui/alert-dialog'
import { Button } from '@/components/ui/button'
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from '@/components/ui/popover'

export interface PipelineNodeData extends Record<string, unknown> {
  label: string
  type: string
  isSource?: boolean
  status?: string
  pipelineId?: number
  destinationId?: number
  isError?: boolean
  errorMessage?: string
  errorCount?: number
  isHighlighted?: boolean
}

export function PipelineNode({ data }: NodeProps<Node<PipelineNodeData>>) {
  const isSource = data.isSource
  const isError = data.isError
  const isHighlighted = data.isHighlighted
  const queryClient = useQueryClient()

  const { mutate: deleteDestination, isPending: isDeleting } = useMutation({
    mutationFn: async () => {
      if (!data.pipelineId || !data.destinationId) return
      return pipelinesRepo.removeDestination(
        data.pipelineId,
        data.destinationId
      )
    },
    onSuccess: () => {
      toast.success('Destination removed')
      queryClient.invalidateQueries({ queryKey: ['pipelines'] })
      queryClient.invalidateQueries({ queryKey: ['pipeline', data.pipelineId] })
    },
    onError: (error) => {
      toast.error(`Failed to remove destination: ${error}`)
    },
  })

  return (
    <div
      className={cn(
        'group relative min-w-50 rounded-xl border-2 bg-background transition-all hover:shadow-lg dark:bg-[#1d252f]',
        isSource
          ? 'border-blue-500/50 shadow-blue-500/20'
          : isError
            ? 'animate-error-pulse border-red-500 shadow-red-500/30'
            : isHighlighted
              ? 'animate-bounce border-yellow-500 shadow-yellow-500/50 ring-2 ring-yellow-400 ring-offset-2'
              : 'border-emerald-500/50 shadow-emerald-500/20'
      )}
    >
      {/* Error glow effect */}
      {isError && !isSource && (
        <div className='pointer-events-none absolute inset-0 animate-pulse rounded-xl bg-red-500/10' />
      )}

      {/* Header with Gradient */}
      <div
        className={cn(
          'flex items-center justify-between rounded-t-[10px] px-4 py-3 text-white',
          isSource
            ? 'bg-linear-to-r from-blue-600 to-indigo-600'
            : isError
              ? 'bg-linear-to-r from-red-600 to-rose-600'
              : isHighlighted
                ? 'bg-linear-to-r from-yellow-500 to-orange-500'
                : 'bg-linear-to-r from-emerald-600 to-teal-600'
        )}
      >
        <div className='flex items-center gap-2 overflow-hidden'>
          <Database className='h-4 w-4 shrink-0' />
          <span className='truncate text-sm font-semibold' title={data.label}>
            {data.label}
          </span>
        </div>

        {!isSource && (
          <div className='flex items-center gap-1'>
            {/* Error Info Popover */}
            {isError && data.errorMessage && (
              <Popover>
                <PopoverTrigger asChild>
                  <Button
                    variant='ghost'
                    size='icon'
                    className='h-6 w-6 text-white/70 hover:bg-white/20 hover:text-white'
                    onClick={(e) => e.stopPropagation()}
                  >
                    <Info className='h-3 w-3' />
                  </Button>
                </PopoverTrigger>
                <PopoverContent
                  className='max-h-60 w-80 overflow-auto'
                  onClick={(e) => e.stopPropagation()}
                >
                  <div className='space-y-2'>
                    <h4 className='flex items-center gap-2 font-medium text-destructive'>
                      <Info className='h-4 w-4' />
                      Error Details
                    </h4>
                    <p className='text-sm wrap-break-word whitespace-pre-wrap text-muted-foreground'>
                      {data.errorMessage}
                    </p>
                  </div>
                </PopoverContent>
              </Popover>
            )}

            {/* Delete Button */}
            <AlertDialog>
              <AlertDialogTrigger asChild>
                <Button
                  variant='ghost'
                  size='icon'
                  className='-mr-2 h-6 w-6 text-white/70 hover:bg-white/20 hover:text-white'
                  onClick={(e) => e.stopPropagation()}
                  disabled={isDeleting}
                >
                  <Trash className='h-3 w-3' />
                </Button>
              </AlertDialogTrigger>
              <AlertDialogContent onClick={(e) => e.stopPropagation()}>
                <AlertDialogHeader>
                  <AlertDialogTitle>Delete Destination</AlertDialogTitle>
                  <AlertDialogDescription>
                    Are you sure you want to remove this destination from the
                    pipeline? This action cannot be undone.
                  </AlertDialogDescription>
                </AlertDialogHeader>
                <AlertDialogFooter>
                  <AlertDialogCancel onClick={(e) => e.stopPropagation()}>
                    Cancel
                  </AlertDialogCancel>
                  <AlertDialogAction
                    className='bg-destructive text-white hover:bg-destructive/90'
                    onClick={(e) => {
                      e.stopPropagation()
                      deleteDestination()
                    }}
                  >
                    Delete
                  </AlertDialogAction>
                </AlertDialogFooter>
              </AlertDialogContent>
            </AlertDialog>
          </div>
        )}
      </div>

      {/* Body */}
      <div className='p-4'>
        <div className='flex items-center justify-between text-xs text-muted-foreground'>
          <div className='flex items-center gap-1.5'>
            <Server className='h-3 w-3' />
            <span className='font-medium tracking-wider uppercase'>
              {data.type}
            </span>
            {data.errorCount !== undefined && data.errorCount > 0 && (
              <div className='ml-2 flex items-center gap-1.5 rounded-full border border-red-200 bg-red-100 px-2 py-0.5 text-red-600'>
                <span className='text-[10px] font-bold'>{data.errorCount}</span>
                <span className='text-[10px] font-medium'>Error</span>
              </div>
            )}
          </div>
          {data.status && (
            <div className='flex items-center gap-1.5'>
              <span
                className={cn(
                  'h-2 w-2 rounded-full',
                  isError
                    ? 'animate-pulse bg-red-500'
                    : data.status === 'START'
                      ? 'bg-green-500'
                      : 'bg-yellow-500'
                )}
              />
              <span>{isError ? 'ERROR' : data.status}</span>
            </div>
          )}
        </div>
      </div>

      {/* Handles */}
      {isSource ? (
        <Handle
          type='source'
          position={Position.Right}
          className='-right-2! h-3! w-3! border-2 border-white bg-blue-600!'
        />
      ) : (
        <Handle
          type='target'
          position={Position.Left}
          className={cn(
            '-left-2! h-3! w-3! border-2 border-white',
            isError ? 'bg-red-600!' : 'bg-emerald-600!'
          )}
        />
      )}
    </div>
  )
}
