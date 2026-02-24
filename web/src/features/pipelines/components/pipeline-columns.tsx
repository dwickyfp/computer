import { formatDistanceToNow } from 'date-fns'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { useNavigate } from '@tanstack/react-router'
import { type ColumnDef } from '@tanstack/react-table'
import { type Pipeline } from '@/repo/pipelines'
import { pipelinesRepo } from '@/repo/pipelines'
import { Workflow, FolderInput, FolderSync, AlertCircle } from 'lucide-react'
import { toast } from 'sonner'
import { cn } from '@/lib/utils'
import { Button } from '@/components/ui/button'
import { Switch } from '@/components/ui/switch'
import { PipelineAnimatedArrow } from './pipeline-animated-arrow.tsx'
import { PipelineRowActions } from './pipeline-row-actions.tsx'
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from '@/components/ui/tooltip'

export const pipelineColumns: ColumnDef<Pipeline>[] = [
  {
    id: 'details',
    header: () => <div className='text-center font-semibold'>Action</div>,
    cell: ({ row }) => (
      <div className='flex items-center justify-center space-x-2'>
        <PipelineStatusSwitch pipeline={row.original} />
        <PipelineDetailsButton pipelineId={row.original.id} />
      </div>
    ),
    meta: { title: 'Action' },
  },
  {
    accessorKey: 'name',
    header: 'Name',
    cell: ({ row }) => (
      <div className='w-[150px] font-medium'>{row.getValue('name')}</div>
    ),
    meta: { title: 'Name' },
  },
  {
    id: 'pipelines',
    header: 'Pipelines',
    cell: ({ row }) => {
      const sourceName = row.original.source?.name || 'Unknown Source'
      const destinations = row.original.destinations || []
      const destCount = destinations.length
      const firstDestName = destinations[0]?.destination?.name

      return (
        <div className='flex items-center gap-3'>
          {/* Source */}
          <div className='flex items-center justify-end gap-2'>
            <div className='flex flex-col items-end'>
              <span className='text-sm leading-none font-semibold'>
                {sourceName}
              </span>
              <span className='mt-0.5 text-[10px] font-medium tracking-wider text-muted-foreground uppercase'>
                Source
              </span>
            </div>
            <div className='rounded-md border border-blue-100 bg-blue-50 p-1.5 text-blue-600 dark:border-blue-500/20 dark:bg-blue-500/10 dark:text-blue-400'>
              <FolderInput className='h-3.5 w-3.5' />
            </div>
          </div>

          {/* Connection */}
          <div className='-mx-1 shrink-0'>
            <PipelineAnimatedArrow />
          </div>

          {/* Destination */}
          <div className='flex items-center gap-2'>
            <div className='rounded-md border border-purple-100 bg-purple-50 p-1.5 text-purple-600 dark:border-purple-500/20 dark:bg-purple-500/10 dark:text-purple-400'>
              <FolderSync className='h-3.5 w-3.5' />
            </div>
            <div className='flex flex-col'>
              {destCount > 1 ? (
                <span className='text-sm leading-none font-semibold'>
                  {destCount} Destinations
                </span>
              ) : (
                <span className='text-sm leading-none font-semibold'>
                  {firstDestName || 'No Destination'}
                </span>
              )}
              <span className='mt-0.5 text-[10px] font-medium tracking-wider text-muted-foreground uppercase'>
                Destination
              </span>
            </div>
          </div>
        </div>
      )
    },
    meta: { title: 'Pipelines' },
  },

  {
    accessorKey: 'status',
    header: 'Status',
    cell: ({ row }) => {
      const status = row.getValue('status') as string
      const isRunning = status === 'START'
      const isRefresh = status === 'REFRESH'
      const isPaused = status === 'PAUSE'

      // Determine display text and styling
      let displayText = status
      let dotColor = 'bg-gray-400 dark:bg-gray-500'
      let bgColor = 'bg-gray-50 dark:bg-gray-900/50'
      let textColor = 'text-gray-700 dark:text-gray-300'

      if (isRunning) {
        displayText = 'Running'
        dotColor = 'bg-green-500 dark:bg-green-400'
        bgColor = 'bg-green-50 dark:bg-green-950/50'
        textColor = 'text-green-700 dark:text-green-400'
      } else if (isRefresh) {
        displayText = 'Refreshing'
        dotColor = 'bg-blue-500 dark:bg-blue-400'
        bgColor = 'bg-blue-50 dark:bg-blue-950/50'
        textColor = 'text-blue-700 dark:text-blue-400'
      } else if (isPaused) {
        displayText = 'Paused'
        dotColor = 'bg-gray-400 dark:bg-gray-500'
        bgColor = 'bg-gray-50 dark:bg-gray-900/50'
        textColor = 'text-gray-600 dark:text-gray-400'
      }

      return (
        <div
          className={cn(
            'inline-flex items-center gap-2 rounded-md px-2.5 py-1 text-sm font-medium',
            bgColor,
            textColor
          )}
        >
          <span className={cn('h-2 w-2 rounded-full', dotColor)} />
          {displayText}
        </div>
      )
    },
    filterFn: (row, id, value) => {
      return value.includes(row.getValue(id))
    },
    meta: { title: 'Status' },
  },

  {
    accessorKey: 'last_refresh_at',
    header: 'Last Refresh',
    cell: ({ row }) => {
      const lastRefreshAt = row.original.last_refresh_at
      if (!lastRefreshAt) {
        return <div className='text-sm text-muted-foreground'>Never</div>
      }
      return (
        <div className='text-sm'>
          {formatDistanceToNow(new Date(lastRefreshAt), { addSuffix: true })}
        </div>
      )
    },
    meta: { title: 'Last Refresh' },
  },

  {
    id: 'actions',
    cell: ({ row }) => <PipelineRowActions row={row} />,
    meta: { title: 'Actions' },
  },
]

function PipelineDetailsButton({ pipelineId }: { pipelineId: number }) {
  const navigate = useNavigate()
  return (
    <Button
      variant='outline'
      size='icon'
      className='h-8 w-8 p-0'
      onClick={() =>
        navigate({
          to: '/pipelines/$pipelineId',
          params: { pipelineId: String(pipelineId) },
        })
      }
    >
      <Workflow className='h-4 w-4' />
    </Button>
  )
}

function PipelineStatusSwitch({ pipeline }: { pipeline: Pipeline }) {
  const queryClient = useQueryClient()
  const isRunning = pipeline.status === 'START' || pipeline.status === 'REFRESH'
  
  // Check if source has required configurations
  const isPublicationEnabled = pipeline.source?.is_publication_enabled ?? false
  const isReplicationEnabled = pipeline.source?.is_replication_enabled ?? false
  const canActivate = isPublicationEnabled && isReplicationEnabled

  const { mutate, isPending } = useMutation({
    mutationFn: async (checked: boolean) => {
      // Prevent activation if source requirements not met
      if (checked && !canActivate) {
        const missingRequirements = []
        if (!isPublicationEnabled) missingRequirements.push('publication')
        if (!isReplicationEnabled) missingRequirements.push('replication slot')
        
        toast.error(
          `Cannot activate pipeline: ${missingRequirements.join(' and ')} not configured on source database`,
          { duration: 4000 }
        )
        throw new Error('Source requirements not met')
      }
      
      if (checked) {
        return pipelinesRepo.start(pipeline.id)
      } else {
        return pipelinesRepo.pause(pipeline.id)
      }
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['pipelines'] })
      toast.success('Pipeline status updated')
    },
    onError: (error) => {
      toast.error(`Failed to update status: ${error}`)
    },
  })

  const switchElement = (
    <div className="flex items-center gap-2">
      <Switch
        checked={isRunning}
        onCheckedChange={(checked) => mutate(checked)}
        disabled={isPending || (!isRunning && !canActivate)}
      />
      {!isRunning && !canActivate && (
        <AlertCircle className='h-3.5 w-3.5 text-red-500 dark:text-red-400' />
      )}
    </div>
  )
  
  // Wrap with tooltip if requirements not met
  if (!isRunning && !canActivate) {
    const missingRequirements = []
    if (!isPublicationEnabled) missingRequirements.push('Publication')
    if (!isReplicationEnabled) missingRequirements.push('Replication slot')
    
    return (
      <TooltipProvider>
        <Tooltip>
          <TooltipTrigger asChild>
            {switchElement}
          </TooltipTrigger>
          <TooltipContent>
            <p className='font-semibold text-xs'>Cannot activate</p>
            <p className='text-xs'>Missing: {missingRequirements.join(', ')}</p>
          </TooltipContent>
        </Tooltip>
      </TooltipProvider>
    )
  }
  
  return switchElement
}
