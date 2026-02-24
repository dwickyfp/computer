import { useMutation, useQueryClient } from '@tanstack/react-query'
import { toast } from 'sonner'
import { Play, Pause, AlertCircle } from 'lucide-react'
import { cn } from '@/lib/utils'
import { pipelinesRepo, type Pipeline } from '@/repo/pipelines'
import { useState } from 'react'
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from '@/components/ui/tooltip'

interface PipelineStatusSwitchProps {
  pipeline: Pipeline
}

export function PipelineStatusSwitch({ pipeline }: PipelineStatusSwitchProps) {
  const queryClient = useQueryClient()
  const isRunning = pipeline.status === 'START' || pipeline.status === 'REFRESH'
  
  // Check if source has required configurations
  const isPublicationEnabled = pipeline.source?.is_publication_enabled ?? false
  const isReplicationEnabled = pipeline.source?.is_replication_enabled ?? false
  const canActivate = isPublicationEnabled && isReplicationEnabled
  
  // Optimistic state for immediate UI feedback
  const [optimisticState, setOptimisticState] = useState<boolean | null>(null)
  const displayState = optimisticState !== null ? optimisticState : isRunning

  const { mutate } = useMutation({
    mutationFn: async (checked: boolean) => {
      if (checked) {
        return pipelinesRepo.start(pipeline.id)
      } else {
        return pipelinesRepo.pause(pipeline.id)
      }
    },
    onSuccess: async () => {
      // Invalidate queries to get fresh data
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ['pipelines'] }),
        queryClient.invalidateQueries({ queryKey: ['pipeline', pipeline.id] })
      ])
      
      // Wait a bit for the fresh data to arrive and be processed
      setTimeout(() => {
        // Clear optimistic state after data is fully synced
        setOptimisticState(null)
      }, 500)
      
      toast.success(
        `Pipeline ${displayState ? 'started' : 'paused'} successfully`
      )
    },
    onError: (error) => {
      // Revert to actual state on error
      setOptimisticState(null)
      toast.error(`Failed to update status: ${error}`)
    },
  })

  const handleToggle = async () => {
    const newState = !displayState
    
    // Prevent activation if source requirements not met
    if (newState && !canActivate) {
      const missingRequirements = []
      if (!isPublicationEnabled) missingRequirements.push('publication')
      if (!isReplicationEnabled) missingRequirements.push('replication slot')
      
      toast.error(
        `Cannot activate pipeline: ${missingRequirements.join(' and ')} not configured on source database`,
        { duration: 4000 }
      )
      return
    }
    
    // Cancel any outgoing refetches
    await queryClient.cancelQueries({ queryKey: ['pipeline', pipeline.id] })
    
    // Set optimistic state for immediate animation
    setOptimisticState(newState)
    
    // Wait for animation to complete (200ms)
    await new Promise(resolve => setTimeout(resolve, 200))
    
    // Then send API request
    mutate(newState)
  }

  const switchButton = (
    <div className='flex items-center gap-2'>
      {/* Status Label */}
      <span
        className={cn(
          'text-xs font-semibold tracking-wide transition-colors duration-200',
          displayState
            ? 'text-emerald-600 dark:text-emerald-400'
            : 'text-slate-500 dark:text-slate-400'
        )}
      >
        {displayState ? 'Running' : 'Paused'}
      </span>

      {/* Custom Animated Switch */}
      <button
        onClick={handleToggle}
        disabled={!displayState && !canActivate}
        className={cn(
          'group relative inline-flex h-7 w-14 items-center rounded-full transition-all duration-200 ease-out focus:outline-none focus-visible:ring-2 focus-visible:ring-offset-1',
          displayState
            ? 'bg-gradient-to-r from-emerald-500 to-green-500 shadow-md shadow-emerald-500/30 hover:shadow-emerald-500/40 focus-visible:ring-emerald-500 dark:from-emerald-600 dark:to-green-600'
            : !canActivate
            ? 'bg-gradient-to-r from-red-300 to-red-400 shadow-sm shadow-red-400/20 focus-visible:ring-red-400 dark:from-red-600 dark:to-red-700 cursor-not-allowed opacity-60'
            : 'bg-gradient-to-r from-slate-300 to-slate-400 shadow-sm shadow-slate-400/20 hover:shadow-slate-400/30 focus-visible:ring-slate-400 dark:from-slate-600 dark:to-slate-700'
        )}
        aria-label={displayState ? 'Pause pipeline' : 'Start pipeline'}
      >
        {/* Animated Background Pulse */}
        {displayState && (
          <span className='absolute inset-0 animate-pulse rounded-full bg-gradient-to-r from-emerald-400/50 to-green-400/50 blur-sm' />
        )}

        {/* Toggle Circle/Slider */}
        <span
          className={cn(
            'relative inline-flex h-5 w-5 transform items-center justify-center rounded-full bg-white shadow-md transition-all duration-200 ease-out dark:bg-slate-900',
            displayState ? 'translate-x-8' : 'translate-x-1',
            'group-hover:scale-105'
          )}
        >
          {displayState ? (
            <Play
              className='h-3 w-3 text-emerald-600 transition-transform duration-200 group-hover:scale-110 dark:text-emerald-400'
              fill='currentColor'
            />
          ) : (
            <Pause
              className='h-3 w-3 text-slate-600 transition-transform duration-200 group-hover:scale-110 dark:text-slate-400'
              fill='currentColor'
            />
          )}
        </span>

        {/* Background Icons (subtle) */}
        <span className='absolute inset-0 flex items-center justify-between px-1.5 opacity-40'>
          <Pause
            className={cn(
              'h-2.5 w-2.5 text-white transition-all duration-200',
              !displayState
                ? 'translate-x-0 opacity-100'
                : '-translate-x-2 opacity-0'
            )}
          />
          <Play
            className={cn(
              'h-2.5 w-2.5 text-white transition-all duration-200',
              displayState
                ? 'translate-x-0 opacity-100'
                : 'translate-x-2 opacity-0'
            )}
          />
        </span>
      </button>

      {/* Warning Icon for Invalid State */}
      {!displayState && !canActivate && (
        <AlertCircle className='h-4 w-4 text-red-500 dark:text-red-400' />
      )}

      {/* Status Indicator Dot */}
      <div className='flex items-center'>
        <span className='relative flex h-2 w-2 transition-all duration-200'>
          {displayState && (
            <span className='absolute inline-flex h-full w-full animate-ping rounded-full bg-emerald-400 opacity-75' />
          )}
          <span
            className={cn(
              'relative inline-flex h-2 w-2 rounded-full transition-colors duration-200',
              displayState
                ? 'bg-emerald-500 shadow-md shadow-emerald-500/50'
                : 'bg-slate-400 dark:bg-slate-600'
            )}
          />
        </span>
      </div>
    </div>
  )

  // Wrap with tooltip if requirements not met
  if (!displayState && !canActivate) {
    const missingRequirements = []
    if (!isPublicationEnabled) missingRequirements.push('Publication')
    if (!isReplicationEnabled) missingRequirements.push('Replication slot')
    
    return (
      <TooltipProvider>
        <Tooltip>
          <TooltipTrigger asChild>
            {switchButton}
          </TooltipTrigger>
          <TooltipContent>
            <p className='font-semibold'>Cannot activate pipeline</p>
            <p className='text-xs'>Missing: {missingRequirements.join(', ')}</p>
            <p className='text-xs text-muted-foreground'>Configure source database first</p>
          </TooltipContent>
        </Tooltip>
      </TooltipProvider>
    )
  }

  return switchButton
}
