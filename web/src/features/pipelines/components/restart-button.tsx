import { useState } from 'react'
import { RotateCw, Lock } from 'lucide-react'
import { cn } from '@/lib/utils'
import { toast } from 'sonner'

interface RestartButtonProps {
  onRestart: () => Promise<void>
  disabled?: boolean
}

export function RestartButton({ onRestart, disabled = false }: RestartButtonProps) {
  const [isRestarting, setIsRestarting] = useState(false)

  const handleClick = async () => {
    if (disabled || isRestarting) return

    setIsRestarting(true)
    try {
      await onRestart()
    } catch (error) {
      console.error('Restart failed:', error)
      toast.error('Failed to restart. Please try again.')
    } finally {
      // Keep animation running for a bit longer for visual feedback
      setTimeout(() => {
        setIsRestarting(false)
      }, 600)
    }
  }

  return (
    <div className="relative inline-flex items-center">
      <button
        onClick={handleClick}
        disabled={disabled || isRestarting}
        className={cn(
          'group relative inline-flex h-10 w-10 items-center justify-center overflow-hidden rounded-lg border-2 transition-all duration-300 focus:outline-none focus-visible:ring-2 focus-visible:ring-cyan-500/50 focus-visible:ring-offset-2 disabled:cursor-not-allowed disabled:opacity-50',
          isRestarting
            ? 'border-cyan-500 bg-cyan-500/10 text-cyan-600 shadow-md shadow-cyan-500/20 dark:text-cyan-400'
            : 'border-cyan-600 bg-transparent text-cyan-600 hover:border-cyan-500 hover:bg-cyan-500/5 hover:shadow-sm hover:shadow-cyan-500/10 dark:text-cyan-400'
        )}
      >
        {/* Subtle Particles Background */}
        {isRestarting && (
          <>
            <span className='absolute left-0 top-0 h-full w-full'>
              <span className='absolute left-[20%] top-[40%] h-1 w-1 animate-ping rounded-full bg-white/30' />
            </span>
            <span className='absolute left-0 top-0 h-full w-full'>
              <span
                className='absolute left-[70%] top-[55%] h-1 w-1 animate-ping rounded-full bg-white/30'
                style={{ animationDelay: '0.3s' }}
              />
            </span>
          </>
        )}

        {/* Icon with Rotation Animation */}
        <span className='relative flex items-center justify-center'>
          <RotateCw
            className={cn(
              'h-4 w-4 transition-all duration-500',
              isRestarting && 'animate-spin-slow'
            )}
          />
        </span>

        {/* Single Progress Bar */}
        {isRestarting && (
          <span className='absolute bottom-0 left-0 h-0.5 w-full overflow-hidden rounded-b-lg bg-cyan-900/20'>
            <span className='absolute left-0 top-0 h-full w-full origin-left animate-progress bg-gradient-to-r from-cyan-300 to-blue-300' />
          </span>
        )}
      </button>
      
      {/* Lock Icon when disabled */}
      {disabled && !isRestarting && (
        <div className="absolute -right-2 -top-2 rounded-full bg-red-500 p-1 shadow-sm dark:bg-red-600">
          <Lock className="h-2.5 w-2.5 text-white" />
        </div>
      )}
    </div>
  )
}
