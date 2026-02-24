import { cn } from '@/lib/utils'
import { List, Orbit } from 'lucide-react'
import { useRef, useEffect, useState } from 'react'

export type SmartTagView = 'list' | 'visualization'

interface FloatingViewToggleProps {
  activeView: SmartTagView
  onViewChange: (view: SmartTagView) => void
}

export function FloatingViewToggle({
  activeView,
  onViewChange,
}: FloatingViewToggleProps) {
  const listButtonRef = useRef<HTMLButtonElement>(null)
  const vizButtonRef = useRef<HTMLButtonElement>(null)
  const [sliderStyle, setSliderStyle] = useState<{
    left: number
    width: number
  }>({ left: 0, width: 0 })

  useEffect(() => {
    const updateSliderPosition = () => {
      const activeButton = activeView === 'list' ? listButtonRef.current : vizButtonRef.current
      if (activeButton) {
        const rect = activeButton.getBoundingClientRect()
        const parentRect = activeButton.parentElement?.getBoundingClientRect()
        if (parentRect) {
          setSliderStyle({
            left: rect.left - parentRect.left,
            width: rect.width,
          })
        }
      }
    }

    // Initial position
    updateSliderPosition()

    // Update on resize
    window.addEventListener('resize', updateSliderPosition)
    return () => window.removeEventListener('resize', updateSliderPosition)
  }, [activeView])

  return (
    <div className="fixed bottom-6 left-1/2 z-50 -translate-x-1/2">
      <div className="relative flex items-center gap-1 rounded-full border bg-background/80 p-1.5 shadow-lg backdrop-blur-md">
        {/* Sliding background indicator */}
        <div
          className="absolute top-1.5 h-[calc(100%-0.75rem)] rounded-full bg-primary shadow-sm transition-all duration-300 ease-out"
          style={{
            left: `${sliderStyle.left}px`,
            width: `${sliderStyle.width}px`,
          }}
        />
        
        <button
          ref={listButtonRef}
          onClick={() => onViewChange('list')}
          className={cn(
            'relative z-10 flex items-center gap-2 rounded-full px-4 py-2 text-sm font-medium transition-colors duration-200',
            activeView === 'list'
              ? 'text-primary-foreground'
              : 'text-muted-foreground hover:text-foreground'
          )}
        >
          <List className="h-4 w-4" />
          List Tags
        </button>
        <button
          ref={vizButtonRef}
          onClick={() => onViewChange('visualization')}
          className={cn(
            'relative z-10 flex items-center gap-2 rounded-full px-4 py-2 text-sm font-medium transition-colors duration-200',
            activeView === 'visualization'
              ? 'text-primary-foreground'
              : 'text-muted-foreground hover:text-foreground'
          )}
        >
          <Orbit className="h-4 w-4" />
          3D Visualization
        </button>
      </div>
    </div>
  )
}
