import { cn } from '@/lib/utils'

interface AnimatedSeparatorProps {
  className?: string
}

export function AnimatedSeparator({ className }: AnimatedSeparatorProps) {
  return (
    <div className={cn('relative h-px w-full overflow-hidden bg-border', className)}>
      {/* Base line */}
      <div className="absolute inset-0 bg-border" />
      
      {/* Outer glow layer - creates the shiny effect around the lightning */}
      <div 
        className="absolute -inset-y-3 w-64 animate-lightning blur-[6px]"
        style={{
          background: `linear-gradient(90deg, 
            transparent 0%, 
            rgba(81, 50, 152, 0) 5%, 
            rgba(81, 50, 152, 0.5) 30%, 
            rgba(81, 50, 152, 0.8) 50%, 
            rgba(81, 50, 152, 0.5) 70%, 
            rgba(81, 50, 152, 0) 95%, 
            transparent 100%
          )`,
        }}
      />
      
      {/* Inner bright core - the main lightning bolt */}
      <div 
        className="absolute -inset-y-1.5 w-64 animate-lightning blur-[1px]"
        style={{
          background: `linear-gradient(90deg, 
            transparent 0%, 
            rgba(81, 50, 152, 0) 10%, 
            rgba(81, 50, 152, 0.9) 35%, 
            rgba(81, 50, 152, 1) 50%, 
            rgba(81, 50, 152, 0.9) 65%, 
            rgba(81, 50, 152, 0) 90%, 
            transparent 100%
          )`,
        }}
      />

      <style>{`
        @keyframes lightning {
          0% {
            left: -8rem;
            opacity: 0;
          }
          10% {
            opacity: 1;
          }
          90% {
            opacity: 1;
          }
          100% {
            left: 100%;
            opacity: 0;
          }
        }

        .animate-lightning {
          animation: lightning 3s ease-in-out infinite;
        }
      `}</style>
    </div>
  )
}
