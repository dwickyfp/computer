import { Link } from '@tanstack/react-router'
import { type ReactNode } from 'react'
import { cn } from '@/lib/utils'

type DashboardPanelVariant = 'glass' | 'dense'

interface DashboardPanelProps {
  children: ReactNode
  className?: string
  contentClassName?: string
  headerAction?: ReactNode
  headerSlot?: ReactNode
  title?: ReactNode
  description?: ReactNode
  noPadding?: boolean
  interactive?: boolean
  href?: string
  variant?: DashboardPanelVariant
}

export function DashboardPanel({
  children,
  className,
  contentClassName,
  headerAction,
  headerSlot,
  title,
  description,
  noPadding = false,
  interactive = false,
  href,
  variant = 'glass',
}: DashboardPanelProps) {
  const slot = headerSlot ?? headerAction
  const isInteractive = interactive || Boolean(href)
  const panelClassName = cn(
    'dashboard-panel group flex min-h-0 flex-col rounded-[28px] text-card-foreground no-underline',
    className
  )

  const content = (
    <>
      {(title || description || slot) && (
        <div className='relative z-10 flex items-start justify-between gap-4 px-5 pt-5 sm:px-6 sm:pt-6'>
          <div className='min-w-0 space-y-1'>
            {title && (
              <h3 className='font-manrope text-base font-semibold leading-tight tracking-tight text-foreground'>
                {title}
              </h3>
            )}
            {description && (
              <p className='max-w-[44ch] text-sm leading-5 text-muted-foreground/85'>
                {description}
              </p>
            )}
          </div>
          {slot && <div className='relative z-10 shrink-0'>{slot}</div>}
        </div>
      )}

      <div
        className={cn(
          'relative z-10 flex min-h-0 flex-1 flex-col',
          !noPadding && 'px-5 pb-5 pt-4 sm:px-6 sm:pb-6 sm:pt-5',
          contentClassName
        )}
      >
        {children}
      </div>
    </>
  )

  if (href) {
    return (
      <Link
        to={href as never}
        preload='intent'
        className={panelClassName}
        data-interactive={isInteractive ? 'true' : 'false'}
        data-variant={variant}
      >
        {content}
      </Link>
    )
  }

  return (
    <div
      className={panelClassName}
      data-interactive={isInteractive ? 'true' : 'false'}
      data-variant={variant}
    >
      {content}
    </div>
  )
}
