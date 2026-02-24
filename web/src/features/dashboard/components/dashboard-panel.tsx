import { cn } from '@/lib/utils'
import { type ReactNode } from 'react'

interface DashboardPanelProps {
    children: ReactNode
    className?: string
    headerAction?: ReactNode
    title?: ReactNode
    description?: ReactNode
    noPadding?: boolean
}

export function DashboardPanel({
    children,
    className,
    headerAction,
    title,
    description,
    noPadding = false,
}: DashboardPanelProps) {
    return (
        <div
            className={cn(
                'group relative flex flex-col overflow-hidden rounded-sm border bg-card/50 text-card-foreground shadow-sm transition-all hover:border-primary/50 hover:shadow-md',
                className
            )}
        >
            {(title || headerAction) && (
                <div className='flex items-center justify-between border-b border-border/50 px-4 py-3'>
                    <div className='flex flex-col gap-0.5'>
                        {title && (
                            <h3 className='font-semibold leading-none tracking-tight text-sm'>
                                {title}
                            </h3>
                        )}
                        {description && (
                            <p className='text-[10px] text-muted-foreground uppercase tracking-wider font-medium'>
                                {description}
                            </p>
                        )}
                    </div>
                    {headerAction && <div className='flex items-center'>{headerAction}</div>}
                </div>
            )}
            <div className={cn('flex-1', !noPadding && 'p-4')}>{children}</div>
        </div>
    )
}
