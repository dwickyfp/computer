import { cn } from '@/lib/utils'
import { type ReactNode } from 'react'

interface DashboardGridProps {
    children: ReactNode
    className?: string
}

export function DashboardGrid({ children, className }: DashboardGridProps) {
    return (
        <div
            className={cn(
                'grid auto-rows-min items-start gap-4 md:grid-cols-2 xl:grid-cols-12 xl:gap-5',
                className
            )}
        >
            {children}
        </div>
    )
}
