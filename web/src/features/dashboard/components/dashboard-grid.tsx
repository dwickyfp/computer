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
                'grid auto-rows-min gap-2 md:grid-cols-2 lg:grid-cols-12 xl:grid-cols-24',
                className
            )}
        >
            {children}
        </div>
    )
}
