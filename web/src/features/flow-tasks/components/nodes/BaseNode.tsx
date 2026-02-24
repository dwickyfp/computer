/**
 * Shared base node wrapper used by all Flow Task node types.
 * Left-to-right layout: target handle on the LEFT, source handle on the RIGHT.
 */

import { memo } from 'react'
import { Handle, Position } from '@xyflow/react'
import { cn } from '@/lib/utils'
import { Eye } from 'lucide-react'
import { Button } from '@/components/ui/button'

interface BaseNodeProps {
    id: string
    selected?: boolean
    accentColor: string          // tailwind border/bg class for the accent bar, e.g. "bg-blue-500"
    bgColor: string              // tailwind bg class for the card body
    iconColor: string            // tailwind text class, e.g. "text-blue-600"
    icon: React.ReactNode
    label: string
    subtitle?: string
    hasTarget?: boolean          // whether to show the left Handle
    hasSource?: boolean          // whether to show the right Handle
    onPreview?: () => void
    children?: React.ReactNode
}

export const BaseNode = memo(function BaseNode({
    selected,
    accentColor,
    bgColor,
    iconColor,
    icon,
    label,
    subtitle,
    hasTarget = true,
    hasSource = true,
    onPreview,
    children,
}: BaseNodeProps) {
    return (
        <div
            className={cn(
                'relative flex rounded-lg shadow-sm w-56 transition-all duration-150',
                bgColor,
                selected ? 'ring-2 ring-offset-1 ring-offset-background' : '',
                selected ? accentColor.replace('bg-', 'ring-') : '',
                'hover:shadow-md group'
            )}
        >
            {hasTarget && (
                <Handle
                    type="target"
                    position={Position.Left}
                    className="!w-3 !h-3 !border-2 !bg-background"
                />
            )}

            {/* Left accent bar */}
            <div className={cn('w-1 rounded-l-lg flex-shrink-0', accentColor)} />

            {/* Card content */}
            <div className="flex-1 min-w-0">
                {/* Header */}
                <div className="flex items-center gap-2 px-3 py-2 border-b border-border/50">
                    <span className={cn('flex-shrink-0', iconColor)}>{icon}</span>
                    <div className="flex-1 min-w-0">
                        <p className="text-xs font-semibold truncate">{label}</p>
                        {subtitle && (
                            <p className="text-[10px] text-muted-foreground truncate">{subtitle}</p>
                        )}
                    </div>
                    {onPreview && (
                        <Button
                            size="icon"
                            variant="ghost"
                            className="h-5 w-5 opacity-0 group-hover:opacity-100 transition-opacity"
                            onClick={(e) => {
                                e.stopPropagation()
                                onPreview()
                            }}
                            title="Preview node output"
                        >
                            <Eye className="h-3 w-3" />
                        </Button>
                    )}
                </div>

                {/* Body */}
                {children && (
                    <div className="px-3 py-2 text-[10px] text-muted-foreground space-y-1">
                        {children}
                    </div>
                )}
            </div>

            {hasSource && (
                <Handle
                    type="source"
                    position={Position.Right}
                    className="!w-3 !h-3 !border-2 !bg-background"
                />
            )}
        </div>
    )
})
