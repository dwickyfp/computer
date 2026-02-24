import { memo } from 'react'
import { type NodeProps } from '@xyflow/react'
import { Table2 } from 'lucide-react'
import { BaseNode } from './BaseNode'

export const PivotNode = memo(function PivotNode({ id, selected, data }: NodeProps) {
    const label = (data?.label as string) || 'Pivot'
    const pivotType = (data?.pivot_type as string) || 'PIVOT'
    const pivotCol = (data?.pivot_column as string) || ''

    return (
        <BaseNode
            id={id}
            selected={selected}
            accentColor="bg-pink-500"
            bgColor="bg-pink-50 dark:bg-pink-950/30"
            iconColor="text-pink-600"
            icon={<Table2 className="h-3.5 w-3.5" />}
            label={label}
            subtitle={`${pivotType}${pivotCol ? ` on ${pivotCol}` : ''}`}
        >
            {!pivotCol && (
                <span className="italic text-muted-foreground">Not configured</span>
            )}
        </BaseNode>
    )
})
