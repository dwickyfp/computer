import { memo } from 'react'
import { type NodeProps } from '@xyflow/react'
import { BarChart2 } from 'lucide-react'
import { BaseNode } from './BaseNode'

export const AggregateNode = memo(function AggregateNode({ id, selected, data }: NodeProps) {
    const label = (data?.label as string) || 'Aggregate'
    const groupBy = (data?.group_by as string[]) || []
    const aggs = (data?.aggregations as unknown[]) || []

    return (
        <BaseNode
            id={id}
            selected={selected}
            accentColor="bg-violet-500"
            bgColor="bg-violet-50 dark:bg-violet-950/30"
            iconColor="text-violet-600"
            icon={<BarChart2 className="h-3.5 w-3.5" />}
            label={label}
            subtitle={groupBy.length ? `Group by: ${groupBy.slice(0, 2).join(', ')}${groupBy.length > 2 ? '…' : ''}` : undefined}
        >
            {aggs.length > 0
                ? <span>{aggs.length} aggregation{aggs.length > 1 ? 's' : ''}</span>
                : <span className="italic text-muted-foreground">No aggregations</span>
            }
        </BaseNode>
    )
})
