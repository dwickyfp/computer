import { memo } from 'react'
import { type NodeProps } from '@xyflow/react'
import { PlusCircle } from 'lucide-react'
import { BaseNode } from './BaseNode'

export const NewRowsNode = memo(function NewRowsNode({ id, selected, data }: NodeProps) {
    const label = (data?.label as string) || 'New Rows'
    const columns = (data?.columns as unknown[]) || []
    const count = columns.length

    return (
        <BaseNode
            id={id}
            selected={selected}
            accentColor="bg-amber-500"
            bgColor="bg-amber-50 dark:bg-amber-950/30"
            iconColor="text-amber-600"
            icon={<PlusCircle className="h-3.5 w-3.5" />}
            label={label}
            subtitle={count > 0 ? `${count} column${count > 1 ? 's' : ''} added` : undefined}
        >
            {count === 0 && (
                <span className="italic text-muted-foreground">No columns defined</span>
            )}
        </BaseNode>
    )
})
