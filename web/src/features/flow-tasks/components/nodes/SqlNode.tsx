import { memo } from 'react'
import { type NodeProps } from '@xyflow/react'
import { Code2 } from 'lucide-react'
import { BaseNode } from './BaseNode'

export const SqlNode = memo(function SqlNode({ id, selected, data }: NodeProps) {
    const label = (data?.label as string) || 'SQL'
    const expr = (data?.sql_expression as string) || ''
    const preview = expr
        ? expr.length > 60
            ? expr.slice(0, 60) + '…'
            : expr
        : undefined

    return (
        <BaseNode
            id={id}
            selected={selected}
            accentColor="bg-indigo-500"
            bgColor="bg-indigo-50 dark:bg-indigo-950/30"
            iconColor="text-indigo-600"
            icon={<Code2 className="h-3.5 w-3.5" />}
            label={label}
            subtitle={preview}
        >
            {!expr && (
                <span className="italic text-muted-foreground">No SQL expression</span>
            )}
        </BaseNode>
    )
})
