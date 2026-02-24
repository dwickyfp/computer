import { memo } from 'react'
import { type NodeProps } from '@xyflow/react'
import { Database, Filter } from 'lucide-react'
import { BaseNode } from './BaseNode'

export const InputNode = memo(function InputNode({ id, selected, data }: NodeProps) {
    const label = (data?.label as string) || 'Input'
    const tableName = (data?.table_name as string) || ''
    const schemaName = (data?.schema_name as string) || 'public'
    const filterSql = (data?.filter_sql as string) || ''
    const hasFilter = !!filterSql.trim()

    return (
        <BaseNode
            id={id}
            selected={selected}
            accentColor="bg-emerald-500"
            bgColor="bg-emerald-50 dark:bg-emerald-950/30"
            iconColor="text-emerald-600"
            icon={<Database className="h-3.5 w-3.5" />}
            label={label}
            subtitle={tableName ? `${schemaName}.${tableName}` : undefined}
            hasTarget={false}
        >
            {!tableName && (
                <span className="italic text-muted-foreground">No table selected</span>
            )}
            {hasFilter && (
                <div className="flex items-center gap-1 text-[10px] text-emerald-600 dark:text-emerald-400 mt-0.5">
                    <Filter className="h-2.5 w-2.5" />
                    <span>Filtered</span>
                </div>
            )}
        </BaseNode>
    )
})
