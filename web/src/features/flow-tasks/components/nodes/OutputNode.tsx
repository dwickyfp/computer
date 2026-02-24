import { memo } from 'react'
import { type NodeProps } from '@xyflow/react'
import { HardDriveDownload } from 'lucide-react'
import { BaseNode } from './BaseNode'

export const OutputNode = memo(function OutputNode({ id, selected, data }: NodeProps) {
    const label = (data?.label as string) || 'Output'
    const targetTable = (data?.table_name as string) || ''
    const writeMode = (data?.write_mode as string) || 'APPEND'
    const schemaName = (data?.schema_name as string) || 'public'

    return (
        <BaseNode
            id={id}
            selected={selected}
            accentColor="bg-rose-500"
            bgColor="bg-rose-50 dark:bg-rose-950/30"
            iconColor="text-rose-600"
            icon={<HardDriveDownload className="h-3.5 w-3.5" />}
            label={label}
            subtitle={targetTable ? `${schemaName}.${targetTable}` : undefined}
            hasSource={false}
        >
            <div className="flex justify-between">
                <span>Mode:</span>
                <span className="font-medium">{writeMode}</span>
            </div>
            {!targetTable && (
                <span className="italic text-muted-foreground">No table configured</span>
            )}
        </BaseNode>
    )
})
