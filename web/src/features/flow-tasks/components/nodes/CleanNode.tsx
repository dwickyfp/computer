import { memo } from 'react'
import { type NodeProps } from '@xyflow/react'
import { Sparkles } from 'lucide-react'
import { BaseNode } from './BaseNode'

export const CleanNode = memo(function CleanNode({ id, selected, data }: NodeProps) {
    const label = (data?.label as string) || 'Clean'
    const ops: string[] = []
    if (data?.drop_nulls) ops.push('Drop nulls')
    if (data?.deduplicate) ops.push('Deduplicate')
    if (data?.filter_expr) ops.push('Filter')
    const selectCols = data?.select_columns as string[] | undefined
    if (selectCols?.length) ops.push(`Select ${selectCols.length} cols`)
    const renameCols = data?.rename_columns as Record<string, string> | undefined
    const renameCount = renameCols ? Object.keys(renameCols).length : 0
    if (renameCount > 0) ops.push(`Rename ${renameCount} col${renameCount !== 1 ? 's' : ''}`)
    const castCols = data?.cast_columns as Array<{ column: string; target_type: string }> | undefined
    if (castCols?.length) ops.push(`Cast ${castCols.length} col${castCols.length !== 1 ? 's' : ''}`)
    const expressions = data?.expressions as Array<{ expr: string; alias: string }> | undefined
    if (expressions?.length) ops.push(`${expressions.length} expr${expressions.length !== 1 ? 's' : ''}`)

    return (
        <BaseNode
            id={id}
            selected={selected}
            accentColor="bg-sky-500"
            bgColor="bg-sky-50 dark:bg-sky-950/30"
            iconColor="text-sky-600"
            icon={<Sparkles className="h-3.5 w-3.5" />}
            label={label}
            subtitle={ops.length ? ops.join(' · ') : undefined}
        >
            {ops.length === 0 && (
                <span className="italic text-muted-foreground">No transformations</span>
            )}
        </BaseNode>
    )
})
