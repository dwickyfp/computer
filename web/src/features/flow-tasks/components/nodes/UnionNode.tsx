import { memo } from 'react'
import { type NodeProps, Handle, Position } from '@xyflow/react'
import { Rows } from 'lucide-react'
import { BaseNode } from './BaseNode'

/**
 * UnionNode — two LEFT target handles (input_a / input_b) + single RIGHT source.
 * Handle IDs must match the edge `targetHandle` field when building edges.
 */
export const UnionNode = memo(function UnionNode({ id, selected, data }: NodeProps) {
    const label = (data?.label as string) || 'Union'
    const unionAll = (data?.union_all as boolean) ?? true
    const inputIds = (data?.input_ids as string[]) || []

    return (
        <BaseNode
            id={id}
            selected={selected}
            accentColor="bg-teal-500"
            bgColor="bg-teal-50 dark:bg-teal-950/30"
            iconColor="text-teal-600"
            icon={<Rows className="h-3.5 w-3.5" />}
            label={label}
            subtitle={unionAll ? 'UNION ALL' : 'UNION DISTINCT'}
            hasTarget={false}   // we render custom dual handles below
        >
            {/* Two input labels */}
            <div className="flex flex-col gap-0.5">
                <div className="flex items-center gap-1">
                    <span className="inline-flex items-center justify-center w-3.5 h-3.5 rounded-sm bg-teal-200 dark:bg-teal-800 text-[8px] font-bold text-teal-700 dark:text-teal-300">A</span>
                    <span>Input A</span>
                </div>
                <div className="flex items-center gap-1">
                    <span className="inline-flex items-center justify-center w-3.5 h-3.5 rounded-sm bg-teal-200 dark:bg-teal-800 text-[8px] font-bold text-teal-700 dark:text-teal-300">B</span>
                    <span>Input B</span>
                </div>
                {inputIds.length > 0
                    ? <span className="mt-0.5">{inputIds.length} input{inputIds.length > 1 ? 's' : ''}</span>
                    : <span className="italic text-muted-foreground mt-0.5">No inputs configured</span>
                }
            </div>

            {/* Custom dual target handles */}
            <Handle
                type="target"
                id="input_a"
                position={Position.Left}
                style={{ top: '35%' }}
                className="!w-3 !h-3 !border-2 !bg-background"
            />
            <Handle
                type="target"
                id="input_b"
                position={Position.Left}
                style={{ top: '65%' }}
                className="!w-3 !h-3 !border-2 !bg-background"
            />
        </BaseNode>
    )
})
