import { memo } from 'react'
import { type NodeProps, Handle, Position } from '@xyflow/react'
import { GitMerge } from 'lucide-react'
import { BaseNode } from './BaseNode'

/**
 * JoinNode — two LEFT target handles (left / right input) + single RIGHT source.
 * Handle IDs must match the edge `targetHandle` field when building edges.
 */
export const JoinNode = memo(function JoinNode({ id, selected, data }: NodeProps) {
    const label = (data?.label as string) || 'Join'
    const joinType = (data?.join_type as string) || 'INNER'
    const leftKeys = (data?.left_keys as unknown[]) || []

    return (
        <BaseNode
            id={id}
            selected={selected}
            accentColor="bg-orange-500"
            bgColor="bg-orange-50 dark:bg-orange-950/30"
            iconColor="text-orange-600"
            icon={<GitMerge className="h-3.5 w-3.5" />}
            label={label}
            subtitle={`${joinType} JOIN`}
            hasTarget={false}   // we render custom dual handles below
        >
            {/* Two input labels to hint which handle is which */}
            <div className="flex flex-col gap-0.5">
                <div className="flex items-center gap-1">
                    <span className="inline-flex items-center justify-center w-3.5 h-3.5 rounded-sm bg-orange-200 dark:bg-orange-800 text-[8px] font-bold text-orange-700 dark:text-orange-300">L</span>
                    <span>Left input</span>
                </div>
                <div className="flex items-center gap-1">
                    <span className="inline-flex items-center justify-center w-3.5 h-3.5 rounded-sm bg-orange-200 dark:bg-orange-800 text-[8px] font-bold text-orange-700 dark:text-orange-300">R</span>
                    <span>Right input</span>
                </div>
                {leftKeys.length > 0
                    ? <span className="mt-0.5">{leftKeys.length} join key{leftKeys.length > 1 ? 's' : ''}</span>
                    : <span className="italic text-muted-foreground mt-0.5">No join keys</span>
                }
            </div>

            {/* Custom dual target handles */}
            <Handle
                type="target"
                id="left"
                position={Position.Left}
                style={{ top: '35%' }}
                className="!w-3 !h-3 !border-2 !bg-background"
            />
            <Handle
                type="target"
                id="right"
                position={Position.Left}
                style={{ top: '65%' }}
                className="!w-3 !h-3 !border-2 !bg-background"
            />
        </BaseNode>
    )
})
