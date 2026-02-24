/**
 * NoteNode — a freestanding sticky-note node for annotating the canvas.
 * • No source / target handles — never connects to other nodes.
 * • Inline editable textarea — type directly on the canvas.
 * • Resizable via NodeResizer (visible when selected).
 */

import { memo, useCallback } from 'react'
import { type NodeProps, NodeResizer } from '@xyflow/react'
import { StickyNote } from 'lucide-react'
import { cn } from '@/lib/utils'
import { useFlowTaskStore } from '../../store/flow-task-store'

export const NoteNode = memo(function NoteNode({ id, selected }: NodeProps) {
    // Read note content directly from the store to avoid stale prop issues
    // when the user types fast.
    const content =
        (useFlowTaskStore((s) =>
            s.nodes.find((n) => n.id === id)?.data?.note_content
        ) as string) ?? ''

    const handleChange = useCallback(
        (e: React.ChangeEvent<HTMLTextAreaElement>) => {
            useFlowTaskStore
                .getState()
                .updateNodeData(id, { note_content: e.target.value })
        },
        [id],
    )

    return (
        <div
            className={cn(
                'relative flex flex-col rounded-md shadow-sm w-full h-full',
                'bg-amber-50 dark:bg-amber-950/40',
                'border border-amber-300 dark:border-amber-700',
                'transition-all duration-150',
                selected
                    ? 'ring-2 ring-offset-1 ring-offset-background ring-amber-400'
                    : '',
            )}
        >
            <NodeResizer
                minWidth={160}
                minHeight={90}
                isVisible={selected}
                lineClassName="!border-amber-400"
                handleClassName="!h-2.5 !w-2.5 !bg-white !border-2 !border-amber-400 !rounded-sm"
            />

            {/* Header bar */}
            <div className="flex items-center gap-1.5 px-2 py-1 border-b border-amber-200 dark:border-amber-800 flex-shrink-0">
                <StickyNote className="h-3 w-3 text-amber-500 dark:text-amber-400 flex-shrink-0" />
                <span className="text-[10px] font-semibold text-amber-600 dark:text-amber-300 uppercase tracking-wide select-none">
                    Note
                </span>
            </div>

            {/* Editable text area — 'nodrag' prevents accidentally dragging while typing */}
            <textarea
                className={cn(
                    'nodrag flex-1 resize-none bg-transparent px-2 py-1.5',
                    'text-xs text-amber-900 dark:text-amber-100',
                    'placeholder:text-amber-400/60',
                    'focus:outline-none',
                    'rounded-b-md',
                )}
                placeholder="Type your note here…"
                value={content}
                onChange={handleChange}
                onClick={(e) => e.stopPropagation()}
            />
        </div>
    )
})
