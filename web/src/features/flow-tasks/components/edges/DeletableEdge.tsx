import { BaseEdge, EdgeLabelRenderer, type EdgeProps, getBezierPath, useReactFlow } from '@xyflow/react'
import { Trash2 } from 'lucide-react'
import { useState } from 'react'

export function DeletableEdge({
    id,
    sourceX,
    sourceY,
    targetX,
    targetY,
    sourcePosition,
    targetPosition,
    style = {},
    markerEnd,
}: EdgeProps) {
    const { deleteElements } = useReactFlow()
    const [isHovered, setIsHovered] = useState(false)
    const [edgePath, labelX, labelY] = getBezierPath({
        sourceX,
        sourceY,
        sourcePosition,
        targetX,
        targetY,
        targetPosition,
    })

    // Callback to delete this edge
    const onDelete = (event: React.MouseEvent) => {
        event.stopPropagation() // Prevent selecting the edge on click
        deleteElements({ edges: [{ id }] })
    }

    return (
        <g 
            onMouseEnter={() => setIsHovered(true)} 
            onMouseLeave={() => setIsHovered(false)}
            className="group"
        >
            {/* 1. Visible edge path */}
            <BaseEdge path={edgePath} markerEnd={markerEnd} style={style} />

            {/* 2. Invisible wide path for easier hovering */}
            <path
                d={edgePath}
                fill="none"
                strokeOpacity={0}
                strokeWidth={20}
                className="react-flow__edge-path-selector"
            />

            {/* 3. Delete button at midpoint */}
            <EdgeLabelRenderer>
                <div
                    style={{
                        position: 'absolute',
                        transform: `translate(-50%, -50%) translate(${labelX}px,${labelY}px)`,
                        pointerEvents: 'all',
                        opacity: isHovered ? 1 : 0,
                        transition: 'opacity 0.2s',
                    }}
                    className="nodrag nopan"
                >
                    <button
                        className="flex items-center justify-center w-6 h-6 bg-background border border-border rounded-full shadow-sm hover:bg-destructive/10 hover:border-destructive hover:text-destructive transition-colors cursor-pointer"
                        onClick={onDelete}
                        title="Delete connection"
                    >
                        <Trash2 className="w-3.5 h-3.5 text-red-500" />
                    </button>
                </div>
            </EdgeLabelRenderer>
        </g>
    )
}
