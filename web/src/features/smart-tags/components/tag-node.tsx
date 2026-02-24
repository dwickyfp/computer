import { Handle, Position, type NodeProps, type Node } from '@xyflow/react'
import { Hash } from 'lucide-react'
import { cn } from '@/lib/utils'
import { useTheme } from '@/context/theme-provider'

export interface TagNodeData extends Record<string, unknown> {
  label: string
  usageCount: number
  /** Normalized size 0–1 based on usage_count relative to max */
  sizeRatio: number
}

export function TagNode({ data }: NodeProps<Node<TagNodeData>>) {
  const { theme } = useTheme()
  const isDark = theme === 'dark'

  // Scale circle between 36px and 80px depending on usage
  const size = Math.round(36 + data.sizeRatio * 44)
  const fontSize = size < 50 ? 9 : size < 65 ? 10 : 11

  return (
    <div className="group relative flex flex-col items-center">
      {/* Glow */}
      <div
        className={isDark ? "absolute rounded-full bg-indigo-500/20 blur-md" : "absolute rounded-full bg-blue-400/15 blur-md"}
        style={{ width: size + 16, height: size + 16 }}
      />

      {/* Circle */}
      <div
        className={cn(
          'relative flex items-center justify-center rounded-full',
          isDark ? 'border-2 border-indigo-400/60 bg-indigo-600 shadow-lg shadow-indigo-500/30' : 'border-2 border-blue-400/70 bg-blue-500 shadow-lg shadow-blue-400/30',
          'transition-transform duration-200 hover:scale-110 cursor-pointer',
        )}
        style={{ width: size, height: size }}
      >
        <Hash className="h-3 w-3 text-white/70 absolute top-1 right-1" style={{ display: size > 50 ? undefined : 'none' }} />
      </div>

      {/* Label */}
      <span
        className={isDark ? "mt-1.5 max-w-[100px] truncate text-center font-medium text-indigo-200" : "mt-1.5 max-w-[100px] truncate text-center font-medium text-slate-700"}
        style={{ fontSize }}
        title={data.label}
      >
        {data.label}
      </span>

      {/* Usage count */}
      <span className={isDark ? "text-[9px] text-indigo-400/70" : "text-[9px] text-slate-600"}>
        {data.usageCount} {data.usageCount === 1 ? 'use' : 'uses'}
      </span>

      {/* Invisible handles for edges */}
      <Handle
        type="source"
        position={Position.Right}
        className="opacity-0! w-1! h-1!"
      />
      <Handle
        type="target"
        position={Position.Left}
        className="opacity-0! w-1! h-1!"
      />
    </div>
  )
}
