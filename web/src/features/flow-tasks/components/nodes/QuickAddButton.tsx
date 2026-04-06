/**
 * QuickAddButton — a small "+" button that appears on the left side of the
 * source (right) handle when a node is hovered.
 *
 * Clicking it opens a popover listing all connectable node types. Selecting
 * one creates the new node (positioned to the right, stacked slightly if
 * siblings already occupy that column) and automatically draws an edge.
 */
import { useState } from 'react'
import type { FlowNodeType } from '@/repo/flow-tasks'
import {
  Database,
  Sparkles,
  BarChart2,
  GitMerge,
  Rows,
  Table2,
  PlusCircle,
  HardDriveDownload,
  Code2,
  Plus,
} from 'lucide-react'
import { cn } from '@/lib/utils'
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from '@/components/ui/popover'
import { useFlowTaskStore } from '../../store/flow-task-store'

// ─── Node catalogue (one entry per addable type) ──────────────────────────────

interface NodeOption {
  type: FlowNodeType
  label: string
  description: string
  icon: React.ReactNode
  iconBg: string
  iconText: string
}

const NODE_OPTIONS: NodeOption[] = [
  {
    type: 'input',
    label: 'Input',
    description: 'Read from a source table',
    icon: <Database className='h-3.5 w-3.5' />,
    iconBg: 'bg-emerald-100 dark:bg-emerald-900/40',
    iconText: 'text-emerald-600 dark:text-emerald-400',
  },
  {
    type: 'clean',
    label: 'Clean',
    description: 'Filter, rename, cast columns',
    icon: <Sparkles className='h-3.5 w-3.5' />,
    iconBg: 'bg-sky-100 dark:bg-sky-900/40',
    iconText: 'text-sky-600 dark:text-sky-400',
  },
  {
    type: 'aggregate',
    label: 'Aggregate',
    description: 'Group by and aggregate',
    icon: <BarChart2 className='h-3.5 w-3.5' />,
    iconBg: 'bg-violet-100 dark:bg-violet-900/40',
    iconText: 'text-violet-600 dark:text-violet-400',
  },
  {
    type: 'join',
    label: 'Join',
    description: 'Join two datasets',
    icon: <GitMerge className='h-3.5 w-3.5' />,
    iconBg: 'bg-orange-100 dark:bg-orange-900/40',
    iconText: 'text-orange-600 dark:text-orange-400',
  },
  {
    type: 'union',
    label: 'Union',
    description: 'Stack datasets vertically',
    icon: <Rows className='h-3.5 w-3.5' />,
    iconBg: 'bg-teal-100 dark:bg-teal-900/40',
    iconText: 'text-teal-600 dark:text-teal-400',
  },
  {
    type: 'pivot',
    label: 'Pivot',
    description: 'Reshape rows ↔ columns',
    icon: <Table2 className='h-3.5 w-3.5' />,
    iconBg: 'bg-pink-100 dark:bg-pink-900/40',
    iconText: 'text-pink-600 dark:text-pink-400',
  },
  {
    type: 'new_rows',
    label: 'New Rows',
    description: 'Inject static rows',
    icon: <PlusCircle className='h-3.5 w-3.5' />,
    iconBg: 'bg-amber-100 dark:bg-amber-900/40',
    iconText: 'text-amber-600 dark:text-amber-400',
  },
  {
    type: 'sql',
    label: 'SQL',
    description: 'Custom SQL expression',
    icon: <Code2 className='h-3.5 w-3.5' />,
    iconBg: 'bg-indigo-100 dark:bg-indigo-900/40',
    iconText: 'text-indigo-600 dark:text-indigo-400',
  },
  {
    type: 'output',
    label: 'Output',
    description: 'Write to a destination',
    icon: <HardDriveDownload className='h-3.5 w-3.5' />,
    iconBg: 'bg-rose-100 dark:bg-rose-900/40',
    iconText: 'text-rose-600 dark:text-rose-400',
  },
]

// ─── Unique ID generator ──────────────────────────────────────────────────────

function genId(type: string) {
  return `${type}_${Date.now()}_${Math.floor(Math.random() * 9999)}`
}

// ─── Component ────────────────────────────────────────────────────────────────

interface QuickAddButtonProps {
  nodeId: string
}

export function QuickAddButton({ nodeId }: QuickAddButtonProps) {
  const [open, setOpen] = useState(false)
  const { nodes, addNode, onConnect, selectNode } = useFlowTaskStore()

  const handleAdd = (opt: NodeOption) => {
    const sourceNode = nodes.find((n) => n.id === nodeId)
    if (!sourceNode) return

    // Count nodes that are already roughly in the same column to the right,
    // so we can stack the new node below them and avoid hard overlaps.
    const sameColumn = nodes.filter(
      (n) =>
        n.position.x >= sourceNode.position.x + 200 &&
        n.position.x <= sourceNode.position.x + 400 &&
        Math.abs(n.position.y - sourceNode.position.y) < 150
    ).length

    const newId = genId(opt.type)

    addNode({
      id: newId,
      type: opt.type,
      position: {
        x: sourceNode.position.x + 320,
        y: sourceNode.position.y + sameColumn * 140,
      },
      data: { label: opt.label },
    })

    // Auto-connect source → new node
    onConnect({
      source: nodeId,
      target: newId,
      sourceHandle: null,
      targetHandle: null,
    })

    // Select the new node so the config drawer opens
    selectNode(newId)

    setOpen(false)
  }

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        {/* Positioned just inside the right edge of the node, left of the handle dot */}
        <button
          className={cn(
            'absolute top-1/2 -right-1.5 z-20 -translate-y-1/2',
            'h-4 w-4 rounded-full',
            'flex items-center justify-center',
            'border border-border bg-background shadow-sm',
            'text-muted-foreground',
            'transition-transform duration-150',
            'hover:scale-110 hover:border-primary hover:bg-primary hover:text-primary-foreground',
            open && 'border-primary bg-primary text-primary-foreground'
          )}
          onClick={(e) => {
            // Prevent ReactFlow from deselecting the node on click
            e.stopPropagation()
          }}
          title='Add connected node'
        >
          <Plus className='h-2.5 w-2.5' strokeWidth={2.5} />
        </button>
      </PopoverTrigger>

      <PopoverContent
        side='right'
        align='start'
        sideOffset={20}
        className='w-52 p-2'
        onClick={(e) => e.stopPropagation()}
      >
        <p className='mb-2 px-1 text-[10px] font-semibold tracking-wider text-muted-foreground uppercase'>
          Add connected node
        </p>

        <div className='space-y-0.5'>
          {NODE_OPTIONS.map((opt) => (
            <button
              key={opt.type}
              className='flex w-full items-center gap-2.5 rounded-md px-2 py-1.5 text-left transition-colors hover:bg-muted'
              onClick={() => handleAdd(opt)}
            >
              <span
                className={cn(
                  'flex h-6 w-6 shrink-0 items-center justify-center rounded',
                  opt.iconBg,
                  opt.iconText
                )}
              >
                {opt.icon}
              </span>
              <div className='min-w-0'>
                <p className='text-xs leading-none font-medium'>{opt.label}</p>
                <p className='mt-0.5 truncate text-[10px] text-muted-foreground'>
                  {opt.description}
                </p>
              </div>
            </button>
          ))}
        </div>
      </PopoverContent>
    </Popover>
  )
}
