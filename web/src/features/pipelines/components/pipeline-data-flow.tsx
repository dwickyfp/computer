import { useMemo, useEffect, useRef } from 'react'
import { useQuery } from '@tanstack/react-query'
import { type Pipeline, pipelinesRepo } from '@/repo/pipelines'
import {
  ReactFlow,
  Background,
  Controls,
  type Node,
  type Edge,
  Position,
  MarkerType,
  Handle,
  ReactFlowProvider,
  useReactFlow,
} from '@xyflow/react'
import '@xyflow/react/dist/style.css'
import { Table2, Layers } from 'lucide-react'
import { useTheme } from '@/context/theme-provider'
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from '@/components/ui/tooltip'

// Custom Node Component for consistent styling with cleaner design
const CustomNode = ({ data }: { data: any }) => {
  const isSource = data.isSource
  const isDestGroup = data.isDestGroup

  // Source Database Node (Blue)
  if (isSource) {
    return (
      <div className='group relative'>
        <Handle
          type='target'
          position={Position.Left}
          className='h-2.5! w-2.5! border-2! border-white! bg-blue-500/50! transition-all group-hover:scale-110 group-hover:bg-blue-500! dark:border-gray-900!'
        />

        <div className='relative w-65 rounded-lg border-2 border-blue-500/40 bg-white shadow-sm transition-all duration-200 hover:-translate-y-0.5 hover:border-blue-500/60 hover:shadow-lg dark:bg-gray-950'>
          {/* Subtle glow effect */}
          <div className='absolute inset-0 rounded-lg bg-blue-500/5' />

          <div className='relative flex items-center gap-3 px-4 py-3.5'>
            <div className='flex h-9 w-9 shrink-0 items-center justify-center rounded-md border border-blue-500/20 bg-blue-500/10'>
              <Table2 className='h-4.5 w-4.5 text-blue-600 dark:text-blue-400' />
            </div>
            <TooltipProvider delayDuration={200}>
              <Tooltip>
                <TooltipTrigger asChild>
                  <div className='min-w-0 flex-1'>
                    <p className='truncate text-sm font-semibold text-gray-900 dark:text-gray-100'>
                      {data.label}
                    </p>
                  </div>
                </TooltipTrigger>
                <TooltipContent side='top' className='max-w-75'>
                  <p>{data.label}</p>
                </TooltipContent>
              </Tooltip>
            </TooltipProvider>
          </div>
        </div>

        <Handle
          type='source'
          position={Position.Right}
          className='h-2.5! w-2.5! border-2! border-white! bg-blue-500/50! transition-all group-hover:scale-110 group-hover:bg-blue-500! dark:border-gray-900!'
        />
      </div>
    )
  }

  // Destination Group Node (Purple)
  if (isDestGroup) {
    return (
      <div className='group relative'>
        <Handle
          type='target'
          position={Position.Left}
          className='h-2.5! w-2.5! border-2! border-white! bg-purple-500/50! transition-all group-hover:scale-110 group-hover:bg-purple-500! dark:border-gray-900!'
        />

        <div className='relative w-65 rounded-lg border-2 border-purple-500/40 bg-white shadow-sm transition-all duration-200 hover:-translate-y-0.5 hover:border-purple-500/60 hover:shadow-lg dark:bg-gray-950'>
          {/* Subtle glow effect */}
          <div className='absolute inset-0 rounded-lg bg-purple-500/5' />

          <div className='relative flex items-center gap-3 px-4 py-3.5'>
            <div className='flex h-9 w-9 shrink-0 items-center justify-center rounded-md border border-purple-500/20 bg-purple-500/10'>
              <Layers className='h-4.5 w-4.5 text-purple-600 dark:text-purple-400' />
            </div>
            <TooltipProvider delayDuration={200}>
              <Tooltip>
                <TooltipTrigger asChild>
                  <div className='min-w-0 flex-1'>
                    <p className='truncate text-sm font-semibold text-gray-900 dark:text-gray-100'>
                      {data.label}
                    </p>
                  </div>
                </TooltipTrigger>
                <TooltipContent side='top' className='max-w-75'>
                  <p>{data.label}</p>
                </TooltipContent>
              </Tooltip>
            </TooltipProvider>
          </div>
        </div>

        <Handle
          type='source'
          position={Position.Right}
          className='h-2.5! w-2.5! border-2! border-white! bg-purple-500/50! transition-all group-hover:scale-110 group-hover:bg-purple-500! dark:border-gray-900!'
        />
      </div>
    )
  }

  // Target Table Node (Green) - with record count
  return (
    <div className='group relative'>
      <Handle
        type='target'
        position={Position.Left}
        className='h-2.5! w-2.5! border-2! border-white! bg-emerald-500/50! transition-all group-hover:scale-110 group-hover:bg-emerald-500! dark:border-gray-900!'
      />

      <div className='relative w-65 rounded-lg border-2 border-emerald-500/40 bg-white shadow-sm transition-all duration-200 hover:-translate-y-0.5 hover:border-emerald-500/60 hover:shadow-lg dark:bg-gray-950'>
        {/* Subtle glow effect */}
        <div className='absolute inset-0 rounded-lg bg-emerald-500/5' />

        <div className='relative space-y-2.5 px-4 py-3'>
          {/* Table name */}
          <div className='flex items-center gap-3'>
            <div className='flex h-9 w-9 shrink-0 items-center justify-center rounded-md border border-emerald-500/20 bg-emerald-500/10'>
              <Table2 className='h-4.5 w-4.5 text-emerald-600 dark:text-emerald-400' />
            </div>
            <TooltipProvider delayDuration={200}>
              <Tooltip>
                <TooltipTrigger asChild>
                  <div className='min-w-0 flex-1'>
                    <p className='truncate text-sm font-semibold text-gray-900 dark:text-gray-100'>
                      {data.label}
                    </p>
                  </div>
                </TooltipTrigger>
                <TooltipContent side='top' className='max-w-75'>
                  <p>{data.label}</p>
                </TooltipContent>
              </Tooltip>
            </TooltipProvider>
          </div>

          {/* Record count */}
          <div className='flex items-center justify-between px-1'>
            <span className='text-[10px] font-semibold tracking-wider text-gray-500 uppercase dark:text-gray-400'>
              Records
            </span>
            <div className='flex items-center gap-1.5 rounded-md border border-emerald-500/20 bg-emerald-500/10 px-2.5 py-1'>
              <span className='font-mono text-sm font-bold text-emerald-600 dark:text-emerald-400'>
                {data.totalCount?.toLocaleString() || '0'}
              </span>
            </div>
          </div>
        </div>
      </div>

      <Handle
        type='source'
        position={Position.Right}
        className='h-2.5! w-2.5! border-2! border-white! bg-emerald-500/50! transition-all group-hover:scale-110 group-hover:bg-emerald-500! dark:border-gray-900!'
      />
    </div>
  )
}

const nodeTypes = {
  custom: CustomNode,
}

interface PipelineDataFlowProps {
  pipeline: Pipeline
}

export function PipelineDataFlow({ pipeline }: PipelineDataFlowProps) {
  return (
    <ReactFlowProvider>
      <FlowContent pipeline={pipeline} />
    </ReactFlowProvider>
  )
}

function FlowContent({ pipeline }: PipelineDataFlowProps) {
  const { theme } = useTheme()
  const { fitView } = useReactFlow()
  const containerRef = useRef<HTMLDivElement>(null)

  // Fetch stats to calculate edge labels
  const { data: stats } = useQuery({
    queryKey: ['pipeline-stats', pipeline.id],
    queryFn: () => pipelinesRepo.getStats(pipeline.id),
    refetchInterval: 5000,
  })

  // Helper to sum today's stats
  const calcTotal = (dailyStats: any[]) => {
    if (!dailyStats) return 0
    const today = new Date().toLocaleDateString('en-CA')
    const entry = dailyStats.find((d: any) => d.date.startsWith(today))
    return entry ? entry.count : 0
  }

  // Calculate totals & build lineage graph
  const { nodes, edges } = useMemo(() => {
    const nodes: Node[] = []
    const edges: Edge[] = []

    if (!pipeline) return { nodes, edges }

    // Constants
    const X_ROOT = 50
    const X_SOURCE = 400
    const X_DEST_GROUP = 800
    const X_TARGET = 1200
    const ROW_HEIGHT = 150

    // Build a comprehensive map: LEFT JOIN enabled tables with stats
    // Key: table_name, Value: Array of { destination, table_sync, stat? }
    const flowMap = new Map<string, Array<{
      destination: any
      tableSync: any
      stat?: any
    }>>()

    // First, get ALL enabled tables from pipeline configuration
    pipeline.destinations?.forEach((dest) => {
      dest.table_syncs?.forEach((sync) => {
        const tableName = sync.table_name
        const list = flowMap.get(tableName) || []
        
        // Find matching stat for this destination + table sync
        const matchingStat = stats?.find(
          (s) => 
            s.table_name === tableName && 
            s.pipeline_destination_id === dest.id &&
            s.pipeline_destination_table_sync_id === sync.id
        )
        
        list.push({
          destination: dest,
          tableSync: sync,
          stat: matchingStat, // May be undefined if no data streamed yet
        })
        
        flowMap.set(tableName, list)
      })
    })

    let currentY = 50

    // 1. Source DB Node (Root) - Position usually centered relative to everything,
    // but for simplicity, let's just place it top-left or centered if we calculate total height.
    // We'll fix it at the top for now, or maybe vertically center it later.
    // Let's place it at top-left.
    nodes.push({
      id: 'source-root',
      type: 'custom',
      position: { x: X_ROOT, y: currentY },
      data: {
        label: pipeline.source?.name || 'Source DB',
        isSource: true,
      },
      sourcePosition: Position.Right,
      selectable: false, // Disable selection
    })

    // Track Root Y center for "Source DB" edge connections if we wanted to center root.
    // But let's just flow downwards.

    // Iterate through each Source Table Group
    flowMap.forEach((items, sourceTableName) => {
      // Group items by Destination ID
      const destGroups = new Map<number, typeof items>()
      items.forEach((item) => {
        const destId = item.destination.id
        const list = destGroups.get(destId) || []
        list.push(item)
        destGroups.set(destId, list)
      })

      // Calculate total height for this Source Group based on all destinations
      let groupTotalRows = 0
      destGroups.forEach((groupItems) => {
        groupTotalRows += Math.max(1, groupItems.length)
      })
      const sourceGroupHeight = groupTotalRows * ROW_HEIGHT

      // Source Node Position (Vertically centered for this entire group)
      const sourceY = currentY + sourceGroupHeight / 2 - ROW_HEIGHT / 2

      const sourceNodeId = `src-tbl-${sourceTableName}`
      nodes.push({
        id: sourceNodeId,
        type: 'custom',
        position: { x: X_SOURCE, y: sourceY },
        data: {
          label: sourceTableName,
          isSource: true,
        },
        sourcePosition: Position.Right,
        targetPosition: Position.Left,
        selectable: false, // Disable selection
      })

      // Edge Root -> Source
      edges.push({
        id: `e-root-${sourceTableName}`,
        source: 'source-root',
        target: sourceNodeId,
        style: { stroke: '#cbd5e1', strokeWidth: 2, strokeDasharray: '5,5' },
        type: 'smoothstep',
        animated: true,
      })

      // Iterate through Destination Groups to place Dest Nodes & Target Nodes
      let currentDestY = currentY
      destGroups.forEach((groupItems, destId) => {
        const destRowCount = Math.max(1, groupItems.length)
        const destGroupHeight = destRowCount * ROW_HEIGHT

        // Destination Node Position (Centered for its targets)
        const destY = currentDestY + destGroupHeight / 2 - ROW_HEIGHT / 2
        const destName = groupItems[0].destination.destination.name || `Dest ${destId}`
        const destNodeId = `dst-group-${sourceTableName}-${destId}`

        nodes.push({
          id: destNodeId,
          type: 'custom',
          position: { x: X_DEST_GROUP, y: destY },
          data: {
            label: destName,
            isDestGroup: true,
            isSource: false,
          },
          sourcePosition: Position.Right,
          targetPosition: Position.Left,
        })

        // Edge Source -> Destination
        edges.push({
          id: `e-${sourceTableName}-${destNodeId}`,
          source: sourceNodeId,
          target: destNodeId,
          type: 'smoothstep',
          animated: true,
          style: {
            stroke: '#94a3b8',
            strokeWidth: 1.5,
            strokeDasharray: '4,4',
          },
          markerEnd: { type: MarkerType.ArrowClosed, color: '#94a3b8' },
        })

        // Place Target Nodes for this Destination
        groupItems.forEach((item, idx) => {
          const targetTableName = item.tableSync.table_name_target || item.tableSync.table_name
          const syncId = item.tableSync.id

          const targetNodeId = `dst-tbl-sync-${syncId}`

          // Target Y position
          const targetY = currentDestY + idx * ROW_HEIGHT

          // Calculate total count from stat if exists, otherwise 0
          const totalCount = item.stat ? calcTotal(item.stat.daily_stats) : 0

          nodes.push({
            id: targetNodeId,
            type: 'custom',
            position: { x: X_TARGET, y: targetY },
            data: {
              label: targetTableName,
              subLabel: destName,
              isSource: false,
              totalCount: totalCount,
              destId: item.destination.id,
            },
            targetPosition: Position.Left,
          })

          // Edge Destination -> Target
          edges.push({
            id: `e-${destNodeId}-${targetNodeId}`,
            source: destNodeId,
            target: targetNodeId,
            type: 'smoothstep',
            animated: true,
            style: { stroke: '#64748b', strokeWidth: 1.5 },
            label: `${totalCount.toLocaleString()}`,
            labelStyle: {
              fill: 'var(--foreground)',
              fontWeight: 600,
              fontSize: 10,
            },
            labelBgStyle: { fill: 'var(--card)', fillOpacity: 0.9 },
            markerEnd: {
              type: MarkerType.ArrowClosed,
              color: '#64748b',
              width: 15,
              height: 15,
            },
          })
        })

        // Move Y cursor for next destination group
        currentDestY += destGroupHeight
      })

      // Advance Main Y for next Source Group + padding
      currentY += sourceGroupHeight + 40
    })

    return { nodes, edges }
  }, [pipeline, stats])

  // Fit view when tab becomes visible
  useEffect(() => {
    const observer = new IntersectionObserver(
      (entries) => {
        entries.forEach((entry) => {
          if (entry.isIntersecting) {
            setTimeout(() => {
              fitView({ duration: 400 })
            }, 100)
          }
        })
      },
      { threshold: 0.1 }
    )

    if (containerRef.current) {
      observer.observe(containerRef.current)
    }

    return () => observer.disconnect()
  }, [fitView])

  return (
    <div ref={containerRef} className='relative h-[600px] rounded-lg border bg-background [&_.react-flow__controls]:border-border [&_.react-flow__controls]:bg-background [&_.react-flow__controls]:shadow-md [&_.react-flow__controls-button]:border-border [&_.react-flow__controls-button]:bg-background [&_.react-flow__controls-button]:fill-foreground [&_.react-flow__controls-button:hover]:bg-muted'>
      <ReactFlow
        nodes={nodes}
        edges={edges}
        nodeTypes={nodeTypes}
        colorMode={theme}
        fitView
      >
        <Background className='bg-background!' color='var(--border)' />
        <Controls />
      </ReactFlow>
    </div>
  )
}
