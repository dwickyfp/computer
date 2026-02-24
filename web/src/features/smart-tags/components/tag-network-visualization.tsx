import { useCallback, useEffect, useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import {
  ReactFlow,
  Background,
  Controls,
  MiniMap,
  type Node,
  type Edge,
  Position,
  useNodesState,
  useEdgesState,
  useReactFlow,
} from '@xyflow/react'
import '@xyflow/react/dist/style.css'
import { Link, type HistoryState } from '@tanstack/react-router'
import { Database, ExternalLink, Hash, Loader2, Network, Table2, X } from 'lucide-react'
import { ScrollArea } from '@/components/ui/scroll-area'
import { useTheme } from '@/context/theme-provider'
import { tagsRepo } from '@/repo/tags'
import { ShootingStars } from './shooting-stars'
import { TagNode, type TagNodeData } from './tag-node'

const nodeTypes = { tagNode: TagNode }

/**
 * Circular layout helper — distributes nodes in concentric rings.
 * Inner ring: high-usage nodes. Outer ring: low-usage.
 */
function computeCircularLayout(
  rawNodes: { id: number; tag: string; usage_count: number }[],
  width: number,
  height: number,
) {
  const sorted = [...rawNodes].sort((a, b) => b.usage_count - a.usage_count)
  const maxUsage = Math.max(...sorted.map((n) => n.usage_count), 1)
  const cx = width / 2
  const cy = height / 2

  // Split into inner (top 30%) and outer rings
  const innerCount = Math.max(1, Math.ceil(sorted.length * 0.3))
  const innerRadius = Math.min(width, height) * 0.18
  const outerRadius = Math.min(width, height) * 0.38

  const nodes: Node<TagNodeData>[] = sorted.map((raw, i) => {
    const isInner = i < innerCount
    const ringIndex = isInner ? i : i - innerCount
    const ringSize = isInner ? innerCount : sorted.length - innerCount
    const radius = isInner ? innerRadius : outerRadius
    const angle = (2 * Math.PI * ringIndex) / Math.max(ringSize, 1) - Math.PI / 2

    return {
      id: String(raw.id),
      type: 'tagNode',
      position: {
        x: cx + radius * Math.cos(angle),
        y: cy + radius * Math.sin(angle),
      },
      data: {
        label: raw.tag,
        usageCount: raw.usage_count,
        sizeRatio: raw.usage_count / maxUsage,
      },
      sourcePosition: Position.Right,
      targetPosition: Position.Left,
    }
  })

  return nodes
}

/**
 * Component to automatically fit view when the network becomes visible.
 * Must be inside ReactFlow to access the fitView function.
 */
function FitViewOnMount({ isVisible }: { isVisible: boolean }) {
  const { fitView } = useReactFlow()
  const [hasFitted, setHasFitted] = useState(false)

  useEffect(() => {
    if (isVisible && !hasFitted) {
      // Small delay to ensure DOM is ready
      const timer = setTimeout(() => {
        fitView({ padding: 0.4, duration: 400 })
        setHasFitted(true)
      }, 100)
      return () => clearTimeout(timer)
    }
  }, [isVisible, hasFitted, fitView])

  return null
}

export function TagNetworkVisualization({ isVisible = true }: { isVisible?: boolean }) {
  const { theme } = useTheme()
  const isDark = theme === 'dark'

  const { data, isLoading, error } = useQuery({
    queryKey: ['tag-relations'],
    queryFn: tagsRepo.getRelations,
  })

  const [selectedNode, setSelectedNode] = useState<{
    id: string
    label: string
    usageCount: number
  } | null>(null)

  // Controls slide-in visibility (separate from selectedNode so we can animate out)
  const [panelVisible, setPanelVisible] = useState(false)

  // Fetch tag usage when a node is selected
  const { data: usageData, isLoading: isUsageLoading } = useQuery({
    queryKey: ['tag-usage', selectedNode?.id],
    queryFn: () => tagsRepo.getUsage(Number(selectedNode!.id)),
    enabled: !!selectedNode,
  })

  // Animate panel in after selectedNode is set
  useEffect(() => {
    if (selectedNode) {
      // Small delay so the DOM renders before the transition kicks in
      const timer = setTimeout(() => setPanelVisible(true), 30)
      return () => clearTimeout(timer)
    } else {
      setPanelVisible(false)
    }
  }, [selectedNode])

  const initialNodes = useMemo(() => {
    if (!data?.nodes.length) return []
    return computeCircularLayout(data.nodes, 1200, 800)
  }, [data?.nodes])

  const initialEdges = useMemo<Edge[]>(() => {
    if (!data?.edges.length) return []
    return data.edges.map((e) => ({
      id: `e-${e.source}-${e.target}`,
      source: String(e.source),
      target: String(e.target),
      animated: false,
      style: {
        stroke: isDark ? 'rgba(99, 102, 241, 0.25)' : 'rgba(59, 130, 246, 0.25)',
        strokeWidth: Math.min(1 + e.shared_tables, 4),
      },
      label: e.shared_tables > 1 ? String(e.shared_tables) : undefined,
      labelStyle: { fill: isDark ? 'rgba(165, 165, 215, 0.7)' : 'rgba(71, 85, 105, 0.7)', fontSize: 9 },
      labelBgStyle: { fill: 'transparent' },
    }))
  }, [data?.edges, isDark])

  const [nodes, setNodes, onNodesChange] = useNodesState(initialNodes)
  const [edges, setEdges, onEdgesChange] = useEdgesState(initialEdges)

  // Sync when data arrives
  useMemo(() => {
    if (initialNodes.length) setNodes(initialNodes)
  }, [initialNodes])

  useMemo(() => {
    if (initialEdges.length) setEdges(initialEdges)
  }, [initialEdges])

  const onNodeClick = useCallback(
    (_event: React.MouseEvent, node: Node<TagNodeData>) => {
      // Highlight connected edges
      const connectedEdgeIds = new Set<string>()
      const connectedNodeIds = new Set<string>([node.id])

      edges.forEach((e) => {
        if (e.source === node.id || e.target === node.id) {
          connectedEdgeIds.add(e.id)
          connectedNodeIds.add(e.source)
          connectedNodeIds.add(e.target)
        }
      })

      setEdges((prev) =>
        prev.map((e) => ({
          ...e,
          style: {
            ...e.style,
            stroke: connectedEdgeIds.has(e.id)
              ? (isDark ? 'rgba(129, 140, 248, 0.8)' : 'rgba(59, 130, 246, 0.8)')
              : (isDark ? 'rgba(99, 102, 241, 0.1)' : 'rgba(59, 130, 246, 0.1)'),
            strokeWidth: connectedEdgeIds.has(e.id)
              ? Math.min((e.style?.strokeWidth as number ?? 1) + 1, 5)
              : 1,
          },
          animated: connectedEdgeIds.has(e.id),
        }))
      )

      setSelectedNode({
        id: node.id,
        label: node.data.label,
        usageCount: node.data.usageCount,
      })
    },
    [edges, setEdges, isDark]
  )

  const onPaneClick = useCallback(() => {
    // Reset edges to normal
    setEdges((prev) =>
      prev.map((e) => ({
        ...e,
        style: {
          ...e.style,
          stroke: isDark ? 'rgba(99, 102, 241, 0.25)' : 'rgba(59, 130, 246, 0.25)',
          strokeWidth: Math.min(1 + (data?.edges.find((de) => e.id === `e-${de.source}-${de.target}`)?.shared_tables ?? 0), 4),
        },
        animated: false,
      }))
    )
    // Slide out first, then clear
    setPanelVisible(false)
    setTimeout(() => setSelectedNode(null), 300)
  }, [data?.edges, setEdges, isDark])

  if (isLoading) {
    return (
      <div className="flex h-[calc(100vh-12rem)] items-center justify-center">
        <div className="flex flex-col items-center gap-3 text-muted-foreground">
          <Loader2 className="h-8 w-8 animate-spin" />
          <span className="text-sm">Loading tag network...</span>
        </div>
      </div>
    )
  }

  if (error || !data) {
    return (
      <div className="flex h-[calc(100vh-12rem)] items-center justify-center">
        <div className="text-center text-muted-foreground">
          <Hash className="mx-auto h-12 w-12 opacity-50" />
          <p className="mt-2 text-sm">Failed to load tag relations.</p>
        </div>
      </div>
    )
  }

  if (data.nodes.length === 0) {
    return (
      <div className="flex h-[calc(100vh-12rem)] items-center justify-center">
        <div className="text-center text-muted-foreground">
          <Hash className="mx-auto h-12 w-12 opacity-50" />
          <p className="mt-2 text-sm">No tags found. Create some tags first.</p>
        </div>
      </div>
    )
  }

  return (
    <div className="space-y-4">
      {/* Title */}
      <div>
        <h2 className="text-2xl font-bold tracking-tight">Tag Network</h2>
        <p className="text-muted-foreground">
          Visualize tag relationships across your data pipeline.
        </p>
      </div>

      {/* Flow Canvas */}
      <div className={isDark ? "relative h-[calc(100vh-14rem)] w-full rounded-lg border bg-[#0a0a1a] overflow-hidden" : "relative h-[calc(100vh-14rem)] w-full rounded-lg border bg-slate-50"}>
      {isDark && <ShootingStars />}
      <ReactFlow
        nodes={nodes}
        edges={edges}
        nodeTypes={nodeTypes}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onNodeClick={onNodeClick}
        onPaneClick={onPaneClick}
        colorMode={theme}
        fitView
        fitViewOptions={{ padding: 0.4 }}
        proOptions={{ hideAttribution: true }}
        minZoom={0.3}
        maxZoom={2}
      >
        <FitViewOnMount isVisible={isVisible} />
        <Background color={isDark ? "rgba(99, 102, 241, 0.08)" : "rgba(148, 163, 184, 0.15)"} gap={24} />
        <Controls
          showInteractive={false}
          className={isDark ? "[&_.react-flow__controls]:!bg-[#1a1a2e] [&_.react-flow__controls]:!border-indigo-800/30" : "[&_.react-flow__controls]:!bg-white [&_.react-flow__controls]:!border-slate-300"}
        />
        <MiniMap
          nodeColor={isDark ? "rgb(99, 102, 241)" : "rgb(59, 130, 246)"}
          maskColor={isDark ? "rgba(0, 0, 0, 0.6)" : "rgba(241, 245, 249, 0.6)"}
          className={isDark ? "!bg-[#0d0d1f] !border-indigo-900/30" : "!bg-white !border-slate-300"}
          pannable
          zoomable
        />
      </ReactFlow>

      {/* Info overlay */}
      <div className={isDark ? "absolute top-4 left-4 rounded-lg border bg-card px-4 py-3 shadow-lg" : "absolute top-4 left-4 rounded-lg border border-slate-300 bg-white/90 px-4 py-3 backdrop-blur-sm shadow-md"}>
        <h3 className={isDark ? "text-sm font-semibold text-card-foreground" : "text-sm font-semibold text-blue-700"}>Tag Network</h3>
        <p className={isDark ? "mt-1 text-xs text-muted-foreground" : "mt-1 text-xs text-slate-600"}>
          {data.nodes.length} tags · {data.edges.length} connections
        </p>
        <p className={isDark ? "mt-0.5 text-[10px] text-muted-foreground/70" : "mt-0.5 text-[10px] text-slate-500"}>
          Tags connected when sharing same tables.
          Click a node to highlight connections. Drag to rearrange.
        </p>
      </div>

      {/* Selected tag usage panel — slides in from right */}
      {selectedNode && (
        <div
          className={isDark ? "absolute top-0 right-0 z-50 h-full w-[340px] border-l bg-card shadow-2xl transition-transform duration-300 ease-out" : "absolute top-0 right-0 z-50 h-full w-[340px] border-l border-slate-300 bg-white/95 shadow-2xl backdrop-blur-md transition-transform duration-300 ease-out"}
          style={{
            transform: panelVisible ? 'translateX(0)' : 'translateX(100%)',
          }}
        >
          {/* Panel header */}
          <div className={isDark ? "flex items-center justify-between border-b px-4 py-3" : "flex items-center justify-between border-b border-slate-300 px-4 py-3"}>
            <div className="flex items-center gap-2 min-w-0">
              <Hash className={isDark ? "h-4 w-4 shrink-0 text-primary" : "h-4 w-4 shrink-0 text-blue-600"} />
              <span className={isDark ? "truncate font-semibold text-card-foreground" : "truncate font-semibold text-slate-900"}>
                {selectedNode.label}
              </span>
            </div>
            <button
              onClick={onPaneClick}
              className={isDark ? "rounded-md p-1 text-muted-foreground hover:bg-accent hover:text-accent-foreground transition-colors" : "rounded-md p-1 text-slate-500 hover:bg-slate-100 hover:text-slate-700 transition-colors"}
            >
              <X className="h-4 w-4" />
            </button>
          </div>

          <div className={isDark ? "px-4 py-2 text-xs text-muted-foreground border-b" : "px-4 py-2 text-xs text-slate-600 border-b border-slate-200"}>
            Used {selectedNode.usageCount} {selectedNode.usageCount === 1 ? 'time' : 'times'} ·{' '}
            {edges.filter((e) => e.animated).length} related tags
          </div>

          {/* Usage tree */}
          <ScrollArea className="h-[calc(100%-90px)]">
            <div className="p-4 space-y-4">
              {isUsageLoading ? (
                <div className={isDark ? "flex h-20 items-center justify-center text-muted-foreground" : "flex h-20 items-center justify-center text-slate-600"}>
                  <Loader2 className="h-5 w-5 animate-spin mr-2" />
                  <span className="text-xs">Loading usage...</span>
                </div>
              ) : !usageData?.usage.length ? (
                <div className={isDark ? "text-xs text-muted-foreground text-center py-4" : "text-xs text-slate-500 text-center py-4"}>
                  No active usage found for this tag.
                </div>
              ) : (
                usageData.usage.map((pipeline, i) => (
                  <div key={i} className="space-y-2">
                    <Link
                      to="/pipelines/$pipelineId"
                      params={{ pipelineId: String(pipeline.pipeline_id) }}
                      className={isDark ? "group/link flex items-center gap-2 text-sm font-medium text-card-foreground hover:text-primary transition-colors" : "group/link flex items-center gap-2 text-sm font-medium text-slate-900 hover:text-blue-600 transition-colors"}
                    >
                      <Network className={isDark ? "h-4 w-4 text-primary" : "h-4 w-4 text-blue-600"} />
                      {pipeline.pipeline_name}
                      <ExternalLink className="h-3 w-3 opacity-50 shrink-0" />
                    </Link>
                    <div className={isDark ? "ml-2 pl-4 border-l-2 border-border space-y-2" : "ml-2 pl-4 border-l-2 border-slate-300 space-y-2"}>
                      {pipeline.destinations.map((dest, j) => (
                        <div key={j} className="space-y-1">
                          <Link
                            to="/pipelines/$pipelineId"
                            params={{ pipelineId: String(pipeline.pipeline_id) }}
                            state={{ highlightDestination: dest.destination_id } as HistoryState}
                            className={isDark ? "group/link flex items-center gap-2 text-xs font-medium text-card-foreground/90 hover:text-primary transition-colors cursor-pointer w-fit" : "group/link flex items-center gap-2 text-xs font-medium text-slate-700 hover:text-blue-600 transition-colors cursor-pointer w-fit"}
                          >
                            <Database className="h-3 w-3" />
                            {dest.destination_name}
                            <ExternalLink className="h-2.5 w-2.5 opacity-50 shrink-0" />
                          </Link>
                          <div className={isDark ? "space-y-0.5 ml-1 pl-3 border-l border-border" : "space-y-0.5 ml-1 pl-3 border-l border-slate-200"}>
                            {dest.tables.map((table, k) => (
                              <Link
                                key={k}
                                to="/pipelines/$pipelineId"
                                params={{ pipelineId: String(pipeline.pipeline_id) }}
                                state={{
                                  openDrawer: true,
                                  openDrawerDestinationId: dest.destination_id,
                                  highlightTable: table,
                                } as HistoryState}
                                className={isDark ? "group/link flex items-center gap-2 text-xs text-muted-foreground py-0.5 hover:text-primary transition-colors cursor-pointer w-fit" : "group/link flex items-center gap-2 text-xs text-slate-600 py-0.5 hover:text-blue-600 transition-colors cursor-pointer w-fit"}
                              >
                                <Table2 className="h-3 w-3 opacity-70" />
                                {table}
                                <ExternalLink className="h-2.5 w-2.5 opacity-50 shrink-0" />
                              </Link>
                            ))}
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>
                ))
              )}
            </div>
          </ScrollArea>
        </div>
      )}
      </div>
    </div>
  )
}
