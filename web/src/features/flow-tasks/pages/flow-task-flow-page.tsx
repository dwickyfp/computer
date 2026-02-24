/**
 * FlowTaskFlowPage — full-screen ReactFlow editor for a single Flow Task.
 *
 * Layout:
 *   ┌──────────────────────────────────────────────────────┐
 *   │  Top bar: breadcrumb / Save / Run / status           │
 *   ├──────────────────────────────────────────────────────┤
 *   │ Palette │        ReactFlow Canvas          │ Config  │
 *   │  (w-56) │                                 │  Panel  │
 *   │         │                                 │  (w-72) │
 *   │         │                                 │         │
 *   │         ├─────────────────────────────────┤         │
 *   │         │  Preview Drawer (slide up)       │         │
 *   └─────────┴─────────────────────────────────┴─────────┘
 */

import { useCallback, useEffect, useRef, useState } from 'react'
import { useParams, Link } from '@tanstack/react-router'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
    ReactFlow,
    Background,
    Controls,
    MiniMap,
    ReactFlowProvider,
    MarkerType,
    type OnConnect,
    type NodeMouseHandler,
} from '@xyflow/react'
import '@xyflow/react/dist/style.css'

import { Button } from '@/components/ui/button'
import {
    Breadcrumb,
    BreadcrumbItem,
    BreadcrumbLink,
    BreadcrumbList,
    BreadcrumbPage,
    BreadcrumbSeparator,
} from '@/components/ui/breadcrumb'
import { Badge } from '@/components/ui/badge'
import { Switch } from '@/components/ui/switch'
import { Label } from '@/components/ui/label'
import {
    Save,
    Play,
    Loader2,
    ChevronLeft,
    AlertCircle,
    Square,
    History,
} from 'lucide-react'
import { toast } from 'sonner'

import { type FlowGraph, type FlowNode, type FlowEdge, flowTasksRepo } from '@/repo/flow-tasks'
import { useFlowTaskStore } from '../store/flow-task-store'
import { useTheme } from '@/context/theme-provider'
import { Search } from '@/components/search'
import { ThemeSwitch } from '@/components/theme-switch'
import { NodePalette } from '../components/NodePalette'
import { NodeEditorDrawer } from '../components/NodeEditorDrawer'
import { NodeContextMenu } from '../components/NodeContextMenu'
import { DeletableEdge } from '../components/edges/DeletableEdge'
import { VersionHistoryDialog } from '../components/VersionHistoryDialog'

// Node type registry — maps node type strings to components
import { InputNode } from '../components/nodes/InputNode'
import { CleanNode } from '../components/nodes/CleanNode'
import { AggregateNode } from '../components/nodes/AggregateNode'
import { JoinNode } from '../components/nodes/JoinNode'
import { UnionNode } from '../components/nodes/UnionNode'
import { PivotNode } from '../components/nodes/PivotNode'
import { NewRowsNode } from '../components/nodes/NewRowsNode'
import { OutputNode } from '../components/nodes/OutputNode'
import { NoteNode } from '../components/nodes/NoteNode'
import { SqlNode } from '../components/nodes/SqlNode'

const nodeTypes = {
    input: InputNode,
    clean: CleanNode,
    aggregate: AggregateNode,
    join: JoinNode,
    union: UnionNode,
    pivot: PivotNode,
    new_rows: NewRowsNode,
    sql: SqlNode,
    output: OutputNode,
    note: NoteNode,
}

const edgeTypes = {
    default: DeletableEdge,
}

let nodeIdCounter = 1
function generateNodeId(type: string) {
    return `${type}_${nodeIdCounter++}_${Date.now()}`
}

// ─── Inner canvas component (must be inside ReactFlowProvider) ─────────────────

function FlowCanvas({ flowTaskId }: { flowTaskId: number }) {
    const queryClient = useQueryClient()
    const reactFlowRef = useRef<HTMLDivElement>(null)
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const rfInstance = useRef<any>(null)
    const [pollingTaskId, setPollingTaskId] = useState<string | null>(null)
    const [autoSave, setAutoSave] = useState(false)
    const [versionHistoryOpen, setVersionHistoryOpen] = useState(false)
    const autoSaveTimer = useRef<ReturnType<typeof setTimeout> | null>(null)
    const [lastSavedAt, setLastSavedAt] = useState<Date | null>(null)
    const [lastSavedLabel, setLastSavedLabel] = useState('')
    /** Tracks the session ID of the most recently triggered preview. Used to
     *  discard Celery task results that arrive from an older preview request. */
    const previewSessionRef = useRef<string | null>(null)

    // Update relative label every 10 s
    useEffect(() => {
        function formatRelative(d: Date) {
            const diff = Math.floor((Date.now() - d.getTime()) / 1000)
            if (diff < 10) return 'Saved just now'
            if (diff < 60) return `Saved ${diff}s ago`
            const m = Math.floor(diff / 60)
            if (m < 60) return `Saved ${m}m ago`
            const h = Math.floor(m / 60)
            if (h < 24) return `Saved ${h}h ago`
            return `Saved ${d.toLocaleDateString()}`
        }
        if (!lastSavedAt) { setLastSavedLabel(''); return }
        setLastSavedLabel(formatRelative(lastSavedAt))
        const id = setInterval(() => setLastSavedLabel(formatRelative(lastSavedAt)), 10_000)
        return () => clearInterval(id)
    }, [lastSavedAt])

    // Right-click context menu state
    const [ctxMenu, setCtxMenu] = useState<{
        x: number
        y: number
        nodeId: string
        nodeLabel: string
    } | null>(null)

    const {
        nodes,
        edges,
        isDirty,
        setNodes,
        setEdges,
        onNodesChange,
        onEdgesChange,
        onConnect: storeOnConnect,
        addNode,
        selectNode,
        markClean,
        openPreview,
        setPreviewResult,
        setPreviewError,
        setRequestPreview,
        closePreview,
    } = useFlowTaskStore()

    // Always close the preview drawer when entering, relative clean up on exit
    useEffect(() => {
        closePreview()
        return () => selectNode(null)
    }, [])  // eslint-disable-line react-hooks/exhaustive-deps

    // Load graph from API
    const { data: ftResp } = useQuery({
        queryKey: ['flow-tasks', flowTaskId],
        queryFn: () => flowTasksRepo.get(flowTaskId),
    })

    const { isLoading: graphLoading, data: graphData, isError: graphIsError } = useQuery({
        queryKey: ['flow-tasks', flowTaskId, 'graph'],
        queryFn: () => flowTasksRepo.getGraph(flowTaskId),
        retry: false,
    })

    useEffect(() => {
        if (graphData) {
            const graph = graphData.data
            // Backend returns nodes_json / edges_json (not nodes / edges)
            if (Array.isArray(graph.nodes_json)) setNodes(graph.nodes_json as FlowNode[])
            if (Array.isArray(graph.edges_json)) setEdges(graph.edges_json as FlowEdge[])
            markClean()
            // Use updated_at from graph response if available, else now
            const savedAt = graph.updated_at ? new Date(graph.updated_at) : new Date()
            setLastSavedAt(savedAt)
        }
    }, [graphData, setNodes, setEdges, markClean])

    useEffect(() => {
        if (graphIsError) {
            // Graph not yet saved — start empty
            setNodes([])
            setEdges([])
            markClean()
        }
    }, [graphIsError, setNodes, setEdges, markClean])

    // Poll Celery task
    const { data: runStatusData } = useQuery({
        queryKey: ['flow-task-run-status', pollingTaskId],
        queryFn: () => flowTasksRepo.getTaskStatus(pollingTaskId!),
        enabled: !!pollingTaskId,
        refetchInterval: 2000,
        select: (r) => r.data,
    })

    useEffect(() => {
        if (!runStatusData) return
        if (runStatusData.state === 'SUCCESS' || runStatusData.state === 'FAILURE') {
            setPollingTaskId(null)
            setTimeout(() => {
                queryClient.invalidateQueries({ queryKey: ['flow-tasks', flowTaskId] })
            }, 300)
            if (runStatusData.state === 'SUCCESS') {
                toast.success('Run completed successfully!')
            } else {
                toast.error('Run failed.')
            }
        }
    }, [runStatusData, flowTaskId, queryClient])

    // Poll preview task
    const [previewPollingTaskId, setPreviewPollingTaskId] = useState<string | null>(null)

    const { data: previewStatusData } = useQuery({
        queryKey: ['flow-task-preview-status', previewPollingTaskId],
        queryFn: () => flowTasksRepo.getTaskStatus(previewPollingTaskId!),
        enabled: !!previewPollingTaskId,
        refetchInterval: 1500,
        select: (r) => r.data,
    })

    useEffect(() => {
        if (!previewStatusData) return
        if (previewStatusData.state === 'SUCCESS') {
            setPreviewPollingTaskId(null)
            // Guard: ignore stale results from a previous preview session.
            // If the user triggered a new preview before this one resolved,
            // previewSessionRef.current will no longer match the session ID
            // that was active when this particular Celery task was submitted.
            const currentSession = useFlowTaskStore.getState().preview.previewSessionId
            if (currentSession !== previewSessionRef.current) return
            setPreviewResult(previewStatusData.result as never)
        } else if (previewStatusData.state === 'FAILURE') {
            setPreviewPollingTaskId(null)
            const currentSession = useFlowTaskStore.getState().preview.previewSessionId
            if (currentSession !== previewSessionRef.current) return
            setPreviewError((previewStatusData.error as string) || 'Preview failed')
        }
    }, [previewStatusData, setPreviewResult, setPreviewError])

    // ─── Save ──────────────────────────────────────────────────────────────────

    const saveMutation = useMutation({
        mutationFn: (_opts?: { silent?: boolean }) => {
            const graph: FlowGraph = { nodes, edges }
            return flowTasksRepo.saveGraph(flowTaskId, graph)
        },
        onSuccess: (_data, opts) => {
            markClean()
            setLastSavedAt(new Date())
            if (!opts?.silent) toast.success('Graph saved')
            // Invalidate only the flow-task detail (status/name), NOT the graph query.
            // Invalidating the graph query would trigger a refetch that calls setNodes([]),
            // clearing the canvas with the freshly saved data.
            setTimeout(
                () => {
                    queryClient.invalidateQueries({
                        queryKey: ['flow-tasks', flowTaskId],
                        exact: true,
                    })
                    queryClient.invalidateQueries({
                        queryKey: ['flow-task-versions', flowTaskId],
                    })
                },
                300
            )
        },
        onError: () => toast.error('Failed to save graph'),
    })

    // ─── Auto-save: trigger 2 s after last change when enabled ────────────────

    useEffect(() => {
        if (!autoSave || !isDirty) return
        if (autoSaveTimer.current) clearTimeout(autoSaveTimer.current)
        autoSaveTimer.current = setTimeout(() => {
            saveMutation.mutate({ silent: true })
        }, 2000)
        return () => {
            if (autoSaveTimer.current) clearTimeout(autoSaveTimer.current)
        }
        // saveMutation is stable (useMutation), safe to omit from deps
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [autoSave, isDirty, nodes, edges])

    // ─── Run ───────────────────────────────────────────────────────────────────

    const runMutation = useMutation({
        mutationFn: () => flowTasksRepo.run(flowTaskId),
        onSuccess: (resp) => {
            setPollingTaskId(resp.data.celery_task_id)
            toast.info('Flow task started')
        },
        onError: () => toast.error('Failed to trigger run'),
    })

    const cancelMutation = useMutation({
        mutationFn: () => flowTasksRepo.cancelRun(flowTaskId),
        onSuccess: () => {
            setPollingTaskId(null)
            toast.info('Run cancelled')
            setTimeout(() => {
                queryClient.invalidateQueries({ queryKey: ['flow-tasks', flowTaskId], exact: true })
            }, 300)
        },
        onError: () => toast.error('Failed to cancel run'),
    })

    // ─── Drag-and-drop new nodes onto canvas ───────────────────────────────────

    const onDrop = useCallback(
        (event: React.DragEvent) => {
            event.preventDefault()
            if (!rfInstance.current) return

            const nodeType = event.dataTransfer.getData('application/reactflow-node-type') as FlowNode['type']
            const nodeLabel = event.dataTransfer.getData('application/reactflow-node-label')
            if (!nodeType) return

            // screenToFlowPosition takes raw viewport (clientX/Y) coords directly —
            // do NOT subtract container bounds, that causes double-offset and places
            // the node far off-screen (only visible in the minimap).
            const position = rfInstance.current.screenToFlowPosition({
                x: event.clientX,
                y: event.clientY,
            })

            const isNote = nodeType === 'note'
            const newNode: FlowNode = {
                id: generateNodeId(nodeType),
                type: nodeType,
                position,
                data: { label: nodeLabel },
                ...(isNote ? { width: 220, height: 130 } : {}),
            }
            addNode(newNode)
        },
        [rfInstance, addNode]
    )

    const onDragOver = useCallback((event: React.DragEvent) => {
        event.preventDefault()
        event.dataTransfer.dropEffect = 'move'
    }, [])

    // ─── Node click → select ───────────────────────────────────────────────────

    const onNodeClick: NodeMouseHandler = useCallback(
        (_event, node) => {
            selectNode(node.id)
        },
        [selectNode]
    )

    const onPaneClick = useCallback(() => {
        selectNode(null)
        setCtxMenu(null)
    }, [selectNode])

    // Right-click a node → show context menu ONLY (no config panel open)
    const onNodeContextMenu: NodeMouseHandler = useCallback(
        (event, node) => {
            event.preventDefault()
            setCtxMenu({
                x: event.clientX,
                y: event.clientY,
                nodeId: node.id,
                nodeLabel: (node.data.label as string) || node.type || '',
            })
        },
        []
    )

    // ─── Preview a node ────────────────────────────────────────────────────────

    const handlePreviewNode = useCallback(
        async (nodeId: string, nodeLabel: string) => {
            const graphSnapshot: FlowGraph = { nodes, edges }
            // Resolve the node type from the current graph state
            const nodeType = nodes.find((n) => n.id === nodeId)?.type ?? 'unknown'

            // Reset the drawer to a loading state IMMEDIATELY so the user
            // never sees stale data from a previous node while the API call is
            // in-flight. Pass an empty celeryTaskId for now; it will be updated
            // once the response arrives.
            const sessionId = openPreview(nodeId, nodeLabel, nodeType, '')
            previewSessionRef.current = sessionId

            try {
                const resp = await flowTasksRepo.previewNode(flowTaskId, {
                    node_id: nodeId,
                    nodes: graphSnapshot.nodes,
                    edges: graphSnapshot.edges,
                    limit: 500,
                })
                const { task_id } = resp.data
                // Guard: if the user triggered another preview while we were
                // waiting for this API call, bail out so we don't overwrite it.
                if (previewSessionRef.current !== sessionId) return
                setPreviewPollingTaskId(task_id)
            } catch {
                toast.error('Failed to submit preview')
            }
        },
        [nodes, edges, flowTaskId, openPreview]
    )

    // Register handler in store so NodeConfigPanel can call it
    useEffect(() => {
        setRequestPreview(handlePreviewNode)
        return () => setRequestPreview(null)
    }, [handlePreviewNode, setRequestPreview])

    const ft = ftResp?.data
    const isRunning = ft?.status === 'RUNNING' || !!pollingTaskId
    const { resolvedTheme } = useTheme()

    return (
        <div className="flex flex-col h-screen overflow-hidden">

            {/* Top Bar */}
            <div className="flex items-center gap-3 px-4 py-2 border-b border-border bg-background z-10 flex-shrink-0">
                <Breadcrumb>
                    <BreadcrumbList>
                        <BreadcrumbItem>
                            <BreadcrumbLink asChild>
                                <Link to="/flow-tasks" className="flex items-center gap-1">
                                    <ChevronLeft className="h-3 w-3" />
                                    Flow Tasks
                                </Link>
                            </BreadcrumbLink>
                        </BreadcrumbItem>
                        <BreadcrumbSeparator />
                        {ft && (
                            <>
                                <BreadcrumbItem>
                                    <BreadcrumbLink asChild>
                                        <Link
                                            to="/flow-tasks/$flowTaskId"
                                            params={{ flowTaskId: String(flowTaskId) }}
                                        >
                                            {ft.name}
                                        </Link>
                                    </BreadcrumbLink>
                                </BreadcrumbItem>
                                <BreadcrumbSeparator />
                            </>
                        )}
                        <BreadcrumbItem>
                            <BreadcrumbPage>Flow Editor</BreadcrumbPage>
                        </BreadcrumbItem>
                    </BreadcrumbList>
                </Breadcrumb>

                <div className="ml-auto flex items-center gap-2">
                    {lastSavedLabel && (
                        <span className="text-[11px] text-muted-foreground">{lastSavedLabel}</span>
                    )}
                    {isDirty && (
                        <Badge variant="outline" className="text-sky-500 border-sky-500/40 bg-sky-500/10 text-[10px]">
                            <AlertCircle className="h-2.5 w-2.5 mr-1" />
                            Unsaved
                        </Badge>
                    )}
                    <div className="flex items-center gap-1.5 border border-border rounded-md px-2.5 py-1">
                        <Switch
                            id="auto-save"
                            checked={autoSave}
                            onCheckedChange={setAutoSave}
                            className="h-4 w-7 [&_span]:h-3 [&_span]:w-3"
                        />
                        <Label htmlFor="auto-save" className="text-[11px] text-muted-foreground cursor-pointer select-none">
                            {autoSave ? (
                                saveMutation.isPending ? (
                                    <span className="flex items-center gap-1">
                                        <Loader2 className="h-2.5 w-2.5 animate-spin" /> Saving…
                                    </span>
                                ) : 'Auto-save on'
                            ) : 'Auto-save'}
                        </Label>
                    </div>
                    <Button
                        size="sm"
                        variant="outline"
                        onClick={() => saveMutation.mutate({})}
                        disabled={saveMutation.isPending || !isDirty}
                    >
                        {saveMutation.isPending ? (
                            <Loader2 className="h-3.5 w-3.5 animate-spin mr-1.5" />
                        ) : (
                            <Save className="h-3.5 w-3.5 mr-1.5" />
                        )}
                        Save
                    </Button>
                    <Button
                        size="sm"
                        onClick={() => runMutation.mutate()}
                        disabled={runMutation.isPending || isRunning}
                    >
                        {isRunning ? (
                            <Loader2 className="h-3.5 w-3.5 animate-spin mr-1.5" />
                        ) : (
                            <Play className="h-3.5 w-3.5 mr-1.5" />
                        )}
                        Run
                    </Button>
                    {isRunning && (
                        <Button
                            size="sm"
                            variant="destructive"
                            onClick={() => cancelMutation.mutate()}
                            disabled={cancelMutation.isPending}
                        >
                            {cancelMutation.isPending ? (
                                <Loader2 className="h-3.5 w-3.5 animate-spin mr-1.5" />
                            ) : (
                                <Square className="h-3.5 w-3.5 mr-1.5" />
                            )}
                            Cancel
                        </Button>
                    )}
                    <Button
                        size="sm"
                        variant="outline"
                        onClick={() => setVersionHistoryOpen(true)}
                    >
                        <History className="h-3.5 w-3.5 mr-1.5" />
                        History
                    </Button>
                    <div className="w-px h-5 bg-border" />
                    <Search />
                    <ThemeSwitch />
                </div>
            </div>

            {/* Body */}
            <div className="flex flex-1 overflow-hidden">
                {/* Left Palette */}
                <div className="w-56 flex-shrink-0 border-r border-border">
                    <NodePalette />
                </div>

                {/* Canvas + overlaid config panel */}
                <div
                    className="flex-1 relative"
                    ref={reactFlowRef}
                    onDrop={onDrop}
                    onDragOver={onDragOver}
                >
                    {graphLoading ? (
                        <div className="flex items-center justify-center h-full">
                            <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
                        </div>
                    ) : (
                        <ReactFlow
                            nodes={nodes}
                            edges={edges}
                            nodeTypes={nodeTypes}
                            edgeTypes={edgeTypes}
                            colorMode={resolvedTheme}
                            onNodesChange={onNodesChange}
                            onEdgesChange={onEdgesChange}
                            onConnect={storeOnConnect as OnConnect}
                            onInit={(inst) => { rfInstance.current = inst }}
                            onNodeClick={onNodeClick}
                            onPaneClick={onPaneClick}
                            onNodeContextMenu={onNodeContextMenu}
                            defaultViewport={{ x: 80, y: 80, zoom: 0.85 }}
                            minZoom={0.25}
                            maxZoom={2}
                            fitView={false}
                            deleteKeyCode="Delete"
                            defaultEdgeOptions={{
                                animated: true,
                                style: { stroke: '#6366f1', strokeWidth: 2 },
                                markerEnd: {
                                    type: MarkerType.ArrowClosed,
                                    width: 18,
                                    height: 18,
                                    color: '#6366f1',
                                },
                            }}
                        >
                            <Background gap={16} />
                            <Controls />
                            <MiniMap
                                nodeColor={(n) => {
                                    if (n.type === 'input') return '#10b981'
                                    if (n.type === 'output') return '#f43f5e'
                                    return '#6366f1'
                                }}
                                className="rounded-md border border-border"
                            />
                        </ReactFlow>
                    )}

                    {/* Bottom Drawer for config and preview */}
                    <NodeEditorDrawer />

                    {/* Node right-click context menu */}
                    {ctxMenu && (
                        <NodeContextMenu
                            x={ctxMenu.x}
                            y={ctxMenu.y}
                            nodeId={ctxMenu.nodeId}
                            nodeLabel={ctxMenu.nodeLabel}
                            onEdit={() => selectNode(ctxMenu.nodeId)}
                            onPreview={() =>
                                handlePreviewNode(ctxMenu.nodeId, ctxMenu.nodeLabel)
                            }
                            onDelete={() => {
                                useFlowTaskStore.getState().removeNode(ctxMenu.nodeId)
                            }}
                            onClose={() => setCtxMenu(null)}
                        />
                    )}
                </div>
            </div>

            {/* Version History Dialog */}
            <VersionHistoryDialog
                open={versionHistoryOpen}
                onOpenChange={setVersionHistoryOpen}
                flowTaskId={flowTaskId}
            />
        </div>
    )
}

// ─── Page wrapper with ReactFlowProvider ───────────────────────────────────────

export default function FlowTaskFlowPage() {
    const { flowTaskId } = useParams({
        from: '/_authenticated/flow-tasks/$flowTaskId/flow',
    })
    const id = parseInt(flowTaskId)

    return (
        <ReactFlowProvider>
            <FlowCanvas flowTaskId={id} />
        </ReactFlowProvider>
    )
}
