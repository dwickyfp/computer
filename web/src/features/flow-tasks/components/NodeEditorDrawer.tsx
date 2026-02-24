import { useCallback, useEffect, useRef, useState } from 'react'
import { useParams } from '@tanstack/react-router'
import { useFlowTaskStore } from '../store/flow-task-store'
import { Button } from '@/components/ui/button'
import {
    X,
    Loader2,
    AlertCircle,
    GripHorizontal,
    Table2,
    BarChart2,
    Layers2,
    Settings2,
} from 'lucide-react'
import { cn } from '@/lib/utils'

// Types & Recharts
import type { ColumnProfile } from '@/repo/flow-tasks'

// Components
import { NodeTypeConfig } from './NodeTypeConfig'
// Rename PreviewDrawer's table/chart builders if we extract them or just inline them here.
// For now, let's assume we can import or define them in this file.
import { PreviewTable, ChartBuilder, ProfilingResults } from './PreviewDrawerComponents'

const MIN_HEIGHT = 160
const MAX_HEIGHT = 800
const DEFAULT_HEIGHT = 380

// ─── Node type badge ───────────────────────────────────────────────────────────

const NODE_TYPE_COLORS: Record<string, string> = {
    input: 'bg-blue-100 text-blue-700 dark:bg-blue-900/40 dark:text-blue-300',
    clean: 'bg-green-100 text-green-700 dark:bg-green-900/40 dark:text-green-300',
    aggregate: 'bg-violet-100 text-violet-700 dark:bg-violet-900/40 dark:text-violet-300',
    join: 'bg-orange-100 text-orange-700 dark:bg-orange-900/40 dark:text-orange-300',
    union: 'bg-teal-100 text-teal-700 dark:bg-teal-900/40 dark:text-teal-300',
    pivot: 'bg-pink-100 text-pink-700 dark:bg-pink-900/40 dark:text-pink-300',
    new_rows: 'bg-yellow-100 text-yellow-700 dark:bg-yellow-900/40 dark:text-yellow-300',
    output: 'bg-gray-100 text-gray-700 dark:bg-gray-800 dark:text-gray-300',
}

function NodeTypeBadge({ nodeType }: { nodeType: string }) {
    const colorClass = NODE_TYPE_COLORS[nodeType] ?? 'bg-muted text-muted-foreground'
    return (
        <span className={cn('inline-flex items-center rounded px-1.5 py-0.5 text-[10px] font-semibold uppercase tracking-wide', colorClass)}>
            {nodeType.replace('_', ' ')}
        </span>
    )
}

// ─── Main drawer ───────────────────────────────────────────────────────────────

export function NodeEditorDrawer() {
    const {
        nodes,
        selectedNodeId,
        selectNode,
        updateNodeData,
        preview,
        closePreview,
        activeDrawerTab: tab,
        setActiveDrawerTab: setTab,
        requestPreview
    } = useFlowTaskStore()

    const [height, setHeight] = useState(DEFAULT_HEIGHT)
    const [visible, setVisible] = useState(false)

    // Lift state to persist across tab switches for charts
    const [chartCfg, setChartCfg] = useState<any>(null)
    const lastColsParams = useRef<string>('')

    const dragging = useRef(false)
    const startY = useRef(0)
    const startH = useRef(0)

    // Extract flow task ID for schema resolving
    let flowTaskId: number | null = null
    try {
        const params = useParams({ strict: false }) as Record<string, string>
        const raw = params['flowTaskId'] ?? params['flow_task_id'] ?? params['id'] ?? ''
        flowTaskId = raw ? parseInt(raw) : null
    } catch {
        flowTaskId = null
    }

    const selectedNode = (nodes ?? []).find((n) => n.id === selectedNodeId)
    // Drawer is active if a node is selected OR if preview is forcefully open
    const isActive = !!selectedNode || preview.isOpen

    // Trigger enter animation
    useEffect(() => {
        if (isActive) {
            requestAnimationFrame(() => setVisible(true))
        } else {
            setVisible(false)
            // Reset state
            setChartCfg(null)
            lastColsParams.current = ''
        }
    }, [isActive])

    // Auto-trigger preview when tab is a preview tab and the node hasn't been previewed yet
    // (e.g. user selects a different node while already viewing Data Preview)
    useEffect(() => {
        if (selectedNode?.type === 'note' && tab !== 'config') {
            setTab('config')
            return
        }
        if (tab !== 'config' && selectedNode && requestPreview) {
            if (preview.nodeId !== selectedNode.id && !preview.isLoading) {
                requestPreview(selectedNode.id, (selectedNode.data.label as string) || selectedNode.type)
            }
        }
    }, [tab, selectedNode?.id, selectedNode?.type, requestPreview, preview.nodeId, preview.isLoading, setTab])

    const handleTabClick = (newTab: 'config' | 'table' | 'chart' | 'profiling') => {
        if (newTab !== 'config' && selectedNode && requestPreview) {
            // Force refetch if switching from config, ensuring latest data is shown
            if (tab === 'config') {
                requestPreview(selectedNode.id, (selectedNode.data.label as string) || selectedNode.type)
            }
        }
        setTab(newTab)
    }

    // Drag to resize logic
    const onMouseDown = useCallback((e: React.MouseEvent) => {
        e.preventDefault()
        dragging.current = true
        startY.current = e.clientY
        startH.current = height
        document.body.style.cursor = 'ns-resize'
    }, [height])

    useEffect(() => {
        const onMove = (e: MouseEvent) => {
            if (!dragging.current) return
            const delta = startY.current - e.clientY
            setHeight(Math.min(MAX_HEIGHT, Math.max(MIN_HEIGHT, startH.current + delta)))
        }
        const onUp = () => {
            dragging.current = false
            document.body.style.cursor = ''
        }
        window.addEventListener('mousemove', onMove)
        window.addEventListener('mouseup', onUp)
        return () => {
            window.removeEventListener('mousemove', onMove)
            window.removeEventListener('mouseup', onUp)
        }
    }, [])

    if (!isActive) return null

    // Node data for Config Tab
    const update = selectedNode ? (patch: any) => updateNodeData(selectedNode.id, patch) : () => { }

    // Preview data for other tabs
    const cols = preview.result?.columns ?? []
    const rows = preview.result?.rows ?? []

    // Helper for labels
    const nodeLabelToUse = selectedNode
        ? (selectedNode.data.label as string) || selectedNode.type
        : preview.nodeLabel
    const nodeTypeToUse = selectedNode ? selectedNode.type : preview.nodeType

    return (
        <div
            className={cn(
                'absolute bottom-0 left-4 right-4 z-20 border-t border-x border-border bg-background shadow-[0_-8px_30px_rgb(0,0,0,0.12)] rounded-t-xl flex flex-col',
                'transition-transform duration-300 ease-out',
                visible ? 'translate-y-0' : 'translate-y-full'
            )}
            style={{ height }}
        >
            {/* Resize handle */}
            <div
                onMouseDown={onMouseDown}
                className="flex items-center justify-center h-3 cursor-ns-resize hover:bg-muted/50 transition-colors group shrink-0"
            >
                <GripHorizontal className="h-3 w-3 text-muted-foreground/40 group-hover:text-muted-foreground transition-colors" />
            </div>

            {/* Header / Tabs */}
            <div className="flex items-center justify-between px-4 py-2 border-b border-border/50 shrink-0 bg-muted/10">
                <div className="flex items-center gap-3">
                    {/* Node Info */}
                    <div className="flex items-center gap-2 pr-4 border-r border-border/50 mr-2">
                        {nodeTypeToUse && <NodeTypeBadge nodeType={nodeTypeToUse} />}
                        <span className="text-sm font-semibold truncate max-w-[200px]">
                            {nodeLabelToUse}
                        </span>
                    </div>

                    {/* Tab Navigation */}
                    <div className="flex items-center gap-1 p-0.5 rounded-md bg-muted/60">
                        <button
                            className={cn(
                                'flex items-center gap-1.5 px-3 py-1.5 rounded text-xs font-medium transition-all',
                                tab === 'config'
                                    ? 'bg-background text-foreground shadow-sm ring-1 ring-border/40'
                                    : 'text-muted-foreground hover:text-foreground'
                            )}
                            onClick={() => handleTabClick('config')}
                        >
                            <Settings2 className="h-3.5 w-3.5" />
                            Configuration
                        </button>
                        {nodeTypeToUse !== 'note' && (
                            <>
                                <button
                                    className={cn(
                                        'flex items-center gap-1.5 px-3 py-1.5 rounded text-xs font-medium transition-all',
                                        tab === 'table'
                                            ? 'bg-background text-foreground shadow-sm ring-1 ring-border/40'
                                            : 'text-muted-foreground hover:text-foreground'
                                    )}
                                    onClick={() => handleTabClick('table')}
                                >
                                    <Table2 className="h-3.5 w-3.5" />
                                    Data Preview
                                </button>
                                <button
                                    className={cn(
                                        'flex items-center gap-1.5 px-3 py-1.5 rounded text-xs font-medium transition-all',
                                        tab === 'chart'
                                            ? 'bg-background text-foreground shadow-sm ring-1 ring-border/40'
                                            : 'text-muted-foreground hover:text-foreground',
                                        !preview.result && tab !== 'config' && 'opacity-40 pointer-events-none'
                                    )}
                                    onClick={() => handleTabClick('chart')}
                                >
                                    <BarChart2 className="h-3.5 w-3.5" />
                                    Chart
                                </button>
                                <button
                                    className={cn(
                                        'flex items-center gap-1.5 px-3 py-1.5 rounded text-xs font-medium transition-all',
                                        tab === 'profiling'
                                            ? 'bg-background text-foreground shadow-sm ring-1 ring-border/40'
                                            : 'text-muted-foreground hover:text-foreground',
                                        !(preview.result)?.profile && tab !== 'config' && 'opacity-40 pointer-events-none'
                                    )}
                                    onClick={() => handleTabClick('profiling')}
                                >
                                    <Layers2 className="h-3.5 w-3.5" />
                                    Profiling
                                </button>
                            </>
                        )}
                    </div>

                    {/* Meta info for preview */}
                    {tab !== 'config' && preview.result && (
                        <span className="text-[11px] text-muted-foreground ml-2">
                            {preview.result.row_count} row{preview.result.row_count !== 1 ? 's' : ''}, {preview.result.elapsed_ms}ms
                        </span>
                    )}
                </div>

                <Button
                    variant="ghost"
                    size="icon"
                    className="h-7 w-7 text-muted-foreground hover:bg-muted"
                    onClick={() => {
                        selectNode(null)
                        closePreview()
                    }}
                >
                    <X className="h-4 w-4" />
                </Button>
            </div>

            {/* Content Body */}
            <div className="flex-1 overflow-hidden relative bg-background">
                {/* CONFIGURATION TAB */}
                {tab === 'config' && selectedNode && (
                    <div className="absolute inset-0 overflow-y-auto px-6 py-4">
                        <div className="max-w-5xl mx-auto space-y-6">
                            <NodeTypeConfig
                                type={selectedNode.type}
                                data={selectedNode.data}
                                update={update}
                                nodeId={selectedNode.id}
                                flowTaskId={flowTaskId}
                            />
                        </div>
                    </div>
                )}

                {/* PREVIEW TABS LOADING/ERROR */}
                {tab !== 'config' && preview.isLoading && (
                    <div className="absolute inset-0 flex items-center justify-center gap-2 text-muted-foreground bg-background/50 backdrop-blur-sm z-10">
                        <Loader2 className="h-5 w-5 animate-spin" />
                        <span className="text-sm font-medium">Running preview…</span>
                    </div>
                )}

                {tab !== 'config' && preview.error && !preview.isLoading && (
                    <div className="absolute inset-0 p-6 overflow-auto">
                        <div className="flex items-start gap-3 p-4 rounded-lg bg-destructive/10 text-destructive shadow-sm border border-destructive/20 max-w-4xl mx-auto">
                            <AlertCircle className="h-5 w-5 mt-0.5 flex-shrink-0" />
                            <div>
                                <h4 className="font-semibold text-sm mb-1">Preview failed</h4>
                                <pre className="text-xs whitespace-pre-wrap font-mono leading-relaxed">{preview.error}</pre>
                            </div>
                        </div>
                    </div>
                )}

                {/* EMPTY STATE FOR PREVIEW */}
                {tab !== 'config' && !preview.result && !preview.isLoading && !preview.error && (
                    <div className="absolute inset-0 flex items-center justify-center">
                        <div className="text-center">
                            <Table2 className="h-8 w-8 text-muted-foreground/30 mx-auto mb-3" />
                            <p className="text-sm text-muted-foreground">Click the "Preview" button on a node to see its data here.</p>
                        </div>
                    </div>
                )}

                {/* RESULT TABS */}
                {tab !== 'config' && preview.result && !preview.isLoading && (
                    <div className="absolute inset-0">
                        {tab === 'table' && (
                            <div className="h-full overflow-auto">
                                <PreviewTable
                                    columns={cols}
                                    columnTypes={preview.result.column_types}
                                    rows={rows}
                                />
                            </div>
                        )}
                        {tab === 'chart' && (
                            <ChartBuilder
                                columns={cols}
                                rows={rows}
                                height={height - 52} // Subtract header/handle approx height
                                cfg={chartCfg}
                                onUpdate={setChartCfg}
                            />
                        )}
                        {tab === 'profiling' && preview.result?.profile && (
                            <div className="h-full overflow-auto p-4 bg-muted/5">
                                <div className="max-w-5xl mx-auto">
                                    <ProfilingResults profile={preview.result.profile as ColumnProfile[]} />
                                </div>
                            </div>
                        )}
                    </div>
                )}
            </div>
        </div>
    )
}
