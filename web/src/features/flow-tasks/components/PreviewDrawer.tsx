/**
 * PreviewDrawer — bottom slide-up panel that shows the node preview result.
 * Supports drag-to-resize via the top resize handle.
 * Has two tabs: Table (raw data) and Chart (visual builder using recharts).
 */

import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { useFlowTaskStore } from '../store/flow-task-store'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import {
    Select,
    SelectContent,
    SelectItem,
    SelectTrigger,
    SelectValue,
} from '@/components/ui/select'
import {
    X,
    Loader2,
    AlertCircle,
    GripHorizontal,
    Table2,
    BarChart2,
    TrendingUp,
    AreaChart,
    PieChart as PieChartIcon,
    Layers2,
} from 'lucide-react'
import { cn } from '@/lib/utils'
import {
    BarChart,
    Bar,
    LineChart,
    Line,
    AreaChart as RAreaChart,
    Area,
    PieChart,
    Pie,
    Cell,
    XAxis,
    YAxis,
    CartesianGrid,
    Tooltip,
    Legend,
    ResponsiveContainer,
} from 'recharts'

const MIN_HEIGHT = 160
const MAX_HEIGHT = 640
const DEFAULT_HEIGHT = 280

// ─── Types ─────────────────────────────────────────────────────────────────────

type ChartType = 'bar' | 'stacked_bar' | 'line' | 'area' | 'pie'
type AggFunc   = 'none' | 'count' | 'min' | 'max' | 'sum' | 'median' | 'average'
type SortMode  = 'none' | 'x_asc' | 'x_desc' | 'y_asc' | 'y_desc'

interface ChartConfig {
    chartType: ChartType
    xColumn: string
    yColumns: string[]
    aggregate: AggFunc
    groupBy: string
    sort: SortMode
    limit: LimitMode
    xLabel: string
    yLabel: string
    showXLabel: boolean
    showYLabel: boolean
}

// ─── Constants ─────────────────────────────────────────────────────────────────

const CHART_PALETTE = [
    '#6366f1', '#10b981', '#f59e0b', '#ef4444', '#3b82f6',
    '#8b5cf6', '#ec4899', '#14b8a6', '#f97316', '#84cc16',
]

const CHART_TYPES: { value: ChartType; label: string; icon: React.ReactNode }[] = [
    { value: 'bar',         label: 'Bar chart',          icon: <BarChart2 className="h-3.5 w-3.5" /> },
    { value: 'stacked_bar', label: 'Stacked bar chart',  icon: <Layers2 className="h-3.5 w-3.5" /> },
    { value: 'line',        label: 'Line chart',         icon: <TrendingUp className="h-3.5 w-3.5" /> },
    { value: 'area',        label: 'Area chart',         icon: <AreaChart className="h-3.5 w-3.5" /> },
    { value: 'pie',         label: 'Pie chart',          icon: <PieChartIcon className="h-3.5 w-3.5" /> },
]

const AGG_FUNCS: { value: AggFunc; label: string }[] = [
    { value: 'none',    label: 'None'    },
    { value: 'count',   label: 'Count'   },
    { value: 'min',     label: 'Min'     },
    { value: 'max',     label: 'Max'     },
    { value: 'sum',     label: 'Sum'     },
    { value: 'median',  label: 'Median'  },
    { value: 'average', label: 'Average' },
]

const SORT_MODES: { value: SortMode; label: string }[] = [
    { value: 'none',   label: 'None'          },
    { value: 'x_asc',  label: 'By X ascending'  },
    { value: 'x_desc', label: 'By X descending' },
    { value: 'y_asc',  label: 'By Y ascending'  },
    { value: 'y_desc', label: 'By Y descending' },
]

type LimitMode = 'none' | 'top_10' | 'top_50' | 'top_100' | 'bottom_10' | 'bottom_50' | 'bottom_100'

const LIMIT_MODES: { value: LimitMode; label: string }[] = [
    { value: 'none',       label: 'None'       },
    { value: 'top_10',     label: 'Top 10'     },
    { value: 'top_50',     label: 'Top 50'     },
    { value: 'top_100',    label: 'Top 100'    },
    { value: 'bottom_10',  label: 'Bottom 10'  },
    { value: 'bottom_50',  label: 'Bottom 50'  },
    { value: 'bottom_100', label: 'Bottom 100' },
]

// ─── Node type badge ───────────────────────────────────────────────────────────

const NODE_TYPE_COLORS: Record<string, string> = {
    input:     'bg-blue-100 text-blue-700 dark:bg-blue-900/40 dark:text-blue-300',
    clean:     'bg-green-100 text-green-700 dark:bg-green-900/40 dark:text-green-300',
    aggregate: 'bg-violet-100 text-violet-700 dark:bg-violet-900/40 dark:text-violet-300',
    join:      'bg-orange-100 text-orange-700 dark:bg-orange-900/40 dark:text-orange-300',
    union:     'bg-teal-100 text-teal-700 dark:bg-teal-900/40 dark:text-teal-300',
    pivot:     'bg-pink-100 text-pink-700 dark:bg-pink-900/40 dark:text-pink-300',
    new_rows:  'bg-yellow-100 text-yellow-700 dark:bg-yellow-900/40 dark:text-yellow-300',
    output:    'bg-gray-100 text-gray-700 dark:bg-gray-800 dark:text-gray-300',
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

export function PreviewDrawer() {
    const { preview, closePreview, configPanelWidth, selectedNodeId } = useFlowTaskStore()
    const [height, setHeight]   = useState(DEFAULT_HEIGHT)
    const [visible, setVisible] = useState(false)
    const [tab, setTab]         = useState<'table' | 'chart' | 'profiling'>('table')
    
    // Lift state to persist across tab switches
    const [chartCfg, setChartCfg] = useState<ChartConfig>(() => defaultConfig([]))
    // Track columns to detect node changes
    const lastColsParams = useRef<string>('')

    const dragging  = useRef(false)
    const startY    = useRef(0)
    const startH    = useRef(0)

    // Trigger enter animation & reset state on close
    useEffect(() => {
        if (preview.isOpen) {
            setHeight(DEFAULT_HEIGHT)
            setTab('table')
            requestAnimationFrame(() => setVisible(true))
        } else {
            setVisible(false)
            // Reset chart config when drawer closes
            setChartCfg(defaultConfig([]))
            lastColsParams.current = ''
        }
    }, [preview.isOpen])

    // Update chart config when columns change (e.g. new node preview)
    useEffect(() => {
        if (!preview.result) return
        const cols = preview.result.columns
        const key = JSON.stringify(cols)
        if (key !== lastColsParams.current) {
            lastColsParams.current = key
            setChartCfg(defaultConfig(cols))
        }
    }, [preview.result])

    const onMouseDown = useCallback((e: React.MouseEvent) => {
        e.preventDefault()
        dragging.current = true
        startY.current = e.clientY
        startH.current = height
    }, [height])

    useEffect(() => {
        const onMove = (e: MouseEvent) => {
            if (!dragging.current) return
            const delta = startY.current - e.clientY
            setHeight(Math.min(MAX_HEIGHT, Math.max(MIN_HEIGHT, startH.current + delta)))
        }
        const onUp = () => { dragging.current = false }
        window.addEventListener('mousemove', onMove)
        window.addEventListener('mouseup', onUp)
        return () => {
            window.removeEventListener('mousemove', onMove)
            window.removeEventListener('mouseup', onUp)
        }
    }, [])

    if (!preview.isOpen) return null

    const rightOffset = selectedNodeId ? configPanelWidth : 0
    const cols = preview.result?.columns ?? []
    const rows = preview.result?.rows    ?? []

    return (
        <div
            className={cn(
                'absolute bottom-0 left-0 right-0 z-10 border-t border-x border-border bg-background shadow-lg mx-4 rounded-t-xl',
                'transition-transform duration-300 ease-out',
                visible ? 'translate-y-0' : 'translate-y-full'
            )}
            style={{ right: rightOffset }}
        >
            {/* Resize handle */}
            <div
                onMouseDown={onMouseDown}
                className="flex items-center justify-center h-3 cursor-ns-resize hover:bg-muted/50 transition-colors group shrink-0"
            >
                <GripHorizontal className="h-3 w-3 text-muted-foreground/50 group-hover:text-muted-foreground transition-colors" />
            </div>

            {/* Header */}
            <div className="flex items-center justify-between px-4 py-1.5 border-b border-border/50 shrink-0">
                <div className="flex items-center gap-3">
                    {/* Tab switcher */}
                    <div className="flex items-center gap-0.5 p-0.5 rounded-md bg-muted">
                        <button
                            className={cn(
                                'flex items-center gap-1.5 px-2.5 py-1 rounded text-[11px] font-medium transition-all',
                                tab === 'table'
                                    ? 'bg-background text-foreground shadow-sm ring-1 ring-border/40'
                                    : 'text-muted-foreground hover:text-foreground'
                            )}
                            onClick={() => setTab('table')}
                        >
                            <Table2 className="h-3 w-3" />
                            Table
                        </button>
                        <button
                            className={cn(
                                'flex items-center gap-1.5 px-2.5 py-1 rounded text-[11px] font-medium transition-all',
                                tab === 'chart'
                                    ? 'bg-background text-foreground shadow-sm ring-1 ring-border/40'
                                    : 'text-muted-foreground hover:text-foreground',
                                !preview.result && 'opacity-40 pointer-events-none'
                            )}
                            onClick={() => setTab('chart')}
                        >
                            <BarChart2 className="h-3 w-3" />
                            Chart
                        </button>
                        <button
                            className={cn(
                                'flex items-center gap-1.5 px-2.5 py-1 rounded text-[11px] font-medium transition-all',
                                tab === 'profiling'
                                    ? 'bg-background text-foreground shadow-sm ring-1 ring-border/40'
                                    : 'text-muted-foreground hover:text-foreground',
                                !(preview.result)?.profile && 'opacity-40 pointer-events-none'
                            )}
                            onClick={() => setTab('profiling')}
                        >
                            <Layers2 className="h-3 w-3" />
                            Profiling
                        </button>
                    </div>

                    {/* Meta info */}
                    {preview.nodeType && <NodeTypeBadge nodeType={preview.nodeType} />}
                    {preview.nodeLabel && (
                        <span className="text-xs text-muted-foreground">— {preview.nodeLabel}</span>
                    )}
                    {preview.result && (
                        <span className="text-xs text-muted-foreground">
                            ({preview.result.row_count} row{preview.result.row_count !== 1 ? 's' : ''},&nbsp;
                            {preview.result.elapsed_ms}ms)
                        </span>
                    )}
                </div>
                <Button variant="ghost" size="icon" className="h-6 w-6" onClick={closePreview}>
                    <X className="h-3.5 w-3.5" />
                </Button>
            </div>

            {/* Body */}
            <div className="overflow-auto" style={{ height }}>
                {preview.isLoading && (
                    <div className="flex items-center justify-center h-full gap-2 text-muted-foreground">
                        <Loader2 className="h-4 w-4 animate-spin" />
                        <span className="text-sm">Running preview…</span>
                    </div>
                )}

                {preview.error && !preview.isLoading && (
                    <div className="flex items-start gap-2 p-3 rounded-md bg-destructive/10 text-destructive m-2">
                        <AlertCircle className="h-4 w-4 mt-0.5 flex-shrink-0" />
                        <pre className="text-xs whitespace-pre-wrap font-mono">{preview.error}</pre>
                    </div>
                )}

                {preview.result && !preview.isLoading && (
                    <>
                        {tab === 'table' && (
                            <div className="overflow-auto h-full">
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
                                height={height}
                                cfg={chartCfg}
                                onUpdate={setChartCfg}
                            />
                        )}
                        {tab === 'profiling' && preview.result?.profile && (
                            <ProfilingResults profile={preview.result.profile as ColumnProfile[]} />
                        )}
                    </>
                )}
            </div>
        </div>
    )
}

// ─── Table ─────────────────────────────────────────────────────────────────────

interface PreviewTableProps {
    columns: string[]
    columnTypes: Record<string, string>
    rows: unknown[][]
}

function PreviewTable({ columns, columnTypes, rows }: PreviewTableProps) {
    if (columns.length === 0) {
        return (
            <div className="flex items-center justify-center h-full text-muted-foreground text-sm">
                No columns returned.
            </div>
        )
    }

    return (
        <table className="text-xs border-collapse w-full">
            <thead>
                <tr className="border-b border-border bg-muted/90 sticky top-0 z-20 shadow-sm">
                    {columns.map((col) => (
                        <th key={col} className="px-2 py-1 text-left font-semibold whitespace-nowrap">
                            <div>{col}</div>
                            {columnTypes[col] && (
                                <div className="font-normal text-muted-foreground text-[9px]">
                                    {columnTypes[col]}
                                </div>
                            )}
                        </th>
                    ))}
                </tr>
            </thead>
            <tbody>
                {rows.map((row, ri) => (
                    <tr
                        key={ri}
                        className={cn(
                            'border-b border-border/30',
                            ri % 2 === 0 ? 'bg-background' : 'bg-muted/20'
                        )}
                    >
                        {(row as unknown[]).map((cell, ci) => (
                            <td key={ci} className="px-2 py-1 whitespace-nowrap font-mono">
                                {cell === null || cell === undefined ? (
                                    <span className="italic text-muted-foreground">NULL</span>
                                ) : (
                                    String(cell)
                                )}
                            </td>
                        ))}
                    </tr>
                ))}
            </tbody>
        </table>
    )
}

// ─── Chart Builder ─────────────────────────────────────────────────────────────

interface ChartBuilderProps {
    columns: string[]
    rows: unknown[][]
    height: number
    cfg: ChartConfig
    onUpdate: (cfg: ChartConfig) => void
}

function defaultConfig(columns: string[]): ChartConfig {
    return {
        chartType: 'bar',
        xColumn: columns[0] ?? '',
        yColumns: columns.length > 1 ? [columns[1]] : [columns[0] ?? ''],
        aggregate: 'sum',
        groupBy: '',
        sort: 'none',
        limit: 'none',
        xLabel: '',
        yLabel: '',
        showXLabel: true,
        showYLabel: true,
    }
}

function applyAgg(values: number[], func: AggFunc): number {
    if (!values.length) return 0
    switch (func) {
        case 'count':   return values.length
        case 'min':     return Math.min(...values)
        case 'max':     return Math.max(...values)
        case 'sum':     return values.reduce((a, b) => a + b, 0)
        case 'median': {
            const sorted = [...values].sort((a, b) => a - b)
            const mid = Math.floor(sorted.length / 2)
            return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2
        }
        case 'average': return values.reduce((a, b) => a + b, 0) / values.length
        default: return values[values.length - 1] // none → last value
    }
}

function buildChartData(
    columns: string[],
    rows: unknown[][],
    cfg: ChartConfig
): Record<string, unknown>[] {
    const xIdx = columns.indexOf(cfg.xColumn)
    if (xIdx < 0) return []

    const yIdxs = cfg.yColumns.map((c) => columns.indexOf(c)).filter((i) => i >= 0)
    if (!yIdxs.length) return []

    // Group by X (or xColumn + groupBy)
    const grouped = new Map<string, Record<string, number[]>>()

    for (const row of rows) {
        const xVal   = String(row[xIdx] ?? '')
        const yGroup: Record<string, number[]> = grouped.get(xVal) ?? {}

        for (const yi of yIdxs) {
            const yCol = columns[yi]
            const num  = Number(row[yi])
            if (!yGroup[yCol]) yGroup[yCol] = []
            if (!isNaN(num)) yGroup[yCol].push(num)
        }
        grouped.set(xVal, yGroup)
    }

    let data = Array.from(grouped.entries()).map(([xVal, yGroup]) => {
        const entry: Record<string, unknown> = { __x: xVal }
        for (const yCol of cfg.yColumns) {
            const vals = yGroup[yCol] ?? []
            entry[yCol] = cfg.aggregate === 'none' ? vals[0] ?? 0 : applyAgg(vals, cfg.aggregate)
        }
        return entry
    })

    // Sort
    if (cfg.sort !== 'none') {
        const firstY = cfg.yColumns[0]
        if (cfg.sort === 'x_asc')  data.sort((a, b) => String(a.__x).localeCompare(String(b.__x)))
        if (cfg.sort === 'x_desc') data.sort((a, b) => String(b.__x).localeCompare(String(a.__x)))
        if (cfg.sort === 'y_asc')  data.sort((a, b) => (Number(a[firstY]) - Number(b[firstY])))
        if (cfg.sort === 'y_desc') data.sort((a, b) => (Number(b[firstY]) - Number(a[firstY])))
    }

    // Limit
    if (cfg.limit && cfg.limit !== 'none') {
        const [dir, countStr] = cfg.limit.split('_')
        const count = parseInt(countStr, 10)
        if (dir === 'top') {
            data = data.slice(0, count)
        } else {
            // bottom means slice from end
            data = data.slice(-count)
        }
    }

    return data
}

function ChartBuilder({ columns, rows, height, cfg, onUpdate }: ChartBuilderProps) {
    // Reset when columns change handled by parent now

    const update = (patch: Partial<ChartConfig>) =>
        onUpdate({ ...cfg, ...patch })

    const chartData = useMemo(
        () => buildChartData(columns, rows, cfg),
        [columns, rows, cfg]
    )

    const chartH = height - 8 // full-height available for chart panel

    return (
        <div className="flex h-full">
            {/* ── Config panel (left 260px, always scrollable) ── */}
            <div
                className="w-[260px] shrink-0 border-r border-border overflow-y-scroll overflow-x-hidden bg-muted/5"
                style={{ WebkitOverflowScrolling: 'touch', scrollBehavior: 'smooth' }}
            >
                <div className="p-4 space-y-6">

                    {/* Chart Type */}
                    <div className="space-y-1.5">
                        <Label className="text-[10px] text-muted-foreground font-semibold uppercase tracking-wider">Chart Type</Label>
                        <Select value={cfg.chartType} onValueChange={(v) => update({ chartType: v as ChartType })}>
                            <SelectTrigger className="h-8 text-xs bg-background/50">
                                <SelectValue />
                            </SelectTrigger>
                            <SelectContent>
                                {CHART_TYPES.map((ct) => (
                                    <SelectItem key={ct.value} value={ct.value}>
                                        <div className="flex items-center gap-2">
                                            {ct.icon}
                                            {ct.label}
                                        </div>
                                    </SelectItem>
                                ))}
                            </SelectContent>
                        </Select>
                    </div>

                    <div className="h-px bg-border/40" />

                    {/* Data Configuration */}
                    <div className="space-y-4">
                        {/* X-Axis */}
                        <div className="space-y-1.5">
                            <Label className="text-[10px] text-muted-foreground font-semibold uppercase tracking-wider">X-Axis</Label>
                            <Select value={cfg.xColumn} onValueChange={(v) => update({ xColumn: v })}>
                                <SelectTrigger className="h-8 text-xs bg-background/50">
                                    <SelectValue placeholder="Select column" />
                                </SelectTrigger>
                                <SelectContent>
                                    {columns.map((c) => (
                                        <SelectItem key={c} value={c}>{c}</SelectItem>
                                    ))}
                                </SelectContent>
                            </Select>
                        </div>

                        {/* Y-Axis (Multiple) */}
                        <div className="space-y-2">
                            <div className="flex items-center justify-between">
                                <Label className="text-[10px] text-muted-foreground font-semibold uppercase tracking-wider">Y-Axis</Label>
                                <button
                                    className="text-[10px] font-medium text-indigo-500 hover:text-indigo-600 transition-colors flex items-center gap-0.5"
                                    onClick={() => update({ yColumns: [...cfg.yColumns, columns[0] ?? ''] })}
                                >
                                    <span>+</span> Add
                                </button>
                            </div>
                            <div className="space-y-2">
                                {cfg.yColumns.map((yCol, idx) => (
                                    <div key={idx} className="flex gap-1.5">
                                        <Select
                                            value={yCol}
                                            onValueChange={(v) => {
                                                const next = [...cfg.yColumns]
                                                next[idx] = v
                                                update({ yColumns: next })
                                            }}
                                        >
                                            <SelectTrigger className="h-8 text-xs bg-background/50 flex-1">
                                                <SelectValue />
                                            </SelectTrigger>
                                            <SelectContent>
                                                {columns.map((c) => (
                                                    <SelectItem key={c} value={c}>{c}</SelectItem>
                                                ))}
                                            </SelectContent>
                                        </Select>
                                        {cfg.yColumns.length > 1 && (
                                            <Button
                                                variant="ghost"
                                                size="icon"
                                                className="h-8 w-8 text-muted-foreground hover:text-destructive shrink-0"
                                                onClick={() =>
                                                    update({ yColumns: cfg.yColumns.filter((_, i) => i !== idx) })
                                                }
                                            >
                                                <X className="h-3.5 w-3.5" />
                                            </Button>
                                        )}
                                    </div>
                                ))}
                            </div>
                        </div>
                    </div>

                    <div className="h-px bg-border/40" />

                    {/* Options (Sort, Agg, Group) */}
                    <div className="space-y-4">
                        {/* Sort */}
                        <div className="flex items-center justify-between">
                            <Label className="text-[10px] text-muted-foreground font-semibold uppercase tracking-wider">Sort</Label>
                            <Select value={cfg.sort} onValueChange={(v) => update({ sort: v as SortMode })}>
                                <SelectTrigger className="h-7 text-xs w-[140px] bg-background/50">
                                    <SelectValue />
                                </SelectTrigger>
                                <SelectContent>
                                    {SORT_MODES.map((s) => (
                                        <SelectItem key={s.value} value={s.value}>{s.label}</SelectItem>
                                    ))}
                                </SelectContent>
                            </Select>
                        </div>

                        {/* Aggregate */}
                        <div className="flex items-center justify-between">
                            <Label className="text-[10px] text-muted-foreground font-semibold uppercase tracking-wider">Aggregate</Label>
                            <Select value={cfg.aggregate} onValueChange={(v) => update({ aggregate: v as AggFunc })}>
                                <SelectTrigger className="h-7 text-xs w-[140px] bg-background/50">
                                    <SelectValue />
                                </SelectTrigger>
                                <SelectContent>
                                    {AGG_FUNCS.map((f) => (
                                        <SelectItem key={f.value} value={f.value}>{f.label}</SelectItem>
                                    ))}
                                </SelectContent>
                            </Select>
                        </div>

                        {/* Group By */}
                        <div className="flex items-center justify-between">
                            <Label className="text-[10px] text-muted-foreground font-semibold uppercase tracking-wider">Group By</Label>
                            <Select value={cfg.groupBy || '__none'} onValueChange={(v) => update({ groupBy: v === '__none' ? '' : v })}>
                                <SelectTrigger className="h-7 text-xs w-[140px] bg-background/50">
                                    <SelectValue />
                                </SelectTrigger>
                                <SelectContent>
                                    <SelectItem value="__none">None</SelectItem>
                                    {columns.map((c) => (
                                        <SelectItem key={c} value={c}>{c}</SelectItem>
                                    ))}
                                </SelectContent>
                            </Select>
                        </div>

                        {/* Limit */}
                        <div className="flex items-center justify-between">
                            <Label className="text-[10px] text-muted-foreground font-semibold uppercase tracking-wider">Limit</Label>
                            <Select value={cfg.limit ?? 'none'} onValueChange={(v) => update({ limit: v as LimitMode })}>
                                <SelectTrigger className="h-7 text-xs w-[140px] bg-background/50">
                                    <SelectValue />
                                </SelectTrigger>
                                <SelectContent>
                                    {LIMIT_MODES.map((l) => (
                                        <SelectItem key={l.value} value={l.value}>{l.label}</SelectItem>
                                    ))}
                                </SelectContent>
                            </Select>
                        </div>
                    </div>

                    <div className="h-px bg-border/40" />

                    {/* Appearance */}
                    <div className="space-y-4">
                        {/* X-Label */}
                        <div className="space-y-2">
                            <div className="flex items-center justify-between">
                                <Label className="text-[10px] text-muted-foreground font-semibold uppercase tracking-wider">X-Axis Label</Label>
                                <button
                                    onClick={() => update({ showXLabel: !cfg.showXLabel })}
                                    className={cn(
                                        'relative inline-flex h-4 w-7 items-center rounded-full transition-colors shrink-0',
                                        cfg.showXLabel ? 'bg-indigo-500' : 'bg-muted'
                                    )}
                                >
                                    <span
                                        className={cn(
                                            'inline-block h-3 w-3 rounded-full bg-white shadow transition-transform',
                                            cfg.showXLabel ? 'translate-x-3.5' : 'translate-x-0.5'
                                        )}
                                    />
                                </button>
                            </div>
                            {cfg.showXLabel && (
                                <Input
                                    className="h-8 text-xs bg-background/50"
                                    placeholder="Enter label..."
                                    value={cfg.xLabel}
                                    onChange={(e) => update({ xLabel: e.target.value })}
                                />
                            )}
                        </div>

                        {/* Y-Label */}
                        <div className="space-y-2">
                            <div className="flex items-center justify-between">
                                <Label className="text-[10px] text-muted-foreground font-semibold uppercase tracking-wider">Y-Axis Label</Label>
                                <button
                                    onClick={() => update({ showYLabel: !cfg.showYLabel })}
                                    className={cn(
                                        'relative inline-flex h-4 w-7 items-center rounded-full transition-colors shrink-0',
                                        cfg.showYLabel ? 'bg-indigo-500' : 'bg-muted'
                                    )}
                                >
                                    <span
                                        className={cn(
                                            'inline-block h-3 w-3 rounded-full bg-white shadow transition-transform',
                                            cfg.showYLabel ? 'translate-x-3.5' : 'translate-x-0.5'
                                        )}
                                    />
                                </button>
                            </div>
                            {cfg.showYLabel && (
                                <Input
                                    className="h-8 text-xs bg-background/50"
                                    placeholder="Enter label..."
                                    value={cfg.yLabel}
                                    onChange={(e) => update({ yLabel: e.target.value })}
                                />
                            )}
                        </div>
                    </div>

                </div>
            </div>

            {/* ── Chart (right) ── */}
            <div className="flex-1 min-w-0 p-4">
                <ChartRenderer cfg={cfg} data={chartData} height={chartH - 24} />
            </div>
        </div>
    )
}

// ─── Chart renderer ────────────────────────────────────────────────────────────

interface ChartRendererProps {
    cfg: ChartConfig
    data: Record<string, unknown>[]
    height: number
}

function ChartRenderer({ cfg, data, height }: ChartRendererProps) {
    if (!data.length) {
        return (
            <div className="flex items-center justify-center h-full text-muted-foreground text-sm">
                No data to display. Select valid X and Y columns.
            </div>
        )
    }

    const xLabel = cfg.showXLabel ? (cfg.xLabel || cfg.xColumn) : undefined
    const yLabel = cfg.showYLabel ? (cfg.yLabel || (cfg.yColumns.length === 1 ? `${cfg.aggregate === 'none' ? '' : (cfg.aggregate.charAt(0).toUpperCase() + cfg.aggregate.slice(1)) + ' of '}${cfg.yColumns[0]}` : '')) : undefined

    const commonGridProps = {
        strokeDasharray: '3 3',
        stroke: 'var(--border)',
        strokeOpacity: 0.5,
    }
    const tooltipStyle = {
        backgroundColor: 'var(--background)',
        border: '1px solid var(--border)',
        borderRadius: 6,
        fontSize: 11,
    }

    // Pie chart
    if (cfg.chartType === 'pie') {
        const pieData = data.map((d) => ({
            name: String(d.__x),
            value: Number(d[cfg.yColumns[0]] ?? 0),
        }))
        return (
            <ResponsiveContainer width="100%" height={height}>
                <PieChart>
                    <Pie
                        data={pieData}
                        dataKey="value"
                        nameKey="name"
                        cx="50%"
                        cy="50%"
                        outerRadius="70%"
                        label={({ name, percent }) => `${name} (${((percent ?? 0) * 100).toFixed(0)}%)`}
                        labelLine
                    >
                        {pieData.map((_, i) => (
                            <Cell key={i} fill={CHART_PALETTE[i % CHART_PALETTE.length]} />
                        ))}
                    </Pie>
                    <Tooltip contentStyle={tooltipStyle} />
                    <Legend wrapperStyle={{ fontSize: 10 }} />
                </PieChart>
            </ResponsiveContainer>
        )
    }

    const sharedAxisProps = {
        tick: { fontSize: 10, fill: 'var(--muted-foreground)' },
        axisLine: { stroke: 'var(--border)' },
        tickLine: { stroke: 'var(--border)' },
    }

    const renderBars = (stacked = false) =>
        cfg.yColumns.map((yCol, i) => (
            <Bar
                key={yCol}
                dataKey={yCol}
                fill={CHART_PALETTE[i % CHART_PALETTE.length]}
                radius={stacked ? undefined : [2, 2, 0, 0]}
                stackId={stacked ? 'stack' : undefined}
            />
        ))

    if (cfg.chartType === 'bar' || cfg.chartType === 'stacked_bar') {
        return (
            <ResponsiveContainer width="100%" height={height}>
                <BarChart data={data} margin={{ top: 4, right: 12, bottom: xLabel ? 20 : 4, left: yLabel ? 28 : 0 }}>
                    <CartesianGrid vertical={false} {...commonGridProps} />
                    <XAxis dataKey="__x" {...sharedAxisProps} label={xLabel ? { value: xLabel, position: 'insideBottom', offset: -8, fontSize: 10 } : undefined} />
                    <YAxis {...sharedAxisProps} label={yLabel ? { value: yLabel, angle: -90, position: 'insideLeft', offset: 14, fontSize: 10 } : undefined} />
                    <Tooltip contentStyle={tooltipStyle} />
                    {cfg.yColumns.length > 1 && <Legend wrapperStyle={{ fontSize: 10 }} />}
                    {renderBars(cfg.chartType === 'stacked_bar')}
                </BarChart>
            </ResponsiveContainer>
        )
    }

    if (cfg.chartType === 'line') {
        return (
            <ResponsiveContainer width="100%" height={height}>
                <LineChart data={data} margin={{ top: 4, right: 12, bottom: xLabel ? 20 : 4, left: yLabel ? 28 : 0 }}>
                    <CartesianGrid {...commonGridProps} />
                    <XAxis dataKey="__x" {...sharedAxisProps} label={xLabel ? { value: xLabel, position: 'insideBottom', offset: -8, fontSize: 10 } : undefined} />
                    <YAxis {...sharedAxisProps} label={yLabel ? { value: yLabel, angle: -90, position: 'insideLeft', offset: 14, fontSize: 10 } : undefined} />
                    <Tooltip contentStyle={tooltipStyle} />
                    {cfg.yColumns.length > 1 && <Legend wrapperStyle={{ fontSize: 10 }} />}
                    {cfg.yColumns.map((yCol, i) => (
                        <Line
                            key={yCol}
                            type="monotone"
                            dataKey={yCol}
                            stroke={CHART_PALETTE[i % CHART_PALETTE.length]}
                            strokeWidth={2}
                            dot={data.length < 50}
                            activeDot={{ r: 4 }}
                        />
                    ))}
                </LineChart>
            </ResponsiveContainer>
        )
    }

    if (cfg.chartType === 'area') {
        return (
            <ResponsiveContainer width="100%" height={height}>
                <RAreaChart data={data} margin={{ top: 4, right: 12, bottom: xLabel ? 20 : 4, left: yLabel ? 28 : 0 }}>
                    <defs>
                        {cfg.yColumns.map((yCol, i) => (
                            <linearGradient key={yCol} id={`area-grad-${i}`} x1="0" y1="0" x2="0" y2="1">
                                <stop offset="5%" stopColor={CHART_PALETTE[i % CHART_PALETTE.length]} stopOpacity={0.4} />
                                <stop offset="95%" stopColor={CHART_PALETTE[i % CHART_PALETTE.length]} stopOpacity={0.05} />
                            </linearGradient>
                        ))}
                    </defs>
                    <CartesianGrid {...commonGridProps} />
                    <XAxis dataKey="__x" {...sharedAxisProps} label={xLabel ? { value: xLabel, position: 'insideBottom', offset: -8, fontSize: 10 } : undefined} />
                    <YAxis {...sharedAxisProps} label={yLabel ? { value: yLabel, angle: -90, position: 'insideLeft', offset: 14, fontSize: 10 } : undefined} />
                    <Tooltip contentStyle={tooltipStyle} />
                    {cfg.yColumns.length > 1 && <Legend wrapperStyle={{ fontSize: 10 }} />}
                    {cfg.yColumns.map((yCol, i) => (
                        <Area
                            key={yCol}
                            type="monotone"
                            dataKey={yCol}
                            stroke={CHART_PALETTE[i % CHART_PALETTE.length]}
                            strokeWidth={2}
                            fill={`url(#area-grad-${i})`}
                        />
                    ))}
                </RAreaChart>
            </ResponsiveContainer>
        )
    }

    return null
}

// ─── Profiling ─────────────────────────────────────────────────────────────────

interface ColumnProfile {
    column: string
    type: string
    total_count: number
    null_count: number
    null_percent: number
    distinct_count: number
    min?: unknown
    max?: unknown
    mean?: number
    top_values?: Array<{ value: unknown; count: number; percent: number }>
}

function ProfilingResults({ profile }: { profile: ColumnProfile[] }) {
    if (!profile || profile.length === 0) {
        return (
            <div className="flex items-center justify-center h-full text-sm text-muted-foreground">
                No profiling data available.
            </div>
        )
    }

    return (
        <div className="overflow-auto h-full">
            <table className="w-full text-xs border-collapse">
                <thead className="sticky top-0 bg-background z-10">
                    <tr className="text-left text-muted-foreground border-b">
                        <th className="p-2 font-medium">Column</th>
                        <th className="p-2 font-medium">Type</th>
                        <th className="p-2 font-medium text-right">Total</th>
                        <th className="p-2 font-medium text-right">Nulls</th>
                        <th className="p-2 font-medium text-right">Null %</th>
                        <th className="p-2 font-medium text-right">Distinct</th>
                        <th className="p-2 font-medium text-right">Min</th>
                        <th className="p-2 font-medium text-right">Max</th>
                        <th className="p-2 font-medium text-right">Mean</th>
                        <th className="p-2 font-medium">Top Values</th>
                    </tr>
                </thead>
                <tbody>
                    {profile.map((col) => (
                        <tr key={col.column} className="border-b border-border/30 hover:bg-muted/30">
                            <td className="p-2 font-mono font-medium">{col.column}</td>
                            <td className="p-2">
                                <span className="rounded bg-muted px-1.5 py-0.5 text-[10px]">
                                    {col.type}
                                </span>
                            </td>
                            <td className="p-2 text-right tabular-nums">{col.total_count.toLocaleString()}</td>
                            <td className="p-2 text-right tabular-nums">{col.null_count.toLocaleString()}</td>
                            <td className="p-2 text-right tabular-nums">
                                <span className={cn(col.null_percent > 50 && 'text-destructive font-medium')}>
                                    {col.null_percent.toFixed(1)}%
                                </span>
                            </td>
                            <td className="p-2 text-right tabular-nums">{col.distinct_count.toLocaleString()}</td>
                            <td className="p-2 text-right tabular-nums font-mono">
                                {col.min != null ? String(col.min) : '—'}
                            </td>
                            <td className="p-2 text-right tabular-nums font-mono">
                                {col.max != null ? String(col.max) : '—'}
                            </td>
                            <td className="p-2 text-right tabular-nums">
                                {col.mean != null ? col.mean.toFixed(2) : '—'}
                            </td>
                            <td className="p-2">
                                {col.top_values && col.top_values.length > 0 ? (
                                    <div className="flex flex-wrap gap-1">
                                        {col.top_values.slice(0, 3).map((tv, i) => (
                                            <span
                                                key={i}
                                                className="inline-flex items-center gap-0.5 rounded bg-muted px-1 py-0.5 text-[10px]"
                                            >
                                                <span className="font-mono truncate max-w-[60px]">{String(tv.value)}</span>
                                                <span className="text-muted-foreground">({tv.count})</span>
                                            </span>
                                        ))}
                                    </div>
                                ) : (
                                    <span className="text-muted-foreground">—</span>
                                )}
                            </td>
                        </tr>
                    ))}
                </tbody>
            </table>
        </div>
    )
}
