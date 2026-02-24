import { useMemo } from 'react'
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
import { cn } from '@/lib/utils'
import { Label } from '@/components/ui/label'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import {
    Select,
    SelectContent,
    SelectItem,
    SelectTrigger,
    SelectValue,
} from '@/components/ui/select'
import {
    X,
    BarChart2,
    TrendingUp,
    AreaChart,
    PieChart as PieChartIcon,
    Layers2,
} from 'lucide-react'

// Types
import type { ColumnProfile } from '@/repo/flow-tasks'

// ─── Table ─────────────────────────────────────────────────────────────────────

interface PreviewTableProps {
    columns: string[]
    columnTypes: Record<string, string>
    rows: unknown[][]
}

export function PreviewTable({ columns, columnTypes, rows }: PreviewTableProps) {
    if (columns.length === 0) {
        return (
            <div className="flex items-center justify-center h-full text-muted-foreground text-sm p-8">
                No columns returned.
            </div>
        )
    }

    return (
        <table className="text-xs border-collapse w-full">
            <thead>
                <tr className="border-b border-border bg-muted/90 sticky top-0 z-20 shadow-sm">
                    {columns.map((col) => (
                        <th key={col} className="px-3 py-2 text-left font-semibold whitespace-nowrap">
                            <div>{col}</div>
                            {columnTypes[col] && (
                                <div className="font-normal text-muted-foreground text-[10px] mt-0.5">
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
                            'border-b border-border/30 hover:bg-muted/30 transition-colors',
                            ri % 2 === 0 ? 'bg-background' : 'bg-muted/10'
                        )}
                    >
                        {(row as unknown[]).map((cell, ci) => (
                            <td key={ci} className="px-3 py-1.5 whitespace-nowrap font-mono">
                                {cell === null || cell === undefined ? (
                                    <span className="italic text-muted-foreground/60">NULL</span>
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

type ChartType = 'bar' | 'stacked_bar' | 'line' | 'area' | 'pie'
type AggFunc = 'none' | 'count' | 'min' | 'max' | 'sum' | 'median' | 'average'
type SortMode = 'none' | 'x_asc' | 'x_desc' | 'y_asc' | 'y_desc'
type LimitMode = 'none' | 'top_10' | 'top_50' | 'top_100' | 'bottom_10' | 'bottom_50' | 'bottom_100'

export interface ChartConfig {
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

const CHART_PALETTE = [
    '#6366f1', '#10b981', '#f59e0b', '#ef4444', '#3b82f6',
    '#8b5cf6', '#ec4899', '#14b8a6', '#f97316', '#84cc16',
]

const CHART_TYPES: { value: ChartType; label: string; icon: React.ReactNode }[] = [
    { value: 'bar', label: 'Bar chart', icon: <BarChart2 className="h-4 w-4" /> },
    { value: 'stacked_bar', label: 'Stacked bar', icon: <Layers2 className="h-4 w-4" /> },
    { value: 'line', label: 'Line chart', icon: <TrendingUp className="h-4 w-4" /> },
    { value: 'area', label: 'Area chart', icon: <AreaChart className="h-4 w-4" /> },
    { value: 'pie', label: 'Pie chart', icon: <PieChartIcon className="h-4 w-4" /> },
]

const AGG_FUNCS: { value: AggFunc; label: string }[] = [
    { value: 'none', label: 'None' },
    { value: 'count', label: 'Count' },
    { value: 'min', label: 'Min' },
    { value: 'max', label: 'Max' },
    { value: 'sum', label: 'Sum' },
    { value: 'median', label: 'Median' },
    { value: 'average', label: 'Average' },
]

const SORT_MODES: { value: SortMode; label: string }[] = [
    { value: 'none', label: 'None' },
    { value: 'x_asc', label: 'X Ascending' },
    { value: 'x_desc', label: 'X Descending' },
    { value: 'y_asc', label: 'Y Ascending' },
    { value: 'y_desc', label: 'Y Descending' },
]

const LIMIT_MODES: { value: LimitMode; label: string }[] = [
    { value: 'none', label: 'None' },
    { value: 'top_10', label: 'Top 10' },
    { value: 'top_50', label: 'Top 50' },
    { value: 'top_100', label: 'Top 100' },
    { value: 'bottom_10', label: 'Bottom 10' },
    { value: 'bottom_50', label: 'Bottom 50' },
    { value: 'bottom_100', label: 'Bottom 100' },
]

export function defaultChartConfig(columns: string[]): ChartConfig {
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

// ... internal agg & build logic ...
function applyAgg(values: number[], func: AggFunc): number {
    if (!values.length) return 0
    switch (func) {
        case 'count': return values.length
        case 'min': return Math.min(...values)
        case 'max': return Math.max(...values)
        case 'sum': return values.reduce((a, b) => a + b, 0)
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

    const grouped = new Map<string, Record<string, number[]>>()

    for (const row of rows) {
        const xVal = String(row[xIdx] ?? '')
        const yGroup: Record<string, number[]> = grouped.get(xVal) ?? {}

        for (const yi of yIdxs) {
            const yCol = columns[yi]
            const num = Number(row[yi])
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

    if (cfg.sort !== 'none') {
        const firstY = cfg.yColumns[0]
        if (cfg.sort === 'x_asc') data.sort((a, b) => String(a.__x).localeCompare(String(b.__x)))
        if (cfg.sort === 'x_desc') data.sort((a, b) => String(b.__x).localeCompare(String(a.__x)))
        if (cfg.sort === 'y_asc') data.sort((a, b) => (Number(a[firstY]) - Number(b[firstY])))
        if (cfg.sort === 'y_desc') data.sort((a, b) => (Number(b[firstY]) - Number(a[firstY])))
    }

    if (cfg.limit && cfg.limit !== 'none') {
        const [dir, countStr] = cfg.limit.split('_')
        const count = parseInt(countStr, 10)
        if (dir === 'top') data = data.slice(0, count)
        else data = data.slice(-count)
    }

    return data
}

interface ChartBuilderProps {
    columns: string[]
    rows: unknown[][]
    height: number
    cfg: ChartConfig | null
    onUpdate: (cfg: ChartConfig) => void
}

export function ChartBuilder({ columns, rows, height, cfg, onUpdate }: ChartBuilderProps) {
    // If not managed yet, init
    const effectiveCfg = cfg || defaultChartConfig(columns)

    const update = (patch: Partial<ChartConfig>) => onUpdate({ ...effectiveCfg, ...patch })

    const chartData = useMemo(
        () => buildChartData(columns, rows, effectiveCfg),
        [columns, rows, effectiveCfg]
    )

    const chartH = height - 8 // full-height available for chart panel

    return (
        <div className="flex" style={{ height }}>
            {/* ── Config panel (left 260px, always scrollable) ── */}
            <div
                className="w-[280px] shrink-0 border-r border-border overflow-y-scroll overflow-x-hidden bg-muted/10"
                style={{ WebkitOverflowScrolling: 'touch', scrollBehavior: 'smooth' }}
            >
                <div className="p-5 space-y-6">

                    {/* Chart Type */}
                    <div className="space-y-2">
                        <Label className="text-xs text-muted-foreground font-semibold uppercase tracking-wider">Chart Type</Label>
                        <Select value={effectiveCfg.chartType} onValueChange={(v) => update({ chartType: v as ChartType })}>
                            <SelectTrigger className="h-9 text-xs bg-background">
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
                        <div className="space-y-2">
                            <Label className="text-xs text-muted-foreground font-semibold uppercase tracking-wider">X-Axis</Label>
                            <Select value={effectiveCfg.xColumn} onValueChange={(v) => update({ xColumn: v })}>
                                <SelectTrigger className="h-9 text-xs bg-background">
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
                                <Label className="text-xs text-muted-foreground font-semibold uppercase tracking-wider">Y-Axis</Label>
                                <button
                                    className="text-[11px] font-medium text-indigo-500 hover:text-indigo-600 transition-colors flex items-center gap-0.5"
                                    onClick={() => update({ yColumns: [...effectiveCfg.yColumns, columns[0] ?? ''] })}
                                >
                                    <span>+</span> Add
                                </button>
                            </div>
                            <div className="space-y-2.5">
                                {effectiveCfg.yColumns.map((yCol, idx) => (
                                    <div key={idx} className="flex gap-2">
                                        <Select
                                            value={yCol}
                                            onValueChange={(v) => {
                                                const next = [...effectiveCfg.yColumns]
                                                next[idx] = v
                                                update({ yColumns: next })
                                            }}
                                        >
                                            <SelectTrigger className="h-9 text-xs bg-background flex-1">
                                                <SelectValue />
                                            </SelectTrigger>
                                            <SelectContent>
                                                {columns.map((c) => (
                                                    <SelectItem key={c} value={c}>{c}</SelectItem>
                                                ))}
                                            </SelectContent>
                                        </Select>
                                        {effectiveCfg.yColumns.length > 1 && (
                                            <Button
                                                variant="ghost"
                                                size="icon"
                                                className="h-9 w-9 text-muted-foreground hover:text-destructive shrink-0"
                                                onClick={() =>
                                                    update({ yColumns: effectiveCfg.yColumns.filter((_, i) => i !== idx) })
                                                }
                                            >
                                                <X className="h-4 w-4" />
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
                        <div className="flex items-center justify-between gap-4">
                            <Label className="text-xs text-muted-foreground font-semibold uppercase tracking-wider">Sort</Label>
                            <Select value={effectiveCfg.sort} onValueChange={(v) => update({ sort: v as SortMode })}>
                                <SelectTrigger className="h-8 text-xs w-[140px] bg-background">
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
                        <div className="flex items-center justify-between gap-4">
                            <Label className="text-xs text-muted-foreground font-semibold uppercase tracking-wider">Aggregate</Label>
                            <Select value={effectiveCfg.aggregate} onValueChange={(v) => update({ aggregate: v as AggFunc })}>
                                <SelectTrigger className="h-8 text-xs w-[140px] bg-background">
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
                        <div className="flex items-center justify-between gap-4">
                            <Label className="text-xs text-muted-foreground font-semibold uppercase tracking-wider">Group By</Label>
                            <Select value={effectiveCfg.groupBy || '__none'} onValueChange={(v) => update({ groupBy: v === '__none' ? '' : v })}>
                                <SelectTrigger className="h-8 text-xs w-[140px] bg-background">
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
                        <div className="flex items-center justify-between gap-4">
                            <Label className="text-xs text-muted-foreground font-semibold uppercase tracking-wider">Limit</Label>
                            <Select value={effectiveCfg.limit ?? 'none'} onValueChange={(v) => update({ limit: v as LimitMode })}>
                                <SelectTrigger className="h-8 text-xs w-[140px] bg-background">
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
                        <div className="space-y-2">
                            <div className="flex items-center justify-between">
                                <Label className="text-xs text-muted-foreground font-semibold uppercase tracking-wider">X-Axis Label</Label>
                                <button
                                    onClick={() => update({ showXLabel: !effectiveCfg.showXLabel })}
                                    className={cn(
                                        'relative inline-flex h-4 w-7 items-center rounded-full transition-colors shrink-0',
                                        effectiveCfg.showXLabel ? 'bg-indigo-500' : 'bg-muted-foreground/30'
                                    )}
                                >
                                    <span
                                        className={cn(
                                            'inline-block h-3 w-3 rounded-full bg-white shadow transition-transform',
                                            effectiveCfg.showXLabel ? 'translate-x-3.5' : 'translate-x-0.5'
                                        )}
                                    />
                                </button>
                            </div>
                            {effectiveCfg.showXLabel && (
                                <Input
                                    className="h-8 text-xs bg-background"
                                    placeholder="Enter label..."
                                    value={effectiveCfg.xLabel}
                                    onChange={(e) => update({ xLabel: e.target.value })}
                                />
                            )}
                        </div>

                        <div className="space-y-2">
                            <div className="flex items-center justify-between">
                                <Label className="text-xs text-muted-foreground font-semibold uppercase tracking-wider">Y-Axis Label</Label>
                                <button
                                    onClick={() => update({ showYLabel: !effectiveCfg.showYLabel })}
                                    className={cn(
                                        'relative inline-flex h-4 w-7 items-center rounded-full transition-colors shrink-0',
                                        effectiveCfg.showYLabel ? 'bg-indigo-500' : 'bg-muted-foreground/30'
                                    )}
                                >
                                    <span
                                        className={cn(
                                            'inline-block h-3 w-3 rounded-full bg-white shadow transition-transform',
                                            effectiveCfg.showYLabel ? 'translate-x-3.5' : 'translate-x-0.5'
                                        )}
                                    />
                                </button>
                            </div>
                            {effectiveCfg.showYLabel && (
                                <Input
                                    className="h-8 text-xs bg-background"
                                    placeholder="Enter label..."
                                    value={effectiveCfg.yLabel}
                                    onChange={(e) => update({ yLabel: e.target.value })}
                                />
                            )}
                        </div>
                    </div>

                </div>
            </div>

            {/* ── Chart (right) ── */}
            <div className="flex-1 min-w-0 p-6 bg-background">
                <ChartRenderer cfg={effectiveCfg} data={chartData} height={chartH - 24} />
            </div>
        </div>
    )
}

function ChartRenderer({ cfg, data, height }: { cfg: ChartConfig, data: Record<string, unknown>[], height: number }) {
    if (!data.length) {
        return (
            <div className="flex items-center justify-center h-full text-muted-foreground text-sm border border-border/40 rounded-lg bg-muted/5 border-dashed">
                <div className="text-center space-y-2">
                    <BarChart2 className="h-8 w-8 text-muted-foreground/30 mx-auto" />
                    <p>No data to display. Select valid X and Y columns.</p>
                </div>
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
        borderRadius: 8,
        fontSize: 12,
        boxShadow: '0 4px 6px -1px rgb(0 0 0 / 0.1)',
    }

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
                        outerRadius="75%"
                        label={({ name, percent }) => `${name} (${((percent ?? 0) * 100).toFixed(0)}%)`}
                        labelLine={{ stroke: 'var(--border)', strokeWidth: 1 }}
                    >
                        {pieData.map((_, i) => (
                            <Cell key={i} fill={CHART_PALETTE[i % CHART_PALETTE.length]} />
                        ))}
                    </Pie>
                    <Tooltip contentStyle={tooltipStyle} />
                    <Legend wrapperStyle={{ fontSize: 12, paddingTop: 20 }} />
                </PieChart>
            </ResponsiveContainer>
        )
    }

    const sharedAxisProps = {
        tick: { fontSize: 11, fill: 'var(--muted-foreground)' },
        axisLine: { stroke: 'var(--border)' },
        tickLine: { stroke: 'var(--border)' },
    }

    const renderBars = (stacked = false) =>
        cfg.yColumns.map((yCol, i) => (
            <Bar
                key={yCol}
                dataKey={yCol}
                fill={CHART_PALETTE[i % CHART_PALETTE.length]}
                radius={stacked ? undefined : [4, 4, 0, 0]}
                stackId={stacked ? 'stack' : undefined}
            />
        ))

    if (cfg.chartType === 'bar' || cfg.chartType === 'stacked_bar') {
        return (
            <ResponsiveContainer width="100%" height={height}>
                <BarChart data={data} margin={{ top: 12, right: 32, bottom: xLabel ? 32 : 12, left: yLabel ? 42 : 12 }}>
                    <CartesianGrid vertical={false} {...commonGridProps} />
                    <XAxis dataKey="__x" {...sharedAxisProps} label={xLabel ? { value: xLabel, position: 'insideBottom', offset: -16, fontSize: 12, fill: 'var(--foreground)' } : undefined} />
                    <YAxis {...sharedAxisProps} label={yLabel ? { value: yLabel, angle: -90, position: 'insideLeft', offset: 24, fontSize: 12, fill: 'var(--foreground)' } : undefined} />
                    <Tooltip contentStyle={tooltipStyle} cursor={{ fill: 'var(--muted)', opacity: 0.4 }} />
                    {cfg.yColumns.length > 1 && <Legend wrapperStyle={{ fontSize: 12, paddingTop: 20 }} />}
                    {renderBars(cfg.chartType === 'stacked_bar')}
                </BarChart>
            </ResponsiveContainer>
        )
    }

    if (cfg.chartType === 'line') {
        return (
            <ResponsiveContainer width="100%" height={height}>
                <LineChart data={data} margin={{ top: 12, right: 32, bottom: xLabel ? 32 : 12, left: yLabel ? 42 : 12 }}>
                    <CartesianGrid {...commonGridProps} />
                    <XAxis dataKey="__x" {...sharedAxisProps} label={xLabel ? { value: xLabel, position: 'insideBottom', offset: -16, fontSize: 12, fill: 'var(--foreground)' } : undefined} />
                    <YAxis {...sharedAxisProps} label={yLabel ? { value: yLabel, angle: -90, position: 'insideLeft', offset: 24, fontSize: 12, fill: 'var(--foreground)' } : undefined} />
                    <Tooltip contentStyle={tooltipStyle} />
                    {cfg.yColumns.length > 1 && <Legend wrapperStyle={{ fontSize: 12, paddingTop: 20 }} />}
                    {cfg.yColumns.map((yCol, i) => (
                        <Line
                            key={yCol}
                            type="monotone"
                            dataKey={yCol}
                            stroke={CHART_PALETTE[i % CHART_PALETTE.length]}
                            strokeWidth={3}
                            dot={data.length < 50 ? { r: 4, strokeWidth: 2 } : false}
                            activeDot={{ r: 6, strokeWidth: 0 }}
                        />
                    ))}
                </LineChart>
            </ResponsiveContainer>
        )
    }

    if (cfg.chartType === 'area') {
        return (
            <ResponsiveContainer width="100%" height={height}>
                <RAreaChart data={data} margin={{ top: 12, right: 32, bottom: xLabel ? 32 : 12, left: yLabel ? 42 : 12 }}>
                    <defs>
                        {cfg.yColumns.map((yCol, i) => (
                            <linearGradient key={yCol} id={`area-grad-${i}`} x1="0" y1="0" x2="0" y2="1">
                                <stop offset="5%" stopColor={CHART_PALETTE[i % CHART_PALETTE.length]} stopOpacity={0.5} />
                                <stop offset="95%" stopColor={CHART_PALETTE[i % CHART_PALETTE.length]} stopOpacity={0.05} />
                            </linearGradient>
                        ))}
                    </defs>
                    <CartesianGrid {...commonGridProps} />
                    <XAxis dataKey="__x" {...sharedAxisProps} label={xLabel ? { value: xLabel, position: 'insideBottom', offset: -16, fontSize: 12, fill: 'var(--foreground)' } : undefined} />
                    <YAxis {...sharedAxisProps} label={yLabel ? { value: yLabel, angle: -90, position: 'insideLeft', offset: 24, fontSize: 12, fill: 'var(--foreground)' } : undefined} />
                    <Tooltip contentStyle={tooltipStyle} />
                    {cfg.yColumns.length > 1 && <Legend wrapperStyle={{ fontSize: 12, paddingTop: 20 }} />}
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

export function ProfilingResults({ profile }: { profile: ColumnProfile[] }) {
    if (!profile || profile.length === 0) {
        return (
            <div className="flex items-center justify-center h-full text-sm text-muted-foreground p-8">
                No profiling data available.
            </div>
        )
    }

    return (
        <table className="w-full text-sm border-collapse bg-background rounded-lg shadow-sm ring-1 ring-border">
            <thead className="sticky top-0 bg-muted/80 backdrop-blur-md z-10">
                <tr className="text-left text-muted-foreground border-b border-border shadow-sm">
                    <th className="p-3 font-semibold uppercase tracking-wider text-xs">Column</th>
                    <th className="p-3 font-semibold uppercase tracking-wider text-xs">Type</th>
                    <th className="p-3 font-semibold text-right uppercase tracking-wider text-xs">Total</th>
                    <th className="p-3 font-semibold text-right uppercase tracking-wider text-xs">Nulls</th>
                    <th className="p-3 font-semibold text-right uppercase tracking-wider text-xs">Null %</th>
                    <th className="p-3 font-semibold text-right uppercase tracking-wider text-xs">Distinct</th>
                    <th className="p-3 font-semibold text-right uppercase tracking-wider text-xs">Min</th>
                    <th className="p-3 font-semibold text-right uppercase tracking-wider text-xs">Max</th>
                    <th className="p-3 font-semibold text-right uppercase tracking-wider text-xs">Mean</th>
                    <th className="p-3 font-semibold uppercase tracking-wider text-xs">Top Values</th>
                </tr>
            </thead>
            <tbody>
                {profile.map((col) => (
                    <tr key={col.column} className="border-b border-border/40 hover:bg-muted/30 transition-colors">
                        <td className="p-3 font-mono font-medium">{col.column}</td>
                        <td className="p-3">
                            <span className="rounded bg-muted/60 px-2 py-1 text-xs border border-border/50">
                                {col.type}
                            </span>
                        </td>
                        <td className="p-3 text-right tabular-nums">{col.total_count.toLocaleString()}</td>
                        <td className="p-3 text-right tabular-nums">{col.null_count.toLocaleString()}</td>
                        <td className="p-3 text-right tabular-nums">
                            <span className={cn(col.null_percent > 50 && 'text-destructive font-medium bg-destructive/10 px-1.5 py-0.5 rounded')}>
                                {col.null_percent.toFixed(1)}%
                            </span>
                        </td>
                        <td className="p-3 text-right tabular-nums">{col.distinct_count.toLocaleString()}</td>
                        <td className="p-3 text-right tabular-nums font-mono text-xs">
                            {col.min != null ? String(col.min) : '—'}
                        </td>
                        <td className="p-3 text-right tabular-nums font-mono text-xs">
                            {col.max != null ? String(col.max) : '—'}
                        </td>
                        <td className="p-3 text-right tabular-nums text-xs">
                            {col.mean != null ? col.mean.toFixed(2) : '—'}
                        </td>
                        <td className="p-3">
                            {col.top_values && col.top_values.length > 0 ? (
                                <div className="flex flex-wrap gap-1.5">
                                    {col.top_values.slice(0, 3).map((tv, i) => (
                                        <span
                                            key={i}
                                            className="inline-flex items-center gap-1 rounded bg-background border border-border px-1.5 py-0.5 text-[11px] shadow-sm"
                                        >
                                            <span className="font-mono truncate max-w-[80px] font-medium" title={String(tv.value)}>
                                                {String(tv.value)}
                                            </span>
                                            <span className="text-muted-foreground/80 border-l border-border pl-1">
                                                {tv.count}
                                            </span>
                                        </span>
                                    ))}
                                </div>
                            ) : (
                                <span className="text-muted-foreground italic">—</span>
                            )}
                        </td>
                    </tr>
                ))}
            </tbody>
        </table>
    )
}
