/**
 * NodeConfigPanel — right-side drawer showing the config form for the selected node.
 * Opens when a node is selected on the canvas.
 *
 * Column selectors use useNodeSchema() which runs the DuckDB CTE chain
 * up to the target node (LIMIT 0) in the worker to derive the real output schema —
 * correctly reflecting post-aggregate, post-clean, post-pivot columns.
 */

import { useFlowTaskStore } from '../store/flow-task-store'
import { cn } from '@/lib/utils'
import { useQuery } from '@tanstack/react-query'
import { useRef, useState, useEffect, useCallback } from 'react'
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
import { Switch } from '@/components/ui/switch'
import { Separator } from '@/components/ui/separator'
import {
    Command,
    CommandEmpty,
    CommandGroup,
    CommandInput,
    CommandItem,
    CommandList,
} from '@/components/ui/command'
import {
    Popover,
    PopoverContent,
    PopoverTrigger,
} from '@/components/ui/popover'
import { X, Trash2, Eye, Loader2, Plus, GripVertical, AlertCircle, Check, ChevronsUpDown } from 'lucide-react'
import type { FlowNodeType, FlowNodeData, WriteMode } from '@/repo/flow-tasks'
import type { ColumnInfo } from '@/repo/flow-tasks'
import { sourcesRepo } from '@/repo/sources'
import { destinationsRepo } from '@/repo/destinations'
import { useNodeSchema } from '../hooks/useNodeSchema'
import { useParams } from '@tanstack/react-router'

// ─── Shared helper components ─────────────────────────────────────────────────

function ColumnSelect({
    columns,
    value,
    onChange,
    placeholder = 'Select column…',
    isLoading = false,
    className,
}: {
    columns: ColumnInfo[]
    value: string
    onChange: (v: string) => void
    placeholder?: string
    isLoading?: boolean
    className?: string
}) {
    if (isLoading) {
        return (
            <div className="flex items-center gap-1.5 h-7 text-xs text-muted-foreground">
                <Loader2 className="h-3 w-3 animate-spin" /> Resolving schema…
            </div>
        )
    }
    return (
        <Select value={value || ''} onValueChange={onChange}>
            <SelectTrigger className={cn("h-7 text-xs", className)}>
                <SelectValue placeholder={placeholder} />
            </SelectTrigger>
            <SelectContent>
                {columns.length === 0 ? (
                    <div className="px-2 py-1.5 text-xs text-muted-foreground">
                        No columns — check upstream config
                    </div>
                ) : (
                    columns.map((c) => (
                        <SelectItem key={c.column_name} value={c.column_name}>
                            <span>{c.column_name}</span>
                            <span className="ml-1.5 text-[10px] text-muted-foreground font-mono">
                                {c.data_type}
                            </span>
                        </SelectItem>
                    ))
                )}
            </SelectContent>
        </Select>
    )
}

function MultiColumnSelect({
    columns,
    selected,
    onChange,
    isLoading = false,
}: {
    columns: ColumnInfo[]
    selected: string[]
    onChange: (cols: string[]) => void
    isLoading?: boolean
}) {
    if (isLoading) {
        return (
            <div className="flex items-center gap-1.5 h-7 text-xs text-muted-foreground">
                <Loader2 className="h-3 w-3 animate-spin" /> Resolving schema…
            </div>
        )
    }
    if (columns.length === 0) {
        return (
            <div className="text-xs text-muted-foreground">No columns — check upstream</div>
        )
    }
    return (
        <div className="flex flex-wrap gap-1 pt-0.5">
            {columns.map((c) => {
                const active = selected.includes(c.column_name)
                return (
                    <button
                        key={c.column_name}
                        type="button"
                        onClick={() =>
                            onChange(
                                active
                                    ? selected.filter((s) => s !== c.column_name)
                                    : [...selected, c.column_name],
                            )
                        }
                        className={`rounded px-1.5 py-0.5 text-[10px] border transition-colors ${
                            active
                                ? 'bg-violet-600 border-violet-600 text-white'
                                : 'border-border text-muted-foreground hover:border-violet-400'
                        }`}
                    >
                        {c.column_name}
                    </button>
                )
            })}
        </div>
    )
}

// ─── Main panel ────────────────────────────────────────────────────────────────

export function NodeConfigPanel() {
    const { nodes, selectedNodeId, selectNode, updateNodeData, removeNode, requestPreview, setConfigPanelWidth } =
        useFlowTaskStore()

    // Extract flow task ID from route params
    let flowTaskId: number | null = null
    try {
        const params = useParams({ strict: false }) as Record<string, string>
        const raw = params['flowTaskId'] ?? params['flow_task_id'] ?? params['id'] ?? ''
        flowTaskId = raw ? parseInt(raw) : null
    } catch {
        flowTaskId = null
    }

    // ── Resizable panel ───────────────────────────────────────────────────────
    // Strategy: write width directly to DOM via RAF (no React state during drag
    // = zero re-renders of form content). `will-change: width` promotes the
    // panel to its own GPU layer so the browser composites it independently.
    // RAF throttles updates to exactly 1 per display frame (max 60fps).
    // Only on mouseup do we commit to React state (one re-render total).
    const MIN_WIDTH = 256
    const MAX_WIDTH = 640
    const DEFAULT_WIDTH = 288
    const [panelWidth, setPanelWidth] = useState(DEFAULT_WIDTH)
    const innerRef = useRef<HTMLDivElement>(null)
    const dragging = useRef(false)
    const startX = useRef(0)
    const startW = useRef(0)
    const rafId = useRef<number | null>(null)

    const onResizeMouseDown = useCallback((e: React.MouseEvent) => {
        e.preventDefault()
        dragging.current = true
        startX.current = e.clientX
        startW.current = innerRef.current ? innerRef.current.offsetWidth : panelWidth
        // Lock cursor globally so it doesn't flicker when moving off the handle
        document.body.style.cursor = 'col-resize'
        document.body.style.userSelect = 'none'
    }, [panelWidth])

    useEffect(() => {
        // Init store with default
        setConfigPanelWidth(DEFAULT_WIDTH)
    }, [setConfigPanelWidth])

    useEffect(() => {
        const onMove = (e: MouseEvent) => {
            if (!dragging.current || !innerRef.current) return
            // Cancel any pending frame — use latest mouse position only
            if (rafId.current !== null) cancelAnimationFrame(rafId.current)
            rafId.current = requestAnimationFrame(() => {
                if (!innerRef.current) return
                const delta = startX.current - e.clientX
                const next = Math.min(MAX_WIDTH, Math.max(MIN_WIDTH, startW.current + delta))
                innerRef.current.style.width = `${next}px`
                rafId.current = null
            })
        }
        const onUp = () => {
            if (!dragging.current) return
            dragging.current = false
            document.body.style.cursor = ''
            document.body.style.userSelect = ''
            if (rafId.current !== null) {
                cancelAnimationFrame(rafId.current)
                rafId.current = null
            }
            // Commit final value to state — one re-render, panel keeps its size on remount
            if (innerRef.current) {
                const w = innerRef.current.offsetWidth
                setPanelWidth(w)
                setConfigPanelWidth(w)
            }
        }
        window.addEventListener('mousemove', onMove)
        window.addEventListener('mouseup', onUp)
        return () => {
            window.removeEventListener('mousemove', onMove)
            window.removeEventListener('mouseup', onUp)
        }
    }, [setConfigPanelWidth])

    const selectedNode = (nodes ?? []).find((n) => n.id === selectedNodeId)
    if (!selectedNode) return null

    const update = (patch: Partial<FlowNodeData>) =>
        updateNodeData(selectedNode.id, patch)

    return (
        // Outer shell: absolute right-0, full height, no own width — overlays canvas
        // Width changes to the inner div never trigger canvas reflow.
        <div className="absolute top-0 right-0 h-full z-20 pointer-events-none">
            <div
                ref={innerRef}
                className="relative flex flex-col h-full border-l border-border bg-background overflow-y-auto animate-in slide-in-from-right duration-200 pointer-events-auto"
                style={{ width: panelWidth, willChange: 'width' }}
            >
                {/* Drag-to-resize handle */}
                <div
                    onMouseDown={onResizeMouseDown}
                    className="absolute left-0 top-0 h-full w-3 cursor-col-resize flex items-center justify-center hover:bg-muted/50 transition-colors z-10 group"
                    title="Drag to resize"
                >
                    <GripVertical className="h-4 w-4 text-muted-foreground/40 group-hover:text-muted-foreground transition-colors" />
                </div>
            {/* Header */}
            <div className="flex items-center justify-between px-3 py-2 border-b border-border/50">
                <p className="text-sm font-semibold capitalize">
                    {selectedNode.type === 'note' ? 'Note' : `${selectedNode.type} Config`}
                </p>
                <div className="flex gap-1">
                    {selectedNode.type !== 'note' && (
                    <Button
                        variant="ghost"
                        size="icon"
                        className="h-6 w-6 text-violet-500 hover:text-violet-600"
                        title="Preview node output"
                        onClick={() =>
                            requestPreview?.(
                                selectedNode.id,
                                selectedNode.data.label ?? selectedNode.type,
                            )
                        }
                    >
                        <Eye className="h-3.5 w-3.5" />
                    </Button>
                    )}
                    <Button
                        variant="ghost"
                        size="icon"
                        className="h-6 w-6 text-destructive hover:text-destructive"
                        title="Delete node"
                        onClick={() => removeNode(selectedNode.id)}
                    >
                        <Trash2 className="h-3.5 w-3.5" />
                    </Button>
                    <Button
                        variant="ghost"
                        size="icon"
                        className="h-6 w-6"
                        onClick={() => selectNode(null)}
                    >
                        <X className="h-3.5 w-3.5" />
                    </Button>
                </div>
            </div>

            {/* Common: Label */}
            {/* Common: Label — hidden for note nodes (they use note_content) */}
            {selectedNode.type !== 'note' && (
            <div className="px-3 py-3 space-y-3">
                <Field label="Label">
                    <Input
                        className="h-7 text-xs"
                        value={(selectedNode.data.label as string) || ''}
                        onChange={(e) => update({ label: e.target.value })}
                        placeholder="Node label"
                    />
                </Field>
            </div>
            )}

            {selectedNode.type !== 'note' && <Separator />}

            {/* Type-specific config */}
            <div className="px-3 py-3 space-y-3">
                <NodeTypeConfig
                    type={selectedNode.type}
                    data={selectedNode.data}
                    update={update}
                    nodeId={selectedNode.id}
                    flowTaskId={flowTaskId}
                />
            </div>
            </div>{/* end inner panel */}
        </div> // end outer shell
    )
}

// ─── Per-type config forms ─────────────────────────────────────────────────────

type ConfigFormProps = {
    data: FlowNodeData
    update: (patch: Partial<FlowNodeData>) => void
    nodeId: string
    flowTaskId: number | null
}

function NodeTypeConfig({
    type,
    data,
    update,
    nodeId,
    flowTaskId,
}: {
    type: FlowNodeType
    data: FlowNodeData
    update: (patch: Partial<FlowNodeData>) => void
    nodeId: string
    flowTaskId: number | null
}) {
    switch (type) {
        case 'input':
            return <InputConfig data={data} update={update} nodeId={nodeId} flowTaskId={flowTaskId} />
        case 'clean':
            return <CleanConfig data={data} update={update} nodeId={nodeId} flowTaskId={flowTaskId} />
        case 'aggregate':
            return <AggregateConfig data={data} update={update} nodeId={nodeId} flowTaskId={flowTaskId} />
        case 'join':
            return <JoinConfig data={data} update={update} nodeId={nodeId} flowTaskId={flowTaskId} />
        case 'new_rows':
            return <NewRowsConfig data={data} update={update} nodeId={nodeId} flowTaskId={flowTaskId} />
        case 'union':
            return <UnionConfig data={data} update={update} nodeId={nodeId} flowTaskId={flowTaskId} />
        case 'pivot':
            return <PivotConfig data={data} update={update} nodeId={nodeId} flowTaskId={flowTaskId} />
        case 'output':
            return <OutputConfig data={data} update={update} nodeId={nodeId} flowTaskId={flowTaskId} />
        case 'sql':
            return <SqlConfig data={data} update={update} nodeId={nodeId} flowTaskId={flowTaskId} />
        case 'note':
            return <NoteConfig data={data} update={update} nodeId={nodeId} flowTaskId={flowTaskId} />
        default:
            return (
                <p className="text-xs text-muted-foreground">No config for this node type.</p>
            )
    }
}

// ─── Input ─────────────────────────────────────────────────────────────────────

function InputConfig({ data, update, nodeId, flowTaskId }: ConfigFormProps) {
    const sourceType = (data.source_type as 'POSTGRES' | 'SNOWFLAKE') || 'POSTGRES'
    const sourceId = data.source_id as number | undefined
    const destinationId = data.destination_id as number | undefined

    // Resolve input node schema for column-aware filter builder
    const { columns: inputColumns, isLoading: schemaLoading } = useNodeSchema(flowTaskId, nodeId)

    // Fetch all postgres sources (always fetched when type is POSTGRES)
    const { data: sourcesData, isLoading: sourcesLoading } = useQuery({
        queryKey: ['sources'],
        queryFn: () => sourcesRepo.getAll(),
        enabled: sourceType === 'POSTGRES',
        staleTime: 30_000,
    })

    // Fetch all destinations (used for both SNOWFLAKE and POSTGRES destination options)
    const { data: destsData, isLoading: destsLoading } = useQuery({
        queryKey: ['destinations'],
        queryFn: () => destinationsRepo.getAll(),
        enabled: true,
        staleTime: 30_000,
    })

    const snowflakeDests = destsData?.destinations.filter(
        (d) => d.type.toLowerCase().includes('snowflake')
    ) ?? []

    // Postgres destinations (target DBs that can also be read from)
    const postgresDests = destsData?.destinations.filter(
        (d) => d.type.toLowerCase().includes('postgres') || d.type.toLowerCase().includes('postgresql')
    ) ?? []

    // For POSTGRES mode: encode selection as "src:<id>" or "dst:<id>" in a single dropdown
    const pgConnectionValue =
        sourceType === 'POSTGRES'
            ? sourceId != null
                ? `src:${sourceId}`
                : destinationId != null
                    ? `dst:${destinationId}`
                    : ''
            : ''

    const handlePgConnectionChange = (v: string) => {
        if (v.startsWith('src:')) {
            update({ source_id: parseInt(v.slice(4)), destination_id: undefined, table_name: undefined })
        } else if (v.startsWith('dst:')) {
            update({ destination_id: parseInt(v.slice(4)), source_id: undefined, table_name: undefined })
        }
    }

    // Determine which id to use for table fetching
    // For POSTGRES: source_id → sourcesRepo.getAvailableTables, destination_id → destinationsRepo.getTableList
    const pgUseDestTable = sourceType === 'POSTGRES' && !sourceId && !!destinationId

    // Fetch available tables for selected source (postgres)
    const { data: pgTables, isLoading: pgTablesLoading } = useQuery({
        queryKey: ['source-available-tables', sourceId],
        queryFn: () => sourcesRepo.getAvailableTables(sourceId!),
        enabled: sourceType === 'POSTGRES' && !!sourceId,
        staleTime: 0,
        refetchOnMount: 'always',
    })

    // Fetch table list for postgres destination (when destination is selected in POSTGRES mode)
    const { data: pgDestTableData, isLoading: pgDestTablesLoading } = useQuery({
        queryKey: ['destination-tables', destinationId],
        queryFn: () => destinationsRepo.getTableList(destinationId!),
        enabled: pgUseDestTable,
        staleTime: 0,
        refetchOnMount: 'always',
    })

    // Fetch table list for selected snowflake destination
    const { data: sfTableData, isLoading: sfTablesLoading } = useQuery({
        queryKey: ['destination-tables', destinationId],
        queryFn: () => destinationsRepo.getTableList(destinationId!),
        enabled: sourceType === 'SNOWFLAKE' && !!destinationId,
        staleTime: 0,
        refetchOnMount: 'always',
    })

    const tables: string[] =
        sourceType === 'POSTGRES'
            ? pgUseDestTable
                ? (pgDestTableData?.tables ?? [])
                : (pgTables ?? [])
            : (sfTableData?.tables ?? [])

    const tablesLoading =
        sourceType === 'POSTGRES'
            ? pgUseDestTable ? pgDestTablesLoading : pgTablesLoading
            : sfTablesLoading

    const activeId = sourceType === 'POSTGRES' ? (sourceId ?? destinationId) : destinationId
    const pgLoading = sourcesLoading || destsLoading

    return (
        <>
            <Field label="Source Type">
                <Select
                    value={sourceType}
                    onValueChange={(v) =>
                        update({
                            source_type: v as 'POSTGRES' | 'SNOWFLAKE',
                            source_id: undefined,
                            destination_id: undefined,
                            table_name: undefined,
                            schema_name: undefined,
                        })
                    }
                >
                    <SelectTrigger className="h-7 text-xs w-full">
                        <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                        <SelectItem value="POSTGRES">PostgreSQL</SelectItem>
                        <SelectItem value="SNOWFLAKE">Snowflake</SelectItem>
                    </SelectContent>
                </Select>
            </Field>

            {/* Source / Destination selector */}
            <Field label={sourceType === 'POSTGRES' ? 'Connection' : 'Destination'}>
                {sourceType === 'POSTGRES' && pgLoading && (
                    <div className="flex items-center gap-1.5 h-7 text-xs text-muted-foreground">
                        <Loader2 className="h-3 w-3 animate-spin" /> Loading connections…
                    </div>
                )}
                {sourceType === 'SNOWFLAKE' && destsLoading && (
                    <div className="flex items-center gap-1.5 h-7 text-xs text-muted-foreground">
                        <Loader2 className="h-3 w-3 animate-spin" /> Loading destinations…
                    </div>
                )}
                {sourceType === 'POSTGRES' && !pgLoading && (
                    <Select
                        value={pgConnectionValue}
                        onValueChange={handlePgConnectionChange}
                    >
                        <SelectTrigger className="h-7 text-xs w-full">
                            <SelectValue placeholder="Select connection…" />
                        </SelectTrigger>
                        <SelectContent>
                            {(sourcesData?.sources ?? []).length > 0 && (
                                <>
                                    <div className="px-2 py-1 text-[10px] font-semibold text-muted-foreground uppercase tracking-wide">
                                        Sources
                                    </div>
                                    {(sourcesData?.sources ?? []).map((s) => (
                                        <SelectItem key={`src:${s.id}`} value={`src:${s.id}`}>
                                            {s.name}
                                        </SelectItem>
                                    ))}
                                </>
                            )}
                            {postgresDests.length > 0 && (
                                <>
                                    <div className="px-2 py-1 text-[10px] font-semibold text-muted-foreground uppercase tracking-wide">
                                        Destinations
                                    </div>
                                    {postgresDests.map((d) => (
                                        <SelectItem key={`dst:${d.id}`} value={`dst:${d.id}`}>
                                            {d.name}
                                        </SelectItem>
                                    ))}
                                </>
                            )}
                        </SelectContent>
                    </Select>
                )}
                {sourceType === 'SNOWFLAKE' && !destsLoading && (
                    <Select
                        value={destinationId != null ? String(destinationId) : ''}
                        onValueChange={(v) =>
                            update({ destination_id: parseInt(v), table_name: undefined })
                        }
                    >
                        <SelectTrigger className="h-7 text-xs w-full">
                            <SelectValue placeholder="Select destination…" />
                        </SelectTrigger>
                        <SelectContent>
                            {snowflakeDests.map((d) => (
                                <SelectItem key={d.id} value={String(d.id)}>
                                    {d.name}
                                </SelectItem>
                            ))}
                        </SelectContent>
                    </Select>
                )}
            </Field>

            {/* Table selector — shown only when a source/destination is selected */}
            {!!activeId && (
                <Field label="Table">
                    {tablesLoading ? (
                        <div className="flex items-center gap-1.5 h-7 text-xs text-muted-foreground">
                            <Loader2 className="h-3 w-3 animate-spin" /> Loading tables…
                        </div>
                    ) : (
                        <Popover>
                            <PopoverTrigger asChild>
                                <Button
                                    variant="outline"
                                    role="combobox"
                                    className={cn(
                                        "w-full justify-between h-7 text-xs font-normal",
                                        !data.table_name && "text-muted-foreground"
                                    )}
                                >
                                    {data.table_name
                                        ? tables.find((t) => t === data.table_name)
                                        : "Select table..."}
                                    <ChevronsUpDown className="ml-2 h-4 w-4 shrink-0 opacity-50" />
                                </Button>
                            </PopoverTrigger>
                            <PopoverContent className="w-[300px] p-0">
                                <Command>
                                    <CommandInput placeholder="Search table..." className="h-9 text-xs" />
                                    <CommandList>
                                        <CommandEmpty>No table found.</CommandEmpty>
                                        <CommandGroup>
                                            {tables.map((t) => (
                                                <CommandItem
                                                    value={t}
                                                    key={t}
                                                    onSelect={(value) => {
                                                        update({ table_name: value })
                                                    }}
                                                    className="text-xs"
                                                >
                                                    <Check
                                                        className={cn(
                                                            "mr-2 h-4 w-4",
                                                            t === data.table_name
                                                                ? "opacity-100"
                                                                : "opacity-0"
                                                        )}
                                                    />
                                                    {t}
                                                </CommandItem>
                                            ))}
                                        </CommandGroup>
                                    </CommandList>
                                </Command>
                            </PopoverContent>
                        </Popover>
                    )}
                </Field>
            )}

            {/* Schema — only relevant for postgres */}
            {sourceType === 'POSTGRES' && (
                <Field label="Schema">
                    <Input
                        className="h-7 text-xs"
                        value={(data.schema_name as string) || 'public'}
                        onChange={(e) => update({ schema_name: e.target.value })}
                    />
                </Field>
            )}

            <Field label="Alias (optional)">
                <Input
                    className="h-7 text-xs"
                    value={(data.alias as string) || ''}
                    onChange={(e) => update({ alias: e.target.value })}
                    placeholder="cte_alias"
                />
            </Field>

            <Field label="Sample Limit (rows)">
                <Select
                    value={data.sample_limit ? String(data.sample_limit) : '0'}
                    onValueChange={(v) => {
                        const val = parseInt(v)
                        update({ sample_limit: val || undefined })
                    }}
                >
                    <SelectTrigger className="h-7 text-xs w-full">
                        <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                        <SelectItem value="100">100</SelectItem>
                        <SelectItem value="200">200</SelectItem>
                        <SelectItem value="500">500</SelectItem>
                        <SelectItem value="0">No limit (Default)</SelectItem>
                    </SelectContent>
                </Select>
            </Field>

            <Separator className="my-2" />

            {/* Filter Data — WHERE clause applied at source level */}
            <InputFilterSection
                data={data}
                update={update}
                columns={inputColumns}
                isLoading={schemaLoading}
            />
        </>
    )
}

// ─── Input Filter Section ──────────────────────────────────────────────────────

const INPUT_FILTER_OPERATORS = [
    '=', '!=', '>', '<', '>=', '<=',
    'LIKE', 'NOT LIKE', 'IS NULL', 'IS NOT NULL',
    'IN', 'NOT IN', 'BETWEEN',
]

interface InputFilterRow { col: string; op: string; val: string }

function InputFilterSection({
    data,
    update,
    columns,
    isLoading,
}: {
    data: FlowNodeData
    update: (patch: Partial<FlowNodeData>) => void
    columns: ColumnInfo[]
    isLoading: boolean
}) {
    const filterRows: InputFilterRow[] = (data.filter_rows as InputFilterRow[]) || []

    const setFilterRows = (rows: InputFilterRow[]) => {
        const expr = rows
            .filter((r) => r.col && r.op)
            .map((r) => {
                if (r.op === 'IS NULL') return `${r.col} IS NULL`
                if (r.op === 'IS NOT NULL') return `${r.col} IS NOT NULL`
                if (r.op === 'IN' || r.op === 'NOT IN') return `${r.col} ${r.op} (${r.val})`
                if (r.op === 'BETWEEN') {
                    const parts = r.val.split(',').map((s) => s.trim())
                    if (parts.length === 2) return `${r.col} BETWEEN '${parts[0]}' AND '${parts[1]}'`
                    return `${r.col} BETWEEN ${r.val}`
                }
                return `${r.col} ${r.op} '${r.val}'`
            })
            .join(' AND ')
        update({ filter_rows: rows, filter_sql: expr || undefined })
    }

    return (
        <Field label="Filter Data (WHERE)">
            <div className="space-y-1.5">
                {filterRows.map((row, i) => (
                    <div key={i} className="flex gap-1 items-center">
                        <Select
                            value={row.col}
                            onValueChange={(v) => {
                                const next = [...filterRows]
                                next[i] = { ...next[i], col: v }
                                setFilterRows(next)
                            }}
                        >
                            <SelectTrigger className="!h-7 px-2 text-[11px] flex-1 min-w-0">
                                <SelectValue placeholder="col" />
                            </SelectTrigger>
                            <SelectContent>
                                {isLoading ? (
                                    <div className="px-2 py-1 text-xs flex items-center gap-1">
                                        <Loader2 className="h-3 w-3 animate-spin" /> Loading…
                                    </div>
                                ) : (
                                    columns.map((c) => (
                                        <SelectItem key={c.column_name} value={c.column_name}>
                                            <span>{c.column_name}</span>
                                            <span className="ml-1 text-[9px] text-muted-foreground">{c.data_type}</span>
                                        </SelectItem>
                                    ))
                                )}
                            </SelectContent>
                        </Select>
                        <Select
                            value={row.op}
                            onValueChange={(v) => {
                                const next = [...filterRows]
                                next[i] = { ...next[i], op: v }
                                setFilterRows(next)
                            }}
                        >
                            <SelectTrigger className="!h-7 px-2 text-[11px] w-24 shrink-0">
                                <SelectValue placeholder="op" />
                            </SelectTrigger>
                            <SelectContent>
                                {INPUT_FILTER_OPERATORS.map((op) => (
                                    <SelectItem key={op} value={op}>{op}</SelectItem>
                                ))}
                            </SelectContent>
                        </Select>
                        {row.op !== 'IS NULL' && row.op !== 'IS NOT NULL' ? (
                            <Input
                                className="!h-7 !py-0 px-2 text-[11px] flex-1 min-w-0"
                                value={row.val}
                                onChange={(e) => {
                                    const next = [...filterRows]
                                    next[i] = { ...next[i], val: e.target.value }
                                    setFilterRows(next)
                                }}
                                placeholder={row.op === 'BETWEEN' ? 'val1, val2' : row.op === 'IN' || row.op === 'NOT IN' ? "'a','b','c'" : 'value'}
                            />
                        ) : (
                            <div className="flex-1" />
                        )}
                        <Button
                            variant="ghost" size="icon"
                            className="!h-7 !w-7 shrink-0 hover:text-destructive"
                            onClick={() => setFilterRows(filterRows.filter((_, j) => j !== i))}
                        >
                            <X className="h-3 w-3" />
                        </Button>
                    </div>
                ))}
                <Button
                    variant="outline" size="sm"
                    className="h-6 text-[11px] w-full gap-1"
                    onClick={() => setFilterRows([...filterRows, { col: '', op: '=', val: '' }])}
                >
                    <Plus className="h-3 w-3" /> Add filter
                </Button>
                {filterRows.length > 0 && (
                    <div className="rounded-md bg-muted/50 p-2 mt-1">
                        <p className="text-[9px] font-medium text-muted-foreground uppercase mb-0.5">Generated SQL</p>
                        <p className="text-[10px] font-mono text-muted-foreground break-all">
                            {(data.filter_sql as string) || '—'}
                        </p>
                    </div>
                )}
            </div>
        </Field>
    )
}

// ─── Clean ─────────────────────────────────────────────────────────────────────

const FILTER_OPERATORS = [
    '=', '!=', '>', '<', '>=', '<=',
    'LIKE', 'NOT LIKE', 'IS NULL', 'IS NOT NULL', 'IN',
]

interface FilterRow { col: string; op: string; val: string }

function CleanConfig({ data, update, nodeId, flowTaskId }: ConfigFormProps) {
    const { columns, isLoading } = useNodeSchema(flowTaskId, nodeId)
    const filterRows: FilterRow[] = (data.filter_rows as FilterRow[]) || []
    const selectColumns: string[] = (data.select_columns as string[]) || []

    // Rename pairs stored as array for UI; compiled to Record for the compiler.
    // _rename_pairs is the source of truth for the UI list.
    const renamePairs: [string, string][] =
        (data._rename_pairs as [string, string][] | undefined) ??
        Object.entries((data.rename_columns as Record<string, string>) || {})

    const setRenamePairs = (pairs: [string, string][]) => {
        // Build the clean record for the compiler (skip empty keys)
        const record: Record<string, string> = {}
        for (const [from, to] of pairs) {
            if (from) record[from] = to
        }
        update({ rename_columns: record, _rename_pairs: pairs })
    }

    const setFilterRows = (rows: FilterRow[]) => {
        const expr = rows
            .filter((r) => r.col && r.op)
            .map((r) => {
                if (r.op === 'IS NULL') return `${r.col} IS NULL`
                if (r.op === 'IS NOT NULL') return `${r.col} IS NOT NULL`
                if (r.op === 'IN') return `${r.col} IN (${r.val})`
                return `${r.col} ${r.op} '${r.val}'`
            })
            .join(' AND ')
        update({ filter_rows: rows, filter_expr: expr || undefined })
    }

    return (
        <>
            <SwitchField
                label="Drop nulls"
                checked={!!(data.drop_nulls)}
                onChange={(v) => update({ drop_nulls: v })}
            />
            <SwitchField
                label="Deduplicate rows"
                checked={!!(data.deduplicate)}
                onChange={(v) => update({ deduplicate: v })}
            />

            <Field label="Filter rows">
                <div className="space-y-1.5">
                    {filterRows.map((row, i) => (
                        <div key={i} className="flex gap-1 items-center">
                            <Select
                                value={row.col}
                                onValueChange={(v) => {
                                    const next = [...filterRows]
                                    next[i] = { ...next[i], col: v }
                                    setFilterRows(next)
                                }}
                            >
                                <SelectTrigger className="!h-7 px-2 text-[11px] flex-1 min-w-0">
                                    <SelectValue placeholder="col" />
                                </SelectTrigger>
                                <SelectContent>
                                    {isLoading ? (
                                        <div className="px-2 py-1 text-xs flex items-center gap-1">
                                            <Loader2 className="h-3 w-3 animate-spin" /> Loading…
                                        </div>
                                    ) : (
                                        columns.map((c) => (
                                            <SelectItem key={c.column_name} value={c.column_name}>
                                                {c.column_name}
                                            </SelectItem>
                                        ))
                                    )}
                                </SelectContent>
                            </Select>
                            <Select
                                value={row.op}
                                onValueChange={(v) => {
                                    const next = [...filterRows]
                                    next[i] = { ...next[i], op: v }
                                    setFilterRows(next)
                                }}
                            >
                                <SelectTrigger className="!h-7 px-2 text-[11px] w-20 shrink-0">
                                    <SelectValue placeholder="op" />
                                </SelectTrigger>
                                <SelectContent>
                                    {FILTER_OPERATORS.map((op) => (
                                        <SelectItem key={op} value={op}>{op}</SelectItem>
                                    ))}
                                </SelectContent>
                            </Select>
                            {row.op !== 'IS NULL' && row.op !== 'IS NOT NULL' ? (
                                <Input
                                    className="!h-7 !py-0 px-2 text-[11px] flex-1 min-w-0"
                                    value={row.val}
                                    onChange={(e) => {
                                        const next = [...filterRows]
                                        next[i] = { ...next[i], val: e.target.value }
                                        setFilterRows(next)
                                    }}
                                    placeholder="value"
                                />
                            ) : (
                                <div className="flex-1" />
                            )}
                            <Button
                                variant="ghost" size="icon"
                                className="!h-7 !w-7 shrink-0 hover:text-destructive"
                                onClick={() => setFilterRows(filterRows.filter((_, j) => j !== i))}
                            >
                                <X className="h-3 w-3" />
                            </Button>
                        </div>
                    ))}
                    <Button
                        variant="outline" size="sm"
                        className="h-6 text-[11px] w-full gap-1"
                        onClick={() => setFilterRows([...filterRows, { col: '', op: '=', val: '' }])}
                    >
                        <Plus className="h-3 w-3" /> Add filter
                    </Button>
                </div>
            </Field>

            <Field label="Select columns">
                <MultiColumnSelect
                    columns={columns}
                    selected={selectColumns}
                    onChange={(cols) => update({ select_columns: cols })}
                    isLoading={isLoading}
                />
                {selectColumns.length > 0 && (
                    <p className="text-[10px] text-muted-foreground mt-1">
                        {selectColumns.length} column{selectColumns.length !== 1 ? 's' : ''} selected
                    </p>
                )}
            </Field>

            <Field label="Rename columns">
                <div className="space-y-1.5">
                    {renamePairs.map(([from, to], i) => (
                        <div key={i} className="flex gap-1 items-center">
                            <ColumnSelect
                                columns={columns}
                                value={from}
                                onChange={(v) => {
                                    const next = [...renamePairs] as [string, string][]
                                    next[i] = [v, to]
                                    setRenamePairs(next)
                                }}
                                placeholder="Column"
                                isLoading={isLoading}
                            />
                            <span className="text-muted-foreground text-[11px] shrink-0">→</span>
                            <Input
                                className="!h-8 !py-0 px-2 text-[11px] flex-1 min-w-0"
                                value={to}
                                onChange={(e) => {
                                    const next = [...renamePairs] as [string, string][]
                                    next[i] = [from, e.target.value]
                                    setRenamePairs(next)
                                }}
                                placeholder="New name"
                            />
                            <Button
                                variant="ghost" size="icon"
                                className="!h-7 !w-7 shrink-0 hover:text-destructive"
                                onClick={() => setRenamePairs(renamePairs.filter((_, j) => j !== i) as [string, string][])}
                            >
                                <X className="h-3 w-3" />
                            </Button>
                        </div>
                    ))}
                    <Button
                        variant="outline" size="sm"
                        className="h-6 text-[11px] w-full gap-1"
                        onClick={() => setRenamePairs([...renamePairs, ['', '']] as [string, string][])}
                    >
                        <Plus className="h-3 w-3" /> Add rename
                    </Button>
                </div>
            </Field>

            <Separator className="my-2" />

            {/* Cast columns — change column data types */}
            <CastColumnsSection
                data={data}
                update={update}
                columns={columns}
                isLoading={isLoading}
            />

            <Separator className="my-2" />

            {/* SQL Expressions — COALESCE, NULLIF, TRIM, etc. */}
            <ExpressionsSection
                data={data}
                update={update}
                columns={columns}
                isLoading={isLoading}
            />
        </>
    )
}

// ─── Cast Columns Section ──────────────────────────────────────────────────────

const DUCKDB_TYPES = [
    'VARCHAR', 'TEXT', 'INTEGER', 'BIGINT', 'SMALLINT', 'TINYINT',
    'FLOAT', 'DOUBLE', 'DECIMAL', 'NUMERIC',
    'BOOLEAN', 'DATE', 'TIMESTAMP', 'TIMESTAMPTZ', 'TIME',
    'BLOB', 'JSON', 'UUID', 'HUGEINT',
]

interface CastRow { column: string; target_type: string }

function CastColumnsSection({
    data,
    update,
    columns,
    isLoading,
}: {
    data: FlowNodeData
    update: (patch: Partial<FlowNodeData>) => void
    columns: ColumnInfo[]
    isLoading: boolean
}) {
    const castRows: CastRow[] = (data.cast_columns as CastRow[]) || []

    const setCastRows = (rows: CastRow[]) => {
        // Keep all rows (including incomplete) so newly added rows stay visible in the UI.
        // The compiler ignores rows with empty column or target_type.
        update({ cast_columns: rows })
    }

    return (
        <Field label="Cast Columns (Type Conversion)">
            <div className="space-y-1.5">
                {/* Header */}
                {castRows.length > 0 && (
                    <div className="grid grid-cols-[1fr_100px_28px] gap-1 px-1 mb-0.5">
                        <span className="text-[9px] text-muted-foreground uppercase tracking-wider font-semibold">Column</span>
                        <span className="text-[9px] text-muted-foreground uppercase tracking-wider font-semibold">Target Type</span>
                        <span />
                    </div>
                )}
                {castRows.map((row, i) => (
                    <div key={i} className="grid grid-cols-[1fr_100px_28px] gap-1 items-center group">
                        <ColumnSelect
                            columns={columns}
                            value={row.column}
                            onChange={(v) => {
                                const next = [...castRows]
                                next[i] = { ...next[i], column: v }
                                setCastRows(next)
                            }}
                            placeholder="Column…"
                            isLoading={isLoading}
                            className="w-full"
                        />
                        <Select
                            value={row.target_type}
                            onValueChange={(v) => {
                                const next = [...castRows]
                                next[i] = { ...next[i], target_type: v }
                                setCastRows(next)
                            }}
                        >
                            <SelectTrigger className="h-7 text-[11px] w-full">
                                <SelectValue placeholder="Type" />
                            </SelectTrigger>
                            <SelectContent>
                                {DUCKDB_TYPES.map((t) => (
                                    <SelectItem key={t} value={t}>{t}</SelectItem>
                                ))}
                            </SelectContent>
                        </Select>
                        <Button
                            variant="ghost" size="icon"
                            className="h-7 w-7 text-muted-foreground hover:text-destructive opacity-50 group-hover:opacity-100"
                            onClick={() => setCastRows(castRows.filter((_, j) => j !== i))}
                        >
                            <X className="h-3 w-3" />
                        </Button>
                    </div>
                ))}
                <Button
                    variant="outline" size="sm"
                    className="h-6 text-[11px] w-full gap-1"
                    onClick={() => setCastRows([...castRows, { column: '', target_type: 'VARCHAR' }])}
                >
                    <Plus className="h-3 w-3" /> Add cast
                </Button>
            </div>
        </Field>
    )
}

// ─── SQL Expressions Section ───────────────────────────────────────────────────

const EXPRESSION_TEMPLATES = [
    { label: 'COALESCE', template: "COALESCE({col}, '')" },
    { label: 'NULLIF', template: "NULLIF({col}, '')" },
    { label: 'UPPER', template: 'UPPER({col})' },
    { label: 'LOWER', template: 'LOWER({col})' },
    { label: 'TRIM', template: 'TRIM({col})' },
    { label: 'LENGTH', template: 'LENGTH({col})' },
    { label: 'REPLACE', template: "REPLACE({col}, 'old', 'new')" },
    { label: 'SUBSTRING', template: 'SUBSTRING({col}, 1, 10)' },
    { label: 'CONCAT', template: "CONCAT({col}, ' ', {col2})" },
    { label: 'ROUND', template: 'ROUND({col}, 2)' },
    { label: 'ABS', template: 'ABS({col})' },
    { label: 'CEIL', template: 'CEIL({col})' },
    { label: 'FLOOR', template: 'FLOOR({col})' },
    { label: 'DATE_TRUNC', template: "DATE_TRUNC('day', {col})" },
    { label: 'DATE_PART', template: "DATE_PART('year', {col})" },
    { label: 'STRFTIME', template: "STRFTIME({col}, '%Y-%m-%d')" },
    { label: 'CASE WHEN', template: "CASE WHEN {col} IS NULL THEN 'N/A' ELSE {col} END" },
    { label: 'CAST', template: 'CAST({col} AS VARCHAR)' },
    { label: 'TRY_CAST', template: 'TRY_CAST({col} AS INTEGER)' },
    { label: 'REGEXP_REPLACE', template: "REGEXP_REPLACE({col}, '[^0-9]', '')" },
    { label: 'IFNULL', template: "IFNULL({col}, 0)" },
    { label: 'LIST_VALUE', template: "LIST_VALUE({col})" },
    { label: 'STRING_SPLIT', template: "STRING_SPLIT({col}, ',')" },
]

interface ExpressionRow { expr: string; alias: string }

function ExpressionsSection({
    data,
    update,
    columns: _columns,
    isLoading: _isLoading,
}: {
    data: FlowNodeData
    update: (patch: Partial<FlowNodeData>) => void
    columns: ColumnInfo[]
    isLoading: boolean
}) {
    const exprRows: ExpressionRow[] = (data.expressions as ExpressionRow[]) || []

    const setExprRows = (rows: ExpressionRow[]) => {
        update({ expressions: rows })
    }

    const insertTemplate = (index: number, template: string) => {
        const next = [...exprRows]
        next[index] = { ...next[index], expr: template }
        // Auto-generate alias from template function name
        const funcMatch = template.match(/^(\w+)\(/)
        if (funcMatch && !next[index].alias) {
            const rand = Math.floor(Math.random() * 1000)
            next[index].alias = `${funcMatch[1].toLowerCase()}_${rand}`
        }
        setExprRows(next)
    }

    return (
        <Field label="SQL Expressions">
            <div className="space-y-2">
                {exprRows.map((row, i) => (
                    <div
                        key={i}
                        className="space-y-1.5 p-2 rounded-md border border-border/60 bg-muted/20 animate-in fade-in slide-in-from-top-1 duration-200"
                    >
                        {/* Alias + delete */}
                        <div className="flex items-center gap-1.5">
                            <Input
                                className="h-7 text-xs flex-1 font-mono"
                                placeholder="alias_name"
                                value={row.alias}
                                onChange={(e) => {
                                    const next = [...exprRows]
                                    next[i] = { ...next[i], alias: e.target.value }
                                    setExprRows(next)
                                }}
                            />
                            <Button
                                variant="ghost" size="icon"
                                className="h-6 w-6 text-muted-foreground hover:text-destructive shrink-0"
                                onClick={() => setExprRows(exprRows.filter((_, j) => j !== i))}
                            >
                                <X className="h-3 w-3" />
                            </Button>
                        </div>

                        {/* SQL expression input */}
                        <textarea
                            className="w-full min-h-[48px] resize-y rounded-md border border-input bg-background px-2 py-1 text-xs font-mono placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring"
                            placeholder="COALESCE(column_name, 'default')"
                            value={row.expr}
                            onChange={(e) => {
                                const next = [...exprRows]
                                next[i] = { ...next[i], expr: e.target.value }
                                setExprRows(next)
                            }}
                        />

                        {/* Quick-insert template buttons */}
                        <div className="flex flex-wrap gap-0.5">
                            {EXPRESSION_TEMPLATES.slice(0, 8).map((tmpl) => (
                                <button
                                    key={tmpl.label}
                                    type="button"
                                    className="rounded px-1.5 py-0.5 text-[9px] border border-border text-muted-foreground hover:border-sky-400 hover:text-sky-600 transition-colors"
                                    onClick={() => insertTemplate(i, tmpl.template)}
                                    title={tmpl.template}
                                >
                                    {tmpl.label}
                                </button>
                            ))}
                        </div>
                        <Popover>
                            <PopoverTrigger asChild>
                                <button
                                    type="button"
                                    className="text-[9px] text-sky-500 hover:text-sky-600 underline"
                                >
                                    More functions…
                                </button>
                            </PopoverTrigger>
                            <PopoverContent className="w-[260px] p-2">
                                <p className="text-[10px] font-semibold text-muted-foreground mb-1.5">All DuckDB Functions</p>
                                <div className="flex flex-wrap gap-1">
                                    {EXPRESSION_TEMPLATES.map((tmpl) => (
                                        <button
                                            key={tmpl.label}
                                            type="button"
                                            className="rounded px-1.5 py-0.5 text-[10px] border border-border text-muted-foreground hover:border-sky-400 hover:text-sky-600 transition-colors"
                                            onClick={() => insertTemplate(i, tmpl.template)}
                                            title={tmpl.template}
                                        >
                                            {tmpl.label}
                                        </button>
                                    ))}
                                </div>
                            </PopoverContent>
                        </Popover>
                    </div>
                ))}
                <Button
                    variant="outline" size="sm"
                    className="h-6 text-[11px] w-full gap-1"
                    onClick={() => setExprRows([...exprRows, { expr: '', alias: '' }])}
                >
                    <Plus className="h-3 w-3" /> Add expression
                </Button>

                {exprRows.length === 0 && (
                    <div className="rounded-md bg-muted/50 p-2">
                        <p className="text-[10px] text-muted-foreground">
                            Add SQL expressions like <code className="text-[9px] bg-muted px-1 rounded">COALESCE(col, 0)</code>,{' '}
                            <code className="text-[9px] bg-muted px-1 rounded">UPPER(name)</code>,{' '}
                            <code className="text-[9px] bg-muted px-1 rounded">CASE WHEN … END</code>{' '}
                            to transform your data. Use <code className="text-[9px] bg-muted px-1 rounded">{'{col}'}</code> in templates
                            — replace with actual column names.
                        </p>
                    </div>
                )}
            </div>
        </Field>
    )
}

// ─── Aggregate ─────────────────────────────────────────────────────────────────

const AGG_FUNCTIONS = ['COUNT', 'COUNT_DISTINCT', 'SUM', 'AVG', 'MIN', 'MAX', 'FIRST', 'LAST']

interface AggRow { column: string; function: string; alias: string }

function AggregateConfig({ data, update, nodeId, flowTaskId }: ConfigFormProps) {
    const { edges } = useFlowTaskStore()
    // Find the upstream node connected to this aggregate node
    const upstreamNodeId = edges.find((e) => e.target === nodeId)?.source
    
    // Use upstream node for schema (input schema), NOT current node
    const { columns, isLoading } = useNodeSchema(flowTaskId, upstreamNodeId)
    
    const groupBy: string[] = (data.group_by as string[]) || []
    const aggregations: AggRow[] = (data.aggregations as AggRow[]) || []

    // Filter for numeric columns for aggregations (approximate check)
    const numericColumns = columns.filter(c => {
        const t = c.data_type.toLowerCase()
        return t.includes('int') || 
               t.includes('float') || 
               t.includes('double') || 
               t.includes('numeric') || 
               t.includes('real') || 
               t.includes('decimal')
    })

    return (
        <>
            <Field label="Group by columns">
                <MultiColumnSelect
                    columns={columns}
                    selected={groupBy}
                    onChange={(cols) => update({ group_by: cols })}
                    isLoading={isLoading}
                />
            </Field>

            <Field label="Aggregations">
                <div className="space-y-1.5">
                    {/* Header Row */}
                    <div className="grid grid-cols-[1.5fr_100px_1fr_28px] gap-2 px-1 mb-1">
                        <Label className="text-[10px] text-muted-foreground uppercase tracking-wider font-semibold">Column</Label>
                        <Label className="text-[10px] text-muted-foreground uppercase tracking-wider font-semibold">Func</Label>
                        <Label className="text-[10px] text-muted-foreground uppercase tracking-wider font-semibold">Alias</Label>
                        <span />
                    </div>

                    {aggregations.map((row, i) => (
                        <div key={i} className="grid grid-cols-[1.5fr_100px_1fr_28px] gap-2 items-center group">
                            <ColumnSelect
                                columns={numericColumns}
                                value={row.column}
                                onChange={(v) => {
                                    const next = [...aggregations]
                                    const currentAlias = next[i].alias
                                    const func = next[i].function
                                    
                                    // Generate new alias if empty or looks like auto-generated
                                    const isAuto = !currentAlias || /_\d+$/.test(currentAlias)
                                    let newAlias = currentAlias
                                    
                                    if (isAuto && v) {
                                        const cleanCol = v.replace(/\W+/g, '_')
                                        const rand = Math.floor(Math.random() * 1000)
                                        newAlias = `${cleanCol}_${func}_${rand}`
                                    }

                                    next[i] = { ...next[i], column: v, alias: newAlias }
                                    update({ aggregations: next })
                                }}
                                isLoading={isLoading}
                                placeholder="Col..."
                                className="h-8 w-full py-1"
                            />

                            <Select
                                value={row.function}
                                onValueChange={(v) => {
                                    const next = [...aggregations]
                                    const col = next[i].column
                                    const currentAlias = next[i].alias
                                    
                                    // Generate new alias if empty or looks like auto-generated
                                    const isAuto = !currentAlias || /_\d+$/.test(currentAlias)
                                    let newAlias = currentAlias

                                    if (isAuto && col) {
                                        const cleanCol = col.replace(/\W+/g, '_')
                                        const rand = Math.floor(Math.random() * 1000)
                                        newAlias = `${cleanCol}_${v}_${rand}`
                                    }

                                    next[i] = { ...next[i], function: v, alias: newAlias }
                                    update({ aggregations: next })
                                }}
                            >
                                <SelectTrigger className="h-8 text-xs w-full py-1">
                                    <SelectValue placeholder="Func" />
                                </SelectTrigger>
                                <SelectContent>
                                    {AGG_FUNCTIONS.map((f) => (
                                        <SelectItem key={f} value={f}>{f}</SelectItem>
                                    ))}
                                </SelectContent>
                            </Select>

                            <Input
                                className="h-8 text-xs font-mono"
                                value={row.alias}
                                onChange={(e) => {
                                    const next = [...aggregations]
                                    next[i] = { ...next[i], alias: e.target.value }
                                    update({ aggregations: next })
                                }}
                                placeholder="alias"
                            />

                            <Button
                                variant="ghost" size="icon"
                                className="h-8 w-7 text-muted-foreground hover:text-destructive opacity-50 group-hover:opacity-100 transition-opacity"
                                onClick={() =>
                                    update({ aggregations: aggregations.filter((_, j) => j !== i) })
                                }
                            >
                                <X className="h-4 w-4" />
                            </Button>
                        </div>
                    ))}
                    <div className="pt-1">
                        <Button
                            variant="outline" size="sm"
                            className="h-7 text-xs w-full gap-1.5 dashed border-muted-foreground/40 text-muted-foreground hover:text-foreground hover:bg-muted/50"
                            onClick={() =>
                                update({
                                    aggregations: [...aggregations, { column: '', function: 'COUNT', alias: 'count' }],
                                })
                            }
                        >
                            <Plus className="h-3.5 w-3.5" /> Add aggregation
                        </Button>
                    </div>
                </div>
            </Field>
        </>
    )
}

// ─── Join ──────────────────────────────────────────────────────────────────────

function JoinConfig({ data, update, nodeId, flowTaskId }: ConfigFormProps) {
    const { edges } = useFlowTaskStore()

    // Identify upstream nodes connected to the left/right handles
    // Edges targetHandle must match the handle IDs in JoinNode ('left', 'right')
    const leftEdge = edges.find((e) => e.target === nodeId && e.targetHandle === 'left')
    const rightEdge = edges.find((e) => e.target === nodeId && e.targetHandle === 'right')

    // Fetch schemas for upstream nodes
    const { columns: leftCols, isLoading: leftLoading } = useNodeSchema(flowTaskId, leftEdge?.source)
    const { columns: rightCols, isLoading: rightLoading } = useNodeSchema(flowTaskId, rightEdge?.source)

    // Current keys
    const leftKeys = (data.left_keys as string[]) || []
    const rightKeys = (data.right_keys as string[]) || []

    // Combine into pairs for rendering
    // Fallback to at least one empty pair if none exist?
    // No, empty state is valid (though preview will fail).
    // Let's ensure length matches by truncating or padding if desynced (rare).
    const rowCount = Math.max(leftKeys.length, rightKeys.length)

    const updatePair = (index: number, side: 'left' | 'right', value: string) => {
        const newLeft = [...leftKeys]
        const newRight = [...rightKeys]

        // Ensure arrays are long enough
        while (newLeft.length <= index) newLeft.push('')
        while (newRight.length <= index) newRight.push('')

        if (side === 'left') newLeft[index] = value
        else newRight[index] = value

        update({ left_keys: newLeft, right_keys: newRight })
    }

    const addPair = () => {
        update({
            left_keys: [...leftKeys, ''],
            right_keys: [...rightKeys, ''],
        })
    }

    const removePair = (index: number) => {
        const newLeft = leftKeys.filter((_, i) => i !== index)
        const newRight = rightKeys.filter((_, i) => i !== index)
        update({ left_keys: newLeft, right_keys: newRight })
    }

    return (
        <>
            <Field label="Join Type">
                <Select
                    value={(data.join_type as string) || 'INNER'}
                    onValueChange={(v) => update({ join_type: v })}
                >
                    <SelectTrigger className="h-7 text-xs w-full">
                        <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                        {['INNER', 'LEFT', 'RIGHT', 'FULL OUTER', 'CROSS'].map((t) => (
                            <SelectItem key={t} value={t}>
                                {t} JOIN
                            </SelectItem>
                        ))}
                    </SelectContent>
                </Select>
            </Field>

            <Separator className="my-2" />

            <div className="space-y-2">
                <div className="flex items-center justify-between">
                    <Label className="text-[10px] uppercase text-muted-foreground tracking-wider">
                        Join Conditions (ON)
                    </Label>
                    <Button
                        variant="ghost"
                        size="sm"
                        className="h-5 px-1.5 text-[10px]"
                        onClick={addPair}
                    >
                        <Plus className="h-3 w-3 mr-1" /> Add
                    </Button>
                </div>

                {rowCount === 0 && (
                    <div className="text-[10px] text-muted-foreground italic px-1">
                        No join conditions set. The preview will likely fail.
                    </div>
                )}

                <div className="space-y-2">
                    {Array.from({ length: rowCount }).map((_, i) => (
                        <div key={i} className="flex items-center gap-1.5 animate-in fade-in slide-in-from-left-1 duration-200">
                            <div className="flex-1 min-w-0">
                                <ColumnSelect
                                    columns={leftCols}
                                    value={leftKeys[i] || ''}
                                    onChange={(v) => updatePair(i, 'left', v)}
                                    placeholder="Left col..."
                                    isLoading={leftLoading}
                                    className="w-full"
                                />
                            </div>
                            <span className="text-muted-foreground text-[10px]">=</span>
                            <div className="flex-1 min-w-0">
                                <ColumnSelect
                                    columns={rightCols}
                                    value={rightKeys[i] || ''}
                                    onChange={(v) => updatePair(i, 'right', v)}
                                    placeholder="Right col..."
                                    isLoading={rightLoading}
                                    className="w-full"
                                />
                            </div>
                            <Button
                                variant="ghost"
                                size="icon"
                                className="h-7 w-7 text-muted-foreground hover:text-destructive shrink-0"
                                onClick={() => removePair(i)}
                            >
                                <X className="h-3.5 w-3.5" />
                            </Button>
                        </div>
                    ))}
                </div>

                {(!leftEdge || !rightEdge) && (
                    <div className="flex items-start gap-1.5 p-2 rounded bg-muted/50 text-[10px] text-muted-foreground mt-2">
                        <AlertCircle className="h-3 w-3 mt-0.5 shrink-0" />
                        <p>
                            Connect both Left and Right inputs to populate column lists.
                        </p>
                    </div>
                )}
            </div>
        </>
    )
}

// ─── New Rows (Add Columns) ────────────────────────────────────────────────────

type NewRowsColumn = { alias: string; type: 'static' | 'expression'; value?: string; expr?: string }

function NewRowsConfig({ data, update }: ConfigFormProps) {
    const rawCols = (data.columns as NewRowsColumn[] | undefined) || []

    const addColumn = () => {
        update({
            columns: [...rawCols, { alias: 'new_col', type: 'static', value: '' }],
        })
    }

    const removeColumn = (index: number) => {
        update({ columns: rawCols.filter((_, i) => i !== index) })
    }

    const updateColumn = (index: number, patch: Partial<NewRowsColumn>) => {
        const updated = rawCols.map((col, i) => (i === index ? { ...col, ...patch } : col))
        update({ columns: updated })
    }

    return (
        <>
            <div className="flex items-center justify-between">
                <Label className="text-[10px] uppercase text-muted-foreground tracking-wider">
                    Add Columns
                </Label>
                <Button
                    variant="ghost"
                    size="sm"
                    className="h-5 px-1.5 text-[10px]"
                    onClick={addColumn}
                >
                    <Plus className="h-3 w-3 mr-1" /> Add
                </Button>
            </div>

            {rawCols.length === 0 && (
                <div className="text-[10px] text-muted-foreground italic px-1">
                    No columns defined. Click Add to create one.
                </div>
            )}

            <div className="space-y-3">
                {rawCols.map((col, i) => (
                    <div
                        key={i}
                        className="space-y-1.5 p-2 rounded-md border border-border/60 bg-muted/20 animate-in fade-in slide-in-from-top-1 duration-200"
                    >
                        {/* Row header: alias + type toggle + delete */}
                        <div className="flex items-center gap-1.5">
                            <Input
                                className="h-8 text-xs flex-1 font-mono"
                                placeholder="column_name"
                                value={col.alias}
                                onChange={(e) => updateColumn(i, { alias: e.target.value })}
                            />
                            <Select
                                value={col.type}
                                onValueChange={(v: 'static' | 'expression') =>
                                    updateColumn(i, { type: v })
                                }
                            >
                                <SelectTrigger className="h-8 text-xs w-24 shrink-0">
                                    <SelectValue />
                                </SelectTrigger>
                                <SelectContent>
                                    <SelectItem value="static">Static</SelectItem>
                                    <SelectItem value="expression">Expression</SelectItem>
                                </SelectContent>
                            </Select>
                            <Button
                                variant="ghost"
                                size="icon"
                                className="h-6 w-6 text-muted-foreground hover:text-destructive shrink-0"
                                onClick={() => removeColumn(i)}
                            >
                                <X className="h-3 w-3" />
                            </Button>
                        </div>

                        {/* Value/Expression input */}
                        {col.type === 'static' ? (
                            <div className="space-y-0.5">
                                <span className="text-[9px] text-muted-foreground uppercase tracking-wider">Value</span>
                                <Input
                                    className="h-6 text-xs font-mono"
                                    placeholder="e.g. active, 42, true"
                                    value={col.value ?? ''}
                                    onChange={(e) => updateColumn(i, { value: e.target.value })}
                                />
                            </div>
                        ) : (
                            <div className="space-y-0.5">
                                <span className="text-[9px] text-muted-foreground uppercase tracking-wider">SQL Expression</span>
                                <textarea
                                    className="w-full min-h-[56px] resize-y rounded-md border border-input bg-background px-2 py-1 text-xs font-mono placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring"
                                    placeholder={`CASE WHEN status = 'A' THEN 1 ELSE 0 END`}
                                    value={col.expr ?? ''}
                                    onChange={(e) => updateColumn(i, { expr: e.target.value })}
                                />
                            </div>
                        )}
                    </div>
                ))}
            </div>
        </>
    )
}

// ─── Union ─────────────────────────────────────────────────────────────────────

function UnionConfig({ data, update, nodeId: _nodeId, flowTaskId: _flowTaskId }: ConfigFormProps) {
    return (
        <SwitchField
            label="UNION ALL (keep duplicates)"
            checked={(data.union_all as boolean) ?? true}
            onChange={(v) => update({ union_all: v })}
        />
    )
}

// ─── Pivot ─────────────────────────────────────────────────────────────────────

function PivotConfig({ data, update, nodeId, flowTaskId }: ConfigFormProps) {
    const { edges } = useFlowTaskStore()
    const parentId = edges?.find((e) => e.target === nodeId)?.source
    // Use the parent's output schema as our input schema.
    // If no parent, we default to current node (which likely returns empty if invalid) or just empty.
    const { columns, isLoading } = useNodeSchema(flowTaskId, parentId)
    const pivotType = (data.pivot_type as string) || 'PIVOT'

    return (
        <>
            <Field label="Pivot Type">
                <Select
                    value={pivotType}
                    onValueChange={(v) => update({ pivot_type: v as 'PIVOT' | 'UNPIVOT' })}
                >
                    <SelectTrigger className="h-7 text-xs w-full">
                        <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                        <SelectItem value="PIVOT">PIVOT (rows → columns)</SelectItem>
                        <SelectItem value="UNPIVOT">UNPIVOT (columns → rows)</SelectItem>
                    </SelectContent>
                </Select>
            </Field>

            <Field label="Pivot column">
                <ColumnSelect
                    columns={columns}
                    value={(data.pivot_column as string) || ''}
                    onChange={(v) => update({ pivot_column: v })}
                    isLoading={isLoading}
                    className="w-full"
                />
            </Field>

            <Field label="Value column">
                <ColumnSelect
                    columns={columns}
                    value={(data.value_column as string) || ''}
                    onChange={(v) => update({ value_column: v })}
                    isLoading={isLoading}
                    className="w-full"
                />
            </Field>

            <Field label="Pivot values (comma-separated)">
                <Input
                    className="h-7 text-xs"
                    value={((data.pivot_values as string[]) || []).join(', ')}
                    onChange={(e) =>
                        update({
                            pivot_values: e.target.value
                                ? e.target.value.split(',').map((s) => s.trim())
                                : [],
                        })
                    }
                    placeholder="val1, val2, val3"
                />
            </Field>
        </>
    )
}

// ─── Output ────────────────────────────────────────────────────────────────────

function OutputConfig({ data, update }: ConfigFormProps) {
    const { data: destsData, isLoading: destsLoading } = useQuery({
        queryKey: ['destinations'],
        queryFn: () => destinationsRepo.getAll(),
        staleTime: 30_000,
    })

    const destinationId = data.destination_id as number | undefined

    const { data: tablesData, isLoading: tablesLoading } = useQuery({
        queryKey: ['destination-tables', destinationId],
        queryFn: () => destinationsRepo.getTableList(destinationId!),
        enabled: !!destinationId,
        staleTime: 30_000,
    })

    const tables: string[] = tablesData?.tables ?? []

    const upsertKeys: string[] = (data.upsert_keys as string[]) || []
    const upsertKeysText = upsertKeys.join(', ')

    return (
        <>
            <Field label="Destination">
                {destsLoading ? (
                    <div className="flex items-center gap-1.5 h-7 text-xs text-muted-foreground">
                        <Loader2 className="h-3 w-3 animate-spin" /> Loading…
                    </div>
                ) : (
                    <Select
                        value={data.destination_id != null ? String(data.destination_id) : ''}
                        onValueChange={(v) => update({ destination_id: parseInt(v), table_name: undefined })}
                    >
                        <SelectTrigger className="h-7 text-xs w-full">
                            <SelectValue placeholder="Select destination…" />
                        </SelectTrigger>
                        <SelectContent>
                            {(destsData?.destinations ?? []).map((d) => (
                                <SelectItem key={d.id} value={String(d.id)}>
                                    {d.name}
                                </SelectItem>
                            ))}
                        </SelectContent>
                    </Select>
                )}
            </Field>

            <Field label="Schema">
                <Input
                    className="h-7 text-xs"
                    value={(data.schema_name as string) || 'public'}
                    onChange={(e) => update({ schema_name: e.target.value })}
                />
            </Field>

            {!!destinationId && (
                <Field label="Target Table">
                    {tablesLoading ? (
                        <div className="flex items-center gap-1.5 h-7 text-xs text-muted-foreground">
                            <Loader2 className="h-3 w-3 animate-spin" /> Loading tables…
                        </div>
                    ) : (
                        <Popover>
                            <PopoverTrigger asChild>
                                <Button
                                    variant="outline"
                                    role="combobox"
                                    className={cn(
                                        "w-full justify-between h-7 text-xs font-normal",
                                        !data.table_name && "text-muted-foreground"
                                    )}
                                >
                                    {data.table_name
                                        ? tables.find((t) => t === data.table_name) || data.table_name
                                        : "Select table..."}
                                    <ChevronsUpDown className="ml-2 h-4 w-4 shrink-0 opacity-50" />
                                </Button>
                            </PopoverTrigger>
                            <PopoverContent className="w-[300px] p-0">
                                <Command>
                                    <CommandInput placeholder="Search table..." className="h-9 text-xs" />
                                    <CommandList>
                                        <CommandEmpty>No table found.</CommandEmpty>
                                        <CommandGroup>
                                            {tables.map((t) => (
                                                <CommandItem
                                                    value={t}
                                                    key={t}
                                                    onSelect={(value) => {
                                                        update({ table_name: value })
                                                    }}
                                                    className="text-xs"
                                                >
                                                    <Check
                                                        className={cn(
                                                            "mr-2 h-4 w-4",
                                                            t === data.table_name
                                                                ? "opacity-100"
                                                                : "opacity-0"
                                                        )}
                                                    />
                                                    {t}
                                                </CommandItem>
                                            ))}
                                        </CommandGroup>
                                    </CommandList>
                                </Command>
                            </PopoverContent>
                        </Popover>
                    )}
                </Field>
            )}

            <Field label="Write Mode">
                <Select
                    value={(data.write_mode as string) || 'APPEND'}
                    onValueChange={(v) => update({ write_mode: v as WriteMode })}
                >
                    <SelectTrigger className="h-7 text-xs w-full">
                        <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                        <SelectItem value="APPEND">APPEND</SelectItem>
                        <SelectItem value="UPSERT">UPSERT (MERGE)</SelectItem>
                        <SelectItem value="REPLACE">REPLACE (truncate + insert)</SelectItem>
                    </SelectContent>
                </Select>
            </Field>

            <Field label="Upsert keys">
                <Input
                    className="h-7 text-xs"
                    value={upsertKeysText}
                    onChange={(e) => {
                        const cols = e.target.value
                            .split(',')
                            .map((s) => s.trim())
                            .filter(Boolean)
                        update({ upsert_keys: cols })
                    }}
                    placeholder="e.g. id, user_id"
                />
                <p className="text-[10px] text-muted-foreground">
                    Comma-separated column names
                </p>
            </Field>
        </>
    )
}

// ─── Note ──────────────────────────────────────────────────────────────────────

// ─── SQL ───────────────────────────────────────────────────────────────────────

function SqlConfig({ data, update }: ConfigFormProps) {
    const expr = (data.sql_expression as string) ?? ''
    return (
        <>
            <Field label="SQL Expression">
                <textarea
                    className="w-full min-h-[160px] resize-y rounded-md border border-input bg-background px-3 py-2 font-mono text-xs text-foreground placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring"
                    value={expr}
                    onChange={(e) => update({ sql_expression: e.target.value })}
                    placeholder={`SELECT *\nFROM {{input}}\nWHERE status = 'active'`}
                    spellCheck={false}
                />
            </Field>
            <div className="rounded-md bg-muted/50 p-2 space-y-1">
                <p className="text-[10px] font-medium text-muted-foreground">Template variables</p>
                <p className="text-[10px] text-muted-foreground">
                    <code className="rounded bg-muted px-1">{'{{input}}'}</code> — first upstream CTE
                </p>
                <p className="text-[10px] text-muted-foreground">
                    <code className="rounded bg-muted px-1">{'{{input_N}}'}</code> — Nth upstream CTE (0-based)
                </p>
                <p className="text-[10px] text-muted-foreground">
                    <code className="rounded bg-muted px-1">{'{{upstream}}'}</code> — alias for first upstream
                </p>
            </div>
        </>
    )
}

function NoteConfig({ data, update }: ConfigFormProps) {
    const content = (data.note_content as string) ?? ''
    return (
        <Field label="Note content">
            <textarea
                className="w-full min-h-[120px] resize-y rounded-md border border-input bg-background px-3 py-2 text-xs text-foreground placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring"
                value={content}
                onChange={(e) => update({ note_content: e.target.value })}
                placeholder="Type your note here…"
            />
            <p className="text-[10px] text-muted-foreground">
                This text is also editable directly on the canvas.
            </p>
        </Field>
    )
}

// ─── Shared helpers ────────────────────────────────────────────────────────────

function Field({ label, children }: { label: string; children: React.ReactNode }) {
    return (
        <div className="space-y-1">
            <Label className="text-[10px] text-muted-foreground uppercase tracking-wider">
                {label}
            </Label>
            {children}
        </div>
    )
}

function SwitchField({
    label,
    checked,
    onChange,
}: {
    label: string
    checked: boolean
    onChange: (v: boolean) => void
}) {
    return (
        <div className="flex items-center justify-between">
            <Label className="text-xs">{label}</Label>
            <Switch checked={checked} onCheckedChange={onChange} />
        </div>
    )
}
