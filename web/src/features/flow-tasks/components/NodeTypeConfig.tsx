import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { destinationsRepo } from '@/repo/destinations'
import { type FlowNodeData } from '@/repo/flow-tasks'
import { useFlowTaskStore } from '../store/flow-task-store'
import { useNodeSchema } from '../hooks/useNodeSchema'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
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
import {
    Select,
    SelectContent,
    SelectItem,
    SelectTrigger,
    SelectValue,
} from '@/components/ui/select'
import { Loader2, Plus, X, Check, AlertCircle, ChevronsUpDown } from 'lucide-react'
import { cn } from '@/lib/utils'

// Types
type WriteMode = 'APPEND' | 'UPSERT' | 'REPLACE'
type ColumnInfo = { column_name: string; data_type: string }

interface ConfigFormProps {
    data: FlowNodeData
    update: (patch: Partial<FlowNodeData>) => void
    nodeId: string
    flowTaskId: number | null
}

// ─── Helpers ───────────────────────────────────────────────────────────────────

function Field({ label, children, className }: { label: string; children: React.ReactNode; className?: string }) {
    return (
        <div className={cn("space-y-1.5", className)}>
            <Label className="text-[11px] text-muted-foreground uppercase tracking-wider font-semibold">
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
        <div className="flex items-center justify-between p-3 rounded-md border border-border/50 bg-muted/10">
            <Label className="text-sm cursor-pointer" onClick={() => onChange(!checked)}>{label}</Label>
            <Switch checked={checked} onCheckedChange={onChange} />
        </div>
    )
}

// ─── Shared Selectors ──────────────────────────────────────────────────────────

function ColumnSelect({
    columns,
    value,
    onChange,
    placeholder = 'Select column...',
    isLoading = false,
    className,
}: {
    columns: ColumnInfo[]
    value: string
    onChange: (val: string) => void
    placeholder?: string
    isLoading?: boolean
    className?: string
}) {
    const [open, setOpen] = useState(false)
    const selectedText = value || placeholder

    return (
        <Popover open={open} onOpenChange={setOpen}>
            <PopoverTrigger asChild>
                <Button
                    variant="outline"
                    role="combobox"
                    aria-expanded={open}
                    className={cn(
                        'justify-between font-normal bg-background px-3 h-9 text-xs',
                        !value && 'text-muted-foreground',
                        className
                    )}
                >
                    {isLoading ? (
                        <div className="flex items-center gap-2">
                            <Loader2 className="h-3 w-3 animate-spin" />
                            Loading...
                        </div>
                    ) : (
                        <span className="truncate">{selectedText}</span>
                    )}
                    <ChevronsUpDown className="ml-2 h-4 w-4 shrink-0 opacity-50" />
                </Button>
            </PopoverTrigger>
            <PopoverContent className="w-[240px] p-0" align="start">
                <Command>
                    <CommandInput placeholder="Search column..." className="h-9 text-xs" />
                    <CommandList>
                        <CommandEmpty>No column found.</CommandEmpty>
                        <CommandGroup>
                            {columns.map((col) => (
                                <CommandItem
                                    key={col.column_name}
                                    value={col.column_name}
                                    onSelect={(v) => {
                                        onChange(v)
                                        setOpen(false)
                                    }}
                                    className="text-xs"
                                >
                                    <Check
                                        className={cn(
                                            'mr-2 h-4 w-4 shrink-0 border border-primary text-transparent rounded-[4px] transition-colors',
                                            value === col.column_name ? 'bg-primary text-primary-foreground border-primary' : 'border-input opacity-50'
                                        )}
                                    />
                                    <span className="font-mono flex-1 truncate pr-2">{col.column_name}</span>
                                    <span className="text-[10px] text-muted-foreground truncate max-w-[80px]">
                                        {col.data_type}
                                    </span>
                                </CommandItem>
                            ))}
                        </CommandGroup>
                    </CommandList>
                </Command>
            </PopoverContent>
        </Popover>
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
    const [open, setOpen] = useState(false)

    const toggleCol = (colName: string) => {
        if (selected.includes(colName)) {
            onChange(selected.filter((c) => c !== colName))
        } else {
            onChange([...selected, colName])
        }
    }

    const selectedText =
        selected.length === 0
            ? 'Select columns...'
            : selected.length === 1
                ? selected[0]
                : `${selected.length} columns selected`

    return (
        <Popover open={open} onOpenChange={setOpen}>
            <PopoverTrigger asChild>
                <Button
                    variant="outline"
                    role="combobox"
                    aria-expanded={open}
                    className="w-full justify-between h-9 text-xs font-normal bg-background px-3"
                >
                    {isLoading ? (
                        <div className="flex items-center gap-2">
                            <Loader2 className="h-3 w-3 animate-spin" />
                            Loading...
                        </div>
                    ) : (
                        <span className="truncate">{selectedText}</span>
                    )}
                    <ChevronsUpDown className="ml-2 h-4 w-4 shrink-0 opacity-50" />
                </Button>
            </PopoverTrigger>
            <PopoverContent className="w-[300px] p-0" align="start">
                <Command>
                    <CommandInput placeholder="Search columns..." className="h-9 text-xs" />
                    <CommandList>
                        <CommandEmpty>No column found.</CommandEmpty>
                        <CommandGroup>
                            <CommandItem
                                onSelect={() => onChange(selected.length === columns.length ? [] : columns.map(c => c.column_name))}
                                className="text-xs font-medium border-b border-border mb-1 pb-2"
                            >
                                <Check
                                    className={cn(
                                        'mr-2 h-4 w-4 shrink-0 transition-colors rounded-[4px] border',
                                        selected.length === columns.length
                                            ? 'bg-primary text-primary-foreground border-primary'
                                            : selected.length > 0
                                                ? 'bg-primary/50 text-transparent border-primary/50'
                                                : 'border-input opacity-50 text-transparent'
                                    )}
                                />
                                Select All
                            </CommandItem>
                            {columns.map((col) => {
                                const isSelected = selected.includes(col.column_name)
                                return (
                                    <CommandItem
                                        key={col.column_name}
                                        value={col.column_name}
                                        onSelect={() => toggleCol(col.column_name)}
                                        className="text-xs"
                                    >
                                        <Check
                                            className={cn(
                                                'mr-2 h-4 w-4 shrink-0 transition-colors rounded-[4px] border',
                                                isSelected
                                                    ? 'bg-primary text-primary-foreground border-primary'
                                                    : 'border-input opacity-50 text-transparent'
                                            )}
                                        />
                                        <span className="font-mono flex-1">{col.column_name}</span>
                                        <span className="text-[10px] text-muted-foreground mr-1 truncate max-w-[60px]">
                                            {col.data_type}
                                        </span>
                                    </CommandItem>
                                )
                            })}
                        </CommandGroup>
                    </CommandList>
                </Command>
            </PopoverContent>
        </Popover>
    )
}

// ─── Input ─────────────────────────────────────────────────────────────────────

function InputConfig({ data, update }: ConfigFormProps) {
    const { data: sourcesData, isLoading: sourcesLoading } = useQuery({
        queryKey: ['sources'],
        queryFn: async () => {
            const res = await fetch('/api/v1/sources')
            if (!res.ok) throw new Error('Failed to fetch sources')
            return res.json()
        },
        staleTime: 30_000,
    })

    const sourceId = data.source_id as number | undefined

    const { data: tablesData, isLoading: tablesLoading } = useQuery({
        queryKey: ['source-tables', sourceId],
        queryFn: async () => {
            if (!sourceId) return { tables: [] }
            const res = await fetch(`/api/v1/sources/${sourceId}/tables`)
            if (!res.ok) throw new Error('Failed to fetch tables')
            return res.json()
        },
        enabled: !!sourceId,
        staleTime: 30_000,
    })

    const sources = sourcesData?.sources ?? []
    const tables: string[] = tablesData?.tables ?? []

    return (
        <div className="grid grid-cols-2 gap-6">
            <Field label="Source Connection">
                {sourcesLoading ? (
                    <div className="flex items-center gap-2 h-9 text-xs text-muted-foreground bg-muted/20 px-3 rounded-md border border-border/50">
                        <Loader2 className="h-4 w-4 animate-spin" /> Loading…
                    </div>
                ) : (
                    <Select
                        value={data.source_id != null ? String(data.source_id) : ''}
                        onValueChange={(v) => update({ source_id: parseInt(v), table_name: undefined })}
                    >
                        <SelectTrigger className="h-9 text-xs w-full bg-background shadow-sm border border-border/60">
                            <SelectValue placeholder="Select a source database…" />
                        </SelectTrigger>
                        <SelectContent>
                            {sources.map((s: any) => (
                                <SelectItem key={s.id} value={String(s.id)}>
                                    <div className="flex items-center gap-2">
                                        <span className="font-medium text-[13px]">{s.name}</span>
                                        <span className="text-[10px] text-muted-foreground bg-muted px-1.5 py-0.5 rounded uppercase">{s.type}</span>
                                    </div>
                                </SelectItem>
                            ))}
                        </SelectContent>
                    </Select>
                )}
            </Field>

            <Field label="Target Table">
                {!sourceId ? (
                    <div className="h-9 px-3 py-2 text-xs text-muted-foreground border border-dashed border-border rounded-md bg-muted/10 flex items-center italic">
                        Select a source first...
                    </div>
                ) : tablesLoading ? (
                    <div className="flex items-center gap-2 h-9 text-xs text-muted-foreground bg-muted/20 px-3 rounded-md border border-border/50">
                        <Loader2 className="h-4 w-4 animate-spin" /> Loading tables…
                    </div>
                ) : (
                    <Popover>
                        <PopoverTrigger asChild>
                            <Button
                                variant="outline"
                                role="combobox"
                                className={cn(
                                    "w-full justify-between h-9 text-xs font-normal bg-background shadow-sm border border-border/60",
                                    !data.table_name && "text-muted-foreground"
                                )}
                            >
                                {data.table_name || "Select a table..."}
                                <ChevronsUpDown className="ml-2 h-4 w-4 shrink-0 opacity-50" />
                            </Button>
                        </PopoverTrigger>
                        <PopoverContent className="w-[300px] p-0" align="start">
                            <Command>
                                <CommandInput placeholder="Search table..." className="h-9 text-xs" />
                                <CommandList>
                                    <CommandEmpty>No table found.</CommandEmpty>
                                    <CommandGroup>
                                        {tables.map((t) => (
                                            <CommandItem
                                                value={t}
                                                key={t}
                                                onSelect={(value) => update({ table_name: value })}
                                                className="text-xs font-mono"
                                            >
                                                <Check
                                                    className={cn(
                                                        "mr-2 h-4 w-4",
                                                        t === data.table_name ? "opacity-100" : "opacity-0"
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

            <div className="col-span-2">
                <InputFilterSection data={data} update={update} />
            </div>
        </div>
    )
}

function InputFilterSection({ data, update }: { data: FlowNodeData; update: (patch: Partial<FlowNodeData>) => void }) {
    const filterRows = (data.filter_rows as any[]) || []

    const setFilterRows = (rows: any[]) => {
        const expr = rows
            .filter((r) => r.col && r.op)
            .map((r) => {
                if (r.op === 'IS NULL') return `${r.col} IS NULL`
                if (r.op === 'IS NOT NULL') return `${r.col} IS NOT NULL`
                if (r.op === 'IN') return `${r.col} IN (${r.val})`
                return `${r.col} ${r.op} '${r.val}'`
            })
            .join(' AND ')
        update({ filter_rows: rows, filter_sql: expr || undefined })
    }

    return (
        <Field label="Pre-Filter Source Data (Optional)">
            <div className="space-y-3 p-4 rounded-md border border-border bg-muted/5">
                {filterRows.length === 0 && (
                    <div className="text-xs text-muted-foreground italic mb-2">No pre-filters active. All rows will be extracted.</div>
                )}
                {filterRows.map((row, i) => (
                    <div key={i} className="flex gap-2 items-center">
                        <Input
                            className="h-9 text-xs font-mono w-[30%]"
                            value={row.col}
                            onChange={(e) => {
                                const next = [...filterRows]
                                next[i] = { ...next[i], col: e.target.value }
                                setFilterRows(next)
                            }}
                            placeholder="column_name"
                        />
                        <Select
                            value={row.op}
                            onValueChange={(v) => {
                                const next = [...filterRows]
                                next[i] = { ...next[i], op: v }
                                setFilterRows(next)
                            }}
                        >
                            <SelectTrigger className="h-9 text-xs w-[120px] shrink-0 font-mono">
                                <SelectValue placeholder="op" />
                            </SelectTrigger>
                            <SelectContent>
                                {['=', '!=', '>', '<', '>=', '<=', 'LIKE', 'IN', 'IS NULL', 'IS NOT NULL'].map((op) => (
                                    <SelectItem key={op} value={op}>{op}</SelectItem>
                                ))}
                            </SelectContent>
                        </Select>
                        {row.op !== 'IS NULL' && row.op !== 'IS NOT NULL' ? (
                            <Input
                                className="h-9 text-xs font-mono flex-1"
                                value={row.val}
                                onChange={(e) => {
                                    const next = [...filterRows]
                                    next[i] = { ...next[i], val: e.target.value }
                                    setFilterRows(next)
                                }}
                                placeholder="value (e.g. 'active', 42)"
                            />
                        ) : (
                            <div className="flex-1" />
                        )}
                        <Button
                            variant="ghost" size="icon"
                            className="h-9 w-9 shrink-0 hover:text-destructive text-muted-foreground"
                            onClick={() => setFilterRows(filterRows.filter((_, j) => j !== i))}
                        >
                            <X className="h-4 w-4" />
                        </Button>
                    </div>
                ))}
                <div className="flex items-center gap-3">
                    <Button
                        variant="outline" size="sm"
                        className="h-8 text-xs gap-1.5"
                        onClick={() => setFilterRows([...filterRows, { col: '', op: '=', val: '' }])}
                    >
                        <Plus className="h-3.5 w-3.5" /> Add condition
                    </Button>
                    {filterRows.length > 0 && (
                        <div className="flex-1 bg-muted/40 p-1.5 px-3 rounded text-[10px] font-mono border border-border/30 truncate text-muted-foreground">
                            Generated SQL: {(data.filter_sql as string) || '—'}
                        </div>
                    )}
                </div>
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
    const { columns = [], isLoading } = useNodeSchema(flowTaskId, nodeId)
    const filterRows: FilterRow[] = (data.filter_rows as FilterRow[]) || []
    const selectColumns: string[] = (data.select_columns as string[]) || []

    const renamePairs: [string, string][] =
        (data._rename_pairs as [string, string][] | undefined) ??
        Object.entries((data.rename_columns as Record<string, string>) || {})

    const setRenamePairs = (pairs: [string, string][]) => {
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
        <div className="grid grid-cols-2 gap-x-8 gap-y-6">
            <div className="col-span-2 flex gap-6">
                <SwitchField label="Drop nulls (all columns)" checked={!!data.drop_nulls} onChange={(v) => update({ drop_nulls: v })} />
                <SwitchField label="Deduplicate rows (distinct)" checked={!!data.deduplicate} onChange={(v) => update({ deduplicate: v })} />
            </div>

            <Field label="Filter Rows">
                <div className="space-y-3 p-4 rounded-md border border-border bg-muted/5">
                    {filterRows.map((row, i) => (
                        <div key={i} className="flex gap-2 items-center">
                            <ColumnSelect
                                columns={columns}
                                value={row.col}
                                onChange={(v) => {
                                    const next = [...filterRows]
                                    next[i] = { ...next[i], col: v }
                                    setFilterRows(next)
                                }}
                                placeholder="Col"
                                isLoading={isLoading}
                                className="w-[30%]"
                            />
                            <Select
                                value={row.op}
                                onValueChange={(v) => {
                                    const next = [...filterRows]
                                    next[i] = { ...next[i], op: v }
                                    setFilterRows(next)
                                }}
                            >
                                <SelectTrigger className="h-9 text-xs w-[120px] shrink-0 font-mono">
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
                                    className="h-9 text-xs font-mono flex-1"
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
                                className="h-9 w-9 shrink-0 hover:text-destructive text-muted-foreground"
                                onClick={() => setFilterRows(filterRows.filter((_, j) => j !== i))}
                            >
                                <X className="h-4 w-4" />
                            </Button>
                        </div>
                    ))}
                    <Button
                        variant="outline" size="sm"
                        className="h-8 text-xs gap-1.5"
                        onClick={() => setFilterRows([...filterRows, { col: '', op: '=', val: '' }])}
                    >
                        <Plus className="h-3.5 w-3.5" /> Add filter condition
                    </Button>
                </div>
            </Field>

            <Field label="Rename columns">
                <div className="space-y-3 p-4 rounded-md border border-border bg-muted/5">
                    {renamePairs.map(([from, to], i) => (
                        <div key={i} className="flex gap-2 items-center">
                            <ColumnSelect
                                columns={columns}
                                value={from}
                                onChange={(v) => {
                                    const next = [...renamePairs] as [string, string][]
                                    next[i] = [v, to]
                                    setRenamePairs(next)
                                }}
                                placeholder="Original Column"
                                isLoading={isLoading}
                                className="flex-1"
                            />
                            <span className="text-muted-foreground text-[11px] shrink-0 font-mono">-&gt;</span>
                            <Input
                                className="h-9 text-xs font-mono flex-1"
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
                                className="h-9 w-9 shrink-0 hover:text-destructive text-muted-foreground"
                                onClick={() => setRenamePairs(renamePairs.filter((_, j) => j !== i) as [string, string][])}
                            >
                                <X className="h-4 w-4" />
                            </Button>
                        </div>
                    ))}
                    <Button
                        variant="outline" size="sm"
                        className="h-8 text-xs gap-1.5"
                        onClick={() => setRenamePairs([...renamePairs, ['', '']] as [string, string][])}
                    >
                        <Plus className="h-3.5 w-3.5" /> Add rename mapping
                    </Button>
                </div>
            </Field>

            <div className="col-span-2">
                <Field label="Select / Retain columns">
                    <div className="flex items-center gap-4 border border-border/60 bg-muted/10 p-4 rounded-md">
                        <div className="w-1/2">
                            <MultiColumnSelect
                                columns={columns}
                                selected={selectColumns}
                                onChange={(cols) => update({ select_columns: cols })}
                                isLoading={isLoading}
                            />
                        </div>
                        <p className="text-xs text-muted-foreground italic flex-1">
                            {selectColumns.length === 0 ? "If none selected, all columns will be retained." : `${selectColumns.length} columns explicitly selected.`}
                        </p>
                    </div>
                </Field>
            </div>

            <div className="col-span-2">
                <Separator className="my-2" />
            </div>

            <div className="col-span-2">
                <CastColumnsSection data={data} update={update} columns={columns} isLoading={isLoading} />
            </div>

            <div className="col-span-2">
                <Separator className="my-2" />
            </div>

            <div className="col-span-2">
                <ExpressionsSection data={data} update={update} columns={columns} isLoading={isLoading} />
            </div>
        </div>
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

function CastColumnsSection({ data, update, columns, isLoading }: { data: FlowNodeData, update: (patch: Partial<FlowNodeData>) => void, columns: ColumnInfo[], isLoading: boolean }) {
    const castRows: CastRow[] = (data.cast_columns as CastRow[]) || []

    const setCastRows = (rows: CastRow[]) => update({ cast_columns: rows })

    return (
        <Field label="Cast Columns (Type Conversion)">
            <div className="space-y-3 p-4 rounded-md border border-border bg-muted/5">
                {castRows.length > 0 && (
                    <div className="grid grid-cols-[1fr_180px_36px] gap-3 px-1 mb-1">
                        <span className="text-[10px] text-muted-foreground uppercase tracking-wider font-semibold">Column</span>
                        <span className="text-[10px] text-muted-foreground uppercase tracking-wider font-semibold">Target Type</span>
                        <span />
                    </div>
                )}
                {castRows.map((row, i) => (
                    <div key={i} className="grid grid-cols-[1fr_180px_36px] gap-3 items-center group">
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
                            className="w-full h-9"
                        />
                        <Select
                            value={row.target_type}
                            onValueChange={(v) => {
                                const next = [...castRows]
                                next[i] = { ...next[i], target_type: v }
                                setCastRows(next)
                            }}
                        >
                            <SelectTrigger className="h-9 text-xs font-mono">
                                <SelectValue placeholder="Type" />
                            </SelectTrigger>
                            <SelectContent>
                                {DUCKDB_TYPES.map((t) => (
                                    <SelectItem key={t} value={t} className="font-mono text-xs">{t}</SelectItem>
                                ))}
                            </SelectContent>
                        </Select>
                        <Button
                            variant="ghost" size="icon"
                            className="h-9 w-9 text-muted-foreground hover:text-destructive"
                            onClick={() => setCastRows(castRows.filter((_, j) => j !== i))}
                        >
                            <X className="h-4 w-4" />
                        </Button>
                    </div>
                ))}
                <div className="mt-2">
                    <Button
                        variant="outline" size="sm"
                        className="h-8 text-xs gap-1.5"
                        onClick={() => setCastRows([...castRows, { column: '', target_type: 'VARCHAR' }])}
                    >
                        <Plus className="h-3.5 w-3.5" /> Add type cast
                    </Button>
                </div>
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

function ExpressionsSection({ data, update }: { data: FlowNodeData, update: (patch: Partial<FlowNodeData>) => void, columns?: ColumnInfo[], isLoading?: boolean }) {
    const exprRows: ExpressionRow[] = (data.expressions as ExpressionRow[]) || []

    const setExprRows = (rows: ExpressionRow[]) => update({ expressions: rows })

    const insertTemplate = (index: number, template: string) => {
        const next = [...exprRows]
        next[index] = { ...next[index], expr: template }
        const funcMatch = template.match(/^(\w+)\(/)
        if (funcMatch && !next[index].alias) {
            const rand = Math.floor(Math.random() * 1000)
            next[index].alias = `${funcMatch[1].toLowerCase()}_${rand}`
        }
        setExprRows(next)
    }

    return (
        <Field label="Computed Columns (SQL Expressions)">
            <div className="space-y-4 pt-2">
                <div className="grid grid-cols-2 xl:grid-cols-3 gap-4">
                    {exprRows.map((row, i) => (
                        <div key={i} className="flex flex-col gap-2 p-3 rounded-md border border-border/80 bg-muted/10 shadow-sm relative group">
                            <div className="flex items-center gap-2">
                                <Label className="text-[10px] text-muted-foreground uppercase font-semibold">Alias (As)</Label>
                                <Input
                                    className="h-7 text-xs flex-1 font-mono border-muted-foreground/30 focus-visible:ring-primary"
                                    placeholder="new_column_name"
                                    value={row.alias}
                                    onChange={(e) => {
                                        const next = [...exprRows]
                                        next[i] = { ...next[i], alias: e.target.value }
                                        setExprRows(next)
                                    }}
                                />
                                <Button
                                    variant="ghost" size="icon"
                                    className="h-7 w-7 text-muted-foreground hover:text-destructive hover:bg-destructive/10"
                                    onClick={() => setExprRows(exprRows.filter((_, j) => j !== i))}
                                >
                                    <X className="h-3.5 w-3.5" />
                                </Button>
                            </div>

                            <div className="flex-1 flex flex-col pt-1">
                                <textarea
                                    className="w-full h-[72px] resize-none rounded-md border border-input bg-background/50 px-3 py-2 text-xs font-mono placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-primary"
                                    placeholder="COALESCE(column_name, 'default')"
                                    value={row.expr}
                                    onChange={(e) => {
                                        const next = [...exprRows]
                                        next[i] = { ...next[i], expr: e.target.value }
                                        setExprRows(next)
                                    }}
                                    spellCheck={false}
                                />
                            </div>

                            <div className="flex items-center gap-2 justify-between mt-1">
                                <Popover>
                                    <PopoverTrigger asChild>
                                        <button type="button" className="text-[10px] font-medium text-primary hover:text-primary/80 transition-colors">
                                            + Insert function
                                        </button>
                                    </PopoverTrigger>
                                    <PopoverContent className="w-[320px] p-3 shadow-lg">
                                        <p className="text-[11px] font-semibold text-muted-foreground mb-2 uppercase tracking-wide">Common DuckDB Functions</p>
                                        <div className="flex flex-wrap gap-1.5">
                                            {EXPRESSION_TEMPLATES.map((tmpl) => (
                                                <button
                                                    key={tmpl.label}
                                                    type="button"
                                                    className="rounded px-2 py-1 text-[11px] border border-border/60 bg-muted/20 hover:bg-muted text-muted-foreground hover:text-foreground transition-colors font-mono"
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
                        </div>
                    ))}
                </div>

                <Button
                    variant="outline" size="sm"
                    className="h-8 text-xs gap-1.5 border-dashed bg-muted/5 w-[240px]"
                    onClick={() => setExprRows([...exprRows, { expr: '', alias: '' }])}
                >
                    <Plus className="h-3.5 w-3.5" /> Add computed column
                </Button>

                {exprRows.length === 0 && (
                    <p className="text-xs text-muted-foreground italic pl-1">
                        Define new columns using DuckDB SQL expressions (e.g. <code className="text-[10px] bg-muted px-1 py-0.5 rounded font-mono">CASE WHEN x &gt; 10 THEN 'High' ELSE 'Low' END</code>).
                    </p>
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
    const upstreamNodeId = edges.find((e) => e.target === nodeId)?.source
    const { columns = [], isLoading } = useNodeSchema(flowTaskId, upstreamNodeId)

    const groupBy: string[] = (data.group_by as string[]) || []
    const aggregations: AggRow[] = (data.aggregations as AggRow[]) || []

    const numericColumns = columns.filter((c: ColumnInfo) => {
        const t = c.data_type.toLowerCase()
        return t.includes('int') || t.includes('float') || t.includes('double') ||
            t.includes('numeric') || t.includes('real') || t.includes('decimal')
    })

    return (
        <div className="grid grid-cols-2 gap-x-8 gap-y-6">
            <div className="col-span-2">
                <Field label="Group by columns">
                    <MultiColumnSelect
                        columns={columns}
                        selected={groupBy}
                        onChange={(cols) => update({ group_by: cols })}
                        isLoading={isLoading}
                    />
                </Field>
            </div>

            <div className="col-span-2">
                <Field label="Aggregations">
                    <div className="space-y-3 p-4 rounded-md border border-border bg-muted/5">
                        {aggregations.length > 0 && (
                            <div className="grid grid-cols-[1.5fr_120px_1fr_36px] gap-3 px-1 mb-1">
                                <Label className="text-[10px] text-muted-foreground uppercase tracking-wider font-semibold">Column</Label>
                                <Label className="text-[10px] text-muted-foreground uppercase tracking-wider font-semibold">Function</Label>
                                <Label className="text-[10px] text-muted-foreground uppercase tracking-wider font-semibold">Alias</Label>
                                <span />
                            </div>
                        )}

                        {aggregations.map((row, i) => (
                            <div key={i} className="grid grid-cols-[1.5fr_120px_1fr_36px] gap-3 items-center group">
                                <ColumnSelect
                                    columns={numericColumns}
                                    value={row.column}
                                    onChange={(v) => {
                                        const next = [...aggregations]
                                        const currentAlias = next[i].alias
                                        const func = next[i].function

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
                                    placeholder="Select numeric column..."
                                    className="h-9 w-full"
                                />

                                <Select
                                    value={row.function}
                                    onValueChange={(v) => {
                                        const next = [...aggregations]
                                        const col = next[i].column
                                        const currentAlias = next[i].alias

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
                                    <SelectTrigger className="h-9 text-xs w-full">
                                        <SelectValue placeholder="Func" />
                                    </SelectTrigger>
                                    <SelectContent>
                                        {AGG_FUNCTIONS.map((f) => (
                                            <SelectItem key={f} value={f}>{f}</SelectItem>
                                        ))}
                                    </SelectContent>
                                </Select>

                                <Input
                                    className="h-9 text-xs font-mono"
                                    value={row.alias}
                                    onChange={(e) => {
                                        const next = [...aggregations]
                                        next[i] = { ...next[i], alias: e.target.value }
                                        update({ aggregations: next })
                                    }}
                                    placeholder="alias_name"
                                />

                                <Button
                                    variant="ghost" size="icon"
                                    className="h-9 w-9 text-muted-foreground hover:text-destructive"
                                    onClick={() =>
                                        update({ aggregations: aggregations.filter((_, j) => j !== i) })
                                    }
                                >
                                    <X className="h-4 w-4" />
                                </Button>
                            </div>
                        ))}
                        <div className="pt-2">
                            <Button
                                variant="outline" size="sm"
                                className="h-8 text-xs gap-1.5"
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
            </div>
        </div>
    )
}

// ─── Join ──────────────────────────────────────────────────────────────────────

function JoinConfig({ data, update, nodeId, flowTaskId }: ConfigFormProps) {
    const { edges } = useFlowTaskStore()

    const leftEdge = edges.find((e) => e.target === nodeId && e.targetHandle === 'left')
    const rightEdge = edges.find((e) => e.target === nodeId && e.targetHandle === 'right')

    const { columns: leftCols = [], isLoading: leftLoading } = useNodeSchema(flowTaskId, leftEdge?.source)
    const { columns: rightCols = [], isLoading: rightLoading } = useNodeSchema(flowTaskId, rightEdge?.source)

    const leftKeys = (data.left_keys as string[]) || []
    const rightKeys = (data.right_keys as string[]) || []

    const rowCount = Math.max(leftKeys.length, rightKeys.length)

    const updatePair = (index: number, side: 'left' | 'right', value: string) => {
        const newLeft = [...leftKeys]
        const newRight = [...rightKeys]

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
        <div className="grid grid-cols-2 gap-x-8 gap-y-6">
            <Field label="Join Type">
                <Select
                    value={(data.join_type as string) || 'INNER'}
                    onValueChange={(v) => update({ join_type: v })}
                >
                    <SelectTrigger className="h-9 text-xs w-full">
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

            <div className="col-span-2">
                <Field label="Join Conditions (ON)">
                    <div className="space-y-3 p-4 rounded-md border border-border bg-muted/5">
                        {rowCount === 0 && (
                            <div className="text-xs text-muted-foreground italic mb-2">
                                No join conditions set. The preview will likely fail.
                            </div>
                        )}

                        <div className="space-y-3">
                            {Array.from({ length: rowCount }).map((_, i) => (
                                <div key={i} className="flex items-center gap-3">
                                    <div className="flex-1 min-w-0">
                                        <ColumnSelect
                                            columns={leftCols}
                                            value={leftKeys[i] || ''}
                                            onChange={(v) => updatePair(i, 'left', v)}
                                            placeholder="Left table column..."
                                            isLoading={leftLoading}
                                            className="w-full h-9"
                                        />
                                    </div>
                                    <span className="text-muted-foreground text-xs font-mono shrink-0">=</span>
                                    <div className="flex-1 min-w-0">
                                        <ColumnSelect
                                            columns={rightCols}
                                            value={rightKeys[i] || ''}
                                            onChange={(v) => updatePair(i, 'right', v)}
                                            placeholder="Right table column..."
                                            isLoading={rightLoading}
                                            className="w-full h-9"
                                        />
                                    </div>
                                    <Button
                                        variant="ghost"
                                        size="icon"
                                        className="h-9 w-9 text-muted-foreground hover:text-destructive shrink-0"
                                        onClick={() => removePair(i)}
                                    >
                                        <X className="h-4 w-4" />
                                    </Button>
                                </div>
                            ))}
                        </div>

                        <div className="pt-2">
                            <Button
                                variant="outline"
                                size="sm"
                                className="h-8 text-xs gap-1.5"
                                onClick={addPair}
                            >
                                <Plus className="h-3.5 w-3.5" /> Add condition
                            </Button>
                        </div>

                        {(!leftEdge || !rightEdge) && (
                            <div className="flex items-start gap-2 p-3 mt-4 rounded border border-warning/20 bg-warning/10 text-xs text-warning-foreground">
                                <AlertCircle className="h-4 w-4 mt-0.5 shrink-0" />
                                <p>
                                    Connect both Left and Right input handles to populate the column dropdowns.
                                </p>
                            </div>
                        )}
                    </div>
                </Field>
            </div>
        </div>
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
        <div className="grid grid-cols-2 gap-x-8 gap-y-6">
            <div className="col-span-2">
                <Field label="Add New Columns">
                    <div className="space-y-4 pt-2">
                        {rawCols.length === 0 && (
                            <div className="text-xs text-muted-foreground italic mb-2 px-1">
                                No columns defined. Click Add to create one.
                            </div>
                        )}

                        <div className="grid grid-cols-2 xl:grid-cols-3 gap-4">
                            {rawCols.map((col, i) => (
                                <div
                                    key={i}
                                    className="flex flex-col gap-3 p-4 rounded-md border border-border/80 bg-muted/5 shadow-sm relative group"
                                >
                                    {/* Header */}
                                    <div className="flex items-center gap-2">
                                        <Input
                                            className="h-9 text-xs flex-1 font-mono"
                                            placeholder="column_alias"
                                            value={col.alias}
                                            onChange={(e) => updateColumn(i, { alias: e.target.value })}
                                        />
                                        <Select
                                            value={col.type}
                                            onValueChange={(v: 'static' | 'expression') =>
                                                updateColumn(i, { type: v })
                                            }
                                        >
                                            <SelectTrigger className="h-9 text-xs w-[120px] shrink-0">
                                                <SelectValue />
                                            </SelectTrigger>
                                            <SelectContent>
                                                <SelectItem value="static">Static Value</SelectItem>
                                                <SelectItem value="expression">SQL Expression</SelectItem>
                                            </SelectContent>
                                        </Select>
                                        <Button
                                            variant="ghost"
                                            size="icon"
                                            className="h-9 w-9 text-muted-foreground hover:text-destructive shrink-0 bg-transparent hover:bg-destructive/10"
                                            onClick={() => removeColumn(i)}
                                        >
                                            <X className="h-4 w-4" />
                                        </Button>
                                    </div>

                                    {/* Body */}
                                    <div className="flex-1 flex flex-col">
                                        {col.type === 'static' ? (
                                            <div className="space-y-1.5 flex-1">
                                                <Label className="text-[10px] text-muted-foreground uppercase tracking-wider font-semibold">Value</Label>
                                                <Input
                                                    className="h-9 text-xs font-mono"
                                                    placeholder="e.g. active, 42, true"
                                                    value={col.value ?? ''}
                                                    onChange={(e) => updateColumn(i, { value: e.target.value })}
                                                />
                                            </div>
                                        ) : (
                                            <div className="space-y-1.5 flex-1 flex flex-col">
                                                <Label className="text-[10px] text-muted-foreground uppercase tracking-wider font-semibold">SQL Expression</Label>
                                                <textarea
                                                    className="w-full flex-1 min-h-[72px] resize-none rounded-md border border-input bg-background/50 px-3 py-2 text-xs font-mono placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-primary"
                                                    placeholder={`CASE WHEN status = 'A' THEN 1 ELSE 0 END`}
                                                    value={col.expr ?? ''}
                                                    onChange={(e) => updateColumn(i, { expr: e.target.value })}
                                                    spellCheck={false}
                                                />
                                            </div>
                                        )}
                                    </div>
                                </div>
                            ))}
                        </div>

                        <Button
                            variant="outline"
                            size="sm"
                            className="h-8 text-xs gap-1.5 border-dashed bg-muted/5 w-[240px]"
                            onClick={addColumn}
                        >
                            <Plus className="h-3.5 w-3.5" /> Add new column
                        </Button>
                    </div>
                </Field>
            </div>
        </div>
    )
}

// ─── Union ─────────────────────────────────────────────────────────────────────

function UnionConfig({ data, update }: ConfigFormProps) {
    return (
        <div className="grid grid-cols-2 gap-6">
            <div className="col-span-1">
                <Field label="Union Operation">
                    <SwitchField
                        label="UNION ALL (Keep duplicates)"
                        checked={(data.union_all as boolean) ?? true}
                        onChange={(v) => update({ union_all: v })}
                    />
                    <p className="text-xs text-muted-foreground mt-2 px-1">
                        Turn off to perform standard UNION, which removes duplicate rows but is slower.
                    </p>
                </Field>
            </div>
        </div>
    )
}

// ─── Pivot ─────────────────────────────────────────────────────────────────────

function PivotConfig({ data, update, nodeId, flowTaskId }: ConfigFormProps) {
    const { edges } = useFlowTaskStore()
    const parentId = edges?.find((e) => e.target === nodeId)?.source
    const { columns = [], isLoading } = useNodeSchema(flowTaskId, parentId)
    const pivotType = (data.pivot_type as string) || 'PIVOT'

    return (
        <div className="grid grid-cols-2 gap-x-8 gap-y-6">
            <Field label="Pivot Type">
                <Select
                    value={pivotType}
                    onValueChange={(v) => update({ pivot_type: v as 'PIVOT' | 'UNPIVOT' })}
                >
                    <SelectTrigger className="h-9 text-xs w-full">
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
                    className="w-full h-9"
                    placeholder={pivotType === 'PIVOT' ? 'Column to rotate into headers...' : 'Headers to rotate into column...'}
                />
            </Field>

            <Field label="Value column">
                <ColumnSelect
                    columns={columns}
                    value={(data.value_column as string) || ''}
                    onChange={(v) => update({ value_column: v })}
                    isLoading={isLoading}
                    className="w-full h-9"
                    placeholder="Column containing values..."
                />
            </Field>

            <Field label={pivotType === 'PIVOT' ? "Pivot values (comma-separated)" : "Unpivot columns (comma-separated)"}>
                <Input
                    className="h-9 text-xs font-mono"
                    value={((data.pivot_values as string[]) || []).join(', ')}
                    onChange={(e) =>
                        update({
                            pivot_values: e.target.value
                                ? e.target.value.split(',').map((s) => s.trim())
                                : [],
                        })
                    }
                    placeholder={pivotType === 'PIVOT' ? "val1, val2, val3" : "col1, col2, col3"}
                />
            </Field>
        </div>
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
        <div className="grid grid-cols-2 gap-x-8 gap-y-6">
            <Field label="Destination Connection">
                {destsLoading ? (
                    <div className="flex items-center gap-2 h-9 text-xs text-muted-foreground bg-muted/20 px-3 rounded-md border border-border/50">
                        <Loader2 className="h-4 w-4 animate-spin" /> Loading…
                    </div>
                ) : (
                    <Select
                        value={data.destination_id != null ? String(data.destination_id) : ''}
                        onValueChange={(v) => update({ destination_id: parseInt(v), table_name: undefined })}
                    >
                        <SelectTrigger className="h-9 text-xs w-full bg-background shadow-sm border border-border/60">
                            <SelectValue placeholder="Select destination database…" />
                        </SelectTrigger>
                        <SelectContent>
                            {(destsData?.destinations ?? []).map((d) => (
                                <SelectItem key={d.id} value={String(d.id)}>
                                    <div className="flex items-center gap-2">
                                        <span className="font-medium text-[13px]">{d.name}</span>
                                        <span className="text-[10px] text-muted-foreground bg-muted px-1.5 py-0.5 rounded uppercase">{d.type}</span>
                                    </div>
                                </SelectItem>
                            ))}
                        </SelectContent>
                    </Select>
                )}
            </Field>

            <Field label="Schema Name">
                <Input
                    className="h-9 text-xs bg-background shadow-sm border border-border/60"
                    value={(data.schema_name as string) || 'public'}
                    onChange={(e) => update({ schema_name: e.target.value })}
                />
            </Field>

            <Field label="Target Table">
                {!destinationId ? (
                    <div className="h-9 px-3 py-2 text-xs text-muted-foreground border border-dashed border-border rounded-md bg-muted/10 flex items-center italic">
                        Select a destination first...
                    </div>
                ) : tablesLoading ? (
                    <div className="flex items-center gap-2 h-9 text-xs text-muted-foreground bg-muted/20 px-3 rounded-md border border-border/50">
                        <Loader2 className="h-4 w-4 animate-spin" /> Loading tables…
                    </div>
                ) : (
                    <Popover>
                        <PopoverTrigger asChild>
                            <Button
                                variant="outline"
                                role="combobox"
                                className={cn(
                                    "w-full justify-between h-9 text-xs font-normal bg-background shadow-sm border border-border/60",
                                    !data.table_name && "text-muted-foreground"
                                )}
                            >
                                {data.table_name
                                    ? tables.find((t) => t === data.table_name) || data.table_name
                                    : "Select or type table name..."}
                                <ChevronsUpDown className="ml-2 h-4 w-4 shrink-0 opacity-50" />
                            </Button>
                        </PopoverTrigger>
                        <PopoverContent className="w-[300px] p-0" align="start">
                            <Command>
                                <CommandInput placeholder="Search table..." className="h-9 text-xs" />
                                <CommandList>
                                    <CommandEmpty>No table found. Type to create new.</CommandEmpty>
                                    <CommandGroup>
                                        {tables.map((t) => (
                                            <CommandItem
                                                value={t}
                                                key={t}
                                                onSelect={(value) => update({ table_name: value })}
                                                className="text-xs font-mono"
                                            >
                                                <Check
                                                    className={cn(
                                                        "mr-2 h-4 w-4",
                                                        t === data.table_name ? "opacity-100" : "opacity-0"
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

            <div className="col-span-2">
                <Separator className="my-2" />
            </div>

            <Field label="Write Mode">
                <Select
                    value={(data.write_mode as string) || 'APPEND'}
                    onValueChange={(v) => update({ write_mode: v as WriteMode })}
                >
                    <SelectTrigger className="h-9 text-xs w-full bg-background">
                        <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                        <SelectItem value="APPEND">APPEND (Add new rows)</SelectItem>
                        <SelectItem value="UPSERT">UPSERT (Update existing, insert new)</SelectItem>
                        <SelectItem value="REPLACE">REPLACE (Truncate, then insert all)</SelectItem>
                    </SelectContent>
                </Select>
            </Field>

            {data.write_mode === 'UPSERT' && (
                <Field label="Upsert Keys (Primary Keys)">
                    <Input
                        className="h-9 text-xs font-mono bg-background"
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
                    <p className="text-[10px] text-muted-foreground mt-1">
                        Comma-separated columns to uniquely identify rows for updating
                    </p>
                </Field>
            )}
        </div>
    )
}

// ─── SQL / Note ────────────────────────────────────────────────────────────────

function SqlConfig({ data, update }: ConfigFormProps) {
    const expr = (data.sql_expression as string) ?? ''
    return (
        <div className="grid grid-cols-3 gap-6">
            <div className="col-span-2">
                <Field label="SQL Expression">
                    <textarea
                        className="w-full min-h-[320px] resize-y rounded-md border border-border/80 bg-muted/5 px-4 py-3 font-mono text-xs text-foreground placeholder:text-muted-foreground/50 shadow-inner focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-primary"
                        value={expr}
                        onChange={(e) => update({ sql_expression: e.target.value })}
                        placeholder={`SELECT *\nFROM {{input}}\nWHERE status = 'active'`}
                        spellCheck={false}
                    />
                </Field>
            </div>
            <div className="col-span-1">
                <div className="rounded-md border border-border/50 bg-muted/20 p-4 space-y-3 sticky top-0">
                    <h3 className="text-xs font-semibold uppercase tracking-wider text-muted-foreground flex items-center gap-2">
                        <AlertCircle className="h-3.5 w-3.5" /> Template Variables
                    </h3>
                    <div className="space-y-4 pt-1">
                        <div>
                            <code className="text-[11px] font-mono font-medium rounded bg-background border border-border/50 px-1.5 py-0.5 mb-1.5 inline-block text-primary">{'{{input}}'}</code>
                            <p className="text-[11px] text-muted-foreground">The first connected upstream node dataset.</p>
                        </div>
                        <div>
                            <code className="text-[11px] font-mono font-medium rounded bg-background border border-border/50 px-1.5 py-0.5 mb-1.5 inline-block text-primary">{'{{input_N}}'}</code>
                            <p className="text-[11px] text-muted-foreground">The Nth connected upstream node dataset (0-based indexing).</p>
                        </div>
                        <div>
                            <code className="text-[11px] font-mono font-medium rounded bg-background border border-border/50 px-1.5 py-0.5 mb-1.5 inline-block text-primary">{'{{upstream}}'}</code>
                            <p className="text-[11px] text-muted-foreground">Alias for the first upstream node.</p>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    )
}

function NoteConfig({ data, update }: ConfigFormProps) {
    const content = (data.note_content as string) ?? ''
    return (
        <div className="max-w-3xl">
            <Field label="Note content">
                <textarea
                    className="w-full min-h-[200px] resize-y rounded-md border border-border/80 bg-muted/5 px-4 py-3 text-sm text-foreground shadow-inner focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-primary"
                    value={content}
                    onChange={(e) => update({ note_content: e.target.value })}
                    placeholder="Type your documentation or notes here…"
                />
                <p className="text-[11px] text-muted-foreground mt-1 px-1">
                    This text is also editable directly on the canvas node.
                </p>
            </Field>
        </div>
    )
}

// ─── Export Switch ─────────────────────────────────────────────────────────────

export function NodeTypeConfig({ type, data, update, nodeId, flowTaskId }: ConfigFormProps & { type: string }) {
    return (
        <div className="w-full">
            {type !== 'note' && (
                <div className="mb-6 grid grid-cols-3 gap-6">
                    <Field label="Node Label" className="col-span-1">
                        <Input
                            className="h-9 text-sm font-semibold"
                            value={(data.label as string) ?? type}
                            onChange={(e) => update({ label: e.target.value })}
                        />
                    </Field>
                </div>
            )}

            <div className="space-y-6">
                {type === 'input' && <InputConfig data={data} update={update} nodeId={nodeId} flowTaskId={flowTaskId} />}
                {type === 'clean' && <CleanConfig data={data} update={update} nodeId={nodeId} flowTaskId={flowTaskId} />}
                {type === 'aggregate' && <AggregateConfig data={data} update={update} nodeId={nodeId} flowTaskId={flowTaskId} />}
                {type === 'join' && <JoinConfig data={data} update={update} nodeId={nodeId} flowTaskId={flowTaskId} />}
                {type === 'new_rows' && <NewRowsConfig data={data} update={update} nodeId={nodeId} flowTaskId={flowTaskId} />}
                {type === 'union' && <UnionConfig data={data} update={update} nodeId={nodeId} flowTaskId={flowTaskId} />}
                {type === 'pivot' && <PivotConfig data={data} update={update} nodeId={nodeId} flowTaskId={flowTaskId} />}
                {type === 'output' && <OutputConfig data={data} update={update} nodeId={nodeId} flowTaskId={flowTaskId} />}
                {type === 'sql' && <SqlConfig data={data} update={update} nodeId={nodeId} flowTaskId={flowTaskId} />}
                {type === 'note' && <NoteConfig data={data} update={update} nodeId={nodeId} flowTaskId={flowTaskId} />}
            </div>
        </div>
    )
}
