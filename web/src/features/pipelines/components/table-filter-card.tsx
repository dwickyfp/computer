import { useState, useEffect } from 'react'
import { Button } from '@/components/ui/button'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Input } from '@/components/ui/input'
import { Textarea } from '@/components/ui/textarea'
import { Loader2, Plus, Save, X, Calendar as CalendarIcon, Trash2, CornerDownRight, Check, ChevronsUpDown } from 'lucide-react'
import { type TableWithSyncInfo } from '@/repo/pipelines'
import { cn } from '@/lib/utils'
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover'
import { Command, CommandEmpty, CommandGroup, CommandInput, CommandItem, CommandList } from '@/components/ui/command'
import { Calendar } from '@/components/ui/calendar'
import { format } from 'date-fns'
import { ScrollArea } from '@/components/ui/scroll-area'
import { Badge } from '@/components/ui/badge'

// ─── Types ───────────────────────────────────────────────────

interface TableFilterCardProps {
    table: TableWithSyncInfo | null
    open: boolean
    onClose: () => void
    onSave: (filterSql: string) => Promise<void>
    className?: string
}

interface FilterCondition {
    id: string
    column: string
    operator: string
    value: string
    value2?: string
}

interface FilterGroup {
    id: string
    conditions: FilterCondition[]
    intraLogic: 'AND' | 'OR'
}

interface FilterState {
    groups: FilterGroup[]
    interLogic: ('AND' | 'OR')[]
}

/** JSON v2 format stored in DB */
export interface FilterV2Json {
    version: 2
    groups: {
        conditions: { column: string; operator: string; value: string; value2?: string }[]
        intraLogic: 'AND' | 'OR'
    }[]
    interLogic: ('AND' | 'OR')[]
}

// ─── Helpers ─────────────────────────────────────────────────

function uid(): string {
    return typeof crypto !== 'undefined' && crypto.randomUUID
        ? crypto.randomUUID()
        : Math.random().toString(36).substring(2) + Date.now().toString(36)
}

function conditionToSql(c: { column: string; operator: string; value: string; value2?: string }): string {
    if (!c.column) return ''
    const op = c.operator.toUpperCase()
    if (op === 'IS NULL' || op === 'IS NOT NULL') return `${c.column} ${op}`
    if (!c.value && op !== 'IN') return ''
    if (op === 'BETWEEN' && c.value2) return `${c.column} BETWEEN '${c.value}' AND '${c.value2}'`
    if (op === 'LIKE' || op === 'ILIKE') return `${c.column} ${op} '%${c.value}%'`
    if (op === 'IN') {
        const vals = c.value.split(',').map(v => v.trim()).filter(Boolean)
        if (!vals.length) return ''
        const quoted = vals.map(v => /^\d+(\.\d+)?$/.test(v) ? v : `'${v}'`).join(', ')
        return `${c.column} IN (${quoted})`
    }
    const isNum = /^-?\d+(\.\d+)?$/.test(c.value)
    return `${c.column} ${c.operator} ${isNum ? c.value : `'${c.value}'`}`
}

/** Convert a filter_sql string (v2 JSON or legacy) to readable SQL WHERE clause */
export function filterV2ToSql(filterSql: string | null | undefined): string {
    if (!filterSql) return ''
    try {
        const parsed: FilterV2Json = JSON.parse(filterSql)
        if (parsed.version === 2) {
            const groupSqls: string[] = []
            for (const g of parsed.groups) {
                const parts: string[] = []
                for (const c of g.conditions) {
                    const clause = conditionToSql(c)
                    if (clause) parts.push(clause)
                }
                if (!parts.length) continue
                groupSqls.push(parts.length > 1 ? `(${parts.join(` ${g.intraLogic} `)})` : parts[0])
            }
            if (!groupSqls.length) return ''
            let result = groupSqls[0]
            for (let i = 1; i < groupSqls.length; i++) {
                const logic = parsed.interLogic[i - 1] || 'AND'
                result += ` ${logic} ${groupSqls[i]}`
            }
            return result
        }
    } catch { /* legacy format */ }
    // Legacy semicolon format
    const parts = filterSql.split(';').map(s => s.trim()).filter(Boolean)
    return parts.join(' AND ')
}

// ─── Operators ───────────────────────────────────────────────

const OPERATORS_BY_TYPE = {
    boolean: [
        { value: '=', label: 'Equals (=)' },
        { value: '!=', label: 'Not Equals (!=)' },
        { value: 'IS NULL', label: 'Is Null' },
        { value: 'IS NOT NULL', label: 'Is Not Null' },
    ],
    string: [
        { value: '=', label: 'Equals (=)' },
        { value: '!=', label: 'Not Equals (!=)' },
        { value: 'LIKE', label: 'Like (LIKE)' },
        { value: 'ILIKE', label: 'Case Insensitive Like (ILIKE)' },
        { value: 'IN', label: 'In (IN)' },
        { value: 'IS NULL', label: 'Is Null' },
        { value: 'IS NOT NULL', label: 'Is Not Null' },
    ],
    number: [
        { value: '=', label: 'Equals (=)' },
        { value: '!=', label: 'Not Equals (!=)' },
        { value: '>', label: 'Greater Than (>)' },
        { value: '<', label: 'Less Than (<)' },
        { value: '>=', label: 'Greater or Equal (>=)' },
        { value: '<=', label: 'Less or Equal (<=)' },
        { value: 'IN', label: 'In (IN)' },
        { value: 'IS NULL', label: 'Is Null' },
        { value: 'IS NOT NULL', label: 'Is Not Null' },
    ],
    date: [
        { value: '=', label: 'Equals (=)' },
        { value: '!=', label: 'Not Equals (!=)' },
        { value: '>', label: 'Greater Than (>)' },
        { value: '<', label: 'Less Than (<)' },
        { value: '>=', label: 'Greater or Equal (>=)' },
        { value: '<=', label: 'Less or Equal (<=)' },
        { value: 'BETWEEN', label: 'Between' },
        { value: 'IS NULL', label: 'Is Null' },
        { value: 'IS NOT NULL', label: 'Is Not Null' },
    ],
}

// ─── Component ───────────────────────────────────────────────

export function TableFilterCard({
    table,
    open,
    onClose,
    onSave,
    className
}: TableFilterCardProps) {
    const [state, setState] = useState<FilterState>({ groups: [], interLogic: [] })
    const [isSaving, setIsSaving] = useState(false)
    const columns = table?.columns || []

    // ── Parse helpers ──

    const parseLegacySql = (sql: string): Omit<FilterCondition, 'id'>[] => {
        const result: Omit<FilterCondition, 'id'>[] = []
        const pattern = /(?:(\w+)\s+(IS\s+(?:NOT\s+)?NULL))|(?:(\w+)\s+BETWEEN\s+'([^']*)'\s+AND\s+'([^']*)')|(?:(\w+)\s+(LIKE|ILIKE)\s+'%([^%]*)%')|(?:(\w+)\s+IN\s*\(([^)]*)\))|(?:(\w+)\s*(=|!=|>|>=|<=|<)\s*(?:'([^']*)'|(\d+(?:\.\d+)?)|(?:true|false)))/gi
        for (const m of sql.matchAll(pattern)) {
            if (m[1]) result.push({ column: m[1], operator: m[2], value: '' })
            else if (m[3]) result.push({ column: m[3], operator: 'BETWEEN', value: m[4], value2: m[5] })
            else if (m[6]) result.push({ column: m[6], operator: m[7], value: m[8] })
            else if (m[9]) result.push({ column: m[9], operator: 'IN', value: m[10].replace(/'/g, '').trim() })
            else if (m[11]) result.push({ column: m[11], operator: m[12], value: m[13] ?? m[14] })
        }
        return result
    }

    const parseFilterSql = (sql: string): FilterState => {
        if (!sql) return { groups: [], interLogic: [] }
        try {
            const parsed: FilterV2Json = JSON.parse(sql)
            if (parsed.version === 2) {
                return {
                    groups: parsed.groups.map(g => ({
                        id: uid(),
                        conditions: g.conditions.map(c => ({
                            id: uid(), column: c.column, operator: c.operator, value: c.value, value2: c.value2
                        })),
                        intraLogic: g.intraLogic
                    })),
                    interLogic: parsed.interLogic
                }
            }
        } catch { /* not JSON */ }
        const clauses = parseLegacySql(sql)
        return {
            groups: clauses.map(c => ({
                id: uid(),
                conditions: [{ id: uid(), ...c }],
                intraLogic: 'AND' as const
            })),
            interLogic: clauses.length > 1 ? Array(clauses.length - 1).fill('AND') : []
        }
    }

    // ── Load on open/table change ──

    useEffect(() => {
        if (!open) return
        const syncConfig = (table as any)?.sync_config || table?.sync_configs?.[0]
        const filterSql = syncConfig?.filter_sql
        setState(filterSql ? parseFilterSql(filterSql) : { groups: [], interLogic: [] })
    }, [table?.table_name, (table as any)?.sync_config?.id, (table as any)?.sync_config?.filter_sql, table?.sync_configs?.[0]?.id, table?.sync_configs?.[0]?.filter_sql, open])

    // ── Column helpers ──

    const getColumnType = (colName: string) => {
        const col = columns.find(c => c.column_name === colName)
        if (!col) return 'text'
        const type = (col.real_data_type || col.data_type || '').toLowerCase()
        const numericTypes = ['int', 'integer', 'smallint', 'bigint', 'int2', 'int4', 'int8', 'decimal', 'numeric', 'real', 'double precision', 'float', 'float4', 'float8', 'number', 'money']
        if (numericTypes.some(t => type.includes(t))) return 'numeric'
        const datetimeTypes = ['timestamp', 'timestamptz', 'datetime', 'timestamp with time zone', 'timestamp without time zone']
        if (datetimeTypes.some(t => type.includes(t))) return 'datetime'
        if (type.includes('date')) return 'date'
        if (type.includes('time')) return 'text'
        return 'text'
    }

    const getColumnTypeCategory = (columnName: string): 'boolean' | 'string' | 'number' | 'date' => {
        const col = columns.find(c => c.column_name === columnName)
        if (!col) return 'string'
        const type = (col.real_data_type || col.data_type || '').toLowerCase()
        if (type.includes('bool')) return 'boolean'
        if (type.includes('date') || type.includes('time')) return 'date'
        if (type.includes('int') || type.includes('numeric') || type.includes('decimal') || type.includes('float') || type.includes('double') || type.includes('real') || type.includes('money')) return 'number'
        return 'string'
    }

    const getOperatorsForColumn = (columnName: string) => {
        if (!columnName) return OPERATORS_BY_TYPE.string
        return OPERATORS_BY_TYPE[getColumnTypeCategory(columnName)]
    }

    const isBooleanColumn = (columnName: string): boolean => {
        const col = columns.find(c => c.column_name === columnName)
        if (!col) return false
        return (col.real_data_type || col.data_type || '').toLowerCase().includes('bool')
    }

    // ── State mutations ──

    const addGroup = () => {
        const firstCol = columns.length > 0 ? columns[0].column_name : ''
        const newGroup: FilterGroup = {
            id: uid(),
            conditions: [{ id: uid(), column: firstCol, operator: '=', value: '' }],
            intraLogic: 'AND'
        }
        setState(prev => ({
            groups: [...prev.groups, newGroup],
            interLogic: prev.groups.length > 0 ? [...prev.interLogic, 'AND'] : prev.interLogic
        }))
    }

    const removeGroupState = (prev: FilterState, groupId: string): FilterState => {
        const idx = prev.groups.findIndex(g => g.id === groupId)
        if (idx === -1) return prev
        const newGroups = prev.groups.filter(g => g.id !== groupId)
        const newInterLogic = [...prev.interLogic]
        if (idx === 0 && newInterLogic.length > 0) newInterLogic.splice(0, 1)
        else if (idx > 0) newInterLogic.splice(idx - 1, 1)
        return { groups: newGroups, interLogic: newInterLogic }
    }

    const removeGroup = (groupId: string) => setState(prev => removeGroupState(prev, groupId))

    const addConditionToGroup = (groupId: string) => {
        const firstCol = columns.length > 0 ? columns[0].column_name : ''
        setState(prev => ({
            ...prev,
            groups: prev.groups.map(g =>
                g.id === groupId
                    ? { ...g, conditions: [...g.conditions, { id: uid(), column: firstCol, operator: '=', value: '' }] }
                    : g
            )
        }))
    }

    const removeCondition = (groupId: string, condId: string) => {
        setState(prev => {
            const group = prev.groups.find(g => g.id === groupId)
            if (!group) return prev
            if (group.conditions.length <= 1) return removeGroupState(prev, groupId)
            return {
                ...prev,
                groups: prev.groups.map(g =>
                    g.id === groupId
                        ? { ...g, conditions: g.conditions.filter(c => c.id !== condId) }
                        : g
                )
            }
        })
    }

    const updateCondition = (groupId: string, condId: string, field: keyof FilterCondition, val: string) => {
        setState(prev => ({
            ...prev,
            groups: prev.groups.map(g =>
                g.id === groupId
                    ? {
                        ...g,
                        conditions: g.conditions.map(c => {
                            if (c.id !== condId) return c
                            if (field === 'column' && val !== c.column) {
                                const validOps = getOperatorsForColumn(val)
                                const isValid = validOps.some(op => op.value === c.operator)
                                return { ...c, column: val, operator: isValid ? c.operator : '=', value: '', value2: undefined }
                            }
                            if (field === 'operator' && val !== 'BETWEEN') return { ...c, operator: val, value2: undefined }
                            return { ...c, [field]: val }
                        })
                    }
                    : g
            )
        }))
    }

    const toggleIntraLogic = (groupId: string) => {
        setState(prev => ({
            ...prev,
            groups: prev.groups.map(g =>
                g.id === groupId ? { ...g, intraLogic: g.intraLogic === 'AND' ? 'OR' : 'AND' } : g
            )
        }))
    }

    const toggleInterLogic = (index: number) => {
        setState(prev => {
            const newInterLogic = [...prev.interLogic]
            newInterLogic[index] = newInterLogic[index] === 'AND' ? 'OR' : 'AND'
            return { ...prev, interLogic: newInterLogic }
        })
    }

    // ── Save ──

    const buildV2Json = (): string => {
        const json: FilterV2Json = {
            version: 2,
            groups: state.groups.map(g => ({
                conditions: g.conditions.map(c => {
                    const base: FilterV2Json['groups'][0]['conditions'][0] = {
                        column: c.column, operator: c.operator, value: c.value
                    }
                    if (c.value2) base.value2 = c.value2
                    return base
                }),
                intraLogic: g.intraLogic
            })),
            interLogic: state.interLogic
        }
        return JSON.stringify(json)
    }

    const handleSave = async (e: React.MouseEvent) => {
        e.preventDefault()
        e.stopPropagation()
        const validGroups = state.groups.filter(g =>
            g.conditions.some(c => c.column && (
                c.operator === 'IS NULL' || c.operator === 'IS NOT NULL' || c.value
            ))
        )
        const sql = validGroups.length > 0 ? buildV2Json() : ''
        setIsSaving(true)
        try {
            await onSave(sql)
        } finally {
            setIsSaving(false)
        }
    }

    const handleClose = (e: React.MouseEvent) => {
        e.preventDefault()
        e.stopPropagation()
        onClose()
    }

    // ── Preview SQL ──

    const previewSql = filterV2ToSql(buildV2Json())

    if (!open || !table) return null

    return (
        <div
            className={cn(
                "fixed top-2 bottom-2 left-[520px] w-[800px] bg-background border rounded-2xl shadow-2xl flex flex-col max-h-[calc(100vh-1rem)]",
                "animate-in slide-in-from-left-4 duration-300",
                className
            )}
            style={{ zIndex: 100 }}
            onClick={(e) => e.stopPropagation()}
            onMouseDown={(e) => e.stopPropagation()}
        >
            {/* Header */}
            <div className="flex items-center justify-between px-6 py-4 border-b bg-muted/30 flex-shrink-0">
                <div>
                    <h2 className="text-lg font-semibold">Filter Table</h2>
                    <p className="text-sm text-muted-foreground">
                        Configure filters for <span className="font-medium text-foreground">{table.table_name}</span>
                    </p>
                </div>
                <Button variant="ghost" size="icon" onClick={handleClose} className="h-8 w-8">
                    <X className="h-4 w-4" />
                </Button>
            </div>

            {/* Content */}
            <ScrollArea className="flex-1 overflow-y-auto">
                <div className="p-6 space-y-3">
                    {state.groups.map((group, groupIdx) => {
                        const isMulti = group.conditions.length > 1

                        return (
                            <div key={group.id}>
                                {/* Inter-group logic toggle */}
                                {groupIdx > 0 && (
                                    <div className="flex items-center justify-center py-2">
                                        <Button
                                            variant="outline"
                                            size="sm"
                                            className={cn(
                                                "h-7 px-4 text-xs font-bold rounded-full border-2",
                                                state.interLogic[groupIdx - 1] === 'OR'
                                                    ? "border-orange-400 text-orange-600 bg-orange-50 hover:bg-orange-100"
                                                    : "border-blue-400 text-blue-600 bg-blue-50 hover:bg-blue-100"
                                            )}
                                            onClick={() => toggleInterLogic(groupIdx - 1)}
                                        >
                                            {state.interLogic[groupIdx - 1] || 'AND'}
                                        </Button>
                                    </div>
                                )}

                                {/* Group Card */}
                                <div
                                    className={cn(
                                        "border rounded-lg bg-card relative",
                                        isMulti
                                            ? "border-dashed border-2 border-muted-foreground/40"
                                            : "border-solid"
                                    )}
                                >
                                    {/* Group header */}
                                    <div className="flex items-center justify-between px-4 pt-3 pb-1">
                                        <div className="flex items-center gap-2">
                                            {isMulti && (
                                                <Badge variant="secondary" className="text-[10px] h-5">
                                                    Group
                                                </Badge>
                                            )}
                                            <span className="text-xs text-muted-foreground">
                                                {isMulti
                                                    ? `${group.conditions.length} conditions`
                                                    : `Filter #${groupIdx + 1}`}
                                            </span>
                                        </div>
                                        <div className="flex items-center gap-1">
                                            {isMulti && (
                                                <Button
                                                    variant="outline"
                                                    size="sm"
                                                    className={cn(
                                                        "h-6 px-2 text-[10px] font-bold rounded-full",
                                                        group.intraLogic === 'OR'
                                                            ? "border-orange-400 text-orange-600 bg-orange-50 hover:bg-orange-100"
                                                            : "border-blue-400 text-blue-600 bg-blue-50 hover:bg-blue-100"
                                                    )}
                                                    onClick={() => toggleIntraLogic(group.id)}
                                                >
                                                    {group.intraLogic}
                                                </Button>
                                            )}
                                            <Button
                                                variant="ghost"
                                                size="icon"
                                                className="h-6 w-6 text-muted-foreground hover:text-destructive"
                                                onClick={() => removeGroup(group.id)}
                                            >
                                                <Trash2 className="h-3.5 w-3.5" />
                                            </Button>
                                        </div>
                                    </div>

                                    {/* Conditions */}
                                    <div className="px-4 pb-3 space-y-2">
                                        {group.conditions.map((cond, condIdx) => (
                                            <ConditionRow
                                                key={cond.id}
                                                condition={cond}
                                                condIdx={condIdx}
                                                isGrouped={isMulti}
                                                columns={columns}
                                                getColumnType={getColumnType}
                                                getOperatorsForColumn={getOperatorsForColumn}
                                                isBooleanColumn={isBooleanColumn}
                                                onUpdate={(field, val) => updateCondition(group.id, cond.id, field, val)}
                                                onRemove={() => removeCondition(group.id, cond.id)}
                                            />
                                        ))}

                                        {/* Add condition to group */}
                                        <Button
                                            variant="ghost"
                                            size="sm"
                                            className="w-full h-7 text-xs text-muted-foreground hover:text-foreground"
                                            onClick={(e) => { e.stopPropagation(); addConditionToGroup(group.id) }}
                                        >
                                            <Plus className="mr-1 h-3 w-3" /> Add Condition to Group
                                        </Button>
                                    </div>
                                </div>
                            </div>
                        )
                    })}

                    {/* Empty State */}
                    {state.groups.length === 0 && (
                        <div className="text-center py-8 text-muted-foreground border-2 border-dashed rounded-lg">
                            <p className="text-sm font-medium mb-1">No filters configured</p>
                            <p className="text-xs">Click the button below to add a filter</p>
                        </div>
                    )}

                    {/* Add Filter Group Button */}
                    <Button
                        variant="outline"
                        size="sm"
                        onClick={(e) => { e.stopPropagation(); addGroup() }}
                        onMouseDown={(e) => e.stopPropagation()}
                        className="w-full"
                    >
                        <Plus className="mr-2 h-4 w-4" /> Add Filter
                    </Button>

                    {/* SQL Preview */}
                    {previewSql && (
                        <div className="mt-4 p-3 bg-muted/50 border rounded-lg">
                            <p className="text-[10px] font-medium text-muted-foreground uppercase mb-1">SQL Preview</p>
                            <code className="text-xs text-foreground break-all">WHERE {previewSql}</code>
                        </div>
                    )}

                    {columns.length === 0 && (
                        <p className="text-xs text-amber-600 mt-2">
                            Note: No columns available for this table. Filters may not work correctly.
                        </p>
                    )}
                </div>
            </ScrollArea>

            {/* Footer */}
            <div className="flex items-center justify-between px-6 py-4 border-t bg-muted/30 flex-shrink-0">
                <Button
                    variant="ghost"
                    size="sm"
                    onClick={(e) => { e.stopPropagation(); setState({ groups: [], interLogic: [] }) }}
                    className="text-muted-foreground"
                >
                    Reset All
                </Button>
                <div className="flex gap-2">
                    <Button variant="outline" size="sm" onClick={handleClose}>
                        Cancel
                    </Button>
                    <Button onClick={handleSave} disabled={isSaving} size="sm">
                        {isSaving && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
                        {!isSaving && <Save className="mr-2 h-4 w-4" />}
                        Save Filters
                    </Button>
                </div>
            </div>
        </div>
    )
}

// ─── Condition Row ───────────────────────────────────────────

function ConditionRow({
    condition,
    condIdx,
    isGrouped,
    columns,
    getColumnType,
    getOperatorsForColumn,
    isBooleanColumn,
    onUpdate,
    onRemove
}: {
    condition: FilterCondition
    condIdx: number
    isGrouped: boolean
    columns: { column_name: string; real_data_type?: string; data_type?: string }[]
    getColumnType: (colName: string) => string
    getOperatorsForColumn: (colName: string) => { value: string; label: string }[]
    isBooleanColumn: (colName: string) => boolean
    onUpdate: (field: keyof FilterCondition, val: string) => void
    onRemove: () => void
}) {
    const [columnOpen, setColumnOpen] = useState(false)
    const colType = getColumnType(condition.column)
    const isIn = condition.operator === 'IN'
    const isBetween = condition.operator === 'BETWEEN'
    const isNull = condition.operator === 'IS NULL' || condition.operator === 'IS NOT NULL'

    return (
        <div className="flex items-start gap-2">
            {/* Arrow indicator for grouped conditions */}
            {isGrouped && condIdx > 0 && (
                <div className="flex items-center pt-6">
                    <CornerDownRight className="h-4 w-4 text-muted-foreground/60" />
                </div>
            )}

            <div className={cn(
                "flex-1 grid gap-2",
                isBetween
                    ? "grid-cols-[1.2fr_0.8fr_2fr_2fr]"
                    : "grid-cols-[1.2fr_0.8fr_4fr]"
            )}>
                {/* Column */}
                <div>
                    <label className="text-[10px] font-medium text-muted-foreground uppercase mb-1 block">Column</label>
                    <Popover open={columnOpen} onOpenChange={setColumnOpen}>
                        <PopoverTrigger asChild>
                            <Button
                                variant="outline"
                                role="combobox"
                                aria-expanded={columnOpen}
                                className="h-9 w-full justify-between text-xs font-normal"
                            >
                                <span className="truncate">
                                    {condition.column || 'Select...'}
                                </span>
                                <ChevronsUpDown className="ml-1 h-3.5 w-3.5 shrink-0 opacity-50" />
                            </Button>
                        </PopoverTrigger>
                        <PopoverContent className="w-[220px] p-0" style={{ zIndex: 1000 }}>
                            <Command>
                                <CommandInput placeholder="Search columns..." className="text-xs" />
                                <CommandList>
                                    <CommandEmpty>No columns found</CommandEmpty>
                                    <CommandGroup>
                                        {columns.map(col => (
                                            <CommandItem
                                                key={col.column_name}
                                                value={col.column_name}
                                                onSelect={(val) => {
                                                    onUpdate('column', val)
                                                    setColumnOpen(false)
                                                }}
                                                className="text-xs"
                                            >
                                                <Check className={cn("mr-2 h-3.5 w-3.5", condition.column === col.column_name ? "opacity-100" : "opacity-0")} />
                                                {col.column_name}
                                            </CommandItem>
                                        ))}
                                    </CommandGroup>
                                </CommandList>
                            </Command>
                        </PopoverContent>
                    </Popover>
                </div>

                {/* Operator */}
                <div>
                    <label className="text-[10px] font-medium text-muted-foreground uppercase mb-1 block">Operator</label>
                    <Select value={condition.operator} onValueChange={(val) => onUpdate('operator', val)}>
                        <SelectTrigger className="h-9 text-xs">
                            <SelectValue />
                        </SelectTrigger>
                        <SelectContent style={{ zIndex: 1000 }}>
                            {getOperatorsForColumn(condition.column).map(op => (
                                <SelectItem key={op.value} value={op.value}>
                                    <span className="text-xs">{op.label}</span>
                                </SelectItem>
                            ))}
                        </SelectContent>
                    </Select>
                </div>

                {/* Value */}
                <div>
                    <label className="text-[10px] font-medium text-muted-foreground uppercase mb-1 block">Value</label>
                    {isNull ? (
                        <Input className="h-9 text-xs" placeholder="N/A" disabled />
                    ) : isIn ? (
                        <Textarea
                            value={condition.value}
                            onChange={(e) => onUpdate('value', e.target.value)}
                            onClick={(e) => e.stopPropagation()}
                            className="text-xs min-h-[36px] resize-y"
                            placeholder="Enter comma-separated values: 1, 2, 3"
                            rows={2}
                        />
                    ) : isBooleanColumn(condition.column) ? (
                        <Select value={condition.value} onValueChange={(val) => onUpdate('value', val)}>
                            <SelectTrigger className="h-9 text-xs">
                                <SelectValue placeholder="Select value" />
                            </SelectTrigger>
                            <SelectContent style={{ zIndex: 1000 }}>
                                <SelectItem value="true">True</SelectItem>
                                <SelectItem value="false">False</SelectItem>
                            </SelectContent>
                        </Select>
                    ) : colType === 'date' ? (
                        <DatePicker value={condition.value} onChange={(v) => onUpdate('value', v)} />
                    ) : colType === 'datetime' ? (
                        <DateTimePicker value={condition.value} onChange={(v) => onUpdate('value', v)} />
                    ) : colType === 'numeric' ? (
                        <Input
                            value={condition.value}
                            onChange={(e) => {
                                let val = e.target.value;
                                val = val.replace(/[^0-9.-]/g, '');
                                const parts = val.split('.');
                                if (parts.length > 2) val = parts[0] + '.' + parts.slice(1).join('');
                                if (val.indexOf('-') > 0) val = val.slice(0, 1) + val.slice(1).replace(/-/g, '');
                                onUpdate('value', val);
                            }}
                            onClick={(e) => e.stopPropagation()}
                            className="h-9 text-xs"
                            type="number"
                            placeholder="Enter number"
                        />
                    ) : (
                        <Input
                            value={condition.value}
                            onChange={(e) => onUpdate('value', e.target.value)}
                            onClick={(e) => e.stopPropagation()}
                            className="h-9 text-xs"
                            type="text"
                            placeholder="Enter value"
                        />
                    )}
                </div>

                {/* Second Value for BETWEEN */}
                {isBetween && (
                    <div>
                        <label className="text-[10px] font-medium text-muted-foreground uppercase mb-1 block">To Value</label>
                        {colType === 'date' ? (
                            <DatePicker value={condition.value2 || ''} onChange={(v) => onUpdate('value2', v)} />
                        ) : colType === 'datetime' ? (
                            <DateTimePicker value={condition.value2 || ''} onChange={(v) => onUpdate('value2', v)} />
                        ) : colType === 'numeric' ? (
                            <Input
                                value={condition.value2 || ''}
                                onChange={(e) => {
                                    let val = e.target.value;
                                    val = val.replace(/[^0-9.-]/g, '');
                                    const parts = val.split('.');
                                    if (parts.length > 2) val = parts[0] + '.' + parts.slice(1).join('');
                                    if (val.indexOf('-') > 0) val = val.slice(0, 1) + val.slice(1).replace(/-/g, '');
                                    onUpdate('value2', val);
                                }}
                                onClick={(e) => e.stopPropagation()}
                                className="h-9 text-xs"
                                type="number"
                                placeholder="Enter number"
                            />
                        ) : (
                            <Input
                                value={condition.value2 || ''}
                                onChange={(e) => onUpdate('value2', e.target.value)}
                                onClick={(e) => e.stopPropagation()}
                                className="h-9 text-xs"
                                type="text"
                                placeholder="Enter value"
                            />
                        )}
                    </div>
                )}
            </div>

            {/* Remove button */}
            <div className="pt-6">
                <Button
                    variant="ghost"
                    size="icon"
                    className="h-6 w-6 text-muted-foreground hover:text-destructive"
                    onClick={(e) => { e.stopPropagation(); onRemove() }}
                >
                    <Trash2 className="h-3.5 w-3.5" />
                </Button>
            </div>
        </div>
    )
}

// ─── Date Pickers ────────────────────────────────────────────

function DatePicker({ value, onChange }: { value: string; onChange: (val: string) => void }) {
    return (
        <Popover>
            <PopoverTrigger asChild>
                <Button
                    variant="outline"
                    className={cn(
                        "w-full h-9 justify-start text-left font-normal text-xs",
                        !value && "text-muted-foreground"
                    )}
                    onClick={(e) => e.stopPropagation()}
                >
                    <CalendarIcon className="mr-2 h-3 w-3" />
                    {value ? format(new Date(value), 'PP') : <span>Pick date</span>}
                </Button>
            </PopoverTrigger>
            <PopoverContent className="w-auto p-0" align="start" style={{ zIndex: 1000 }}>
                <Calendar
                    mode="single"
                    selected={value ? new Date(value) : undefined}
                    onSelect={(date) => date && onChange(format(date, 'yyyy-MM-dd'))}
                    initialFocus
                />
            </PopoverContent>
        </Popover>
    )
}

function DateTimePicker({ value, onChange }: { value: string; onChange: (val: string) => void }) {
    const parseValue = () => {
        if (!value) return { date: undefined, time: '00:00:00' }
        const parts = value.split(' ')
        const datePart = parts[0]
        const timePart = parts[1] || '00:00:00'
        return {
            date: datePart ? new Date(datePart) : undefined,
            time: timePart
        }
    }

    const { date, time } = parseValue()

    const handleDateChange = (newDate: Date | undefined) => {
        if (newDate) {
            const formattedDate = format(newDate, 'yyyy-MM-dd')
            onChange(`${formattedDate} ${time}`)
        }
    }

    const handleTimeChange = (newTime: string) => {
        if (date) {
            const formattedDate = format(date, 'yyyy-MM-dd')
            onChange(`${formattedDate} ${newTime}`)
        } else {
            onChange(`1970-01-01 ${newTime}`)
        }
    }

    return (
        <Popover>
            <PopoverTrigger asChild>
                <Button
                    variant="outline"
                    className={cn(
                        "w-full h-9 justify-start text-left font-normal text-xs",
                        !value && "text-muted-foreground"
                    )}
                    onClick={(e) => e.stopPropagation()}
                >
                    <CalendarIcon className="mr-2 h-3 w-3" />
                    {value ? value : <span>Pick date & time</span>}
                </Button>
            </PopoverTrigger>
            <PopoverContent className="w-auto p-3" align="start" style={{ zIndex: 1000 }}>
                <Calendar
                    mode="single"
                    selected={date}
                    onSelect={handleDateChange}
                    initialFocus
                />
                <div className="border-t mt-3 pt-3">
                    <label className="text-xs font-medium text-muted-foreground mb-1 block">Time</label>
                    <Input
                        type="time"
                        step="1"
                        value={time}
                        onChange={(e) => handleTimeChange(e.target.value || '00:00:00')}
                        onClick={(e) => e.stopPropagation()}
                        className="h-8 text-xs"
                    />
                </div>
            </PopoverContent>
        </Popover>
    )
}
