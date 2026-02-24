import { useState, useEffect } from 'react'
import { type TableWithSyncInfo } from '@/repo/pipelines'
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
import { X, Plus, Trash2 } from 'lucide-react'

interface FilterModalProps {
  open: boolean
  onOpenChange: (open: boolean) => void
  table: TableWithSyncInfo | null
  onSave: (filterSql: string) => void
}

interface FilterCondition {
  id: string
  column: string
  operator: string
  value: string
}

const OPERATORS = {
  string: [
    { value: 'equals', label: '=' },
    { value: 'not_equals', label: '!=' },
    { value: 'like', label: 'LIKE' },
    { value: 'ilike', label: 'ILIKE' },
    { value: 'starts_with', label: 'Starts with' },
    { value: 'ends_with', label: 'Ends with' },
    { value: 'contains', label: 'Contains' },
  ],
  number: [
    { value: 'equals', label: '=' },
    { value: 'not_equals', label: '!=' },
    { value: 'gt', label: '>' },
    { value: 'gte', label: '>=' },
    { value: 'lt', label: '<' },
    { value: 'lte', label: '<=' },
    { value: 'between', label: 'Between' },
  ],
  date: [
    { value: 'equals', label: '=' },
    { value: 'gt', label: 'After' },
    { value: 'gte', label: 'On or after' },
    { value: 'lt', label: 'Before' },
    { value: 'lte', label: 'On or before' },
    { value: 'between', label: 'Between' },
  ],
}

const getColumnType = (dataType: string): 'string' | 'number' | 'date' => {
  const type = dataType.toLowerCase()
  if (
    type.includes('int') ||
    type.includes('numeric') ||
    type.includes('decimal') ||
    type.includes('float') ||
    type.includes('double') ||
    type.includes('real')
  ) {
    return 'number'
  }
  if (type.includes('date') || type.includes('time') || type.includes('timestamp')) {
    return 'date'
  }
  return 'string'
}

const generateId = () => Math.random().toString(36).substring(2, 9)

export function FilterModal({
  open,
  onOpenChange,
  table,
  onSave,
}: FilterModalProps) {
  const [filters, setFilters] = useState<FilterCondition[]>([])

  // Parse existing filters when modal opens
  useEffect(() => {
    if (open && table?.sync_configs?.[0]?.filter_sql) {
      const parsed = parseFilterSql(table.sync_configs[0].filter_sql)
      setFilters(parsed)
    } else if (open) {
      setFilters([{ id: generateId(), column: '', operator: '', value: '' }])
    }
  }, [open, table])

  const parseFilterSql = (filterSql: string): FilterCondition[] => {
    // Format: column:operator:value;column:operator:value
    if (!filterSql) return []

    return filterSql.split(';').map((part) => {
      const [column, operator, value] = part.split(':')
      return {
        id: generateId(),
        column: column || '',
        operator: operator || '',
        value: value || '',
      }
    })
  }

  const buildFilterSql = (): string => {
    return filters
      .filter((f) => f.column && f.operator && f.value)
      .map((f) => `${f.column}:${f.operator}:${f.value}`)
      .join(';')
  }

  const addFilter = () => {
    setFilters([...filters, { id: generateId(), column: '', operator: '', value: '' }])
  }

  const removeFilter = (id: string) => {
    setFilters(filters.filter((f) => f.id !== id))
  }

  const updateFilter = (id: string, field: keyof FilterCondition, value: string) => {
    setFilters(
      filters.map((f) => (f.id === id ? { ...f, [field]: value } : f))
    )
  }

  const getOperatorsForColumn = (columnName: string) => {
    if (!table?.columns || !columnName) return OPERATORS.string
    
    const column = table.columns.find((c) => c.column_name === columnName)
    if (!column) return OPERATORS.string
    
    const type = getColumnType(column.data_type || '')
    return OPERATORS[type]
  }

  const handleSave = () => {
    const filterSql = buildFilterSql()
    onSave(filterSql)
  }

  if (!open || !table) return null

  return (
    <div
      className="fixed inset-0 z-50 flex items-start justify-start"
      onClick={() => onOpenChange(false)}
    >
      {/* Overlay */}
      <div className="absolute inset-0 bg-black/20" />

      {/* Modal positioned to the right of the drawer */}
      <div
        className="absolute left-[520px] top-[100px] z-50 w-[400px] bg-background border rounded-lg shadow-lg p-4"
        onClick={(e) => e.stopPropagation()}
      >
        {/* Header */}
        <div className="flex items-center justify-between mb-4">
          <h3 className="font-semibold">Column Filter</h3>
          <Button
            variant="ghost"
            size="sm"
            onClick={() => onOpenChange(false)}
            className="h-8 w-8 p-0"
          >
            <X className="h-4 w-4" />
          </Button>
        </div>

        <p className="text-sm text-muted-foreground mb-4">
          Filter data for <span className="font-medium">{table.table_name}</span>
        </p>

        {/* Filters */}
        <div className="space-y-3 max-h-[400px] overflow-y-auto">
          {filters.map((filter, index) => (
            <div key={filter.id} className="border rounded-lg p-3 space-y-2">
              <div className="flex items-center justify-between">
                <span className="text-xs font-medium text-muted-foreground">
                  Condition {index + 1}
                </span>
                {filters.length > 1 && (
                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={() => removeFilter(filter.id)}
                    className="h-6 w-6 p-0"
                  >
                    <Trash2 className="h-3 w-3" />
                  </Button>
                )}
              </div>

              {/* Column */}
              <div>
                <Label className="text-xs">Column</Label>
                <Select
                  value={filter.column}
                  onValueChange={(val) => updateFilter(filter.id, 'column', val)}
                >
                  <SelectTrigger className="h-8">
                    <SelectValue placeholder="Select column" />
                  </SelectTrigger>
                  <SelectContent>
                    {table.columns.map((col) => (
                      <SelectItem key={col.column_name} value={col.column_name}>
                        {col.column_name}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>

              {/* Operator */}
              <div>
                <Label className="text-xs">Operator</Label>
                <Select
                  value={filter.operator}
                  onValueChange={(val) => updateFilter(filter.id, 'operator', val)}
                  disabled={!filter.column}
                >
                  <SelectTrigger className="h-8">
                    <SelectValue placeholder="Select operator" />
                  </SelectTrigger>
                  <SelectContent>
                    {getOperatorsForColumn(filter.column).map((op) => (
                      <SelectItem key={op.value} value={op.value}>
                        {op.label}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>

              {/* Value */}
              <div>
                <Label className="text-xs">Value</Label>
                <Input
                  className="h-8"
                  placeholder="Enter value"
                  value={filter.value}
                  onChange={(e) => updateFilter(filter.id, 'value', e.target.value)}
                  disabled={!filter.operator}
                />
              </div>
            </div>
          ))}
        </div>

        {/* Add Filter */}
        <Button variant="outline" size="sm" onClick={addFilter} className="mt-3 w-full">
          <Plus className="h-4 w-4 mr-1" />
          Add Condition
        </Button>

        {/* Actions */}
        <div className="flex gap-2 mt-4 pt-4 border-t">
          <Button variant="outline" onClick={() => onOpenChange(false)} className="flex-1">
            Cancel
          </Button>
          <Button onClick={handleSave} className="flex-1">
            Save Filter
          </Button>
        </div>
      </div>
    </div>
  )
}
