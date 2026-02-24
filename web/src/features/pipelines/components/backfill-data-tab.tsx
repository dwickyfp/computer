import { useState } from 'react'
import { formatDistanceToNow, format } from 'date-fns'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { backfillApi, type BackfillFilter } from '@/repo/backfill'
import { type Pipeline } from '@/repo/pipelines'
import { filterV2ToSql } from '@/features/pipelines/components/table-filter-card'
import { sourcesRepo } from '@/repo/sources'
import {
  Plus,
  X,
  Loader2,
  AlertCircle,
  CheckCircle2,
  Clock,
  Trash2,
  Square,
  RefreshCw,
} from 'lucide-react'
import { Check, ChevronsUpDown } from 'lucide-react'
import { toast } from 'sonner'
import { cn } from '@/lib/utils'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import {
  Command,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
} from '@/components/ui/command'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from '@/components/ui/dialog'
import {
  HoverCard,
  HoverCardContent,
  HoverCardTrigger,
} from '@/components/ui/hover-card'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from '@/components/ui/popover'
import { Progress } from '@/components/ui/progress'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import { DatePicker } from '@/components/date-picker'

interface BackfillDataTabProps {
  pipelineId: number
  sourceId: number
  pipeline?: Pipeline
}

// Operators grouped by data type
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
    { value: 'IS NULL', label: 'Is Null' },
    { value: 'IS NOT NULL', label: 'Is Not Null' },
  ],
}

const STATUS_CONFIG = {
  PENDING: {
    label: 'Pending',
    color:
      'bg-yellow-100 text-yellow-800 dark:bg-yellow-900/30 dark:text-yellow-400',
    icon: Clock,
  },
  EXECUTING: {
    label: 'Executing',
    color: 'bg-blue-100 text-blue-800 dark:bg-blue-900/30 dark:text-blue-400',
    icon: Loader2,
  },
  COMPLETED: {
    label: 'Completed',
    color:
      'bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-400',
    icon: CheckCircle2,
  },
  FAILED: {
    label: 'Failed',
    color: 'bg-red-100 text-red-800 dark:bg-red-900/30 dark:text-red-400',
    icon: AlertCircle,
  },
  CANCELLED: {
    label: 'Cancelled',
    color: 'bg-red-100 text-red-800 dark:bg-red-900/30 dark:text-red-400',
    icon: X,
  },
}

// Separate component to isolate state and prevent re-render issues
function ColumnSelectWithSearch({
  value,
  onValueChange,
  columns,
  disabled,
}: {
  value: string
  onValueChange: (value: string) => void
  columns: { column_name: string }[]
  disabled?: boolean
}) {
  const [open, setOpen] = useState(false)

  return (
    <div className='space-y-1.5'>
      <Label className='text-xs text-muted-foreground'>Column</Label>
      <Popover open={open} onOpenChange={setOpen}>
        <PopoverTrigger asChild>
          <Button
            variant='outline'
            role='combobox'
            aria-expanded={open}
            disabled={disabled}
            className='h-8 w-full justify-between text-xs font-normal'
          >
            <span className='truncate'>
              {value || 'Select column'}
            </span>
            <ChevronsUpDown className='ml-1 h-3.5 w-3.5 shrink-0 opacity-50' />
          </Button>
        </PopoverTrigger>
        <PopoverContent className='w-[220px] p-0'>
          <Command>
            <CommandInput placeholder='Search columns...' className='text-xs' />
            <CommandList>
              <CommandEmpty>No columns found</CommandEmpty>
              <CommandGroup>
                {columns.map((col) => (
                  <CommandItem
                    key={col.column_name}
                    value={col.column_name}
                    onSelect={(val) => {
                      onValueChange(val)
                      setOpen(false)
                    }}
                    className='text-xs'
                  >
                    <Check
                      className={cn(
                        'mr-2 h-3.5 w-3.5',
                        value === col.column_name ? 'opacity-100' : 'opacity-0'
                      )}
                    />
                    {col.column_name}
                  </CommandItem>
                ))}
              </CommandGroup>
            </CommandList>
          </Command>
        </PopoverContent>
      </Popover>
    </div>
  )
}

function CreateBackfillDialog({ pipelineId, sourceId }: BackfillDataTabProps) {
  const [open, setOpen] = useState(false)
  const [openCombobox, setOpenCombobox] = useState(false)
  const [tableName, setTableName] = useState('')
  const [filters, setFilters] = useState<BackfillFilter[]>([])
  const queryClient = useQueryClient()

  // Fetch available tables for source
  const { data: sourceDetails } = useQuery({
    queryKey: ['source-details', sourceId],
    queryFn: () => sourcesRepo.getDetails(sourceId),
    enabled: open,
  })

  const createMutation = useMutation({
    mutationFn: (data: { table_name: string; filters?: BackfillFilter[] }) =>
      backfillApi.createJob(pipelineId, data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['backfill-jobs', pipelineId] })
      toast.success('Backfill job created successfully')
      setOpen(false)
      resetForm()
    },
    onError: (error: any) => {
      toast.error(
        `Failed to create backfill job: ${error.response?.data?.message || error.response?.data?.detail || error.message}`
      )
    },
  })

  // Handle dialog open state change
  const handleOpenChange = (newOpen: boolean) => {
    if (!newOpen) {
      resetForm()
    }
    setOpen(newOpen)
  }

  const resetForm = () => {
    setTableName('')
    setFilters([])
  }

  const handleTableChange = (value: string) => {
    setTableName(value)
    // Reset filters when table changes
    setFilters([])
    setOpenCombobox(false)
  }

  const addFilter = () => {
    if (!tableName) {
      toast.error('Please select a table first')
      return
    }
    if (filters.length >= 5) {
      toast.error('Maximum 5 filters allowed')
      return
    }
    setFilters([...filters, { column: '', operator: '=', value: '' }])
  }

  const removeFilter = (index: number) => {
    setFilters(filters.filter((_, i) => i !== index))
  }

  const updateFilter = (
    index: number,
    field: keyof BackfillFilter,
    value: string
  ) => {
    const newFilters = [...filters]
    
    // If updating column, reset operator and value to ensure compatibility
    if (field === 'column') {
      const validOperators = getOperatorsForColumn(value)
      const currentOperator = newFilters[index].operator
      
      // Check if current operator is valid for new column type
      const isOperatorValid = validOperators.some(op => op.value === currentOperator)
      
      newFilters[index] = {
        column: value,
        operator: isOperatorValid ? currentOperator : '=',
        value: '', // Reset value when column changes
      }
    } else {
      newFilters[index] = { ...newFilters[index], [field]: value }
    }
    
    setFilters(newFilters)
  }

  // Get columns for selected table (exclude text types)
  const selectedTableColumns = sourceDetails?.tables
    ?.find((t) => t.table_name === tableName)
    ?.schema_table?.filter((col) => {
      const dataType = (col.data_type || col.real_data_type || '').toLowerCase()
      // Exclude text/varchar/char types
      return (
        !dataType.includes('text') &&
        !dataType.includes('char') &&
        !dataType.includes('string')
      )
    })

  // Helper to get column data type
  const getColumnType = (columnName: string): string => {
    const col = selectedTableColumns?.find((c) => c.column_name === columnName)
    return (col?.data_type || col?.real_data_type || '').toLowerCase()
  }

  // Helper to determine column type category
  const getColumnTypeCategory = (columnName: string): 'boolean' | 'string' | 'number' | 'date' => {
    const type = getColumnType(columnName)
    
    // Boolean types
    if (type.includes('bool')) {
      return 'boolean'
    }
    
    // Date/datetime types
    if (type.includes('date') || type.includes('time')) {
      return 'date'
    }
    
    // Numeric types
    if (
      type.includes('int') ||
      type.includes('numeric') ||
      type.includes('decimal') ||
      type.includes('float') ||
      type.includes('double') ||
      type.includes('real') ||
      type.includes('money')
    ) {
      return 'number'
    }
    
    // Default to string
    return 'string'
  }

  // Helper to get operators for a column
  const getOperatorsForColumn = (columnName: string) => {
    if (!columnName) return OPERATORS_BY_TYPE.string
    const category = getColumnTypeCategory(columnName)
    return OPERATORS_BY_TYPE[category]
  }

  // Helper to determine if column is date/datetime
  const isDateColumn = (columnName: string): boolean => {
    const type = getColumnType(columnName)
    return type.includes('date') || type.includes('time')
  }

  // Helper to determine if column is numeric
  const isNumericColumn = (columnName: string): boolean => {
    const type = getColumnType(columnName)
    return (
      type.includes('int') ||
      type.includes('numeric') ||
      type.includes('decimal') ||
      type.includes('float') ||
      type.includes('double') ||
      type.includes('real') ||
      type.includes('money')
    )
  }

  // Helper to determine if column is boolean
  const isBooleanColumn = (columnName: string): boolean => {
    const type = getColumnType(columnName)
    return type.includes('bool')
  }

  const handleSubmit = () => {
    if (!tableName) {
      toast.error('Please select a table')
      return
    }

    // Validate filters
    const validFilters = filters.filter((f) => f.column && f.operator)
    if (filters.length > 0 && validFilters.length !== filters.length) {
      toast.error('Please complete all filter fields')
      return
    }

    createMutation.mutate({
      table_name: tableName,
      filters: validFilters.length > 0 ? validFilters : undefined,
    })
  }

  return (
    <Dialog open={open} onOpenChange={handleOpenChange}>
      <DialogTrigger asChild>
        <Button>
          <Plus className='mr-2 h-4 w-4' />
          Create Backfill
        </Button>
      </DialogTrigger>
      <DialogContent className='max-h-[90vh] overflow-y-auto sm:max-w-4xl'>
        <DialogHeader>
          <DialogTitle>Create Backfill Job</DialogTitle>
          <DialogDescription>
            Backfill historical data from source to destination. Add up to 5
            filters to limit the scope.
          </DialogDescription>
        </DialogHeader>

        <div className='grid gap-6 py-4'>
          {/* Table Selection */}
          <div className='grid gap-2'>
            <Label htmlFor='table'>Table Name</Label>
            <Popover open={openCombobox} onOpenChange={setOpenCombobox}>
              <PopoverTrigger asChild>
                <Button
                  variant='outline'
                  role='combobox'
                  aria-expanded={openCombobox}
                  className='w-full justify-between'
                >
                  {tableName || 'Select a table...'}
                  <ChevronsUpDown className='ml-2 h-4 w-4 shrink-0 opacity-50' />
                </Button>
              </PopoverTrigger>
              <PopoverContent className='w-100 p-0'>
                <Command>
                  <CommandInput placeholder='Search table...' />
                  <CommandList>
                    <CommandEmpty>No table found.</CommandEmpty>
                    <CommandGroup>
                      {sourceDetails?.tables?.map((table) => (
                        <CommandItem
                          key={table.table_name}
                          value={table.table_name}
                          onSelect={handleTableChange}
                        >
                          <Check
                            className={cn(
                              'mr-2 h-4 w-4',
                              tableName === table.table_name
                                ? 'opacity-100'
                                : 'opacity-0'
                            )}
                          />
                          {table.table_name}
                        </CommandItem>
                      ))}
                    </CommandGroup>
                  </CommandList>
                </Command>
              </PopoverContent>
            </Popover>
          </div>

          {/* Filters */}
          <div className='grid gap-4'>
            <div className='flex items-center justify-between'>
              <Label>Filters (Optional)</Label>
              <Button
                type='button'
                variant='outline'
                size='sm'
                onClick={addFilter}
                disabled={filters.length >= 5 || !tableName}
              >
                <Plus className='mr-2 h-3 w-3' />
                Add Filter
              </Button>
            </div>

            {!tableName && filters.length === 0 && (
              <div className='rounded-lg border border-dashed p-8 text-center'>
                <p className='text-sm text-muted-foreground'>
                  Select a table above to enable filters.
                </p>
              </div>
            )}

            {tableName && filters.length === 0 && (
              <div className='rounded-lg border border-dashed p-8 text-center'>
                <p className='text-sm text-muted-foreground'>
                  No filters active. All records from{' '}
                  <span className='font-mono text-foreground'>{tableName}</span>{' '}
                  will be backfilled.
                </p>
              </div>
            )}

            {filters.length > 0 && (
              <div className='grid gap-3'>
                {filters.map((filter, index) => (
                  <div
                    key={index}
                    className='grid grid-cols-[minmax(120px,1fr)_minmax(180px,auto)_minmax(150px,1.5fr)_auto] items-end gap-4 rounded-lg border bg-muted/40 p-3'
                  >
                    <ColumnSelectWithSearch
                      value={filter.column}
                      onValueChange={(value) => updateFilter(index, 'column', value)}
                      columns={selectedTableColumns || []}
                      disabled={!tableName}
                    />

                    <div className='space-y-1.5'>
                      <Label className='text-xs text-muted-foreground'>
                        Operator
                      </Label>
                      <Select
                        value={filter.operator}
                        onValueChange={(value) =>
                          updateFilter(index, 'operator', value)
                        }
                      >
                        <SelectTrigger className='h-8'>
                          <SelectValue />
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

                    <div className='space-y-1.5'>
                      <Label className='text-xs text-muted-foreground'>
                        Value
                      </Label>
                      {filter.operator === 'IS NULL' ||
                      filter.operator === 'IS NOT NULL' ? (
                        <Input className='h-8' placeholder='N/A' disabled />
                      ) : filter.column && isBooleanColumn(filter.column) ? (
                        <Select
                          value={filter.value}
                          onValueChange={(value) =>
                            updateFilter(index, 'value', value)
                          }
                        >
                          <SelectTrigger className='h-8'>
                            <SelectValue placeholder='Select value' />
                          </SelectTrigger>
                          <SelectContent>
                            <SelectItem value='true'>True</SelectItem>
                            <SelectItem value='false'>False</SelectItem>
                          </SelectContent>
                        </Select>
                      ) : filter.column && isDateColumn(filter.column) ? (
                        <DatePicker
                          selected={
                            filter.value ? new Date(filter.value) : undefined
                          }
                          onSelect={(date) =>
                            updateFilter(
                              index,
                              'value',
                              date ? format(date, 'yyyy-MM-dd') : ''
                            )
                          }
                          placeholder='Select date'
                        />
                      ) : filter.operator === 'IN' ? (
                        <Input
                          className='h-8'
                          placeholder={isNumericColumn(filter.column) ? '1, 2, 3' : 'value1, value2, value3'}
                          value={filter.value}
                          onChange={(e) =>
                            updateFilter(index, 'value', e.target.value)
                          }
                        />
                      ) : filter.column && isNumericColumn(filter.column) ? (
                        <Input
                          className='h-8'
                          type='number'
                          placeholder='Enter number'
                          value={filter.value}
                          onChange={(e) =>
                            updateFilter(index, 'value', e.target.value)
                          }
                        />
                      ) : (
                        <Input
                          className='h-8'
                          placeholder='Enter value'
                          value={filter.value}
                          onChange={(e) =>
                            updateFilter(index, 'value', e.target.value)
                          }
                        />
                      )}
                    </div>

                    <Button
                      type='button'
                      variant='ghost'
                      size='icon'
                      className='h-8 w-8 text-muted-foreground hover:text-destructive'
                      onClick={() => removeFilter(index)}
                    >
                      <X className='h-4 w-4' />
                    </Button>
                  </div>
                ))}
              </div>
            )}
          </div>
        </div>

        <DialogFooter>
          <Button variant='outline' onClick={() => handleOpenChange(false)}>
            Cancel
          </Button>
          <Button onClick={handleSubmit} disabled={createMutation.isPending}>
            {createMutation.isPending && (
              <Loader2 className='mr-2 h-4 w-4 animate-spin' />
            )}
            Create Job
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}

export function BackfillDataTab({
  pipelineId,
  sourceId,
  pipeline,
}: BackfillDataTabProps) {
  const queryClient = useQueryClient()
  const [tableFilter, setTableFilter] = useState<string>('')

  // Fetch backfill jobs
  const { data: jobsData, isLoading } = useQuery({
    queryKey: ['backfill-jobs', pipelineId, tableFilter],
    queryFn: () => backfillApi.listJobs(pipelineId),
    refetchInterval: 5000, // Refresh every 5 seconds to show progress
  })

  // Filter jobs by table name if filter is set
  const filteredJobs = tableFilter
    ? jobsData?.items.filter((job) => job.table_name === tableFilter)
    : jobsData?.items

  // Cancel mutation
  const cancelMutation = useMutation({
    mutationFn: (jobId: number) => backfillApi.cancelJob(jobId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['backfill-jobs', pipelineId] })
      toast.success('Backfill job cancelled')
    },
    onError: (error: any) => {
      toast.error(
        `Failed to cancel job: ${error.response?.data?.message || error.response?.data?.detail || error.message}`
      )
    },
  })

  // Delete mutation
  const deleteMutation = useMutation({
    mutationFn: (jobId: number) => backfillApi.deleteJob(jobId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['backfill-jobs', pipelineId] })
      toast.success('Backfill job deleted')
    },
    onError: (error: any) => {
      toast.error(
        `Failed to delete job: ${error.response?.data?.message || error.response?.data?.detail || error.message}`
      )
    },
  })

  const formatNumber = (num: number) => new Intl.NumberFormat().format(num)

  return (
    <div className='space-y-4'>
      {/* Pipeline Refresh Info */}
      {pipeline?.last_refresh_at && (
        <div className='rounded-lg border bg-muted/40 p-3'>
          <div className='flex items-center gap-2 text-sm'>
            <RefreshCw className='h-4 w-4 text-muted-foreground' />
            <span className='font-medium'>Last Refresh:</span>
            <span className='text-muted-foreground'>
              {formatDistanceToNow(new Date(pipeline.last_refresh_at), {
                addSuffix: true,
              })}
            </span>
            <span className='text-xs text-muted-foreground'>
              ({format(new Date(pipeline.last_refresh_at), 'PPpp')})
            </span>
          </div>
        </div>
      )}

      <div className='flex items-center justify-between'>
        <div>
          <h3 className='text-lg font-semibold'>Backfill Jobs</h3>
          <p className='text-sm text-muted-foreground'>
            Manage historical data backfill operations
          </p>
        </div>
        <CreateBackfillDialog pipelineId={pipelineId} sourceId={sourceId} />
      </div>

      {isLoading ? (
        <div className='flex h-64 items-center justify-center'>
          <Loader2 className='h-8 w-8 animate-spin text-muted-foreground' />
        </div>
      ) : jobsData?.items.length === 0 ? (
        <div className='flex h-64 flex-col items-center justify-center rounded-lg border'>
          <p className='mb-4 text-muted-foreground'>No backfill jobs yet</p>
        </div>
      ) : (
        <>
          {/* Table Filter */}
          <div className='flex items-center gap-2'>
            <Label htmlFor='table-filter' className='text-sm font-medium'>
              Filter by Table:
            </Label>
            <Select
              value={tableFilter || 'all'}
              onValueChange={(value) =>
                setTableFilter(value === 'all' ? '' : value)
              }
            >
              <SelectTrigger id='table-filter' className='w-62.5'>
                <SelectValue placeholder='All tables' />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value='all'>All tables</SelectItem>
                {Array.from(
                  new Set(jobsData?.items.map((job) => job.table_name) || [])
                ).map((tableName) => (
                  <SelectItem key={tableName} value={tableName}>
                    {tableName}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>

          <div className='h-[400px] overflow-y-auto rounded-lg border'>
            <Table>
              <TableHeader className='sticky top-0 z-10 bg-background'>
                <TableRow>
                  <TableHead>Table Name</TableHead>
                  <TableHead>Status</TableHead>
                  <TableHead>Records</TableHead>
                  <TableHead>Progress</TableHead>
                  <TableHead>Filters</TableHead>
                  <TableHead>Created</TableHead>
                  <TableHead className='text-right'>Actions</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {filteredJobs?.map((job) => {
                  const StatusIcon = STATUS_CONFIG[job.status].icon
                  return (
                    <TableRow key={job.id}>
                      <TableCell className='font-medium'>
                        {job.table_name}
                      </TableCell>
                      <TableCell>
                        <Badge
                          variant='outline'
                          className={STATUS_CONFIG[job.status].color}
                        >
                          <StatusIcon
                            className={`mr-1 h-3 w-3 ${job.status === 'EXECUTING' ? 'animate-spin' : ''}`}
                          />
                          {STATUS_CONFIG[job.status].label}
                        </Badge>
                      </TableCell>
                      <TableCell>
                        {job.total_record > 0
                          ? `${formatNumber(job.count_record)}/${formatNumber(job.total_record)}`
                          : formatNumber(job.count_record)}
                      </TableCell>
                      <TableCell>
                        <div className='flex items-center gap-2'>
                          <Progress
                            value={
                              job.total_record > 0
                                ? (job.count_record / job.total_record) * 100
                                : 0
                            }
                            className='h-2 w-24'
                          />
                          <span className='text-xs whitespace-nowrap text-muted-foreground'>
                            {job.total_record > 0
                              ? `${Math.round((job.count_record / job.total_record) * 100)}%`
                              : '0%'}
                          </span>
                        </div>
                      </TableCell>
                      <TableCell>
                        {job.filter_sql ? (
                          <HoverCard>
                            <HoverCardTrigger asChild>
                              <span className='cursor-pointer text-xs text-muted-foreground underline decoration-dotted transition-colors hover:text-foreground'>
                                {(() => {
                                  try {
                                    const parsed = JSON.parse(job.filter_sql)
                                    if (parsed.version === 2) {
                                      const count = parsed.groups.reduce((acc: number, g: any) => acc + g.conditions.length, 0)
                                      return `${count} filter(s)`
                                    }
                                  } catch {}
                                  return `${job.filter_sql.split(';').length} filter(s)`
                                })()}
                              </span>
                            </HoverCardTrigger>
                            <HoverCardContent className='w-96' align='start'>
                              <div className='space-y-2'>
                                <h4 className='text-sm font-semibold'>
                                  Applied Filters
                                </h4>
                                <div className='rounded border bg-muted p-2 font-mono text-xs break-all'>
                                  WHERE {filterV2ToSql(job.filter_sql) || job.filter_sql}
                                </div>
                              </div>
                            </HoverCardContent>
                          </HoverCard>
                        ) : (
                          <span className='text-xs text-muted-foreground'>
                            None
                          </span>
                        )}
                      </TableCell>
                      <TableCell className='text-sm text-muted-foreground'>
                        {formatDistanceToNow(new Date(job.created_at), {
                          addSuffix: true,
                        })}
                      </TableCell>
                      <TableCell className='text-right'>
                        <div className='flex justify-end gap-2'>
                          {(job.status === 'PENDING' ||
                            job.status === 'EXECUTING') && (
                            <Button
                              variant='outline'
                              size='icon'
                              onClick={() => cancelMutation.mutate(job.id)}
                              disabled={cancelMutation.isPending}
                              title='Cancel backfill'
                              className='text-red-600 hover:bg-red-50 hover:text-red-700 dark:text-red-500 dark:hover:bg-red-950 dark:hover:text-red-400'
                            >
                              <Square className='h-4 w-4' />
                            </Button>
                          )}
                          {(job.status === 'COMPLETED' ||
                            job.status === 'FAILED' ||
                            job.status === 'CANCELLED') && (
                            <Button
                              variant='ghost'
                              size='icon'
                              onClick={() => deleteMutation.mutate(job.id)}
                              disabled={deleteMutation.isPending}
                              title='Delete backfill'
                              className='text-red-600 hover:bg-red-50 hover:text-red-700 dark:text-red-500 dark:hover:bg-red-950 dark:hover:text-red-400'
                            >
                              <Trash2 className='h-4 w-4' />
                            </Button>
                          )}
                        </div>
                      </TableCell>
                    </TableRow>
                  )
                })}
              </TableBody>
            </Table>
          </div>
        </>
      )}
    </div>
  )
}
