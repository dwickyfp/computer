import { useMemo, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  flexRender,
  getCoreRowModel,
  getFacetedRowModel,
  getFacetedUniqueValues,
  getFilteredRowModel,
  getPaginationRowModel,
  getSortedRowModel,
  useReactTable,
  type ColumnDef,
  type SortingState,
  type VisibilityState,
} from '@tanstack/react-table'
import {
  Download,
  Loader2,
  Lock,
  RefreshCcw,
  Save,
  Search,
} from 'lucide-react'
import { toast } from 'sonner'

import { DataTableFacetedFilter } from '@/components/data-table/faceted-filter'
import { DataTablePagination } from '@/components/data-table'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from '@/components/ui/card'
import { Checkbox } from '@/components/ui/checkbox'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from '@/components/ui/dialog'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import { cn } from '@/lib/utils'
import { sourcesRepo } from '@/repo/sources'
import { KafkaTopicPreviewModal } from './kafka-topic-preview-modal'

const KAFKA_LIVE_REFETCH_MS = 5000

type TableRowData = {
  name: string
  isPublished: boolean
  first_offset?: number | null
  next_offset?: number | null
  message_count?: number
}

interface SourceDetailsListTableProps {
  sourceId: number
  isPublicationEnabled: boolean
  publishedTableNames: string[]
  sourceType?: 'POSTGRES' | 'KAFKA'
}

export function SourceDetailsListTable({
  sourceId: propSourceId,
  isPublicationEnabled,
  publishedTableNames,
  sourceType = 'POSTGRES',
}: SourceDetailsListTableProps) {
  const id = propSourceId
  const isKafkaSource = sourceType === 'KAFKA'
  const objectLabel = isKafkaSource ? 'topic' : 'table'
  const objectLabelPlural = isKafkaSource ? 'topics' : 'tables'
  const streamLabel = isKafkaSource ? 'Registered' : 'Stream'
  const queryClient = useQueryClient()

  const [rowSelection, setRowSelection] = useState({})
  const [sorting, setSorting] = useState<SortingState>([])
  const [columnVisibility, setColumnVisibility] = useState<VisibilityState>({})
  const [globalFilter, setGlobalFilter] = useState('')
  const [processingTable, setProcessingTable] = useState<string | null>(null)
  const [previewTopicName, setPreviewTopicName] = useState<string | null>(null)

  const [presetName, setPresetName] = useState('')
  const [isSavePresetOpen, setIsSavePresetOpen] = useState(false)
  const [isLoadPresetOpen, setIsLoadPresetOpen] = useState(false)
  const [saveMode, setSaveMode] = useState<'new' | 'replace'>('new')
  const [presetToReplace, setPresetToReplace] = useState<string>('')
  const canManageSourceObjects = isKafkaSource || isPublicationEnabled

  const { data: availableTableNames, isLoading: isLoadingTables } = useQuery({
    queryKey: ['source-available-tables', id],
    queryFn: () => sourcesRepo.getAvailableTables(id),
    enabled: !!id && !isKafkaSource,
  })

  const { data: kafkaTopicSummaries, isLoading: isLoadingKafkaTopics } = useQuery({
    queryKey: ['source-kafka-topics-summary', id],
    queryFn: () => sourcesRepo.getKafkaTopicSummaries(id),
    enabled: !!id && isKafkaSource,
    refetchInterval: isKafkaSource ? KAFKA_LIVE_REFETCH_MS : false,
  })

  const { data: presets } = useQuery({
    queryKey: ['source-presets', id],
    queryFn: () => sourcesRepo.getPresets(id),
  })

  const data = useMemo<TableRowData[]>(() => {
    if (isKafkaSource) {
      return (kafkaTopicSummaries ?? []).map((topic) => ({
        name: topic.topic_name,
        isPublished: topic.is_registered,
        first_offset: topic.first_offset,
        next_offset: topic.next_offset,
        message_count: topic.message_count,
      }))
    }

    return (availableTableNames ?? []).map((tableName) => ({
      name: tableName,
      isPublished: publishedTableNames.includes(tableName),
    }))
  }, [
    availableTableNames,
    isKafkaSource,
    kafkaTopicSummaries,
    publishedTableNames,
  ])

  const savePresetMutation = useMutation({
    mutationFn: async () => {
      const selectedObjectNames = Object.keys(rowSelection).map(
        (index) => data[parseInt(index, 10)].name
      )

      if (saveMode === 'new') {
        if (!presetName) {
          throw new Error('Preset name is required')
        }
        return sourcesRepo.createPreset(id, {
          name: presetName,
          table_names: selectedObjectNames,
        })
      }

      if (!presetToReplace) {
        throw new Error('Please select a preset to replace')
      }

      const presetId = parseInt(presetToReplace, 10)
      const existingPreset = presets?.find((preset) => preset.id === presetId)
      if (!existingPreset) {
        throw new Error('Target preset not found')
      }

      return sourcesRepo.updatePreset(presetId, {
        name: existingPreset.name,
        table_names: selectedObjectNames,
      })
    },
    onSuccess: () => {
      toast.success(
        saveMode === 'new'
          ? 'Preset saved successfully'
          : 'Preset updated successfully'
      )
      setIsSavePresetOpen(false)
      setPresetName('')
      setPresetToReplace('')
      setSaveMode('new')
      queryClient.invalidateQueries({ queryKey: ['source-presets', id] })
    },
    onError: (err) => {
      toast.error(err instanceof Error ? err.message : 'Failed to save preset')
    },
  })

  const invalidateKafkaQueries = () => {
    queryClient.invalidateQueries({ queryKey: ['source-kafka-topics-summary', id] })
    queryClient.invalidateQueries({ queryKey: ['source-kafka-topic-preview', id] })
  }

  const registerTableMutation = useMutation({
    mutationFn: async (tableName: string) => {
      setProcessingTable(tableName)
      await sourcesRepo.registerTable(id, tableName)
    },
    onSuccess: (_, tableName) => {
      toast.success(
        `${isKafkaSource ? 'Topic' : 'Table'} ${tableName} ${
          isKafkaSource ? 'registered' : 'added to publication'
        }`
      )
      queryClient.invalidateQueries({ queryKey: ['source-details', id] })
      if (isKafkaSource) {
        invalidateKafkaQueries()
      }
      setProcessingTable(null)
    },
    onError: (err, tableName) => {
      void err
      toast.error(`Failed to add ${objectLabel} ${tableName}`)
      setProcessingTable(null)
    },
  })

  const unregisterTableMutation = useMutation({
    mutationFn: async (tableName: string) => {
      setProcessingTable(tableName)
      await sourcesRepo.unregisterTable(id, tableName)
    },
    onSuccess: (_, tableName) => {
      toast.success(
        `${isKafkaSource ? 'Topic' : 'Table'} ${tableName} ${
          isKafkaSource ? 'unregistered' : 'removed from publication'
        }`
      )
      queryClient.invalidateQueries({ queryKey: ['source-details', id] })
      if (isKafkaSource) {
        invalidateKafkaQueries()
      }
      setProcessingTable(null)
    },
    onError: (err, tableName) => {
      void err
      toast.error(`Failed to remove ${objectLabel} ${tableName}`)
      setProcessingTable(null)
    },
  })

  const columns: ColumnDef<TableRowData>[] = [
    {
      id: 'select',
      header: ({ table }) => (
        <Checkbox
          checked={
            table.getIsAllPageRowsSelected() ||
            (table.getIsSomePageRowsSelected() && 'indeterminate')
          }
          onCheckedChange={(value) => table.toggleAllPageRowsSelected(!!value)}
          aria-label='Select all'
        />
      ),
      cell: ({ row }) => (
        <Checkbox
          checked={row.getIsSelected()}
          onCheckedChange={(value) => row.toggleSelected(!!value)}
          aria-label='Select row'
        />
      ),
      enableSorting: false,
      enableHiding: false,
    },
    {
      accessorKey: 'name',
      header: isKafkaSource ? 'Topic Name' : 'Table Name',
      cell: ({ row }) => <div className='font-medium'>{row.getValue('name')}</div>,
    },
    {
      id: 'status',
      accessorFn: (row) => (row.isPublished ? 'published' : 'available'),
      header: 'Status',
      cell: ({ row }) => {
        const isPublished = row.original.isPublished
        return isPublished ? (
          <Badge
            variant='secondary'
            className='bg-green-100 text-green-800 hover:bg-green-100 dark:bg-green-900/30 dark:text-green-400'
          >
            {streamLabel}
          </Badge>
        ) : (
          <span className='text-xs text-muted-foreground'>Available</span>
        )
      },
      filterFn: (row, columnId, value) => value.includes(row.getValue(columnId)),
    },
  ]

  if (isKafkaSource) {
    columns.push(
      {
        accessorKey: 'first_offset',
        header: 'First Offset',
        cell: ({ row }) => (
          <div className='font-mono text-xs'>{row.original.first_offset ?? '--'}</div>
        ),
      },
      {
        accessorKey: 'next_offset',
        header: 'Next Offset',
        cell: ({ row }) => (
          <div className='font-mono text-xs'>{row.original.next_offset ?? '--'}</div>
        ),
      },
      {
        accessorKey: 'message_count',
        header: 'Message Count',
        cell: ({ row }) => (
          <div className='font-mono text-xs'>{row.original.message_count ?? 0}</div>
        ),
      },
      {
        id: 'preview',
        header: 'Preview',
        cell: ({ row }) => (
          <Button
            variant='outline'
            size='sm'
            className='h-7 text-xs'
            onClick={() => setPreviewTopicName(row.original.name)}
          >
            Preview
          </Button>
        ),
      }
    )
  }

  columns.push({
    id: 'actions',
    header: 'Actions',
    cell: ({ row }) => {
      const isPublished = row.original.isPublished
      const tableName = row.original.name
      const isProcessing = processingTable === tableName

      return isPublished ? (
        <Button
          variant='destructive'
          size='sm'
          className='h-7 text-xs'
          onClick={() => unregisterTableMutation.mutate(tableName)}
          disabled={isProcessing}
        >
          {isProcessing ? <Loader2 className='h-3 w-3 animate-spin' /> : 'Drop'}
        </Button>
      ) : (
        <Button
          variant='default'
          size='sm'
          className='h-7 text-xs'
          onClick={() => registerTableMutation.mutate(tableName)}
          disabled={isProcessing || !canManageSourceObjects}
        >
          {isProcessing ? (
            <Loader2 className='h-3 w-3 animate-spin' />
          ) : !canManageSourceObjects ? (
            <>
              <Lock className='mr-1 h-3 w-3' />
              Add
            </>
          ) : (
            'Add'
          )}
        </Button>
      )
    },
  })

  const table = useReactTable({
    data,
    columns,
    state: {
      sorting,
      columnVisibility,
      rowSelection,
      globalFilter,
    },
    enableRowSelection: true,
    onRowSelectionChange: setRowSelection,
    onSortingChange: setSorting,
    onColumnVisibilityChange: setColumnVisibility,
    onGlobalFilterChange: setGlobalFilter,
    getCoreRowModel: getCoreRowModel(),
    getFilteredRowModel: getFilteredRowModel(),
    getPaginationRowModel: getPaginationRowModel(),
    getSortedRowModel: getSortedRowModel(),
    getFacetedRowModel: getFacetedRowModel(),
    getFacetedUniqueValues: getFacetedUniqueValues(),
    getRowId: (_, index) => index.toString(),
  })

  const handleLoadPreset = (presetTableNames: string[]) => {
    const newSelection: Record<string, boolean> = {}
    let matchCount = 0

    data.forEach((row, index) => {
      if (presetTableNames.includes(row.name)) {
        newSelection[index] = true
        matchCount++
      }
    })

    setRowSelection(newSelection)
    setIsLoadPresetOpen(false)
    toast.success(
      `Loaded preset. Selected ${matchCount} ${matchCount === 1 ? objectLabel : objectLabelPlural}.`
    )
  }

  const refreshTablesMutation = useMutation({
    mutationFn: async () => {
      if (isKafkaSource) {
        return sourcesRepo.getKafkaTopicSummaries(id, true)
      }
      return sourcesRepo.getAvailableTables(id, true)
    },
    onSuccess: (result) => {
      if (isKafkaSource) {
        queryClient.setQueryData(['source-kafka-topics-summary', id], result)
        invalidateKafkaQueries()
        queryClient.invalidateQueries({ queryKey: ['source-details', id] })
      } else {
        queryClient.setQueryData(['source-available-tables', id], result)
      }
      toast.success(
        `${isKafkaSource ? 'Topic' : 'Table'} list refreshed successfully`
      )
    },
    onError: (err) => {
      void err
      toast.error(`Failed to refresh ${objectLabel} list`)
    },
  })

  const isFiltered = table.getState().columnFilters.length > 0
  const selectedCount = Object.keys(rowSelection).length
  const isLoading = isKafkaSource ? isLoadingKafkaTopics : isLoadingTables

  return (
    <>
      <Card className='bg-sidebar'>
        <CardHeader className='flex flex-row items-center justify-between'>
          <div>
            <CardTitle>
              {isKafkaSource ? 'Available Topics' : 'Available Tables'}
            </CardTitle>
            <CardDescription>
              {isKafkaSource
                ? 'Inspect broker topics, preview messages, and register topics for this source. Live stats refresh every 5 seconds.'
                : 'Select tables to save as a preset.'}
            </CardDescription>
          </div>
          <div className='flex gap-2'>
            <Button
              variant='outline'
              size='icon'
              onClick={() => refreshTablesMutation.mutate()}
              disabled={refreshTablesMutation.isPending}
              title={`Refresh ${objectLabel} list from source`}
            >
              <RefreshCcw
                className={cn(
                  'h-4 w-4',
                  refreshTablesMutation.isPending && 'animate-spin'
                )}
              />
            </Button>
            <Dialog open={isLoadPresetOpen} onOpenChange={setIsLoadPresetOpen}>
              <DialogTrigger asChild>
                <Button variant='outline'>
                  <Download className='mr-2 h-4 w-4' />
                  Load Preset
                </Button>
              </DialogTrigger>
              <DialogContent>
                <DialogHeader>
                  <DialogTitle>Load Preset</DialogTitle>
                  <DialogDescription>
                    Select a preset to apply {objectLabelPlural} to the selection.
                  </DialogDescription>
                </DialogHeader>
                <div className='grid max-h-[60vh] gap-2 overflow-y-auto py-4'>
                  {presets?.map((preset) => (
                    <Button
                      key={preset.id}
                      variant='ghost'
                      className='h-auto flex-col items-start justify-start py-3'
                      onClick={() => handleLoadPreset(preset.table_names)}
                    >
                      <div className='font-semibold'>{preset.name}</div>
                      <div className='text-xs text-muted-foreground'>
                        {preset.table_names.length} {objectLabelPlural}
                      </div>
                    </Button>
                  ))}
                  {presets?.length === 0 && (
                    <div className='text-center text-muted-foreground'>
                      No presets found.
                    </div>
                  )}
                </div>
              </DialogContent>
            </Dialog>

            <Dialog open={isSavePresetOpen} onOpenChange={setIsSavePresetOpen}>
              <DialogTrigger asChild>
                <Button variant='outline' disabled={selectedCount === 0}>
                  <Save className='mr-2 h-4 w-4' />
                  Save Preset ({selectedCount})
                </Button>
              </DialogTrigger>
              <DialogContent>
                <DialogHeader>
                  <DialogTitle>Save Preset</DialogTitle>
                  <DialogDescription>
                    Save the {selectedCount} selected {objectLabelPlural} as a preset.
                  </DialogDescription>
                </DialogHeader>
                <div className='grid gap-4 py-4'>
                  <div className='flex flex-col space-y-4'>
                    <div className='flex items-center space-x-4'>
                      <div className='flex items-center space-x-2'>
                        <input
                          type='radio'
                          id='new'
                          name='saveMode'
                          checked={saveMode === 'new'}
                          onChange={() => setSaveMode('new')}
                          className='aspect-square h-4 w-4 rounded-full border border-primary text-primary ring-offset-background focus:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:cursor-not-allowed disabled:opacity-50'
                        />
                        <Label htmlFor='new'>Create New</Label>
                      </div>
                      <div className='flex items-center space-x-2'>
                        <input
                          type='radio'
                          id='replace'
                          name='saveMode'
                          checked={saveMode === 'replace'}
                          onChange={() => setSaveMode('replace')}
                          className='aspect-square h-4 w-4 rounded-full border border-primary text-primary ring-offset-background focus:outline-none focus-visible:ring-2 focus-visible:ring-ring disabled:cursor-not-allowed disabled:opacity-50'
                        />
                        <Label htmlFor='replace'>Replace Existing</Label>
                      </div>
                    </div>

                    {saveMode === 'new' ? (
                      <div className='grid grid-cols-4 items-center gap-4'>
                        <Label htmlFor='name' className='text-right'>
                          Name
                        </Label>
                        <Input
                          id='name'
                          value={presetName}
                          onChange={(e) => setPresetName(e.target.value)}
                          className='col-span-3'
                        />
                      </div>
                    ) : (
                      <div className='grid grid-cols-4 items-center gap-4'>
                        <Label htmlFor='preset' className='text-right'>
                          Preset
                        </Label>
                        <select
                          id='preset'
                          className='col-span-3 flex h-10 w-full items-center justify-between rounded-md border border-input bg-background px-3 py-2 text-sm ring-offset-background placeholder:text-muted-foreground focus:ring-2 focus:ring-ring focus:ring-offset-2 focus:outline-none disabled:cursor-not-allowed disabled:opacity-50 [&>span]:line-clamp-1'
                          value={presetToReplace}
                          onChange={(e) => setPresetToReplace(e.target.value)}
                        >
                          <option value='' disabled>
                            Select a preset...
                          </option>
                          {presets?.map((preset) => (
                            <option key={preset.id} value={preset.id}>
                              {preset.name}
                            </option>
                          ))}
                        </select>
                      </div>
                    )}
                  </div>
                </div>
                <DialogFooter>
                  <Button
                    onClick={() => savePresetMutation.mutate()}
                    disabled={
                      (saveMode === 'new' && !presetName) ||
                      (saveMode === 'replace' && !presetToReplace) ||
                      savePresetMutation.isPending
                    }
                  >
                    {savePresetMutation.isPending && (
                      <Loader2 className='mr-2 h-4 w-4 animate-spin' />
                    )}
                    Save
                  </Button>
                </DialogFooter>
              </DialogContent>
            </Dialog>
          </div>
        </CardHeader>
        <CardContent>
          <div className='flex flex-1 flex-col gap-4'>
            <div className='flex items-center space-x-2'>
              <Search className='h-4 w-4 text-muted-foreground' />
              <Input
                placeholder={`Filter ${objectLabelPlural}...`}
                value={globalFilter ?? ''}
                onChange={(event) => setGlobalFilter(event.target.value)}
                className='h-8 max-w-sm'
              />
              {table.getColumn('status') && (
                <DataTableFacetedFilter
                  column={table.getColumn('status')}
                  title='Status'
                  options={[
                    { label: streamLabel, value: 'published' },
                    { label: 'Available', value: 'available' },
                  ]}
                />
              )}
              {isFiltered && (
                <Button
                  variant='ghost'
                  onClick={() => table.resetColumnFilters()}
                  className='h-8 px-2 lg:px-3'
                >
                  Reset
                </Button>
              )}
            </div>

            {isLoading ? (
              <div className='flex justify-center p-8'>
                <Loader2 className='h-8 w-8 animate-spin' />
              </div>
            ) : (
              <div className='rounded-md border border-border/50'>
                <Table>
                  <TableHeader>
                    {table.getHeaderGroups().map((headerGroup) => (
                      <TableRow key={headerGroup.id}>
                        {headerGroup.headers.map((header) => (
                          <TableHead key={header.id} colSpan={header.colSpan}>
                            {header.isPlaceholder
                              ? null
                              : flexRender(
                                  header.column.columnDef.header,
                                  header.getContext()
                                )}
                          </TableHead>
                        ))}
                      </TableRow>
                    ))}
                  </TableHeader>
                  <TableBody>
                    {table.getRowModel().rows?.length ? (
                      table.getRowModel().rows.map((row) => (
                        <TableRow
                          key={row.id}
                          data-state={row.getIsSelected() && 'selected'}
                        >
                          {row.getVisibleCells().map((cell) => (
                            <TableCell key={cell.id}>
                              {flexRender(
                                cell.column.columnDef.cell,
                                cell.getContext()
                              )}
                            </TableCell>
                          ))}
                        </TableRow>
                      ))
                    ) : (
                      <TableRow>
                        <TableCell colSpan={columns.length} className='h-24 text-center'>
                          No results.
                        </TableCell>
                      </TableRow>
                    )}
                  </TableBody>
                </Table>
              </div>
            )}
            <DataTablePagination table={table} />
          </div>
        </CardContent>
      </Card>

      {isKafkaSource && previewTopicName && (
        <KafkaTopicPreviewModal
          key={previewTopicName}
          sourceId={id}
          topicName={previewTopicName}
          open
          onOpenChange={(open) => !open && setPreviewTopicName(null)}
        />
      )}
    </>
  )
}
