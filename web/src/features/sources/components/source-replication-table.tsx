import { useState } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import {
  type SortingState,
  type VisibilityState,
  flexRender,
  getCoreRowModel,
  getFacetedRowModel,
  getFacetedUniqueValues,
  getFilteredRowModel,
  getPaginationRowModel,
  getSortedRowModel,
  useReactTable,
} from '@tanstack/react-table'
import { type SourceTableInfo, sourcesRepo } from '@/repo/sources'
import { Loader2 } from 'lucide-react'
import { toast } from 'sonner'
import { cn } from '@/lib/utils'
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from '@/components/ui/alert-dialog'
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
  CardDescription,
} from '@/components/ui/card'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import { DataTablePagination, DataTableToolbar } from '@/components/data-table'
import { SourceDetailsSchemaDrawer } from './source-details-schema-drawer'
import { getSourceDetailsTablesColumns } from './source-details-tables-columns'
import { KafkaTopicPreviewModal } from './kafka-topic-preview-modal'

interface SourceReplicationTableProps {
  sourceId: number
  tables: SourceTableInfo[]
  sourceType?: 'POSTGRES' | 'KAFKA'
}

export function SourceReplicationTable({
  sourceId,
  tables,
  sourceType = 'POSTGRES',
}: SourceReplicationTableProps) {
  const [rowSelection, setRowSelection] = useState({})
  const [sorting, setSorting] = useState<SortingState>([])
  const [columnVisibility, setColumnVisibility] = useState<VisibilityState>({})
  const [globalFilter, setGlobalFilter] = useState('')

  // New state for drop confirmation
  const [tableToDrop, setTableToDrop] = useState<string | null>(null)
  const [isProcessingDrop, setIsProcessingDrop] = useState(false)

  // State for schema drawer
  const [schemaDrawerOpen, setSchemaDrawerOpen] = useState(false)
  const [selectedTableId, setSelectedTableId] = useState<number | null>(null)
  const [selectedVersion, setSelectedVersion] = useState<number>(1)
  const [previewTopicName, setPreviewTopicName] = useState<string | null>(null)

  const queryClient = useQueryClient()

  const handleUnregisterTable = async (tableName: string) => {
    setTableToDrop(tableName)
  }

  const handleViewSchema = (tableId: number, version: number) => {
    setSelectedTableId(tableId)
    setSelectedVersion(version)
    setSchemaDrawerOpen(true)
  }

  const handlePreviewTopic = (topicName: string) => {
    setPreviewTopicName(topicName)
  }

  const confirmDropTable = async (tableName: string) => {
    setIsProcessingDrop(true)
    try {
      await sourcesRepo.unregisterTable(sourceId, tableName)
      // Auto-refresh after drop
      await sourcesRepo.refreshSource(sourceId)
      toast.success(
        sourceType === 'KAFKA'
          ? `Topic ${tableName} unregistered successfully`
          : `Table ${tableName} dropped from publication successfully`
      )
      setTimeout(() => {
        queryClient.invalidateQueries({ queryKey: ['source-details', sourceId] })
        queryClient.invalidateQueries({
          queryKey: ['source-kafka-topics-summary', sourceId],
        })
        queryClient.invalidateQueries({
          queryKey: ['source-kafka-topic-preview', sourceId],
        })
      }, 3000)
      setTableToDrop(null)
    } catch (error) {
      toast.error(`Failed to drop table ${tableName}`)
      void error
    } finally {
      setIsProcessingDrop(false)
    }
  }

  // Fetch schema data when drawer is opened
  const selectedTable = tables.find((t) => t.id === selectedTableId)
  const { data: schemaData, isLoading: isLoadingSchema } = useQuery({
    queryKey: ['table-schema', selectedTableId, selectedVersion],
    queryFn: () =>
      selectedTableId
        ? sourcesRepo.getTableSchema(selectedTableId, selectedVersion)
        : Promise.resolve(null),
    enabled: schemaDrawerOpen && !!selectedTableId,
    initialData: () => {
      if (
        selectedTable &&
        selectedTableId &&
        selectedTable.id === selectedTableId &&
        selectedTable.version === selectedVersion &&
        selectedTable.schema_table
      ) {
        return { columns: selectedTable.schema_table, diff: undefined }
      }
      return undefined
    },
  })

  const columns = getSourceDetailsTablesColumns(
    handleUnregisterTable,
    handleViewSchema,
    {
      sourceType,
      onPreview: sourceType === 'KAFKA' ? handlePreviewTopic : undefined,
    }
  )

  const table = useReactTable({
    data: tables,
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
    globalFilterFn: (row, _columnId, filterValue) => {
      const name = String(row.getValue('table_name')).toLowerCase()
      const searchValue = String(filterValue).toLowerCase()
      return name.includes(searchValue)
    },
    getCoreRowModel: getCoreRowModel(),
    getFilteredRowModel: getFilteredRowModel(),
    getPaginationRowModel: getPaginationRowModel(),
    getSortedRowModel: getSortedRowModel(),
    getFacetedRowModel: getFacetedRowModel(),
    getFacetedUniqueValues: getFacetedUniqueValues(),
  })

  return (
    <>
      <Card className='bg-sidebar'>
        <CardHeader>
          <CardTitle>
            {sourceType === 'KAFKA' ? 'Registered Topics' : 'Monitored Tables'}
          </CardTitle>
          <CardDescription>
            {sourceType === 'KAFKA'
              ? 'View and manage Kafka topics currently registered for this source. Live stats refresh every 5 seconds.'
              : 'View and manage tables currently being replicated.'}
          </CardDescription>
        </CardHeader>
        <CardContent>
          <div className='flex flex-col gap-4'>
            <DataTableToolbar
              table={table}
              searchPlaceholder='Filter by table name...'
            />

            <div className='rounded-md border border-border/50'>
              <Table>
                <TableHeader>
                  {table.getHeaderGroups().map((headerGroup) => (
                    <TableRow key={headerGroup.id}>
                      {headerGroup.headers.map((header) => {
                        return (
                          <TableHead
                            key={header.id}
                            colSpan={header.colSpan}
                            className={cn(
                              header.column.columnDef.meta?.className,
                              header.column.columnDef.meta?.thClassName
                            )}
                          >
                            {header.isPlaceholder
                              ? null
                              : flexRender(
                                  header.column.columnDef.header,
                                  header.getContext()
                                )}
                          </TableHead>
                        )
                      })}
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
                          <TableCell
                            key={cell.id}
                            className={cn(
                              'py-2',
                              cell.column.columnDef.meta?.className,
                              cell.column.columnDef.meta?.tdClassName
                            )}
                          >
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
                      <TableCell
                        colSpan={columns.length}
                        className='h-24 text-center text-muted-foreground'
                      >
                        No tables found.
                      </TableCell>
                    </TableRow>
                  )}
                </TableBody>
              </Table>
            </div>

            <DataTablePagination table={table} />
          </div>
        </CardContent>
      </Card>
      <AlertDialog
        open={!!tableToDrop}
        onOpenChange={(open) => !open && setTableToDrop(null)}
      >
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Are you sure?</AlertDialogTitle>
            <AlertDialogDescription>
              {sourceType === 'KAFKA' ? (
                <>
                  This will unregister <strong>{tableToDrop}</strong> from the
                  source topic list.
                </>
              ) : (
                <>
                  This will drop the table <strong>{tableToDrop}</strong> from the
                  publication. This action cannot be undone immediately.
                </>
              )}
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel disabled={isProcessingDrop}>
              Cancel
            </AlertDialogCancel>
            <AlertDialogAction
              onClick={(e) => {
                e.preventDefault()
                if (tableToDrop) confirmDropTable(tableToDrop)
              }}
              className='bg-red-500 text-white hover:bg-destructive/90'
              disabled={isProcessingDrop}
            >
              {isProcessingDrop && (
                <Loader2 className='mr-2 h-4 w-4 animate-spin' />
              )}
              Drop
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      <SourceDetailsSchemaDrawer
        open={schemaDrawerOpen}
        onOpenChange={setSchemaDrawerOpen}
        tableName={selectedTable?.table_name || ''}
        schema={schemaData?.columns || []}
        diff={schemaData?.diff}
        isLoading={isLoadingSchema}
        version={selectedVersion}
      />
      {sourceType === 'KAFKA' && previewTopicName && (
        <KafkaTopicPreviewModal
          key={previewTopicName}
          sourceId={sourceId}
          topicName={previewTopicName}
          open
          onOpenChange={(open) => !open && setPreviewTopicName(null)}
        />
      )}
    </>
  )
}
