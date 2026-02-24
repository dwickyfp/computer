import { useState, useMemo } from 'react'
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
import { cn } from '@/lib/utils'
import {
    Table,
    TableBody,
    TableCell,
    TableHead,
    TableHeader,
    TableRow,
} from '@/components/ui/table'
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card'
import { DataTablePagination, DataTableToolbar } from '@/components/data-table'
import { type SourceTableInfo, sourcesRepo } from '@/repo/sources'
import { useQueryClient } from '@tanstack/react-query'
import { getSourceDetailsTablesColumns } from './source-details-tables-columns'
import { toast } from 'sonner'
import { Loader2 } from 'lucide-react'
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
import { SourceDetailsSchemaDrawer } from './source-details-schema-drawer'
import { useQuery } from '@tanstack/react-query'

interface SourceReplicationTableProps {
    sourceId: number
    tables: SourceTableInfo[]
}

export function SourceReplicationTable({ sourceId, tables }: SourceReplicationTableProps) {
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

    const queryClient = useQueryClient()

    const handleUnregisterTable = async (tableName: string) => {
        setTableToDrop(tableName)
    }

    const handleViewSchema = (tableId: number, version: number) => {
        setSelectedTableId(tableId)
        setSelectedVersion(version)
        setSchemaDrawerOpen(true)
    }

    const confirmDropTable = async (tableName: string) => {
        setIsProcessingDrop(true)
        try {
            await sourcesRepo.unregisterTable(sourceId, tableName)
            // Auto-refresh after drop
            await sourcesRepo.refreshSource(sourceId)
            toast.success(`Table ${tableName} dropped from publication successfully`)
            queryClient.invalidateQueries({ queryKey: ['source-details', sourceId] })
            setTableToDrop(null)
        } catch (error) {
            toast.error(`Failed to drop table ${tableName}`)
            console.error(error)
        } finally {
            setIsProcessingDrop(false)
        }
    }

    // Fetch schema data when drawer is opened
    const selectedTable = tables.find(t => t.id === selectedTableId)
    const { data: schemaData, isLoading: isLoadingSchema } = useQuery({
        queryKey: ['table-schema', selectedTableId, selectedVersion],
        queryFn: () => selectedTableId ? sourcesRepo.getTableSchema(selectedTableId, selectedVersion) : Promise.resolve(null),
        enabled: schemaDrawerOpen && !!selectedTableId,
        initialData: () => {
            if (selectedTable && selectedTableId && selectedTable.id === selectedTableId && selectedTable.version === selectedVersion && selectedTable.schema_table) {
                return { columns: selectedTable.schema_table, diff: undefined }
            }
            return undefined
        }
    })

    const columns = useMemo(() => getSourceDetailsTablesColumns(
        handleUnregisterTable,
        handleViewSchema
    ), [handleUnregisterTable])

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
            <Card>
                <CardHeader>
                    <CardTitle>Monitored Tables</CardTitle>
                    <CardDescription>View and manage tables currently being replicated.</CardDescription>
                </CardHeader>
                <CardContent>
                    <div className="flex flex-col gap-4">
                        <DataTableToolbar
                            table={table}
                            searchPlaceholder='Filter by table name...'
                        />

                        <div className="rounded-md border border-border/50">
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
                                                            "py-2",
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
            <AlertDialog open={!!tableToDrop} onOpenChange={(open) => !open && setTableToDrop(null)}>
                <AlertDialogContent>
                    <AlertDialogHeader>
                        <AlertDialogTitle>Are you sure?</AlertDialogTitle>
                        <AlertDialogDescription>
                            This will drop the table <strong>{tableToDrop}</strong> from the publication. This action cannot be undone immediately.
                        </AlertDialogDescription>
                    </AlertDialogHeader>
                    <AlertDialogFooter>
                        <AlertDialogCancel disabled={isProcessingDrop}>Cancel</AlertDialogCancel>
                        <AlertDialogAction
                            onClick={(e) => {
                                e.preventDefault()
                                if (tableToDrop) confirmDropTable(tableToDrop)
                            }}
                            className="text-white  hover:bg-destructive/90 bg-red-500"
                            disabled={isProcessingDrop}
                        >
                            {isProcessingDrop && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
                            Drop
                        </AlertDialogAction>
                    </AlertDialogFooter>
                </AlertDialogContent>
            </AlertDialog >

            <SourceDetailsSchemaDrawer
                open={schemaDrawerOpen}
                onOpenChange={setSchemaDrawerOpen}
                tableName={selectedTable?.table_name || ''}
                schema={schemaData?.columns || []}
                diff={schemaData?.diff}
                isLoading={isLoadingSchema}
                version={selectedVersion}
            />
        </>
    )
}
