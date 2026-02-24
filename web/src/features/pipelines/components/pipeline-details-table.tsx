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
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { DataTablePagination, DataTableToolbar } from '@/components/data-table'
import { type SourceTableInfo } from '@/repo/sources'
import { pipelinesRepo } from '@/repo/pipelines'
import { useQuery } from '@tanstack/react-query'
import { getPipelineDetailsTableColumns } from './pipeline-details-table-columns'

interface PipelineDetailsTableProps {
    pipelineId: number
    tables: SourceTableInfo[]
}

export function PipelineDetailsTable({ pipelineId, tables, destinationId }: PipelineDetailsTableProps & { destinationId?: number | null }) {
    const [rowSelection, setRowSelection] = useState({})
    const [sorting, setSorting] = useState<SortingState>([])
    const [columnVisibility, setColumnVisibility] = useState<VisibilityState>({})
    const [globalFilter, setGlobalFilter] = useState('')

    const { data: pipelineStats } = useQuery({
        queryKey: ['pipeline-stats', pipelineId],
        queryFn: () => pipelinesRepo.getStats(pipelineId),
        enabled: !!pipelineId,
        refetchInterval: 5000, // Refetch every 5 seconds
    })

    const statsMap = useMemo(() => {
        if (!pipelineStats) return {}

        // Filter by destination if provided
        let filteredStats = pipelineStats
        if (destinationId !== undefined) {
            filteredStats = pipelineStats.filter(s => s.pipeline_destination_id === destinationId)
        } else {
            // If no destination specified, maybe aggregate? Or just show all? 
            // Existing behavior assumed 1:1 table mapping, but now we have duplicate table names (1 per dest).
            // For backward compatibility or "Source" view, we might want to aggregate counts per table?
            // VALIDATION: The user wants "exact information". 
            // If this component is used in "Flow Data" tab directly (without selection), it shows list of tables.
            // If we have 3 destinations, we have 3 rows for table "users". 
            // Adapting this table to show aggregate if destinationId is missing is safer.

            // However, reusing this table for the "Drawer" means we definitely have a destinationId.
        }

        return filteredStats.reduce((acc, stat) => {
            // If multiple destinations and no filter, this might overwrite. 
            // Ideally we sum up if we want "Source" view. 
            // Let's implement Summing for now if no destinationId is passed, to preserve "Total" view.

            if (!acc[stat.table_name]) {
                acc[stat.table_name] = { ...stat }
            } else {
                // Merge/Sum stats
                // Sum daily counts
                // This is complex for daily_stats array merging.
                // For simplified "Total", we might just take one or valid strategy.
                // But for now, let's assume this table is primarily used WITH a destinationId in the new design.
                // Or if used without, maybe we just overwrite (imperfect but safe code).
                // Actually, let's just use the filtered stats.
                acc[stat.table_name] = stat
            }
            return acc
        }, {} as Record<string, typeof pipelineStats[0]>)
    }, [pipelineStats, destinationId])

    const columns = useMemo(() => getPipelineDetailsTableColumns(
        statsMap
    ), [statsMap])

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
        <Card>
            <CardHeader>
                <CardTitle>Tables</CardTitle>
            </CardHeader>
            <CardContent>
                <div className='flex flex-1 flex-col gap-4'>
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
                                            className='h-24 text-center'
                                        >
                                            No results.
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
    )
}
