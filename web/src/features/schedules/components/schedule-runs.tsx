import { useState, useMemo } from 'react'
import {
    flexRender,
    getCoreRowModel,
    useReactTable,
    getSortedRowModel,
    type SortingState,
} from '@tanstack/react-table'
import {
    Loader2,
    AlertCircle,
    RotateCw,
} from 'lucide-react'
import { cn } from '@/lib/utils'
import {
    Table,
    TableBody,
    TableCell,
    TableHead,
    TableHeader,
    TableRow,
} from '@/components/ui/table'
import { getPaginationRowModel } from '@tanstack/react-table'
import { DataTablePagination } from '@/components/data-table'
import { Button } from '@/components/ui/button'
import { historyColumns } from '../data/history-columns'
import { type ScheduleRunHistory } from '@/repo/schedules'

interface ScheduleRunsProps {
    data: ScheduleRunHistory[]
    isLoading: boolean
    onRefresh: () => void
}

export function ScheduleRuns({ data, isLoading, onRefresh }: ScheduleRunsProps) {
    const [sorting, setSorting] = useState<SortingState>([])
    // Pagination state
    const [pagination, setPagination] = useState({
        pageIndex: 0,
        pageSize: 10,
    })

    // Prevent infinite loops by memoizing data and columns
    const columns = useMemo(() => historyColumns, [])
    const memoizedData = useMemo(() => data, [data])

    const table = useReactTable({
        data: memoizedData,
        columns,
        state: {
            sorting,
            pagination,
        },
        autoResetPageIndex: false, // Critical to prevent loops with unstable data
        onSortingChange: setSorting,
        onPaginationChange: setPagination,
        getCoreRowModel: getCoreRowModel(),
        getSortedRowModel: getSortedRowModel(),
        getPaginationRowModel: getPaginationRowModel(),
    })

    return (
        <div className="space-y-4">
            {/* Toolbar */}
            <div className="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-end">
                <Button variant="outline" size="sm" onClick={onRefresh} disabled={isLoading}>
                    <RotateCw className={cn("mr-2 h-4 w-4", isLoading && "animate-spin")} />
                    Refresh
                </Button>
            </div>

            {/* Content using DataTable structure */}
            <div className="rounded-md border bg-sidebar">
                <Table>
                    <TableHeader>
                        {table.getHeaderGroups().map((hg) => (
                            <TableRow key={hg.id} className="bg-muted/50 hover:bg-muted/50 transition-none border-b shadow-sm">
                                {hg.headers.map((h) => (
                                    <TableHead key={h.id} className={cn(h.column.columnDef.meta?.className, "text-xs uppercase tracking-wider text-muted-foreground/70 h-10")}>
                                        {h.isPlaceholder ? null : flexRender(h.column.columnDef.header, h.getContext())}
                                    </TableHead>
                                ))}
                            </TableRow>
                        ))}
                    </TableHeader>
                    <TableBody>
                        {isLoading && data.length === 0 ? (
                            <TableRow>
                                <TableCell colSpan={historyColumns.length} className="h-24 text-center">
                                    <div className="flex items-center justify-center">
                                        <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
                                    </div>
                                </TableCell>
                            </TableRow>
                        ) : table.getRowModel().rows.length ? (
                            table.getRowModel().rows.map((row) => (
                                <TableRow
                                    key={row.id}
                                    className="group border-b border-muted/40 hover:bg-muted/30 data-[state=selected]:bg-muted/50 transition-colors duration-200"
                                >
                                    {row.getVisibleCells().map((cell) => (
                                        <TableCell key={cell.id} className={cn(cell.column.columnDef.meta?.className, "py-2")}>
                                            {flexRender(cell.column.columnDef.cell, cell.getContext())}
                                        </TableCell>
                                    ))}
                                </TableRow>
                            ))
                        ) : (
                            <TableRow>
                                <TableCell colSpan={historyColumns.length} className="h-24 text-center">
                                    <div className="flex flex-col items-center justify-center text-muted-foreground gap-2">
                                        <AlertCircle className="h-8 w-8 opacity-20" />
                                        <p>No runs found matching your criteria</p>
                                    </div>
                                </TableCell>
                            </TableRow>
                        )}
                    </TableBody>
                </Table>
            </div>

            {/* Pagination */}
            <DataTablePagination table={table} />
        </div>
    )
}
