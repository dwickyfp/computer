import { useState, useMemo } from 'react'
import {
    flexRender,
    getCoreRowModel,
    getFacetedRowModel,
    getFacetedUniqueValues,
    getFilteredRowModel,
    getPaginationRowModel,
    getSortedRowModel,
    useReactTable,
    type SortingState,
    type VisibilityState,
    type ColumnDef,
} from '@tanstack/react-table'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { sourcesRepo } from '@/repo/sources'
import { Button } from '@/components/ui/button'
import { Checkbox } from '@/components/ui/checkbox'
import { Input } from '@/components/ui/input'
import {
    Table,
    TableBody,
    TableCell,
    TableHead,
    TableHeader,
    TableRow,
} from '@/components/ui/table'
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card'
import { Loader2, Save, Search, Download, RefreshCcw, Lock } from 'lucide-react'
import { cn } from '@/lib/utils'
import { toast } from 'sonner'
import {
    Dialog,
    DialogContent,
    DialogDescription,
    DialogFooter,
    DialogHeader,
    DialogTitle,
    DialogTrigger,
} from '@/components/ui/dialog'
import { Label } from '@/components/ui/label'
import { Badge } from '@/components/ui/badge'
import { DataTablePagination } from '@/components/data-table'
import { DataTableFacetedFilter } from '@/components/data-table/faceted-filter'

// Internal row type since we just have string[] from API
type TableRowData = {
    name: string
    isPublished: boolean
}

interface SourceDetailsListTableProps {
    sourceId: number
    isPublicationEnabled: boolean
    publishedTableNames: string[]
}

export function SourceDetailsListTable({ sourceId: propSourceId, isPublicationEnabled, publishedTableNames }: SourceDetailsListTableProps) {
    const id = propSourceId
    const queryClient = useQueryClient()
    const [rowSelection, setRowSelection] = useState({})
    const [sorting, setSorting] = useState<SortingState>([])
    const [columnVisibility, setColumnVisibility] = useState<VisibilityState>({})
    const [globalFilter, setGlobalFilter] = useState('')
    const [processingTable, setProcessingTable] = useState<string | null>(null)

    // Preset State
    const [presetName, setPresetName] = useState('')
    const [isSavePresetOpen, setIsSavePresetOpen] = useState(false)
    const [isLoadPresetOpen, setIsLoadPresetOpen] = useState(false)
    const [saveMode, setSaveMode] = useState<'new' | 'replace'>('new')
    const [presetToReplace, setPresetToReplace] = useState<string>('')

    const { data: tablesRaw, isLoading } = useQuery({
        queryKey: ['source-available-tables', id],
        queryFn: () => sourcesRepo.getAvailableTables(id),
        enabled: !!id,
    })

    const { data: presets } = useQuery({
        queryKey: ['source-presets', id],
        queryFn: () => sourcesRepo.getPresets(id),
    })

    const data = useMemo(() => {
        return tablesRaw ? tablesRaw.map(t => ({
            name: t,
            isPublished: publishedTableNames.includes(t)
        })) : []
    }, [tablesRaw, publishedTableNames])

    const savePresetMutation = useMutation({
        mutationFn: async () => {
            const selectedTableNames = Object.keys(rowSelection).map(index => data[parseInt(index)].name)
            if (saveMode === 'new') {
                if (!presetName) throw new Error("Preset name is required")
                return sourcesRepo.createPreset(id, { name: presetName, table_names: selectedTableNames })
            } else {
                if (!presetToReplace) throw new Error("Please select a preset to replace")
                const presetId = parseInt(presetToReplace)
                const existingPreset = presets?.find(p => p.id === presetId)
                if (!existingPreset) throw new Error("Target preset not found")

                return sourcesRepo.updatePreset(presetId, {
                    name: existingPreset.name, // Keep existing name or allow rename? "Replace" usually implies keeping context but updating content. Let's keep name for now/simplification.
                    table_names: selectedTableNames
                })
            }
        },
        onSuccess: () => {
            toast.success(saveMode === 'new' ? "Preset saved successfully" : "Preset updated successfully")
            setIsSavePresetOpen(false)
            setPresetName("")
            setPresetToReplace("")
            setSaveMode('new')
            queryClient.invalidateQueries({ queryKey: ['source-presets', id] })
        },
        onError: (err) => {
            toast.error(err instanceof Error ? err.message : "Failed to save preset")
        }
    })

    const registerTableMutation = useMutation({
        mutationFn: async (tableName: string) => {
            setProcessingTable(tableName)
            await sourcesRepo.registerTable(id, tableName)
        },
        onSuccess: (_, tableName) => {
            toast.success(`Table ${tableName} added to publication`)
            queryClient.invalidateQueries({ queryKey: ['source-details', id] })
            setProcessingTable(null)
        },
        onError: (err, tableName) => {
            toast.error(`Failed to add table ${tableName}`)
            console.error(err)
            setProcessingTable(null)
        }
    })

    const unregisterTableMutation = useMutation({
        mutationFn: async (tableName: string) => {
            setProcessingTable(tableName)
            await sourcesRepo.unregisterTable(id, tableName)
        },
        onSuccess: (_, tableName) => {
            toast.success(`Table ${tableName} removed from publication`)
            queryClient.invalidateQueries({ queryKey: ['source-details', id] })
            setProcessingTable(null)
        },
        onError: (err, tableName) => {
            toast.error(`Failed to remove table ${tableName}`)
            console.error(err)
            setProcessingTable(null)
        }
    })

    // Add to publication logic if needed?
    // User asked to remove "Create Publication" button.
    // I will keep logic for "Save Preset" which relies on selection.
    // Assuming "Add to Publication" (if intended) would be a separate action, but user explicitly asked to change button.
    // Keeping "Save Preset".

    const columns: ColumnDef<TableRowData>[] = [
        {
            id: 'select',
            header: ({ table }) => (
                <Checkbox
                    checked={table.getIsAllPageRowsSelected() || (table.getIsSomePageRowsSelected() && "indeterminate")}
                    onCheckedChange={(value) => table.toggleAllPageRowsSelected(!!value)}
                    aria-label="Select all"
                />
            ),
            cell: ({ row }) => (
                <Checkbox
                    checked={row.getIsSelected()}
                    onCheckedChange={(value) => row.toggleSelected(!!value)}
                    aria-label="Select row"
                />
            ),
            enableSorting: false,
            enableHiding: false,
        },
        {
            accessorKey: 'name',
            header: 'Table Name',
            cell: ({ row }) => <div className="font-medium">{row.getValue('name')}</div>,
        },
        {
            id: 'status',
            accessorFn: row => row.isPublished ? 'published' : 'available',
            header: 'Status',
            cell: ({ row }) => {
                const isPublished = row.original.isPublished;
                return isPublished ? (
                    <Badge variant="secondary" className="bg-green-100 text-green-800 hover:bg-green-100 dark:bg-green-900/30 dark:text-green-400">Stream</Badge>
                ) : (
                    <span className="text-muted-foreground text-xs">Available</span>
                )
            },
            filterFn: (row, id, value) => {
                return value.includes(row.getValue(id))
            },
        },
        {
            id: 'actions',
            header: 'Actions',
            cell: ({ row }) => {
                const isPublished = row.original.isPublished;
                const tableName = row.original.name;
                const isProcessing = processingTable === tableName;

                return isPublished ? (
                    <Button
                        variant="destructive"
                        size="sm"
                        className="h-7 text-xs"
                        onClick={() => unregisterTableMutation.mutate(tableName)}
                        disabled={isProcessing}
                    >
                        {isProcessing ? <Loader2 className="h-3 w-3 animate-spin" /> : "Drop"}
                    </Button>
                ) : (
                    <Button
                        variant="default"
                        size="sm"
                        className="h-7 text-xs"
                        onClick={() => registerTableMutation.mutate(tableName)}
                        disabled={isProcessing || !isPublicationEnabled}
                    >
                        {isProcessing ? (
                            <Loader2 className="h-3 w-3 animate-spin" />
                        ) : !isPublicationEnabled ? (
                            <>
                                <Lock className="mr-1 h-3 w-3" />
                                Add
                            </>
                        ) : (
                            "Add"
                        )}
                    </Button>
                )
            }
        }
    ]

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
        getRowId: (_, index) => index.toString(), // Use index as ID for simplicity with string array
    })

    const handleLoadPreset = (presetTableNames: string[]) => {
        // Clear current selection? Or Merge? "Load" usually implies "Apply this state".
        // The user requirement: "list of table will load and auto check in list, but i want to add mechanism, if list table in presets not exists in list, then skip."

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
        toast.success(`Loaded preset. Selected ${matchCount} tables.`)
    }

    const refreshTablesMutation = useMutation({
        mutationFn: async () => {
            return sourcesRepo.getAvailableTables(id, true)
        },
        onSuccess: (data) => {
            queryClient.setQueryData(['source-available-tables', id], data)
            toast.success("Table list refreshed successfully")
        },
        onError: (err) => {
            toast.error("Failed to refresh table list")
            console.error(err)
        }
    })

    const isFiltered = table.getState().columnFilters.length > 0
    const selectedCount = Object.keys(rowSelection).length

    return (
        <Card>
            <CardHeader className="flex flex-row items-center justify-between">
                <div>
                    <CardTitle>Available Tables</CardTitle>
                    <CardDescription>Select tables to save as a preset.</CardDescription>
                </div>
                <div className="flex gap-2">
                    <Button
                        variant="outline"
                        size="icon"
                        onClick={() => refreshTablesMutation.mutate()}
                        disabled={refreshTablesMutation.isPending}
                        title="Refresh table list from source"
                    >
                        <RefreshCcw className={cn("h-4 w-4", refreshTablesMutation.isPending && "animate-spin")} />
                    </Button>
                    <Dialog open={isLoadPresetOpen} onOpenChange={setIsLoadPresetOpen}>
                        <DialogTrigger asChild>
                            <Button variant="outline">
                                <Download className="mr-2 h-4 w-4" />
                                Load Preset
                            </Button>
                        </DialogTrigger>
                        <DialogContent>
                            <DialogHeader>
                                <DialogTitle>Load Preset</DialogTitle>
                                <DialogDescription>Select a preset to apply tables to the selection.</DialogDescription>
                            </DialogHeader>
                            <div className="grid gap-2 py-4 max-h-[60vh] overflow-y-auto">
                                {presets?.map(preset => (
                                    <Button
                                        key={preset.id}
                                        variant="ghost"
                                        className="justify-start h-auto py-3 flex-col items-start"
                                        onClick={() => handleLoadPreset(preset.table_names)}
                                    >
                                        <div className="font-semibold">{preset.name}</div>
                                        <div className="text-xs text-muted-foreground">{preset.table_names.length} tables</div>
                                    </Button>
                                ))}
                                {presets?.length === 0 && <div className="text-center text-muted-foreground">No presets found.</div>}
                            </div>
                        </DialogContent>
                    </Dialog>

                    <Dialog open={isSavePresetOpen} onOpenChange={setIsSavePresetOpen}>
                        <DialogTrigger asChild>
                            <Button variant="outline" disabled={selectedCount === 0}>
                                <Save className="mr-2 h-4 w-4" />
                                Save Preset ({selectedCount})
                            </Button>
                        </DialogTrigger>
                        <DialogContent>
                            <DialogHeader>
                                <DialogTitle>Save Preset</DialogTitle>
                                <DialogDescription>Save the {selectedCount} selected tables as a preset.</DialogDescription>
                            </DialogHeader>
                            <div className="grid gap-4 py-4">
                                <div className="flex flex-col space-y-4">
                                    <div className="flex items-center space-x-4">
                                        <div className="flex items-center space-x-2">
                                            <input
                                                type="radio"
                                                id="new"
                                                name="saveMode"
                                                checked={saveMode === 'new'}
                                                onChange={() => setSaveMode('new')}
                                                className="aspect-square h-4 w-4 rounded-full border border-primary text-primary ring-offset-background focus:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:cursor-not-allowed disabled:opacity-50"
                                            />
                                            <Label htmlFor="new">Create New</Label>
                                        </div>
                                        <div className="flex items-center space-x-2">
                                            <input
                                                type="radio"
                                                id="replace"
                                                name="saveMode"
                                                checked={saveMode === 'replace'}
                                                onChange={() => setSaveMode('replace')}
                                                className="aspect-square h-4 w-4 rounded-full border border-primary text-primary ring-offset-background focus:outline-none focus-visible:ring-2 focus-visible:ring-ring focus:visible:ring-offset-2 disabled:cursor-not-allowed disabled:opacity-50"
                                            />
                                            <Label htmlFor="replace">Replace Existing</Label>
                                        </div>
                                    </div>

                                    {saveMode === 'new' ? (
                                        <div className="grid grid-cols-4 items-center gap-4">
                                            <Label htmlFor="name" className="text-right">Name</Label>
                                            <Input id="name" value={presetName} onChange={(e) => setPresetName(e.target.value)} className="col-span-3" />
                                        </div>
                                    ) : (
                                        <div className="grid grid-cols-4 items-center gap-4">
                                            <Label htmlFor="preset" className="text-right">Preset</Label>
                                            <select
                                                id="preset"
                                                className="col-span-3 flex h-10 w-full items-center justify-between rounded-md border border-input bg-background px-3 py-2 text-sm ring-offset-background placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-ring focus:ring-offset-2 disabled:cursor-not-allowed disabled:opacity-50 [&>span]:line-clamp-1"
                                                value={presetToReplace}
                                                onChange={(e) => setPresetToReplace(e.target.value)}
                                            >
                                                <option value="" disabled>Select a preset...</option>
                                                {presets?.map(p => (
                                                    <option key={p.id} value={p.id}>{p.name}</option>
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
                                    {savePresetMutation.isPending && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
                                    Save
                                </Button>
                            </DialogFooter>
                        </DialogContent>
                    </Dialog>
                </div>
            </CardHeader>
            <CardContent>
                <div className='flex flex-1 flex-col gap-4'>
                    <div className="flex items-center space-x-2">
                        <Search className="h-4 w-4 text-muted-foreground" />
                        <Input
                            placeholder="Filter tables..."
                            value={globalFilter ?? ""}
                            onChange={(event) => setGlobalFilter(event.target.value)}
                            className="max-w-sm h-8"
                        />
                        {table.getColumn('status') && (
                            <DataTableFacetedFilter
                                column={table.getColumn('status')}
                                title="Status"
                                options={[
                                    { label: 'Stream', value: 'published' },
                                    { label: 'Available', value: 'available' },
                                ]}
                            />
                        )}
                        {isFiltered && (
                            <Button
                                variant="ghost"
                                onClick={() => table.resetColumnFilters()}
                                className="h-8 px-2 lg:px-3"
                            >
                                Reset
                            </Button>
                        )}
                    </div>

                    {isLoading ? (
                        <div className="flex justify-center p-8"><Loader2 className="h-8 w-8 animate-spin" /></div>
                    ) : (
                        <div className='rounded-md border border-border/50'>
                            <Table>
                                <TableHeader>
                                    {table.getHeaderGroups().map((headerGroup) => (
                                        <TableRow key={headerGroup.id}>
                                            {headerGroup.headers.map((header) => {
                                                return (
                                                    <TableHead key={header.id} colSpan={header.colSpan}>
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
                    )}
                    <DataTablePagination table={table} />
                </div>
            </CardContent>
        </Card>
    )
}
