import { type ColumnDef } from '@tanstack/react-table'
import { DataTableColumnHeader } from '@/components/data-table'
import { type SourceTableInfo } from '@/repo/sources'
import { Button } from '@/components/ui/button'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Badge } from '@/components/ui/badge'
import { Eye } from 'lucide-react'


export const getSourceDetailsTablesColumns = (
    onUnregister: ((tableName: string) => void) | undefined,
    onViewSchema?: (tableId: number, version: number) => void
): ColumnDef<SourceTableInfo>[] => {

    const columns: ColumnDef<SourceTableInfo>[] = [

        {
            accessorKey: 'table_name',
            header: ({ column }) => (
                <DataTableColumnHeader column={column} title='Table Name' className="justify-start" />
            ),
            cell: ({ row }) => (
                <div className="font-medium text-sm text-foreground">
                    {row.getValue('table_name')}
                </div>
            ),
            enableSorting: true,
            enableHiding: false,
            meta: {
                title: 'Table Name',
                className: 'w-[35%] min-w-[180px]',
            },
        },

        {
            id: 'schema_version',
            accessorKey: 'version',
            header: ({ column }) => (
                <DataTableColumnHeader column={column} title='Schema Version' className="justify-center w-full" />
            ),
            cell: ({ row }) => {
                const table = row.original
                // Generate list of all versions from 1 to current version
                const versions = Array.from({ length: table.version }, (_, i) => i + 1)

                return (
                    <div className="flex items-center justify-center">
                        <Select
                            value={table.version.toString()}
                            onValueChange={(value) => onViewSchema?.(table.id, parseInt(value))}
                        >
                            <SelectTrigger className="w-[160px] h-8 text-xs bg-muted/40 border-border/50 focus:ring-0 focus:ring-offset-0">
                                <SelectValue placeholder="Version" />
                            </SelectTrigger>
                            <SelectContent>
                                {versions.map((v) => (
                                    <SelectItem key={v} value={v.toString()} className="text-xs">
                                        <div className="flex items-center gap-2">
                                            <span>Version {v}</span>
                                            {v === table.version && (
                                                <Badge variant="outline" className="h-4 px-1.5 text-[10px] font-medium bg-emerald-500/15 text-emerald-500 border-emerald-500/30">
                                                    Active
                                                </Badge>
                                            )}
                                        </div>
                                    </SelectItem>
                                ))}
                            </SelectContent>
                        </Select>
                    </div>
                )
            },
            enableSorting: true,
            meta: {
                title: 'Schema Version',
                className: 'w-[22%] min-w-[160px] text-center',
            },
        },
        {
            id: 'view_schema',
            header: ({ column }) => (
                <DataTableColumnHeader column={column} title='Schema Details' className="justify-center w-full" />
            ),
            cell: ({ row }) => {
                const table = row.original
                return (
                    <div className="flex items-center justify-center">
                        <Button
                            variant="ghost"
                            size="sm"
                            className="h-8 px-3 text-xs hover:bg-muted/60 text-muted-foreground hover:text-foreground gap-1.5"
                            onClick={() => onViewSchema?.(table.id, table.version)}
                        >
                            <Eye className="h-3.5 w-3.5" />
                            <span>View Schema</span>
                        </Button>
                    </div>
                )
            },
            meta: {
                title: 'Schema Details',
                className: 'w-[22%] min-w-[140px] text-center',
            },
        }
    ]

    if (onUnregister) {
        columns.push({
            id: 'actions',
            header: ({ column }) => (
                <DataTableColumnHeader column={column} title='Action' className='justify-center w-full' />
            ),
            cell: ({ row }) => (
                <div className='flex items-center justify-center'>
                    <Button
                        variant='destructive'
                        size='sm'
                        className="h-7 w-16 text-xs font-medium shadow-none"
                        onClick={() => onUnregister(row.original.table_name)}
                    >
                        Drop
                    </Button>
                </div>
            ),
            meta: {
                title: 'Action',
                className: 'w-[21%] min-w-[100px] text-center',
            },
        })
    }

    return columns
}

