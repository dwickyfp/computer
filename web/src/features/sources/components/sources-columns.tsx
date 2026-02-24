import { type ColumnDef } from '@tanstack/react-table'
import { DataTableColumnHeader } from '@/components/data-table'
import { type Source } from '../data/schema'
import { SourcesRowActions } from './sources-row-actions'
import { Logs, Database } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { cn } from '@/lib/utils'
import { formatDistanceToNow } from 'date-fns'
import { CopyButton } from '@/components/copy-button'

export const sourcesColumns: ColumnDef<Source>[] = [
    {
        id: 'detail',
        header: () => <div className="text-center font-semibold">Action</div>,
        cell: ({ row }) => (
            <div className='flex justify-center'>
                <Button
                    variant='outline'
                    size='sm'
                    className='h-8 w-8 p-0'
                    onClick={() => window.location.href = `/sources/${row.original.id}/details`}
                >
                    <Logs className='h-4 w-4' />
                    <span className='sr-only'>Detail</span>
                </Button>
            </div>
        ),
        enableSorting: false,
        enableHiding: false,
        meta: { title: 'Detail' },
    },

    {
        accessorKey: 'name',
        header: ({ column }) => (
            <DataTableColumnHeader column={column} title='Name' />
        ),
        cell: ({ row }) => (
            <div className='flex flex-col'>
                <span className='truncate font-medium'>{row.getValue('name')}</span>
                <span className='truncate text-xs text-muted-foreground'>Postgres</span>
            </div>
        ),
        meta: { title: 'Name' },
    },
    {
        id: 'status',
        header: ({ column }) => (
            <DataTableColumnHeader column={column} title='Status' />
        ),
        cell: ({ row }) => {
            const isReplicationEnabled = row.original.is_replication_enabled
            const isPublicationEnabled = row.original.is_publication_enabled
            const isActive = isReplicationEnabled && isPublicationEnabled

            return (
                <div className='flex items-center gap-2'>
                    <div className={cn("h-2 w-2 rounded-full", isActive ? "bg-green-500" : "bg-zinc-300 dark:bg-zinc-700")} />
                    <span className={cn("text-sm", isActive ? "text-foreground" : "text-muted-foreground")}>
                        {isActive ? "Active" : "Inactive"}
                    </span>
                </div>
            )
        },
        meta: { title: 'Status' },
    },
    {
        id: 'connection',
        header: ({ column }) => (
            <DataTableColumnHeader column={column} title='Connection Details' />
        ),
        cell: ({ row }) => {
            const host = row.original.pg_host
            const port = row.original.pg_port
            const database = row.original.pg_database
            const mainInfo = `${host}:${port}`

            return (
                <div className="flex flex-col gap-1 max-w-[300px]">
                    <div className="flex items-center gap-2 text-sm font-medium">
                        <Database className="h-3.5 w-3.5 text-blue-500" />
                        <span className="truncate">{mainInfo}</span>
                        <CopyButton value={mainInfo} className="opacity-0 group-hover:opacity-100 transition-opacity h-6 w-6" />
                    </div>
                    <span className="text-xs text-muted-foreground truncate" title={database}>{database}</span>
                </div>
            )
        },
        meta: { title: 'Connection' },
    },
    {
        id: 'type',
        header: ({ column }) => (
            <DataTableColumnHeader column={column} title='Type' className="w-[150px]" />
        ),
        cell: () => {
            return (
                <div className='flex items-center gap-2 w-[150px]'>
                    {/* Placeholder icon for Postgres since we don't have a specific one in lucide besides Database, or we could use a custom one if available, sticking to text/color for now or generic DB */}
                    <div className="flex items-center gap-2">
                        <span className='truncate font-medium capitalize'>
                            Postgre<span style={{ color: '#316192' }}>SQL</span>
                        </span>
                    </div>
                </div>
            )
        },
        meta: { title: 'Type' },
    },
    {
        accessorKey: 'total_tables',
        header: ({ column }) => (
            <DataTableColumnHeader column={column} title='Tables' />
        ),
        cell: ({ row }) => (
            <span className='font-medium'>{row.getValue('total_tables')} Tables</span>
        ),
        meta: { title: 'Total Tables' },
    },
    {
        accessorKey: 'created_at',
        header: ({ column }) => (
            <DataTableColumnHeader column={column} title='Created' />
        ),
        cell: ({ row }) => {
            return (
                <span className="text-muted-foreground text-sm">
                    {formatDistanceToNow(new Date(row.getValue('created_at')), { addSuffix: true })}
                </span>
            )
        },
        meta: { title: 'Created' },
    },
    {
        id: 'actions',
        cell: ({ row }) => <SourcesRowActions row={row} />,
        meta: { title: 'Actions' },
    },
]
