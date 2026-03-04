import { formatDistanceToNow } from 'date-fns'
import { type ColumnDef } from '@tanstack/react-table'
import { Badge } from '@/components/ui/badge'
import { DataTableColumnHeader } from '@/components/data-table'
import { type ChainClient } from '../data/schema'
import { ChainClientRowActions } from './chain-client-row-actions'

export const chainClientColumns: ColumnDef<ChainClient>[] = [
  {
    accessorKey: 'name',
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title='Name' />
    ),
    cell: ({ row }) => (
      <div className='flex flex-col'>
        <span className='truncate font-medium'>{row.getValue('name')}</span>
        {row.original.description && (
          <span className='truncate text-xs text-muted-foreground'>
            {row.original.description}
          </span>
        )}
      </div>
    ),
    meta: { title: 'Name' },
  },
  {
    accessorKey: 'url',
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title='URL' />
    ),
    cell: ({ row }) => (
      <span className='truncate font-mono text-sm text-muted-foreground'>
        {row.getValue('url')}
      </span>
    ),
    meta: { title: 'URL' },
  },
  {
    id: 'source_chain_id',
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title='Chain ID' />
    ),
    cell: ({ row }) => {
      const scid = row.original.source_chain_id
      if (scid) {
        return (
          <Badge variant='outline' className='font-mono text-xs'>
            {scid}
          </Badge>
        )
      }
      return (
        <span className='text-xs text-muted-foreground italic'>
          auto-detect
        </span>
      )
    },
    meta: { title: 'Chain ID' },
  },
  {
    id: 'status',
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title='Status' />
    ),
    cell: ({ row }) => {
      const isActive = row.original.is_active
      return (
        <Badge variant={isActive ? 'default' : 'secondary'}>
          {isActive ? 'Active' : 'Inactive'}
        </Badge>
      )
    },
    meta: { title: 'Status' },
  },
  {
    id: 'databases',
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title='Databases' />
    ),
    cell: ({ row }) => (
      <span className='text-sm'>{row.original.databases?.length ?? 0}</span>
    ),
    meta: { title: 'Databases' },
  },
  {
    id: 'last_connected',
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title='Last Connected' />
    ),
    cell: ({ row }) => {
      const lastConnected = row.original.last_connected_at
      if (!lastConnected)
        return <span className='text-sm text-muted-foreground'>Never</span>
      return (
        <span className='text-sm'>
          {formatDistanceToNow(new Date(lastConnected), { addSuffix: true })}
        </span>
      )
    },
    meta: { title: 'Last Connected' },
  },
  {
    id: 'created_at',
    accessorKey: 'created_at',
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title='Created' />
    ),
    cell: ({ row }) => (
      <span className='text-sm'>
        {formatDistanceToNow(new Date(row.original.created_at), {
          addSuffix: true,
        })}
      </span>
    ),
    meta: { title: 'Created' },
  },
  {
    id: 'actions',
    cell: ({ row }) => <ChainClientRowActions row={row} />,
  },
]
