import { formatDistanceToNow } from 'date-fns'
import { type ColumnDef } from '@tanstack/react-table'
import { Snowflake, Database, Eye, Table2 } from 'lucide-react'
import { cn } from '@/lib/utils'
import { Button } from '@/components/ui/button'
import { CopyButton } from '@/components/copy-button'
import { DataTableColumnHeader } from '@/components/data-table'
import { type Destination } from '../data/schema'
import { useDestinations } from './destinations-provider'
import { DestinationsRowActions } from './destinations-row-actions'

function getDestinationConnectionDetails(destination: Destination) {
  const type = destination.type
  const config = destination.config || {}

  if (type === 'KAFKA') {
    return {
      mainInfo: config.bootstrap_servers || 'Not configured',
      subInfo: config.topic_prefix || 'No topic prefix',
      icon: <Database className='h-3.5 w-3.5 text-blue-500' />,
    }
  }

  if (type === 'SNOWFLAKE') {
    return {
      mainInfo: config.account || 'Not configured',
      subInfo:
        [config.database, config.schema].filter(Boolean).join(' / ') ||
        'No database / schema',
      icon: <Snowflake className='h-3.5 w-3.5 text-[#29b5e8]' />,
    }
  }

  return {
    mainInfo:
      [config.host, config.port].filter(Boolean).join(':') || 'Not configured',
    subInfo: config.database || 'No database',
    icon: <Database className='h-3.5 w-3.5 text-blue-500' />,
  }
}

function TotalTablesCell({ row }: { row: { original: Destination } }) {
  const { setOpen, setCurrentRow } = useDestinations()
  const total = row.original.total_tables ?? 0
  const objectLabel = row.original.type === 'KAFKA' ? 'topic' : 'table'

  return (
    <div className='flex items-center gap-2'>
      <div className='flex items-center gap-1.5 text-sm'>
        <Table2 className='h-3.5 w-3.5 text-muted-foreground' />
        <span
          className={cn(
            total === 0
              ? 'text-muted-foreground'
              : 'font-medium text-foreground'
          )}
        >
          {total}
        </span>
      </div>
      <Button
        variant='ghost'
        size='icon'
        className='h-6 w-6 opacity-60 hover:opacity-100'
        title={`View ${objectLabel} list`}
        onClick={(e) => {
          e.stopPropagation()
          setCurrentRow(row.original)
          setOpen('table-list')
        }}
      >
        <Eye className='h-3.5 w-3.5' />
      </Button>
    </div>
  )
}

export const destinationsColumns: ColumnDef<Destination>[] = [
  {
    accessorKey: 'name',
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title='Name' />
    ),
    cell: ({ row }) => (
      <div className='flex flex-col'>
        <span className='truncate font-medium'>{row.getValue('name')}</span>
        <span className='truncate text-xs text-muted-foreground'>
          {row.original.type}
        </span>
      </div>
    ),
    meta: { title: 'Name' },
  },
  {
    accessorKey: 'status',
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title='Status' />
    ),
    cell: ({ row }) => {
      const isActive = row.original.is_used_in_active_pipeline
      return (
        <div className='flex items-center gap-2'>
          <div
            className={cn(
              'h-2 w-2 rounded-full',
              isActive ? 'bg-green-500' : 'bg-zinc-300 dark:bg-zinc-700'
            )}
          />
          <span
            className={cn(
              'text-sm',
              isActive ? 'text-foreground' : 'text-muted-foreground'
            )}
          >
            {isActive ? 'Active' : 'Idle'}
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
      const { mainInfo, subInfo, icon } = getDestinationConnectionDetails(
        row.original
      )

      return (
        <div className='flex max-w-[300px] flex-col gap-1'>
          <div className='flex items-center gap-2 text-sm font-medium'>
            {icon}
            <span className='truncate'>{mainInfo}</span>
            <CopyButton
              value={mainInfo}
              className='h-6 w-6 opacity-0 transition-opacity group-hover:opacity-100'
            />
          </div>
          <span
            className='truncate text-xs text-muted-foreground'
            title={subInfo}
          >
            {subInfo}
          </span>
        </div>
      )
    },
    meta: { title: 'Connection' },
  },
  {
    id: 'total_tables',
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title='Tables' />
    ),
    cell: ({ row }) => <TotalTablesCell row={row} />,
    meta: { title: 'Tables' },
  },
  {
    accessorKey: 'created_at',
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title='Created' />
    ),
    cell: ({ row }) => {
      return (
        <span className='text-sm text-muted-foreground'>
          {formatDistanceToNow(new Date(row.getValue('created_at')), {
            addSuffix: true,
          })}
        </span>
      )
    },
    meta: { title: 'Created' },
  },
  {
    id: 'actions',
    cell: ({ row }) => (
      <div className='flex w-[50px] justify-end'>
        <DestinationsRowActions row={row} />
      </div>
    ),
    meta: { title: 'Actions' },
  },
]
