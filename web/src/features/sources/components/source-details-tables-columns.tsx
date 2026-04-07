import { type ColumnDef } from '@tanstack/react-table'
import { Eye } from 'lucide-react'

import { DataTableColumnHeader } from '@/components/data-table'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { type SourceTableInfo } from '@/repo/sources'

interface SourceDetailsTablesColumnOptions {
  sourceType?: 'POSTGRES' | 'KAFKA'
  onPreview?: (topicName: string) => void
}

export const getSourceDetailsTablesColumns = (
  onUnregister: ((tableName: string) => void) | undefined,
  onViewSchema?: (tableId: number, version: number) => void,
  options: SourceDetailsTablesColumnOptions = {}
): ColumnDef<SourceTableInfo>[] => {
  const { sourceType = 'POSTGRES', onPreview } = options
  const objectLabel = sourceType === 'KAFKA' ? 'Topic Name' : 'Table Name'

  const columns: ColumnDef<SourceTableInfo>[] = [
    {
      accessorKey: 'table_name',
      header: ({ column }) => (
        <DataTableColumnHeader
          column={column}
          title={objectLabel}
          className='justify-start'
        />
      ),
      cell: ({ row }) => (
        <div className='font-medium text-sm text-foreground'>
          {row.getValue('table_name')}
        </div>
      ),
      enableSorting: true,
      enableHiding: false,
      meta: {
        title: objectLabel,
        className: 'w-[26%] min-w-[180px]',
      },
    },
  ]

  if (sourceType === 'KAFKA') {
    columns.push(
      {
        accessorKey: 'first_offset',
        header: ({ column }) => (
          <DataTableColumnHeader
            column={column}
            title='First Offset'
            className='justify-center w-full'
          />
        ),
        cell: ({ row }) => (
          <div className='text-center font-mono text-xs'>
            {row.original.first_offset ?? '--'}
          </div>
        ),
        meta: {
          title: 'First Offset',
          className: 'w-[11%] min-w-[110px] text-center',
        },
      },
      {
        accessorKey: 'next_offset',
        header: ({ column }) => (
          <DataTableColumnHeader
            column={column}
            title='Next Offset'
            className='justify-center w-full'
          />
        ),
        cell: ({ row }) => (
          <div className='text-center font-mono text-xs'>
            {row.original.next_offset ?? '--'}
          </div>
        ),
        meta: {
          title: 'Next Offset',
          className: 'w-[11%] min-w-[110px] text-center',
        },
      },
      {
        accessorKey: 'message_count',
        header: ({ column }) => (
          <DataTableColumnHeader
            column={column}
            title='Message Count'
            className='justify-center w-full'
          />
        ),
        cell: ({ row }) => (
          <div className='text-center font-mono text-xs'>
            {row.original.message_count ?? 0}
          </div>
        ),
        meta: {
          title: 'Message Count',
          className: 'w-[12%] min-w-[120px] text-center',
        },
      }
    )
  }

  columns.push(
    {
      id: 'schema_version',
      accessorKey: 'version',
      header: ({ column }) => (
        <DataTableColumnHeader
          column={column}
          title='Schema Version'
          className='justify-center w-full'
        />
      ),
      cell: ({ row }) => {
        const table = row.original
        const versions = Array.from({ length: table.version }, (_, i) => i + 1)

        return (
          <div className='flex items-center justify-center'>
            <Select
              value={table.version.toString()}
              onValueChange={(value) =>
                onViewSchema?.(table.id, parseInt(value, 10))
              }
            >
              <SelectTrigger className='h-8 w-[160px] border-border/50 bg-muted/40 text-xs focus:ring-0 focus:ring-offset-0'>
                <SelectValue placeholder='Version' />
              </SelectTrigger>
              <SelectContent>
                {versions.map((version) => (
                  <SelectItem
                    key={version}
                    value={version.toString()}
                    className='text-xs'
                  >
                    <div className='flex items-center gap-2'>
                      <span>Version {version}</span>
                      {version === table.version && (
                        <Badge
                          variant='outline'
                          className='h-4 border-emerald-500/30 bg-emerald-500/15 px-1.5 text-[10px] font-medium text-emerald-500'
                        >
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
        className: 'w-[14%] min-w-[160px] text-center',
      },
    },
    {
      id: 'view_schema',
      header: ({ column }) => (
        <DataTableColumnHeader
          column={column}
          title='Schema Details'
          className='justify-center w-full'
        />
      ),
      cell: ({ row }) => {
        const table = row.original
        return (
          <div className='flex items-center justify-center'>
            <Button
              variant='ghost'
              size='sm'
              className='h-8 gap-1.5 px-3 text-xs text-muted-foreground hover:bg-muted/60 hover:text-foreground'
              onClick={() => onViewSchema?.(table.id, table.version)}
            >
              <Eye className='h-3.5 w-3.5' />
              <span>View Schema</span>
            </Button>
          </div>
        )
      },
      meta: {
        title: 'Schema Details',
        className: 'w-[14%] min-w-[140px] text-center',
      },
    }
  )

  if (sourceType === 'KAFKA' && onPreview) {
    columns.push({
      id: 'preview',
      header: ({ column }) => (
        <DataTableColumnHeader
          column={column}
          title='Preview'
          className='justify-center w-full'
        />
      ),
      cell: ({ row }) => (
        <div className='flex items-center justify-center'>
          <Button
            variant='outline'
            size='sm'
            className='h-7 text-xs'
            onClick={() => onPreview(row.original.table_name)}
          >
            Preview
          </Button>
        </div>
      ),
      meta: {
        title: 'Preview',
        className: 'w-[12%] min-w-[110px] text-center',
      },
    })
  }

  if (onUnregister) {
    columns.push({
      id: 'actions',
      header: ({ column }) => (
        <DataTableColumnHeader
          column={column}
          title='Action'
          className='justify-center w-full'
        />
      ),
      cell: ({ row }) => (
        <div className='flex items-center justify-center'>
          <Button
            variant='destructive'
            size='sm'
            className='h-7 w-16 text-xs font-medium shadow-none'
            onClick={() => onUnregister(row.original.table_name)}
          >
            Drop
          </Button>
        </div>
      ),
      meta: {
        title: 'Action',
        className: 'w-[10%] min-w-[100px] text-center',
      },
    })
  }

  return columns
}
