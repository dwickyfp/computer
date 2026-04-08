import { useEffect, useMemo, useState } from 'react'
import {
  flexRender,
  getCoreRowModel,
  useReactTable,
  type ColumnDef,
  type RowSelectionState,
} from '@tanstack/react-table'
import { formatDistanceToNow } from 'date-fns'
import { Trash2 } from 'lucide-react'
import { LongText } from '@/components/long-text'
import { ConfirmDialog } from '@/components/confirm-dialog'
import { DataTableBulkActions } from '@/components/data-table'
import { Button } from '@/components/ui/button'
import { Checkbox } from '@/components/ui/checkbox'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import { type DLQMessage } from '@/repo/dlq-manager'

type Props = {
  hasNextPage: boolean
  isDiscarding: boolean
  isFetchingNextPage: boolean
  isLoading: boolean
  messages: DLQMessage[]
  onDiscardSelected: (messageIds: string[]) => void
  onLoadOlder: () => void
  onPreview: (message: DLQMessage) => void
  totalCount: number
}

function compactKeySummary(key: Record<string, unknown> | null) {
  if (!key || Object.keys(key).length === 0) {
    return 'No key payload'
  }
  return JSON.stringify(key)
}

function formatRelativeTime(value: string | null | undefined) {
  if (!value) return 'Unknown'
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) {
    return value
  }
  return formatDistanceToNow(date, { addSuffix: true })
}

export function DLQMessagesTable({
  hasNextPage,
  isDiscarding,
  isFetchingNextPage,
  isLoading,
  messages,
  onDiscardSelected,
  onLoadOlder,
  onPreview,
  totalCount,
}: Props) {
  const [rowSelection, setRowSelection] = useState<RowSelectionState>({})
  const [confirmOpen, setConfirmOpen] = useState(false)

  useEffect(() => {
    setRowSelection({})
  }, [messages, totalCount])

  const columns = useMemo<ColumnDef<DLQMessage>[]>(
    () => [
      {
        id: 'select',
        header: ({ table }) => (
          <Checkbox
            aria-label='Select all rows'
            checked={
              table.getIsAllPageRowsSelected()
                ? true
                : table.getIsSomePageRowsSelected()
                  ? 'indeterminate'
                  : false
            }
            onCheckedChange={(value) => table.toggleAllPageRowsSelected(Boolean(value))}
          />
        ),
        cell: ({ row }) => (
          <Checkbox
            aria-label={`Select message ${row.original.message_id}`}
            checked={row.getIsSelected()}
            onClick={(event) => event.stopPropagation()}
            onCheckedChange={(value) => row.toggleSelected(Boolean(value))}
          />
        ),
        enableSorting: false,
      },
      {
        accessorKey: 'message_id',
        header: 'Message ID',
        cell: ({ row }) => (
          <LongText className='max-w-[180px] font-mono text-xs'>
            {row.original.message_id}
          </LongText>
        ),
      },
      {
        accessorKey: 'first_failed_at',
        header: 'Failed',
        cell: ({ row }) => (
          <span className='text-sm text-muted-foreground'>
            {formatRelativeTime(row.original.first_failed_at)}
          </span>
        ),
      },
      {
        accessorKey: 'operation',
        header: 'Operation',
        cell: ({ row }) => (
          <span className='font-mono text-xs uppercase'>
            {row.original.operation ?? 'unknown'}
          </span>
        ),
      },
      {
        accessorKey: 'retry_count',
        header: 'Retries',
        cell: ({ row }) => <span>{row.original.retry_count}</span>,
      },
      {
        id: 'key_summary',
        header: 'Key',
        cell: ({ row }) => (
          <LongText className='max-w-[320px] text-sm text-muted-foreground'>
            {compactKeySummary(row.original.key)}
          </LongText>
        ),
      },
    ],
    []
  )

  // eslint-disable-next-line react-hooks/incompatible-library
  const table = useReactTable({
    data: messages,
    columns,
    state: { rowSelection },
    enableRowSelection: true,
    getCoreRowModel: getCoreRowModel(),
    onRowSelectionChange: setRowSelection,
  })

  const selectedIds = table
    .getFilteredSelectedRowModel()
    .rows.map((row) => row.original.message_id)

  return (
    <div className='space-y-4'>
      <div className='flex items-center justify-between gap-3'>
        <div>
          <h3 className='text-lg font-semibold'>Messages</h3>
          <p className='text-sm text-muted-foreground'>
            Previewing {messages.length} of {totalCount} rows without consuming the
            DLQ stream.
          </p>
        </div>
      </div>

      <DataTableBulkActions entityName='row' table={table}>
        <Button
          size='sm'
          variant='destructive'
          onClick={() => setConfirmOpen(true)}
        >
          <Trash2 className='mr-2 h-4 w-4' />
          Cancel DLQ
        </Button>
      </DataTableBulkActions>

      <div className='rounded-md border border-border/50'>
        <Table>
          <TableHeader>
            {table.getHeaderGroups().map((headerGroup) => (
              <TableRow key={headerGroup.id}>
                {headerGroup.headers.map((header) => (
                  <TableHead key={header.id}>
                    {header.isPlaceholder
                      ? null
                      : flexRender(
                          header.column.columnDef.header,
                          header.getContext()
                        )}
                  </TableHead>
                ))}
              </TableRow>
            ))}
          </TableHeader>
          <TableBody>
            {isLoading ? (
              <TableRow>
                <TableCell colSpan={columns.length} className='h-24 text-center'>
                  Loading DLQ messages...
                </TableCell>
              </TableRow>
            ) : table.getRowModel().rows.length ? (
              table.getRowModel().rows.map((row) => (
                <TableRow
                  key={row.id}
                  className='cursor-pointer'
                  data-state={row.getIsSelected() && 'selected'}
                  onClick={() => onPreview(row.original)}
                >
                  {row.getVisibleCells().map((cell) => (
                    <TableCell key={cell.id}>
                      {flexRender(cell.column.columnDef.cell, cell.getContext())}
                    </TableCell>
                  ))}
                </TableRow>
              ))
            ) : (
              <TableRow>
                <TableCell colSpan={columns.length} className='h-24 text-center'>
                  No DLQ rows found for this queue.
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
      </div>

      <div className='flex items-center justify-between gap-3'>
        <p className='text-sm text-muted-foreground'>
          Loaded {messages.length} of {totalCount} rows
        </p>
        {hasNextPage && (
          <Button
            disabled={isFetchingNextPage}
            onClick={onLoadOlder}
            variant='outline'
          >
            {isFetchingNextPage ? 'Loading...' : 'Load older rows'}
          </Button>
        )}
      </div>

      <ConfirmDialog
        destructive
        open={confirmOpen}
        onOpenChange={setConfirmOpen}
        title='Cancel selected DLQ rows'
        desc={`This will permanently discard ${selectedIds.length} selected DLQ row${selectedIds.length === 1 ? '' : 's'}. This action cannot be undone.`}
        confirmText='Cancel DLQ'
        isLoading={isDiscarding}
        disabled={selectedIds.length === 0}
        handleConfirm={() => {
          onDiscardSelected(selectedIds)
          setConfirmOpen(false)
        }}
      />
    </div>
  )
}
