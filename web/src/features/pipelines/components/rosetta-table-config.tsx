import { useState } from 'react'
import { type TableWithSyncInfo, tableSyncRepo } from '@/repo/pipelines'
import { Loader2, Unplug } from 'lucide-react'
import { toast } from 'sonner'
import { cn } from '@/lib/utils'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Switch } from '@/components/ui/switch'

interface RosettaTableConfigProps {
  tables: TableWithSyncInfo[]
  pipelineId: number
  pipelineDestinationId: number
  onRefresh: () => void
}

export function RosettaTableConfig({
  tables,
  pipelineId,
  pipelineDestinationId,
  onRefresh,
}: RosettaTableConfigProps) {
  const [processingTable, setProcessingTable] = useState<string | null>(null)
  const [registeringAll, setRegisteringAll] = useState(false)

  const handleToggleSync = async (table: TableWithSyncInfo) => {
    const isSynced = table.sync_configs && table.sync_configs.length > 0
    setProcessingTable(table.table_name)

    try {
      if (isSynced) {
        await tableSyncRepo.deleteTableSync(
          pipelineId,
          pipelineDestinationId,
          table.table_name
        )
        toast.success(`Removed ${table.table_name} from sync`)
      } else {
        await tableSyncRepo.saveTableSync(pipelineId, pipelineDestinationId, {
          table_name: table.table_name,
        })
        toast.success(`Added ${table.table_name} to sync`)
      }
      onRefresh()
    } catch (error) {
      toast.error('Failed to update table sync')
    } finally {
      setProcessingTable(null)
    }
  }

  const handleRegisterAll = async () => {
    const unregistered = tables
      .filter((t) => !t.sync_configs || t.sync_configs.length === 0)
      .map((t) => t.table_name)

    if (unregistered.length === 0) {
      toast.info('All tables are already registered')
      return
    }

    setRegisteringAll(true)
    try {
      await tableSyncRepo.saveTableSyncBulk(
        pipelineId,
        pipelineDestinationId,
        unregistered
      )
      toast.success(`Registered ${unregistered.length} table(s) for sync`)
      onRefresh()
    } catch (error) {
      toast.error('Failed to register tables')
    } finally {
      setRegisteringAll(false)
    }
  }

  const registeredCount = tables.filter(
    (t) => t.sync_configs && t.sync_configs.length > 0
  ).length

  if (tables.length === 0) {
    return (
      <div className='flex flex-col items-center justify-center py-12 text-center'>
        <Unplug className='mb-3 h-8 w-8 text-muted-foreground' />
        <p className='text-sm font-medium text-muted-foreground'>
          No chain tables available
        </p>
        <p className='mt-1 text-xs text-muted-foreground'>
          Sync tables from the Rosetta Chain page or wait for the remote
          instance to register tables
        </p>
      </div>
    )
  }

  return (
    <div className='space-y-3'>
      {/* Header row with counts and Register All button */}
      <div className='flex items-center justify-between'>
        <div className='flex items-center gap-2'>
          <span className='text-xs text-muted-foreground'>
            {registeredCount}/{tables.length} registered
          </span>
          {registeredCount === tables.length && (
            <Badge variant='secondary' className='text-xs'>
              All synced
            </Badge>
          )}
        </div>
        <Button
          size='sm'
          variant='outline'
          onClick={handleRegisterAll}
          disabled={registeringAll || registeredCount === tables.length}
          className='h-7 text-xs'
        >
          {registeringAll ? (
            <Loader2 className='mr-1.5 h-3 w-3 animate-spin' />
          ) : (
            <Unplug className='mr-1.5 h-3 w-3' />
          )}
          Register All
        </Button>
      </div>

      {/* Table list */}
      {tables.map((table) => {
        const isSynced = table.sync_configs && table.sync_configs.length > 0
        const isProcessing = processingTable === table.table_name

        return (
          <div
            key={table.table_name}
            className={cn(
              'flex items-center justify-between rounded-lg border px-3 py-2.5 transition-colors',
              isSynced
                ? 'border-primary/20 bg-primary/5'
                : 'border-border bg-card'
            )}
          >
            <div className='flex min-w-0 flex-1 items-center gap-2'>
              <Unplug
                className={cn(
                  'h-3.5 w-3.5 flex-shrink-0',
                  isSynced ? 'text-primary' : 'text-muted-foreground'
                )}
              />
              <span
                className={cn(
                  'truncate font-mono text-sm',
                  isSynced ? 'text-foreground' : 'text-muted-foreground'
                )}
              >
                {table.table_name}
              </span>
              {table.columns && table.columns.length > 0 && (
                <span className='flex-shrink-0 text-xs text-muted-foreground'>
                  {table.columns.length} cols
                </span>
              )}
            </div>

            <div className='ml-3 flex-shrink-0'>
              {isProcessing ? (
                <Loader2 className='h-4 w-4 animate-spin text-muted-foreground' />
              ) : (
                <Switch
                  checked={isSynced}
                  onCheckedChange={() => handleToggleSync(table)}
                  aria-label={`Toggle sync for ${table.table_name}`}
                />
              )}
            </div>
          </div>
        )
      })}
    </div>
  )
}
