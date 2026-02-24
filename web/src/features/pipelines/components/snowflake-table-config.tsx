import { useState } from 'react'
import { type TableWithSyncInfo, tableSyncRepo } from '@/repo/pipelines'
import { Switch } from '@/components/ui/switch'
import { Button } from '@/components/ui/button'
import { CheckCircle2, XCircle, Loader2, Play, AlertCircle } from 'lucide-react'
import { toast } from 'sonner'
import { cn } from '@/lib/utils'

interface SnowflakeTableConfigProps {
  tables: TableWithSyncInfo[]
  pipelineId: number
  pipelineDestinationId: number
  onRefresh: () => void
}

export function SnowflakeTableConfig({
  tables,
  pipelineId,
  pipelineDestinationId,
  onRefresh,
}: SnowflakeTableConfigProps) {
  const [initializingTable, setInitializingTable] = useState<string | null>(null)
  const [savingTable, setSavingTable] = useState<string | null>(null)

  const handleToggleSync = async (table: TableWithSyncInfo) => {
    setSavingTable(table.table_name)
    const syncConfig = table.sync_configs?.[0]

    try {
      if (syncConfig) {
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
      setSavingTable(null)
    }
  }

  const handleInitTable = async (table: TableWithSyncInfo) => {
    setInitializingTable(table.table_name)
    try {
      const result = await tableSyncRepo.initSnowflakeTable(
        pipelineId,
        pipelineDestinationId,
        table.table_name
      )
      if (result.status === 'success') {
        toast.success(result.message)
      } else if (result.status === 'skipped') {
        toast.info(result.message)
      } else {
        toast.error(result.message)
      }
      onRefresh()
    } catch (error) {
      toast.error('Failed to initialize table')
    } finally {
      setInitializingTable(null)
    }
  }

  const StatusBadge = ({
    exists,
    label,
  }: {
    exists: boolean
    label: string
  }) => (
    <div className={cn(
      "flex items-center gap-1.5 text-xs px-2 py-1 rounded-md border",
      exists
        ? "bg-emerald-50 text-emerald-700 border-emerald-200 dark:bg-emerald-900/20 dark:text-emerald-400 dark:border-emerald-800"
        : "bg-muted text-muted-foreground border-border"
    )}>
      {exists ? (
        <CheckCircle2 className="h-3.5 w-3.5" />
      ) : (
        <XCircle className="h-3.5 w-3.5" />
      )}
      <span className="font-medium">{label}</span>
    </div>
  )

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div className="text-sm font-medium text-muted-foreground">Tables ({tables.length})</div>
      </div>

      <div className="space-y-3">
        {tables.map((table) => (
          <div
            key={table.table_name}
            className="flex flex-col gap-3 p-4 border rounded-lg bg-card hover:bg-accent/5 transition-colors"
          >
            {/* Top Row: Switch, Name, Init Button */}
            <div className="flex items-start justify-between gap-4">
              <div className="flex items-center gap-3 min-w-0">
                <div className="flex items-center pt-0.5">
                  {savingTable === table.table_name ? (
                    <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" />
                  ) : (
                    <Switch
                      checked={!!table.sync_configs?.[0]}
                      onCheckedChange={() => handleToggleSync(table)}
                    />
                  )}
                </div>
                <div className="min-w-0">
                  <div className="text-sm font-semibold truncate" title={table.table_name}>
                    {table.table_name}
                  </div>
                  <div className="text-xs text-muted-foreground flex items-center gap-1 mt-1">
                    {!table.sync_configs?.[0] ? 'Not Synced' : 'Ready to Sync'}
                  </div>
                </div>
              </div>

              <Button
                variant="outline"
                size="sm"
                onClick={() => handleInitTable(table)}
                disabled={initializingTable === table.table_name || !table.sync_configs?.[0]}
                className="h-8 text-xs shrink-0"
              >
                {initializingTable === table.table_name ? (
                  <Loader2 className="h-3.5 w-3.5 animate-spin mr-1.5" />
                ) : (
                  <Play className="h-3.5 w-3.5 mr-1.5" />
                )}
                Initialize
              </Button>
            </div>

            {/* Bottom Row: Status Indicators */}
            <div className="grid grid-cols-2 sm:grid-cols-4 gap-2">
              <StatusBadge
                exists={table.is_exists_table_landing}
                label="Landing"
              />
              <StatusBadge exists={table.is_exists_stream} label="Stream" />
              <StatusBadge exists={table.is_exists_task} label="Task" />
              <StatusBadge
                exists={table.is_exists_table_destination}
                label="Target"
              />
            </div>
          </div>
        ))}

        {tables.length === 0 && (
          <div className="flex flex-col items-center justify-center py-12 text-center border-2 border-dashed rounded-lg">
            <AlertCircle className="h-8 w-8 text-muted-foreground mb-2" />
            <p className="text-sm font-medium">No tables found</p>
            <p className="text-xs text-muted-foreground">This source has no tables available to configure.</p>
          </div>
        )}
      </div>
    </div>
  )
}
