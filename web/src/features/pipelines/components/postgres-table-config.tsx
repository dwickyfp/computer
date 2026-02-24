import { useState } from 'react'
import { type TableWithSyncInfo, tableSyncRepo, type TableSyncConfig } from '@/repo/pipelines'
import { Switch } from '@/components/ui/switch'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Plus, Loader2, AlertCircle, Database } from 'lucide-react'
import { toast } from 'sonner'
import { cn } from '@/lib/utils'
import { TableBranchNode } from './table-branch-node'
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from '@/components/ui/alert-dialog'

interface PostgresTableConfigProps {
  tables: TableWithSyncInfo[]
  pipelineId: number
  pipelineDestinationId: number
  onRefresh: () => void
  onEditFilter: (table: TableWithSyncInfo, syncConfigId: number) => void
  onEditCustomSql: (table: TableWithSyncInfo, syncConfigId: number) => void
  onEditTargetName: (table: TableWithSyncInfo, syncConfigId: number) => void
  onEditTags: (table: TableWithSyncInfo, syncConfigId: number) => void
  onEditPrimaryKeys: (table: TableWithSyncInfo, syncConfigId: number) => void
}

export function PostgresTableConfig({
  tables,
  pipelineId,
  pipelineDestinationId,
  onRefresh,
  onEditFilter,
  onEditCustomSql,
  onEditTargetName,
  onEditTags,
  onEditPrimaryKeys
}: PostgresTableConfigProps) {
  const [processingTable, setProcessingTable] = useState<string | null>(null)
  const [deleteConfirmOpen, setDeleteConfirmOpen] = useState(false)
  const [pendingDelete, setPendingDelete] = useState<{
    table: TableWithSyncInfo
    syncConfig: TableSyncConfig
  } | null>(null)

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

  const handleAddBranch = async (table: TableWithSyncInfo) => {
    setProcessingTable(table.table_name)
    // Create a new branch with default target name = table_name + _copy if collision?
    // Or just let backend handle default same name (since backend supports it now, but uniqueness might be tricky if not careful).
    // Let's create a new branch with a suffix to be safe visually.
    const existingCount = table.sync_configs.length
    const suffix = existingCount > 0 ? `_${existingCount + 1}` : ''
    const targetName = `${table.table_name}${suffix}`

    try {
      await tableSyncRepo.saveTableSync(pipelineId, pipelineDestinationId, {
        table_name: table.table_name,
        table_name_target: targetName,
        enabled: true
      })
      toast.success(`Added new branch for ${table.table_name}`)
      onRefresh()
    } catch (error) {
      toast.error('Failed to add branch')
    } finally {
      setProcessingTable(null)
    }
  }

  const handleDeleteBranch = async (table: TableWithSyncInfo, syncConfig: TableSyncConfig) => {
    // Open confirmation modal instead of native confirm
    setPendingDelete({ table, syncConfig })
    setDeleteConfirmOpen(true)
  }

  const confirmDelete = async () => {
    if (!pendingDelete) return

    const { table, syncConfig } = pendingDelete
    setProcessingTable(table.table_name)
    setDeleteConfirmOpen(false)

    try {
      // Use the sync config ID to ensure we delete the correct branch
      await tableSyncRepo.deleteTableSyncById(pipelineId, pipelineDestinationId, syncConfig.id)
      toast.success('Branch removed')
      onRefresh()
    } catch (error) {
      toast.error('Failed to remove branch')
    } finally {
      setProcessingTable(null)
      setPendingDelete(null)
    }
  }

  return (
    <div className="space-y-8">
      <div className="flex items-center justify-between">
        <div className="text-sm font-medium text-muted-foreground">Tables ({tables.length})</div>
      </div>

      <div className="space-y-6">
        {[...tables]
          .sort((a, b) => {
            const aActive = a.sync_configs && a.sync_configs.length > 0
            const bActive = b.sync_configs && b.sync_configs.length > 0
            if (aActive === bActive) return 0
            return aActive ? -1 : 1
          })
          .map((table) => {
            const hasBranches = table.sync_configs && table.sync_configs.length > 0
            const isProcessing = processingTable === table.table_name

            return (
              <div key={table.table_name} className="relative pl-4 border-l-2 border-muted hover:border-primary/50 transition-colors">
                {/* Source Node */}
                <div className="mb-4">
                  <div className="flex items-center gap-3">
                    <div className="flex-shrink-0">
                      {isProcessing ? (
                        <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" />
                      ) : (
                        <Switch
                          checked={hasBranches}
                          onCheckedChange={() => handleToggleSync(table)}
                        />
                      )}
                    </div>

                    <div className="flex items-center gap-2">
                      <Database className="h-4 w-4 text-muted-foreground" />
                      <span className={cn("font-semibold text-sm", !hasBranches && "text-muted-foreground")}>
                        {table.table_name}
                      </span>
                      {hasBranches && (
                        <Badge variant="secondary" className="bg-green-100 text-green-800 hover:bg-green-100 dark:bg-green-900/30 dark:text-green-400 text-[10px] h-5 px-1.5">
                          Stream
                        </Badge>
                      )}
                    </div>
                  </div>
                  <div className="text-[10px] text-muted-foreground font-mono mt-0.5 ml-14">
                    {table.columns.length} columns • Source
                  </div>
                </div>

                {/* Branches (Mindmap connections) */}
                <div className="pl-6 space-y-3 relative">
                  {/* Connection Lines Container */}
                  {hasBranches && (
                    <div className="absolute top-0 bottom-4 left-2 w-4 border-l border-b border-border rounded-bl-lg -translate-y-6 -z-10" />
                  )}

                  {table.sync_configs.map((config, idx) => (
                    <div key={config.id || idx} className="relative">
                      {/* SVG Connector for each branch */}
                      <svg className="absolute -left-6 top-1/2 -translate-y-1/2 w-6 h-full pointer-events-none overflow-visible" style={{ height: '40px' }}>
                        <path
                          d="M -16 0 C -8 0, -8 20, 0 20"
                          fill="none"
                          stroke="currentColor"
                          strokeWidth="1.5"
                          className="text-border"
                          transform="translate(0, -20)"
                        />
                        {/* Arrow Head */}
                        <path d="M 0 0 L -4 -2 L -4 2 Z" fill="currentColor" className="text-border" />
                      </svg>

                      <TableBranchNode
                        syncConfig={config}
                        onEditFilter={() => onEditFilter(table, config.id)}
                        onEditCustomSql={() => onEditCustomSql(table, config.id)}
                        onEditTargetName={() => onEditTargetName(table, config.id)}
                        onEditPrimaryKeys={() => onEditPrimaryKeys(table, config.id)}
                        onEditTags={() => onEditTags(table, config.id)}
                        onDelete={() => handleDeleteBranch(table, config)}
                        isDeleting={isProcessing}
                      />
                    </div>
                  ))}

                  {/* Add Branch Button (Node) */}
                  <div className="relative pt-1">
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() => handleAddBranch(table)}
                      disabled={isProcessing}
                      className="h-7 text-xs gap-1.5 border-dashed text-muted-foreground hover:text-primary hover:border-primary hover:bg-primary/5"
                    >
                      <Plus className="h-3 w-3" />
                      Add Destination Target
                    </Button>
                  </div>
                </div>
              </div>
            )
          })}

        {tables.length === 0 && (
          <div className="flex flex-col items-center justify-center py-12 text-center border-2 border-dashed rounded-lg">
            <AlertCircle className="h-8 w-8 text-muted-foreground mb-2" />
            <p className="text-sm font-medium">No available sources found</p>
          </div>
        )}
      </div>

      {/* Delete Confirmation Modal */}
      <AlertDialog open={deleteConfirmOpen} onOpenChange={setDeleteConfirmOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete Table Sync</AlertDialogTitle>
            <AlertDialogDescription>
              Are you sure you want to remove the sync to {' '}
              <span className="font-medium text-foreground">
                {pendingDelete?.syncConfig.table_name_target}
              </span>
              {' '}? This action cannot be undone.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction
              onClick={confirmDelete}
              className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
            >
              Delete
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  )
}


