import { useState } from 'react'
import { type TableWithSyncInfo, tableSyncRepo, type TableSyncConfig } from '@/repo/pipelines'
import { Loader2, Unplug, Settings2, Plus, Database, AlertCircle } from 'lucide-react'
import { toast } from 'sonner'
import { cn } from '@/lib/utils'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Switch } from '@/components/ui/switch'
import { RosettaSchemaRegistration } from './rosetta-schema-registration'
import { Destination } from '@/repo/destinations'
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

interface RosettaTableConfigProps {
  tables: TableWithSyncInfo[]
  pipelineId: number
  pipelineDestinationId: number
  destination?: Destination
  onRefresh: () => void
  onEditFilter: (table: TableWithSyncInfo, syncConfigId: number) => void
  onEditCustomSql: (table: TableWithSyncInfo, syncConfigId: number) => void
  onEditTargetName: (table: TableWithSyncInfo, syncConfigId: number) => void
  onEditTags: (table: TableWithSyncInfo, syncConfigId: number) => void
  onEditPrimaryKeys: (table: TableWithSyncInfo, syncConfigId: number) => void
}

export function RosettaTableConfig({
  tables,
  pipelineId,
  pipelineDestinationId,
  destination,
  onRefresh,
  onEditFilter,
  onEditCustomSql,
  onEditTargetName,
  onEditTags,
  onEditPrimaryKeys
}: RosettaTableConfigProps) {
  const [processingTable, setProcessingTable] = useState<string | null>(null)
  const [registeringAll, setRegisteringAll] = useState(false)
  const [deleteConfirmOpen, setDeleteConfirmOpen] = useState(false)
  const [pendingDelete, setPendingDelete] = useState<{
    table: TableWithSyncInfo
    syncConfig: TableSyncConfig
  } | null>(null)
  
  // Registration Modal State
  const [schemaModalOpen, setSchemaModalOpen] = useState(false)
  const [activeTableForSchema, setActiveTableForSchema] = useState<TableWithSyncInfo | null>(null)
  // For branch specific registration
  const [activeSyncConfigId, setActiveSyncConfigId] = useState<number | null>(null)

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
    setPendingDelete({ table, syncConfig })
    setDeleteConfirmOpen(true)
  }

  const confirmDelete = async () => {
    if (!pendingDelete) return

    const { table, syncConfig } = pendingDelete
    setProcessingTable(table.table_name)
    setDeleteConfirmOpen(false)

    try {
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

  const openSchemaRegistration = (table: TableWithSyncInfo, configId: number) => {
    if (!destination?.chain_client_id) {
      toast.error("Destination does not have a linked chain client ID.")
      return
    }
    setActiveTableForSchema(table)
    setActiveSyncConfigId(configId)
    setSchemaModalOpen(true)
  }

  const registeredCount = tables.filter(
    (t) => t.sync_configs && t.sync_configs.length > 0
  ).length

  if (tables.length === 0) {
    return (
      <div className='flex flex-col items-center justify-center py-12 text-center border-2 border-dashed rounded-lg'>
        <AlertCircle className='mb-3 h-8 w-8 text-muted-foreground' />
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

  // To properly handle registration when table name changes per branch
  // we would construct a temporary copy of table modifying table_name to config.table_name_target
  const activeTableWithBranchName = activeTableForSchema && activeSyncConfigId
    ? {
        ...activeTableForSchema,
        table_name: activeTableForSchema.sync_configs.find(c => c.id === activeSyncConfigId)?.table_name_target || activeTableForSchema.table_name
      }
    : activeTableForSchema;

  return (
    <div className='space-y-8'>
      {/* Header row with counts and Register All button */}
      <div className='flex items-center justify-between'>
        <div className='flex items-center gap-2'>
          <span className='text-xs text-muted-foreground'>
            {registeredCount}/{tables.length} registered
          </span>
          {registeredCount === tables.length && (
            <Badge variant='secondary' className='text-xs bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-400'>
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
                      <Unplug className="h-4 w-4 text-muted-foreground" />
                      <span className={cn("font-semibold text-sm font-mono", !hasBranches && "text-muted-foreground")}>
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
                    {table.columns?.length || 0} columns • Source
                  </div>
                </div>

                {/* Branches (Mindmap connections) */}
                <div className="pl-6 space-y-3 relative">
                  {/* Connection Lines Container */}
                  {hasBranches && (
                    <div className="absolute top-0 bottom-4 left-2 w-4 border-l border-b border-border rounded-bl-lg -translate-y-6 -z-10" />
                  )}

                  {table.sync_configs?.map((config, idx) => (
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
                        <path d="M 0 0 L -4 -2 L -4 2 Z" fill="currentColor" className="text-border" />
                      </svg>

                      <TableBranchNode
                        syncConfig={config}
                        onEditFilter={() => onEditFilter(table, config.id)}
                        onEditCustomSql={() => onEditCustomSql(table, config.id)}
                        onEditTargetName={() => onEditTargetName(table, config.id)}
                        onEditPrimaryKeys={() => onEditPrimaryKeys(table, config.id)}
                        onEditTags={() => onEditTags(table, config.id)}
                        onRegisterSchema={() => openSchemaRegistration(table, config.id)}
                        onDelete={() => handleDeleteBranch(table, config)}
                        isDeleting={isProcessing}
                        hideFilter
                        hideCustomSql
                        hidePrimaryKeys
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
      </div>

      <RosettaSchemaRegistration
        open={schemaModalOpen}
        onOpenChange={setSchemaModalOpen}
        table={activeTableWithBranchName}
        chainId={destination?.chain_client_id as number}
      />
      
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
