import { useState, useEffect } from 'react'
import { type Pipeline, type TableWithSyncInfo, tableSyncRepo } from '@/repo/pipelines'
import { Loader2, Search } from 'lucide-react'
import { toast } from 'sonner'
import { Input } from '@/components/ui/input'
import {
  Sheet,
  SheetContent,
  SheetHeader,
  SheetTitle,
  SheetDescription,
} from '@/components/ui/sheet'
import { PostgresTableConfig } from '@/features/pipelines/components/postgres-table-config'
import { SnowflakeTableConfig } from '@/features/pipelines/components/snowflake-table-config'
import { TableCustomSqlCard } from '@/features/pipelines/components/table-custom-sql-card'
import { TableFilterCard } from '@/features/pipelines/components/table-filter-card'
import { TableTargetNameCard } from '@/features/pipelines/components/table-target-name-card'
import { TagDrawer } from '@/features/pipelines/components/tag-drawer'
import { PrimaryKeyDrawer } from '@/features/pipelines/components/primary-key-drawer'

interface SourceTableDrawerProps {
  open: boolean
  onOpenChange: (open: boolean) => void
  pipeline: Pipeline
  initialDestinationId?: number | null
  initialHighlightTable?: string
}

export function SourceTableDrawer({
  open,
  onOpenChange,
  pipeline,
  initialDestinationId,
  initialHighlightTable,
}: SourceTableDrawerProps) {
  const [selectedDestinationId, setSelectedDestinationId] = useState<
    number | null
  >(null)
  const [tables, setTables] = useState<TableWithSyncInfo[]>([])
  const [loading, setLoading] = useState(false)
  const [searchQuery, setSearchQuery] = useState('')

  // Initialize search query from prop
  useEffect(() => {
    if (initialHighlightTable) {
      setSearchQuery(initialHighlightTable)
    }
  }, [initialHighlightTable])

  // Floating Card State
  const [activeTable, setActiveTable] = useState<TableWithSyncInfo | null>(null)
  const [activeSyncConfigId, setActiveSyncConfigId] = useState<number | null>(
    null
  )
  const [activeMode, setActiveMode] = useState<
    'filter' | 'custom' | 'target' | 'tags' | 'primary-keys' | null
  >(null)

  // Get destinations
  const destinations = pipeline.destinations || []

  // Close floating cards when drawer closes
  useEffect(() => {
    if (!open) {
      setActiveTable(null)
      setActiveMode(null)
      setActiveSyncConfigId(null)
      setSearchQuery('')
    }
  }, [open])

  // Set first destination as default or use initialDestinationId
  useEffect(() => {
    if (open && destinations.length > 0) {
      if (
        initialDestinationId &&
        destinations.some((d) => d.id === initialDestinationId)
      ) {
        // Only use initialDestinationId if it exists in the list
        if (selectedDestinationId !== initialDestinationId) {
          setSelectedDestinationId(initialDestinationId)
        }
      } else if (
        !selectedDestinationId ||
        !destinations.some((d) => d.id === selectedDestinationId)
      ) {
        // Fallback to first destination if no selection or current selection is invalid
        setSelectedDestinationId(destinations[0].id)
      }
    }
  }, [open, destinations, selectedDestinationId, initialDestinationId])

  // Load tables when destination is selected
  // Don't reload if custom SQL drawer (or other floating cards) are open
  useEffect(() => {
    if (open && selectedDestinationId && !activeMode) {
      loadTables()
    }
  }, [open, selectedDestinationId, activeMode])

  const loadTables = async () => {
    if (!selectedDestinationId) return

    setLoading(true)
    try {
      const data = await tableSyncRepo.getDestinationTables(
        pipeline.id,
        selectedDestinationId
      )
      setTables(data)

      // Keep activeTable in sync with freshly loaded data
      if (activeTable && activeSyncConfigId) {
        const refreshed = data.find(
          (t) => t.table_name === activeTable.table_name
        )
        if (refreshed) {
          setActiveTable(refreshed)
        }
      }
    } catch (error) {
      console.error('Failed to load tables:', error)
    } finally {
      setLoading(false)
    }
  }

  // Helper to get current sync config
  const getActiveSyncConfig = () => {
    if (!activeTable || !activeSyncConfigId) return null
    return activeTable.sync_configs?.find((s) => s.id === activeSyncConfigId)
  }

  const handleSaveFilter = async (filterSql: string) => {
    if (!activeTable || !selectedDestinationId || !activeSyncConfigId) return
    const currentConfig = getActiveSyncConfig()

    try {
      await tableSyncRepo.saveTableSync(pipeline.id, selectedDestinationId, {
        id: activeSyncConfigId,
        table_name: activeTable.table_name,
        filter_sql: filterSql,
        custom_sql: currentConfig?.custom_sql,
        table_name_target: currentConfig?.table_name_target,
      })
      toast.success('Filter saved successfully')
      setActiveMode(null)
      loadTables()
    } catch (error) {
      toast.error('Failed to save filter')
    }
  }

  const handleSaveCustomSql = async (customSql: string) => {
    if (!activeTable || !selectedDestinationId || !activeSyncConfigId) return
    const currentConfig = getActiveSyncConfig()

    try {
      await tableSyncRepo.saveTableSync(pipeline.id, selectedDestinationId, {
        id: activeSyncConfigId,
        table_name: activeTable.table_name,
        custom_sql: customSql,
        filter_sql: currentConfig?.filter_sql,
        table_name_target: currentConfig?.table_name_target,
      })
      toast.success('Custom SQL saved successfully')
      setActiveMode(null)
      loadTables()
    } catch (error) {
      toast.error('Failed to save custom SQL')
    }
  }

  const handleSaveTargetName = async (targetName: string) => {
    if (!activeTable || !selectedDestinationId || !activeSyncConfigId) return
    const currentConfig = getActiveSyncConfig()

    try {
      await tableSyncRepo.saveTableSync(pipeline.id, selectedDestinationId, {
        id: activeSyncConfigId,
        table_name: activeTable.table_name,
        table_name_target: targetName,
        custom_sql: currentConfig?.custom_sql,
        filter_sql: currentConfig?.filter_sql,
      })
      toast.success('Target table name saved successfully')
      setActiveMode(null)
      loadTables()
    } catch (error) {
      toast.error('Failed to save target table name')
    }
  }

  const handleValidateTargetName = async (targetName: string) => {
    if (!selectedDestinationId) throw new Error('No destination selected')

    return await tableSyncRepo.validateTargetTable(
      pipeline.id,
      selectedDestinationId,
      targetName
    )
  }

  const handleSavePrimaryKeys = async (keys: string) => {
    if (!activeTable || !selectedDestinationId || !activeSyncConfigId) return
    const currentConfig = getActiveSyncConfig()

    try {
      await tableSyncRepo.saveTableSync(pipeline.id, selectedDestinationId, {
        id: activeSyncConfigId,
        table_name: activeTable.table_name,
        primary_key_column_target: keys.length > 0 ? keys : null,
        custom_sql: currentConfig?.custom_sql,
        filter_sql: currentConfig?.filter_sql,
        table_name_target: currentConfig?.table_name_target,
      })
      // Don't close drawer here, let the component handle it
      // Wait 300ms for DB commit before reloading
      await new Promise(resolve => setTimeout(resolve, 300))
      loadTables()
    } catch (error) {
      throw error // Re-throw so drawer can handle error display
    }
  }

  const getCurrentDestination = () => {
    return destinations.find((d) => d.id === selectedDestinationId)
  }

  const currentDestination = getCurrentDestination()
  const destinationType = currentDestination?.destination?.type || 'POSTGRESQL'

  const filteredTables = tables.filter((table) =>
    table.table_name.toLowerCase().includes(searchQuery.toLowerCase())
  )

  const activeSyncConfig = getActiveSyncConfig()

  return (
    <>
      {/* Manual Backdrop when modal={false} */}
      {open && (
        <div
          className='fixed inset-0 z-40 animate-in bg-black/50 duration-300 fade-in'
          onClick={() => {
            // Close drawer when clicking backdrop
            onOpenChange(false)
          }}
        />
      )}

      {/* Sheet with modal={false} to not block interactions with siblings */}
      <Sheet open={open} onOpenChange={onOpenChange} modal={false}>
        <SheetContent
          side='left'
          className='z-50 flex h-full w-[500px] flex-col gap-0 border-r p-0 shadow-none sm:max-w-[500px]'
          // We remove the internal overlay if needed in sheet.tsx or it's handled by modal=false
          onInteractOutside={(e) => {
            // We allow interaction with outside because we manage backdrop ourselves,
            // so we prevent Radix from handling "outside" click closing logic.
            e.preventDefault()
          }}
        >
          {/* Fixed Header section */}
          <div className='flex-shrink-0 border-b p-6 pb-4'>
            <SheetHeader className='mb-6'>
              <SheetTitle>Source Tables Configuration</SheetTitle>
              <SheetDescription>
                Configure table synchronization for{' '}
                <span className='font-medium text-foreground'>
                  {pipeline.source?.name}
                </span>
              </SheetDescription>
            </SheetHeader>

            <div className='relative'>
              <Search className='absolute top-1/2 left-3 h-4 w-4 -translate-y-1/2 text-muted-foreground' />
              <Input
                placeholder='Search tables...'
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                className='pl-9'
              />
            </div>
          </div>

          {/* Scrollable Content section */}
          <div className='flex-1 overflow-y-auto p-6 pt-4'>
            <div className='space-y-6'>
              {/* Table List */}
              <div className='mt-0'>
                {loading ? (
                  <div className='flex items-center justify-center py-12'>
                    <Loader2 className='h-8 w-8 animate-spin text-muted-foreground' />
                  </div>
                ) : destinationType === 'SNOWFLAKE' ? (
                  <SnowflakeTableConfig
                    tables={filteredTables}
                    pipelineId={pipeline.id}
                    pipelineDestinationId={selectedDestinationId!}
                    onRefresh={loadTables}
                  />
                ) : (
                  <PostgresTableConfig
                    tables={filteredTables}
                    pipelineId={pipeline.id}
                    pipelineDestinationId={selectedDestinationId!}
                    onRefresh={loadTables}
                    onEditFilter={(table, id) => {
                      setActiveTable(table)
                      setActiveSyncConfigId(id)
                      setActiveMode('filter')
                    }}
                    onEditCustomSql={(table, id) => {
                      setActiveTable(table)
                      setActiveSyncConfigId(id)
                      setActiveMode('custom')
                    }}
                    onEditTargetName={(table, id) => {
                      setActiveTable(table)
                      setActiveSyncConfigId(id)
                      setActiveMode('target')
                    }}
                    onEditTags={(table, id) => {
                      setActiveTable(table)
                      setActiveSyncConfigId(id)
                      setActiveMode('tags')
                    }}
                    onEditPrimaryKeys={(table, id) => {
                      setActiveTable(table)
                      setActiveSyncConfigId(id)
                      setActiveMode('primary-keys')
                    }}
                  />
                )}
              </div>
            </div>
          </div>
        </SheetContent>
      </Sheet>

      {/* Floating Panels - Rendered as siblings, higher Z-index */}
      {/* Since modal={false}, these are interactive */}
      {open &&
        activeMode === 'filter' &&
        activeSyncConfig && ( // Pass active config if needed, or component uses table + config
          <TableFilterCard
            // Card might expect table, but probably needs specific config now.
            // Note: TableFilterCard likely uses table.sync_config? We need to verify TableFilterCard implementation.
            // If TableFilterCard expects table.sync_config (singular), we might need to mock or pass specific config.
            // Let's assume for now we might need to adjust TableFilterCard props or mock it.
            // Actually, let's create a temporary object that looks like TableWithSyncInfo but with ONLY the active sync config
            // so we don't have to refactor TableFilterCard yet.
            table={
              {
                ...activeTable!,
                sync_config: activeSyncConfig, // Mapping active config to singular property for compatibility
              } as any
            }
            open={true}
            onClose={() => setActiveMode(null)}
            onSave={handleSaveFilter}
          />
        )}

      {open && activeMode === 'custom' && activeSyncConfig && (
        <TableCustomSqlCard
          table={
            {
              ...activeTable!,
              sync_config: activeSyncConfig,
            } as any
          }
          open={true}
          onClose={() => setActiveMode(null)}
          onSave={handleSaveCustomSql}
          destinationName={currentDestination?.destination.name}
          destinationId={currentDestination?.destination.id}
          sourceName={pipeline.source?.name}
          sourceId={pipeline.source_id}
          pipelineId={pipeline.id}
        />
      )}

      {open && activeMode === 'target' && activeSyncConfig && (
        <TableTargetNameCard
          table={
            {
              ...activeTable!,
              sync_config: activeSyncConfig,
            } as any
          }
          open={true}
          onClose={() => setActiveMode(null)}
          onSave={handleSaveTargetName}
          onValidate={handleValidateTargetName}
        />
      )}

      {open && activeMode === 'tags' && activeSyncConfigId && activeTable && (
        <TagDrawer
          tableSyncId={activeSyncConfigId}
          tableName={activeTable.table_name}
          open={true}
          onClose={() => setActiveMode(null)}
        />
      )}

      {open && activeMode === 'primary-keys' && activeSyncConfig && activeTable && (
        <PrimaryKeyDrawer
          syncConfig={activeSyncConfig}
          columns={activeTable.columns}
          open={true}
          onClose={() => setActiveMode(null)}
          onSave={handleSavePrimaryKeys}
        />
      )}
    </>
  )
}
