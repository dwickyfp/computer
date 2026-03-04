import { useState, useEffect } from 'react'
import { useQuery, useMutation } from '@tanstack/react-query'
import { chainRepo, ChainDatabase } from '@/repo/chains'
import {
  TableWithSyncInfo,
  ColumnSchema,
  tableSyncRepo,
} from '@/repo/pipelines'
import { Loader2, Database, RefreshCw } from 'lucide-react'
import { toast } from 'sonner'
import { Button } from '@/components/ui/button'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { Switch } from '@/components/ui/switch'

interface RosettaSchemaRegistrationProps {
  open: boolean
  onOpenChange: (open: boolean) => void
  table: TableWithSyncInfo | null
  chainId: number // ID of the destination chain client
  // For persisting catalog_database_name back to the sync config
  syncConfigId?: number
  pipelineId?: number
  pipelineDestinationId?: number
  initialDbName?: string | null
}

export function RosettaSchemaRegistration({
  open,
  onOpenChange,
  table,
  chainId,
  syncConfigId,
  pipelineId,
  pipelineDestinationId,
  initialDbName,
}: RosettaSchemaRegistrationProps) {
  const [dbName, setDbName] = useState(initialDbName ?? '')
  const [tableName, setTableName] = useState('')

  // Track schema overrides
  const [schemaUpdates, setSchemaUpdates] = useState<
    Record<string, Partial<ColumnSchema>>
  >({})

  // Sync initialDbName into dbName when dialog opens or initialDbName changes
  useEffect(() => {
    if (open) {
      setDbName(initialDbName ?? '')
    }
  }, [open, initialDbName])

  // Initialize tableName when dialog opens or table changes
  useEffect(() => {
    if (open && table) {
      setTableName(table.table_name)
    }
  }, [open, table])

  // Fetch available databases from the remote chain client
  const {
    data: databases,
    isLoading: loadingDbs,
    refetch: refetchDbs,
  } = useQuery({
    queryKey: ['chain-databases', chainId],
    queryFn: () => chainRepo.getClientDatabases(chainId),
    enabled: open && !!chainId,
  })

  // Sync databases from remote if the list is empty when dialog opens
  const syncDbsMutation = useMutation({
    mutationFn: () => chainRepo.syncClientDatabases(chainId),
    onSuccess: () => refetchDbs(),
  })

  useEffect(() => {
    if (
      open &&
      !!chainId &&
      databases !== undefined &&
      databases.length === 0
    ) {
      syncDbsMutation.mutate()
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open, chainId, databases?.length])

  const registerMutation = useMutation({
    mutationFn: (payload: any) =>
      chainRepo.registerCatalogTable(chainId, payload),
    onSuccess: async (_data, variables) => {
      toast.success(`Successfully registered table to catalog.`)
      // Persist the selected catalog_database_name back to the local sync config
      if (syncConfigId && pipelineId && pipelineDestinationId) {
        try {
          await tableSyncRepo.updateCatalogDatabaseName(
            pipelineId,
            pipelineDestinationId,
            syncConfigId,
            variables.database_name ?? null
          )
        } catch {
          // Non-fatal: the remote registration succeeded, just the local save failed
        }
      }
      onOpenChange(false)
      // Reset form
      setDbName('')
      setTableName('')
      setSchemaUpdates({})
    },
    onError: (err: any) => {
      toast.error(
        `Failed to register catalog table: ${err.message || 'Unknown error'}`
      )
    },
  })

  if (!table) return null

  const handleColumnUpdate = (
    colName: string,
    update: Partial<ColumnSchema>
  ) => {
    setSchemaUpdates((prev) => ({
      ...prev,
      [colName]: { ...(prev[colName] || {}), ...update },
    }))
  }

  const handleRegister = () => {
    if (!dbName) {
      toast.error('Please select or enter a Destination Database')
      return
    }

    if (!tableName) {
      toast.error('Table Name is required')
      return
    }

    // Build schema JSON
    const mergedColumns = table.columns.map((col) => ({
      name: col.column_name,
      type:
        schemaUpdates[col.column_name]?.real_data_type ||
        col.real_data_type ||
        col.data_type,
      nullable:
        schemaUpdates[col.column_name]?.is_nullable !== undefined
          ? schemaUpdates[col.column_name].is_nullable
          : col.is_nullable === 'YES' || col.is_nullable === true,
      primary_key:
        schemaUpdates[col.column_name]?.is_primary_key !== undefined
          ? schemaUpdates[col.column_name].is_primary_key
          : col.is_primary_key,
    }))

    const payload = {
      database_name: dbName,
      table_name: tableName,
      schema_json: mergedColumns,
      source_chain_id: chainId, // Should technically be THIS instance's chain ID, but passing what we have
    }

    registerMutation.mutate(payload)
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className='flex max-h-[90vh] max-w-4xl flex-col overflow-hidden'>
        <DialogHeader>
          <DialogTitle>Register Managed Destination</DialogTitle>
          <DialogDescription>
            Register this table in the remote Rosetta Catalog as a Managed
            Destination.
          </DialogDescription>
        </DialogHeader>

        <div className='flex-1 space-y-6 overflow-auto px-1 py-4'>
          <div className='grid grid-cols-[1fr_1.5fr] gap-4'>
            <div className='space-y-2'>
              <Label>Destination Database</Label>
              {loadingDbs || syncDbsMutation.isPending ? (
                <div className='flex items-center text-sm text-muted-foreground'>
                  <Loader2 className='mr-2 h-4 w-4 animate-spin' /> Loading
                  databases...
                </div>
              ) : (
                <div className='flex items-center gap-2'>
                  <div className='flex w-full items-center gap-2'>
                    <Select value={dbName} onValueChange={setDbName}>
                      <SelectTrigger className='w-full'>
                        <SelectValue placeholder='Select or type database...' />
                      </SelectTrigger>
                      <SelectContent>
                        {databases?.map((db: ChainDatabase) => (
                          <SelectItem key={db.id} value={db.name}>
                            <div className='flex items-center'>
                              <Database className='mr-2 h-4 w-4 text-muted-foreground' />
                              {db.name}
                            </div>
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                    <Button
                      type='button'
                      variant='ghost'
                      size='icon'
                      className='h-9 w-9 flex-shrink-0'
                      disabled={syncDbsMutation.isPending}
                      onClick={() => syncDbsMutation.mutate()}
                      title='Sync databases from remote'
                    >
                      <RefreshCw
                        className={`h-4 w-4 ${syncDbsMutation.isPending ? 'animate-spin' : ''}`}
                      />
                    </Button>
                  </div>
                </div>
              )}
            </div>

            <div className='space-y-2'>
              <Label>Table Name (in Target)</Label>
              <Input
                value={tableName}
                onChange={(e) => setTableName(e.target.value)}
                placeholder={table.table_name}
              />
            </div>
          </div>

          <div className='space-y-3'>
            <Label>Visual Schema Builder</Label>
            <div className='overflow-hidden rounded-md border'>
              <div className='max-h-[300px] overflow-auto'>
                <table className='w-full text-left text-sm'>
                  <thead className='sticky top-0 z-10 bg-muted text-xs text-muted-foreground'>
                    <tr>
                      <th className='px-4 py-2 font-medium'>Field Name</th>
                      <th className='px-4 py-2 font-medium'>Data Type</th>
                      <th className='px-4 py-2 text-center font-medium'>
                        Nullable
                      </th>
                      <th className='px-4 py-2 text-center font-medium'>
                        Primary Key
                      </th>
                    </tr>
                  </thead>
                  <tbody className='divide-y'>
                    {table.columns.map((col) => {
                      const overrides = schemaUpdates[col.column_name] || {}
                      const isNullable =
                        overrides.is_nullable !== undefined
                          ? overrides.is_nullable
                          : col.is_nullable === 'YES' ||
                            col.is_nullable === true
                      const isPk =
                        overrides.is_primary_key !== undefined
                          ? overrides.is_primary_key
                          : col.is_primary_key

                      return (
                        <tr key={col.column_name} className='hover:bg-muted/30'>
                          <td className='px-4 py-2 font-medium'>
                            {col.column_name}
                          </td>
                          <td className='px-4 py-2'>
                            <Input
                              className='h-7 w-full max-w-[150px] font-mono text-xs'
                              value={
                                overrides.real_data_type ||
                                col.real_data_type ||
                                col.data_type ||
                                ''
                              }
                              onChange={(e) =>
                                handleColumnUpdate(col.column_name, {
                                  real_data_type: e.target.value,
                                })
                              }
                            />
                          </td>
                          <td className='px-4 py-2 text-center'>
                            <Switch
                              checked={isNullable as boolean}
                              onCheckedChange={(checked) =>
                                handleColumnUpdate(col.column_name, {
                                  is_nullable: checked,
                                })
                              }
                            />
                          </td>
                          <td className='px-4 py-2 text-center'>
                            <Switch
                              checked={isPk as boolean}
                              onCheckedChange={(checked) =>
                                handleColumnUpdate(col.column_name, {
                                  is_primary_key: checked,
                                })
                              }
                            />
                          </td>
                        </tr>
                      )
                    })}
                  </tbody>
                </table>
              </div>
            </div>
          </div>
        </div>

        <DialogFooter className='border-t pt-4'>
          <Button variant='outline' onClick={() => onOpenChange(false)}>
            Cancel
          </Button>
          <Button
            disabled={!dbName || !tableName || registerMutation.isPending}
            onClick={handleRegister}
          >
            {registerMutation.isPending && (
              <Loader2 className='mr-2 h-4 w-4 animate-spin' />
            )}
            Register Table
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
