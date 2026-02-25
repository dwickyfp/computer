import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { catalogRepo, CatalogDatabase, CatalogTable } from '@/repo/catalog'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import { Card, CardContent, CardHeader } from '@/components/ui/card'
import { ChevronRight, Database, Table as TableIcon, Layers, Loader2 } from 'lucide-react'
import { Badge } from '@/components/ui/badge'
import { format } from 'date-fns'

type ViewState = 'databases' | 'tables' | 'schema'

export function DataExplorer() {
  const [viewState, setViewState] = useState<ViewState>('databases')
  const [selectedDb, setSelectedDb] = useState<CatalogDatabase | null>(null)
  const [selectedTable, setSelectedTable] = useState<CatalogTable | null>(null)

  const [newDbName, setNewDbName] = useState('')
  const queryClient = useQueryClient()

  // Queries
  const { data: databases, isLoading: loadingDbs } = useQuery({
    queryKey: ['catalog-databases'],
    queryFn: catalogRepo.getDatabases,
    staleTime: 5000,
  })

  const { data: tables, isLoading: loadingTables } = useQuery({
    queryKey: ['catalog-tables', selectedDb?.id],
    queryFn: () => catalogRepo.getTables(selectedDb!.id),
    enabled: !!selectedDb && viewState === 'tables',
    staleTime: 5000,
  })

  // Mutations
  const createDbMutation = useMutation({
    mutationFn: (name: string) => catalogRepo.createDatabase({ name, description: '' }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['catalog-databases'] })
      setNewDbName('')
    }
  })

  const handleDbClick = (db: CatalogDatabase) => {
    setSelectedDb(db)
    setViewState('tables')
  }

  const handleTableClick = (table: CatalogTable) => {
    setSelectedTable(table)
    setViewState('schema')
  }

  const handleBreadcrumbClick = (view: ViewState) => {
    if (view === 'databases') {
      setSelectedDb(null)
      setSelectedTable(null)
      setViewState('databases')
    } else if (view === 'tables' && selectedDb) {
      setSelectedTable(null)
      setViewState('tables')
    }
  }

  const getStatusBadge = (status: string) => {
    switch (status.toUpperCase()) {
      case 'ACTIVE':
        return <Badge className='bg-green-500 hover:bg-green-600'>Active</Badge>
      case 'IDLE':
        return <Badge variant='secondary'>Idle</Badge>
      case 'MISSING':
      case 'ERROR':
        return <Badge variant='destructive'>Error</Badge>
      default:
        return <Badge variant='outline'>{status}</Badge>
    }
  }

  return (
    <Card className='h-full flex flex-col'>
      <CardHeader className='pb-3 border-b'>
        {/* Breadcrumb Navigation */}
        <div className='flex items-center space-x-2 text-sm text-muted-foreground'>
          <button 
            onClick={() => handleBreadcrumbClick('databases')}
            className={`flex items-center hover:text-foreground transition-colors ${viewState === 'databases' ? 'text-foreground font-medium' : ''}`}
          >
            <Database className='w-4 h-4 mr-1' />
            Databases
          </button>
          
          {selectedDb && (
            <>
              <ChevronRight className='w-4 h-4' />
              <button 
                onClick={() => handleBreadcrumbClick('tables')}
                className={`flex items-center hover:text-foreground transition-colors ${viewState === 'tables' ? 'text-foreground font-medium' : ''}`}
              >
                <TableIcon className='w-4 h-4 mr-1' />
                {selectedDb.name}
              </button>
            </>
          )}

          {selectedTable && (
            <>
              <ChevronRight className='w-4 h-4' />
              <span className='flex items-center text-foreground font-medium'>
                <Layers className='w-4 h-4 mr-1' />
                {selectedTable.table_name}
              </span>
            </>
          )}
        </div>
      </CardHeader>
      
      <CardContent className='pt-6 flex-1 overflow-auto'>
        {viewState === 'databases' && (
          <div className='space-y-6'>
            <div className='flex gap-4 items-end'>
              <div className='flex-1 max-w-sm'>
                <label className='text-sm font-medium mb-2 block'>Create Logical Database</label>
                <div className='flex gap-2'>
                  <Input 
                    placeholder='public, analytics_raw, etc.' 
                    value={newDbName}
                    onChange={(e) => setNewDbName(e.target.value)}
                  />
                  <Button 
                    disabled={!newDbName || createDbMutation.isPending}
                    onClick={() => createDbMutation.mutate(newDbName)}
                  >
                    {createDbMutation.isPending ? <Loader2 className='w-4 h-4 animate-spin' /> : 'Create'}
                  </Button>
                </div>
              </div>
            </div>

            <div className='border rounded-md'>
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Name</TableHead>
                    <TableHead>Created At</TableHead>
                    <TableHead className='w-[100px]'></TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {loadingDbs ? (
                    <TableRow>
                      <TableCell colSpan={3} className='text-center h-24'><Loader2 className='w-5 h-5 animate-spin mx-auto' /></TableCell>
                    </TableRow>
                  ) : databases?.length === 0 ? (
                    <TableRow>
                      <TableCell colSpan={3} className='text-center text-muted-foreground h-24'>No databases found.</TableCell>
                    </TableRow>
                  ) : databases?.map((db: CatalogDatabase) => (
                    <TableRow key={db.id} className='cursor-pointer hover:bg-muted/50 transition-colors' onClick={() => handleDbClick(db)}>
                      <TableCell className='font-medium flex items-center'><Database className='w-4 h-4 mr-2 text-muted-foreground'/>{db.name}</TableCell>
                      <TableCell>{format(new Date(db.created_at), 'MMM d, yyyy HH:mm')}</TableCell>
                      <TableCell>
                        <ChevronRight className='w-5 h-5 text-muted-foreground ml-auto' />
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </div>
          </div>
        )}

        {viewState === 'tables' && (
          <div className='space-y-4'>
            <div className='flex justify-between items-center'>
              <h3 className='text-lg font-medium'>Tables in {selectedDb?.name}</h3>
              <p className='text-sm text-muted-foreground max-w-md text-right'>
                Tables are registered remotely from Chain Clients via Pipeline Managed Destinations.
              </p>
            </div>
            <div className='border rounded-md'>
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Table Name</TableHead>
                    <TableHead>Stream Name</TableHead>
                    <TableHead>Status</TableHead>
                    <TableHead className='w-[100px]'></TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {loadingTables ? (
                    <TableRow>
                      <TableCell colSpan={4} className='text-center h-24'><Loader2 className='w-5 h-5 animate-spin mx-auto' /></TableCell>
                    </TableRow>
                  ) : tables?.length === 0 ? (
                    <TableRow>
                      <TableCell colSpan={4} className='text-center text-muted-foreground h-24'>No tables registered in this database yet.</TableCell>
                    </TableRow>
                  ) : tables?.map((tbl: CatalogTable) => (
                    <TableRow key={tbl.id} className='cursor-pointer hover:bg-muted/50 transition-colors' onClick={() => handleTableClick(tbl)}>
                      <TableCell className='font-medium flex items-center'><TableIcon className='w-4 h-4 mr-2 text-muted-foreground'/>{tbl.table_name}</TableCell>
                      <TableCell className='text-muted-foreground font-mono text-xs'>{tbl.stream_name}</TableCell>
                      <TableCell>{getStatusBadge(tbl.status)}</TableCell>
                      <TableCell>
                        <ChevronRight className='w-5 h-5 text-muted-foreground ml-auto' />
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </div>
          </div>
        )}

        {viewState === 'schema' && selectedTable && (
          <div className='space-y-6'>
            <div className='flex items-center justify-between'>
              <h3 className='text-lg font-medium'>Schema Definition: {selectedTable.table_name}</h3>
              <div className='flex space-x-2'>
                {getStatusBadge(selectedTable.status)}
              </div>
            </div>

            <Card className='bg-muted/30'>
              <CardContent className='pt-6'>
                <dl className='grid grid-cols-2 gap-4 text-sm'>
                  <div>
                    <dt className='text-muted-foreground'>Stream Name</dt>
                    <dd className='font-mono mt-1'>{selectedTable.stream_name}</dd>
                  </div>
                  <div>
                    <dt className='text-muted-foreground'>Source Chain Client ID</dt>
                    <dd className='mt-1'>{selectedTable.source_chain_id || 'N/A'}</dd>
                  </div>
                  <div>
                    <dt className='text-muted-foreground'>Last Health Check</dt>
                    <dd className='mt-1'>{selectedTable.last_health_check_at ? format(new Date(selectedTable.last_health_check_at), 'MMM d, yyyy HH:mm:ss') : 'Never'}</dd>
                  </div>
                  <div>
                    <dt className='text-muted-foreground'>Registered At</dt>
                    <dd className='mt-1'>{format(new Date(selectedTable.created_at), 'MMM d, yyyy HH:mm')}</dd>
                  </div>
                </dl>
              </CardContent>
            </Card>

            <div>
              <h4 className='font-medium mb-3'>Columns</h4>
              <div className='border rounded-md'>
                <Table>
                  <TableHeader>
                    <TableRow className='bg-muted/50'>
                      <TableHead className='w-[200px]'>Field Name</TableHead>
                      <TableHead>Type</TableHead>
                      <TableHead>Nullable</TableHead>
                      <TableHead>Primary Key</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {/* Assuming schema_json has fields array for this example, adjust based on actual structure */}
                    {Object.keys(selectedTable.schema_json).length === 0 ? (
                      <TableRow>
                        <TableCell colSpan={4} className='text-center text-muted-foreground h-24'>No schema defined.</TableCell>
                      </TableRow>
                    ) : Array.isArray(selectedTable.schema_json) ? (
                      selectedTable.schema_json.map((field: any, idx: number) => (
                        <TableRow key={idx}>
                          <TableCell className='font-medium'>{field.name}</TableCell>
                          <TableCell className='font-mono text-xs text-muted-foreground'>{field.type}</TableCell>
                          <TableCell>{field.nullable ? 'Yes' : 'No'}</TableCell>
                          <TableCell>{field.primary_key ? 'Yes' : 'No'}</TableCell>
                        </TableRow>
                      ))
                    ) : (
                      <TableRow>
                        <TableCell colSpan={4}>
                           <pre className='text-xs p-2 overflow-auto bg-muted rounded'>
                             {JSON.stringify(selectedTable.schema_json, null, 2)}
                           </pre>
                        </TableCell>
                      </TableRow>
                    )}
                  </TableBody>
                </Table>
              </div>
            </div>
          </div>
        )}
      </CardContent>
    </Card>
  )
}
