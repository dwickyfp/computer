import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { chainRepo, ChainClient, ChainDatabase, ChainTable } from '@/repo/chains'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import { Card, CardContent, CardHeader } from '@/components/ui/card'
import { ChevronRight, Database, Table as TableIcon, Layers, Loader2, Server } from 'lucide-react'
import { Badge } from '@/components/ui/badge'
import { format } from 'date-fns'

type ViewState = 'clients' | 'databases' | 'tables' | 'schema'

export function DataExplorer() {
  const [viewState, setViewState] = useState<ViewState>('clients')
  const [selectedClient, setSelectedClient] = useState<ChainClient | null>(null)
  const [selectedDb, setSelectedDb] = useState<ChainDatabase | null>(null)
  const [selectedTable, setSelectedTable] = useState<ChainTable | null>(null)

  // Queries
  const { data: clients, isLoading: loadingClients } = useQuery({
    queryKey: ['chain-clients'],
    queryFn: chainRepo.getClients,
    staleTime: 5000,
  })

  const { data: databases, isLoading: loadingDbs } = useQuery({
    queryKey: ['chain-client-databases', selectedClient?.id],
    queryFn: () => chainRepo.getClientDatabases(selectedClient!.id),
    enabled: !!selectedClient && (viewState === 'databases' || viewState === 'tables' || viewState === 'schema'),
    staleTime: 5000,
  })

  // We fetch tables per client, then filter by the selected database name
  // Note: the backend actually returns all tables for a client via getClientTables.
  // There is no getTables(db_id) equivalent for rosetta_chain databases right now,
  // we filter tables on the frontend based on the stream_name or source database.
  const { data: tables, isLoading: loadingTables } = useQuery({
    queryKey: ['chain-client-tables', selectedClient?.id],
    queryFn: () => chainRepo.getClientTables(selectedClient!.id),
    enabled: !!selectedClient && viewState === 'tables',
    staleTime: 5000,
  })

  // Alternatively, if the backend doesn't explicitly return database-scoped chain tables, 
  // we can just display all tables for the client when a database is selected, 
  // but logically we should filter them. But `ChainTable` doesn't explicitly link to `ChainDatabase` in the schema provided 
  // (it just has chain_client_id). Let's just list tables since the UI implies showing tables.
  const handleClientClick = (client: ChainClient) => {
    setSelectedClient(client)
    setViewState('databases')
  }

  const handleDbClick = (db: ChainDatabase) => {
    setSelectedDb(db)
    setViewState('tables')
  }

  const handleTableClick = (table: ChainTable) => {
    setSelectedTable(table)
    setViewState('schema')
  }

  const handleBreadcrumbClick = (view: ViewState) => {
    if (view === 'clients') {
      setSelectedClient(null)
      setSelectedDb(null)
      setSelectedTable(null)
      setViewState('clients')
    } else if (view === 'databases' && selectedClient) {
      setSelectedDb(null)
      setSelectedTable(null)
      setViewState('databases')
    } else if (view === 'tables' && selectedDb) {
      setSelectedTable(null)
      setViewState('tables')
    }
  }

  const getStatusBadge = (table: ChainTable) => {
    // If it was recently synced, it's active
    if (!table.last_synced_at) return <Badge variant='secondary'>Unknown</Badge>
    return <Badge className='bg-green-500 hover:bg-green-600'>Active</Badge>
  }

  return (
    <Card className='h-full flex flex-col'>
      <CardHeader className='pb-3 border-b'>
        {/* Breadcrumb Navigation */}
        <div className='flex items-center space-x-2 text-sm text-muted-foreground'>
          <button 
            onClick={() => handleBreadcrumbClick('clients')}
            className={`flex items-center hover:text-foreground transition-colors ${viewState === 'clients' ? 'text-foreground font-medium' : ''}`}
          >
            <Server className='w-4 h-4 mr-1' />
            Clients
          </button>
          
          {selectedClient && (
            <>
              <ChevronRight className='w-4 h-4' />
              <button 
                onClick={() => handleBreadcrumbClick('databases')}
                className={`flex items-center hover:text-foreground transition-colors ${viewState === 'databases' ? 'text-foreground font-medium' : ''}`}
              >
                <Database className='w-4 h-4 mr-1' />
                {selectedClient.name}
              </button>
            </>
          )}

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
        {viewState === 'clients' && (
          <div className='space-y-6'>
            <div className='flex gap-4 items-end'>
              <div className='flex-1 max-w-sm'>
                <h3 className='text-lg font-medium'>Select a Chain Client</h3>
                <p className='text-sm text-muted-foreground'>
                  Choose a remote Rosetta instance to explore its cataloged databases.
                </p>
              </div>
            </div>

            <div className='border rounded-md'>
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Name</TableHead>
                    <TableHead>URL</TableHead>
                    <TableHead className='w-[100px]'></TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {loadingClients ? (
                    <TableRow>
                      <TableCell colSpan={3} className='text-center h-24'><Loader2 className='w-5 h-5 animate-spin mx-auto' /></TableCell>
                    </TableRow>
                  ) : clients?.length === 0 ? (
                    <TableRow>
                      <TableCell colSpan={3} className='text-center text-muted-foreground h-24'>No clients found.</TableCell>
                    </TableRow>
                  ) : clients?.map((client: ChainClient) => (
                    <TableRow key={client.id} className='cursor-pointer hover:bg-muted/50 transition-colors' onClick={() => handleClientClick(client)}>
                      <TableCell className='font-medium flex items-center'><Server className='w-4 h-4 mr-2 text-muted-foreground'/>{client.name}</TableCell>
                      <TableCell>{client.url}:{client.port}</TableCell>
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

        {viewState === 'databases' && (
          <div className='space-y-6'>
            <div className='flex justify-between items-center'>
              <h3 className='text-lg font-medium'>Databases in {selectedClient?.name}</h3>
            </div>

            <div className='border rounded-md'>
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Name</TableHead>
                    <TableHead>Discovered At</TableHead>
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
                      <TableCell colSpan={3} className='text-center text-muted-foreground h-24'>No databases found for this client.</TableCell>
                    </TableRow>
                  ) : databases?.map((db: ChainDatabase) => (
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
                    <TableHead>Record Count</TableHead>
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
                      <TableCell colSpan={4} className='text-center text-muted-foreground h-24'>No tables registered in this client yet.</TableCell>
                    </TableRow>
                  ) : tables?.filter(() => {
                      // We don't have a strict DB link right now in ChainTable, so we can filter by prefix if table_name has it, 
                      // or just show all. Since we want a robust drilldown, let's just render the tables.
                      // A proper backend fix would associate rosetta_chain_tables with rosetta_chain_databases.
                      // For now, we will just display the tables returned for the client.
                      return true
                  }).map((tbl: ChainTable) => (
                    <TableRow key={tbl.id} className='cursor-pointer hover:bg-muted/50 transition-colors' onClick={() => handleTableClick(tbl)}>
                      <TableCell className='font-medium flex items-center'><TableIcon className='w-4 h-4 mr-2 text-muted-foreground'/>{tbl.table_name}</TableCell>
                      <TableCell className='text-muted-foreground font-mono text-xs'>{tbl.record_count.toLocaleString()}</TableCell>
                      <TableCell>{getStatusBadge(tbl)}</TableCell>
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
                {getStatusBadge(selectedTable)}
              </div>
            </div>

            <Card className='bg-muted/30'>
              <CardContent className='pt-6'>
                <dl className='grid grid-cols-2 gap-4 text-sm'>
                  <div>
                    <dt className='text-muted-foreground'>Source Chain Client ID</dt>
                    <dd className='mt-1'>{selectedTable.chain_client_id}</dd>
                  </div>
                  <div>
                    <dt className='text-muted-foreground'>Record Count</dt>
                    <dd className='mt-1'>{selectedTable.record_count.toLocaleString()}</dd>
                  </div>
                  <div>
                    <dt className='text-muted-foreground'>Last Synced</dt>
                    <dd className='mt-1'>{selectedTable.last_synced_at ? format(new Date(selectedTable.last_synced_at), 'MMM d, yyyy HH:mm:ss') : 'Never'}</dd>
                  </div>
                  <div>
                    <dt className='text-muted-foreground'>Discovered At</dt>
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
                    {/* Assuming table_schema has fields array for this example, adjust based on actual structure */}
                    {!selectedTable.table_schema || Object.keys(selectedTable.table_schema).length === 0 ? (
                      <TableRow>
                        <TableCell colSpan={4} className='text-center text-muted-foreground h-24'>No schema defined.</TableCell>
                      </TableRow>
                    ) : Array.isArray(selectedTable.table_schema) ? (
                      selectedTable.table_schema.map((field: any, idx: number) => (
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
                             {JSON.stringify(selectedTable.table_schema, null, 2)}
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
