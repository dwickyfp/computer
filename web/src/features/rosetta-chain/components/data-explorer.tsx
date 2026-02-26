import { useState } from 'react'
import { format } from 'date-fns'
import { useQuery } from '@tanstack/react-query'
import {
  chainRepo,
  ChainClient,
  ChainDatabase,
  ChainTable,
} from '@/repo/chains'
import {
  ChevronRight,
  Database,
  Table as TableIcon,
  Layers,
  Loader2,
  Server,
  RefreshCw,
} from 'lucide-react'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardHeader } from '@/components/ui/card'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'

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
    enabled:
      !!selectedClient &&
      (viewState === 'databases' ||
        viewState === 'tables' ||
        viewState === 'schema'),
    staleTime: 5000,
  })

  // We fetch tables per client+database combination
  const {
    data: tables,
    isLoading: loadingTables,
    refetch: refetchTables,
    isFetching: fetchingTables,
  } = useQuery({
    queryKey: ['chain-client-tables', selectedClient?.id, selectedDb?.id],
    queryFn: () =>
      chainRepo.getClientTablesByDatabase(selectedClient!.id, selectedDb!.id),
    enabled:
      !!selectedClient &&
      !!selectedDb &&
      (viewState === 'tables' || viewState === 'schema'),
    staleTime: 0,
    refetchInterval: 30_000,
  })
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
    <Card className='flex h-full flex-col'>
      <CardHeader className='border-b pb-3'>
        {/* Breadcrumb Navigation */}
        <div className='flex items-center space-x-2 text-sm text-muted-foreground'>
          <button
            onClick={() => handleBreadcrumbClick('clients')}
            className={`flex items-center transition-colors hover:text-foreground ${viewState === 'clients' ? 'font-medium text-foreground' : ''}`}
          >
            <Server className='mr-1 h-4 w-4' />
            Clients
          </button>

          {selectedClient && (
            <>
              <ChevronRight className='h-4 w-4' />
              <button
                onClick={() => handleBreadcrumbClick('databases')}
                className={`flex items-center transition-colors hover:text-foreground ${viewState === 'databases' ? 'font-medium text-foreground' : ''}`}
              >
                <Database className='mr-1 h-4 w-4' />
                {selectedClient.name}
              </button>
            </>
          )}

          {selectedDb && (
            <>
              <ChevronRight className='h-4 w-4' />
              <button
                onClick={() => handleBreadcrumbClick('tables')}
                className={`flex items-center transition-colors hover:text-foreground ${viewState === 'tables' ? 'font-medium text-foreground' : ''}`}
              >
                <TableIcon className='mr-1 h-4 w-4' />
                {selectedDb.name}
              </button>
            </>
          )}

          {selectedTable && (
            <>
              <ChevronRight className='h-4 w-4' />
              <span className='flex items-center font-medium text-foreground'>
                <Layers className='mr-1 h-4 w-4' />
                {selectedTable.table_name}
              </span>
            </>
          )}
        </div>
      </CardHeader>

      <CardContent className='flex-1 overflow-auto pt-6'>
        {viewState === 'clients' && (
          <div className='space-y-6'>
            <div className='flex items-end gap-4'>
              <div className='max-w-sm flex-1'>
                <h3 className='text-lg font-medium'>Select a Chain Client</h3>
                <p className='text-sm text-muted-foreground'>
                  Choose a remote Rosetta instance to explore its cataloged
                  databases.
                </p>
              </div>
            </div>

            <div className='rounded-md border'>
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
                      <TableCell colSpan={3} className='h-24 text-center'>
                        <Loader2 className='mx-auto h-5 w-5 animate-spin' />
                      </TableCell>
                    </TableRow>
                  ) : clients?.length === 0 ? (
                    <TableRow>
                      <TableCell
                        colSpan={3}
                        className='h-24 text-center text-muted-foreground'
                      >
                        No clients found.
                      </TableCell>
                    </TableRow>
                  ) : (
                    clients?.map((client: ChainClient) => (
                      <TableRow
                        key={client.id}
                        className='cursor-pointer transition-colors hover:bg-muted/50'
                        onClick={() => handleClientClick(client)}
                      >
                        <TableCell className='flex items-center font-medium'>
                          <Server className='mr-2 h-4 w-4 text-muted-foreground' />
                          {client.name}
                        </TableCell>
                        <TableCell>
                          {client.url}:{client.port}
                        </TableCell>
                        <TableCell>
                          <ChevronRight className='ml-auto h-5 w-5 text-muted-foreground' />
                        </TableCell>
                      </TableRow>
                    ))
                  )}
                </TableBody>
              </Table>
            </div>
          </div>
        )}

        {viewState === 'databases' && (
          <div className='space-y-6'>
            <div className='flex items-center justify-between'>
              <h3 className='text-lg font-medium'>
                Databases in {selectedClient?.name}
              </h3>
            </div>

            <div className='rounded-md border'>
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
                      <TableCell colSpan={3} className='h-24 text-center'>
                        <Loader2 className='mx-auto h-5 w-5 animate-spin' />
                      </TableCell>
                    </TableRow>
                  ) : databases?.length === 0 ? (
                    <TableRow>
                      <TableCell
                        colSpan={3}
                        className='h-24 text-center text-muted-foreground'
                      >
                        No databases found for this client.
                      </TableCell>
                    </TableRow>
                  ) : (
                    databases?.map((db: ChainDatabase) => (
                      <TableRow
                        key={db.id}
                        className='cursor-pointer transition-colors hover:bg-muted/50'
                        onClick={() => handleDbClick(db)}
                      >
                        <TableCell className='flex items-center font-medium'>
                          <Database className='mr-2 h-4 w-4 text-muted-foreground' />
                          {db.name}
                        </TableCell>
                        <TableCell>
                          {format(new Date(db.created_at), 'MMM d, yyyy HH:mm')}
                        </TableCell>
                        <TableCell>
                          <ChevronRight className='ml-auto h-5 w-5 text-muted-foreground' />
                        </TableCell>
                      </TableRow>
                    ))
                  )}
                </TableBody>
              </Table>
            </div>
          </div>
        )}

        {viewState === 'tables' && (
          <div className='space-y-4'>
            <div className='flex items-center justify-between'>
              <h3 className='text-lg font-medium'>
                Tables in {selectedDb?.name}
              </h3>
              <Button
                variant='outline'
                size='sm'
                onClick={() => refetchTables()}
                disabled={fetchingTables}
              >
                <RefreshCw
                  className={`mr-1.5 h-3.5 w-3.5 ${fetchingTables ? 'animate-spin' : ''}`}
                />
                Refresh
              </Button>
            </div>
            <div className='rounded-md border'>
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
                      <TableCell colSpan={4} className='h-24 text-center'>
                        <Loader2 className='mx-auto h-5 w-5 animate-spin' />
                      </TableCell>
                    </TableRow>
                  ) : !tables || tables.length === 0 ? (
                    <TableRow>
                      <TableCell
                        colSpan={4}
                        className='h-24 text-center text-muted-foreground'
                      >
                        No tables registered yet. Tables appear automatically
                        once the chain pipeline starts streaming data.
                      </TableCell>
                    </TableRow>
                  ) : (
                    tables.map((tbl: ChainTable) => (
                      <TableRow
                        key={tbl.id}
                        className='cursor-pointer transition-colors hover:bg-muted/50'
                        onClick={() => handleTableClick(tbl)}
                      >
                        <TableCell className='flex items-center font-medium'>
                          <TableIcon className='mr-2 h-4 w-4 text-muted-foreground' />
                          {tbl.table_name}
                        </TableCell>
                        <TableCell className='font-mono text-xs text-muted-foreground'>
                          {tbl.record_count.toLocaleString()}
                        </TableCell>
                        <TableCell>{getStatusBadge(tbl)}</TableCell>
                        <TableCell>
                          <ChevronRight className='ml-auto h-5 w-5 text-muted-foreground' />
                        </TableCell>
                      </TableRow>
                    ))
                  )}
                </TableBody>
              </Table>
            </div>
          </div>
        )}

        {viewState === 'schema' && selectedTable && (
          <div className='space-y-6'>
            <div className='flex items-center justify-between'>
              <h3 className='text-lg font-medium'>
                Schema Definition: {selectedTable.table_name}
              </h3>
              <div className='flex space-x-2'>
                {getStatusBadge(selectedTable)}
              </div>
            </div>

            <Card className='bg-muted/30'>
              <CardContent className='pt-6'>
                <dl className='grid grid-cols-2 gap-4 text-sm'>
                  <div>
                    <dt className='text-muted-foreground'>
                      Source Chain Client ID
                    </dt>
                    <dd className='mt-1'>{selectedTable.chain_client_id}</dd>
                  </div>
                  <div>
                    <dt className='text-muted-foreground'>Record Count</dt>
                    <dd className='mt-1'>
                      {selectedTable.record_count.toLocaleString()}
                    </dd>
                  </div>
                  <div>
                    <dt className='text-muted-foreground'>Last Synced</dt>
                    <dd className='mt-1'>
                      {selectedTable.last_synced_at
                        ? format(
                            new Date(selectedTable.last_synced_at),
                            'MMM d, yyyy HH:mm:ss'
                          )
                        : 'Never'}
                    </dd>
                  </div>
                  <div>
                    <dt className='text-muted-foreground'>Discovered At</dt>
                    <dd className='mt-1'>
                      {format(
                        new Date(selectedTable.created_at),
                        'MMM d, yyyy HH:mm'
                      )}
                    </dd>
                  </div>
                </dl>
              </CardContent>
            </Card>

            <div>
              <h4 className='mb-3 font-medium'>Columns</h4>
              <div className='rounded-md border'>
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
                    {!selectedTable.table_schema ||
                    Object.keys(selectedTable.table_schema).length === 0 ? (
                      <TableRow>
                        <TableCell
                          colSpan={4}
                          className='h-24 text-center text-muted-foreground'
                        >
                          No schema defined.
                        </TableCell>
                      </TableRow>
                    ) : Array.isArray(selectedTable.table_schema) ? (
                      selectedTable.table_schema.map(
                        (field: any, idx: number) => (
                          <TableRow key={idx}>
                            <TableCell className='font-medium'>
                              {field.name}
                            </TableCell>
                            <TableCell className='font-mono text-xs text-muted-foreground'>
                              {field.type}
                            </TableCell>
                            <TableCell>
                              {field.nullable ? 'Yes' : 'No'}
                            </TableCell>
                            <TableCell>
                              {field.primary_key ? 'Yes' : 'No'}
                            </TableCell>
                          </TableRow>
                        )
                      )
                    ) : (
                      <TableRow>
                        <TableCell colSpan={4}>
                          <pre className='overflow-auto rounded bg-muted p-2 text-xs'>
                            {JSON.stringify(
                              selectedTable.table_schema,
                              null,
                              2
                            )}
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
