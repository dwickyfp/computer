import { useState } from 'react'
import { format } from 'date-fns'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { catalogRepo, CatalogDatabase, CatalogTable } from '@/repo/catalog'
import {
  ChevronRight,
  Database,
  Table as TableIcon,
  Layers,
  Loader2,
  Pencil,
  Plus,
  Trash2,
  HardDrive,
  Check,
  X,
} from 'lucide-react'
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
  AlertDialogTrigger,
} from '@/components/ui/alert-dialog'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardHeader } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'

type ViewState = 'databases' | 'tables' | 'schema'

export function LocalDataExplorer() {
  const queryClient = useQueryClient()
  const [viewState, setViewState] = useState<ViewState>('databases')
  const [selectedDb, setSelectedDb] = useState<CatalogDatabase | null>(null)
  const [selectedTable, setSelectedTable] = useState<CatalogTable | null>(null)

  // Create form state
  const [showCreateForm, setShowCreateForm] = useState(false)
  const [newDbName, setNewDbName] = useState('')
  const [newDbDescription, setNewDbDescription] = useState('')

  // Rename state
  const [renamingDbId, setRenamingDbId] = useState<number | null>(null)
  const [renameValue, setRenameValue] = useState('')

  // ─── Queries ──────────────────────────────────────────────────────────────

  const { data: databases, isLoading: loadingDbs } = useQuery({
    queryKey: ['catalog-databases'],
    queryFn: catalogRepo.getDatabases,
    staleTime: 5000,
    refetchInterval: 30_000,
  })

  const { data: tables, isLoading: loadingTables } = useQuery({
    queryKey: ['catalog-tables', selectedDb?.id],
    queryFn: () => catalogRepo.getTables(selectedDb!.id),
    enabled: !!selectedDb && (viewState === 'tables' || viewState === 'schema'),
    staleTime: 5000,
    refetchInterval: 30_000,
  })

  // ─── Mutations ────────────────────────────────────────────────────────────

  const createMutation = useMutation({
    mutationFn: catalogRepo.createDatabase,
    onSuccess: () => {
      setTimeout(() => {
        queryClient.invalidateQueries({ queryKey: ['catalog-databases'] })
      }, 300)
      setShowCreateForm(false)
      setNewDbName('')
      setNewDbDescription('')
    },
    onError: () => {},
  })

  const deleteMutation = useMutation({
    mutationFn: catalogRepo.deleteDatabase,
    onSuccess: () => {
      setTimeout(() => {
        queryClient.invalidateQueries({ queryKey: ['catalog-databases'] })
      }, 300)
    },
    onError: () => {},
  })

  const renameMutation = useMutation({
    mutationFn: ({ id, name }: { id: number; name: string }) =>
      catalogRepo.updateDatabase(id, { name }),
    onSuccess: () => {
      setRenamingDbId(null)
      setRenameValue('')
      setTimeout(() => {
        queryClient.invalidateQueries({ queryKey: ['catalog-databases'] })
      }, 300)
    },
    onError: () => {},
  })

  const handleRenameStart = (db: CatalogDatabase, e: React.MouseEvent) => {
    e.stopPropagation()
    setRenamingDbId(db.id)
    setRenameValue(db.name)
  }

  const handleRenameConfirm = (id: number, e?: React.MouseEvent) => {
    e?.stopPropagation()
    if (
      renameValue.trim() &&
      renameValue.trim() !==
        databases?.find((d: CatalogDatabase) => d.id === id)?.name
    ) {
      renameMutation.mutate({ id, name: renameValue.trim() })
    } else {
      setRenamingDbId(null)
    }
  }

  const handleRenameCancel = (e?: React.MouseEvent) => {
    e?.stopPropagation()
    setRenamingDbId(null)
    setRenameValue('')
  }

  // ─── Navigation ───────────────────────────────────────────────────────────

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

  const handleCreateSubmit = (e: React.FormEvent) => {
    e.preventDefault()
    if (!newDbName.trim()) return
    createMutation.mutate({
      name: newDbName.trim(),
      description: newDbDescription.trim() || undefined,
    })
  }

  const getStatusBadge = (status: string) => {
    if (status === 'ACTIVE')
      return <Badge className='bg-green-500 hover:bg-green-600'>Active</Badge>
    if (status === 'INACTIVE')
      return <Badge variant='secondary'>Inactive</Badge>
    return <Badge variant='outline'>{status}</Badge>
  }

  return (
    <Card className='flex h-full flex-col'>
      <CardHeader className='border-b pb-3'>
        {/* Breadcrumb */}
        <div className='flex items-center space-x-2 text-sm text-muted-foreground'>
          <button
            onClick={() => handleBreadcrumbClick('databases')}
            className={`flex items-center transition-colors hover:text-foreground ${viewState === 'databases' ? 'font-medium text-foreground' : ''}`}
          >
            <HardDrive className='mr-1 h-4 w-4' />
            My Databases
          </button>

          {selectedDb && (
            <>
              <ChevronRight className='h-4 w-4' />
              <button
                onClick={() => handleBreadcrumbClick('tables')}
                className={`flex items-center transition-colors hover:text-foreground ${viewState === 'tables' ? 'font-medium text-foreground' : ''}`}
              >
                <Database className='mr-1 h-4 w-4' />
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
        {/* ── Databases view ─────────────────────────────────────────────── */}
        {viewState === 'databases' && (
          <div className='space-y-6'>
            <div className='flex items-end justify-between gap-4'>
              <div>
                <h3 className='text-lg font-medium'>Local Databases</h3>
                <p className='text-sm text-muted-foreground'>
                  Logical database containers on this Rosetta instance.
                </p>
              </div>
              <Button
                size='sm'
                onClick={() => {
                  setShowCreateForm(true)
                  setNewDbName('')
                  setNewDbDescription('')
                }}
                disabled={showCreateForm}
              >
                <Plus className='mr-1.5 h-4 w-4' />
                New Database
              </Button>
            </div>

            {/* Inline create form */}
            {showCreateForm && (
              <form
                onSubmit={handleCreateSubmit}
                className='space-y-3 rounded-md border bg-muted/30 p-4'
              >
                <p className='text-sm font-medium'>Create Database</p>
                <div className='grid grid-cols-2 gap-3'>
                  <div className='space-y-1'>
                    <Label htmlFor='db-name'>Name *</Label>
                    <Input
                      id='db-name'
                      placeholder='e.g. prod_warehouse'
                      value={newDbName}
                      onChange={(e) => setNewDbName(e.target.value)}
                      autoFocus
                    />
                  </div>
                  <div className='space-y-1'>
                    <Label htmlFor='db-desc'>Description</Label>
                    <Input
                      id='db-desc'
                      placeholder='Optional description'
                      value={newDbDescription}
                      onChange={(e) => setNewDbDescription(e.target.value)}
                    />
                  </div>
                </div>
                <div className='flex justify-end gap-2'>
                  <Button
                    type='button'
                    variant='outline'
                    size='sm'
                    onClick={() => setShowCreateForm(false)}
                  >
                    Cancel
                  </Button>
                  <Button
                    type='submit'
                    size='sm'
                    disabled={!newDbName.trim() || createMutation.isPending}
                  >
                    {createMutation.isPending && (
                      <Loader2 className='mr-1.5 h-3.5 w-3.5 animate-spin' />
                    )}
                    Create
                  </Button>
                </div>
              </form>
            )}

            <div className='rounded-md border'>
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Name</TableHead>
                    <TableHead>Description</TableHead>
                    <TableHead>Created</TableHead>
                    <TableHead className='w-[100px]'></TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {loadingDbs ? (
                    <TableRow>
                      <TableCell colSpan={4} className='h-24 text-center'>
                        <Loader2 className='mx-auto h-5 w-5 animate-spin' />
                      </TableCell>
                    </TableRow>
                  ) : !databases || databases.length === 0 ? (
                    <TableRow>
                      <TableCell
                        colSpan={4}
                        className='h-24 text-center text-muted-foreground'
                      >
                        No databases yet. Create one to get started.
                      </TableCell>
                    </TableRow>
                  ) : (
                    databases.map((db: CatalogDatabase) => (
                      <TableRow
                        key={db.id}
                        className='cursor-pointer transition-colors hover:bg-muted/50'
                        onClick={() =>
                          renamingDbId === db.id ? undefined : handleDbClick(db)
                        }
                      >
                        <TableCell
                          className='flex items-center font-medium'
                          onClick={(e) =>
                            renamingDbId === db.id
                              ? e.stopPropagation()
                              : undefined
                          }
                        >
                          <Database className='mr-2 h-4 w-4 flex-shrink-0 text-muted-foreground' />
                          {renamingDbId === db.id ? (
                            <input
                              autoFocus
                              className='w-40 border-b border-primary bg-transparent px-1 text-sm font-medium outline-none'
                              value={renameValue}
                              onChange={(e) => setRenameValue(e.target.value)}
                              onClick={(e) => e.stopPropagation()}
                              onKeyDown={(e) => {
                                if (e.key === 'Enter')
                                  handleRenameConfirm(db.id)
                                if (e.key === 'Escape') handleRenameCancel()
                              }}
                            />
                          ) : (
                            db.name
                          )}
                        </TableCell>
                        <TableCell className='text-sm text-muted-foreground'>
                          {db.description ?? '—'}
                        </TableCell>
                        <TableCell className='text-sm text-muted-foreground'>
                          {format(new Date(db.created_at), 'MMM d, yyyy')}
                        </TableCell>
                        <TableCell
                          onClick={(e) => e.stopPropagation()}
                          className='flex items-center gap-0.5'
                        >
                          {renamingDbId === db.id ? (
                            <>
                              <Button
                                variant='ghost'
                                size='icon'
                                className='h-8 w-8 text-green-600 hover:text-green-700'
                                disabled={renameMutation.isPending}
                                onClick={(e) => handleRenameConfirm(db.id, e)}
                              >
                                <Check className='h-4 w-4' />
                              </Button>
                              <Button
                                variant='ghost'
                                size='icon'
                                className='h-8 w-8 text-muted-foreground hover:text-foreground'
                                onClick={handleRenameCancel}
                              >
                                <X className='h-4 w-4' />
                              </Button>
                            </>
                          ) : (
                            <>
                              <Button
                                variant='ghost'
                                size='icon'
                                className='h-8 w-8 text-muted-foreground hover:text-foreground'
                                onClick={(e) => handleRenameStart(db, e)}
                              >
                                <Pencil className='h-3.5 w-3.5' />
                              </Button>
                              <AlertDialog>
                                <AlertDialogTrigger asChild>
                                  <Button
                                    variant='ghost'
                                    size='icon'
                                    className='h-8 w-8 text-muted-foreground hover:text-destructive'
                                    disabled={deleteMutation.isPending}
                                  >
                                    <Trash2 className='h-4 w-4' />
                                  </Button>
                                </AlertDialogTrigger>
                                <AlertDialogContent>
                                  <AlertDialogHeader>
                                    <AlertDialogTitle>
                                      Delete "{db.name}"?
                                    </AlertDialogTitle>
                                    <AlertDialogDescription>
                                      This will permanently delete the database
                                      and all its registered tables. This action
                                      cannot be undone.
                                    </AlertDialogDescription>
                                  </AlertDialogHeader>
                                  <AlertDialogFooter>
                                    <AlertDialogCancel>
                                      Cancel
                                    </AlertDialogCancel>
                                    <AlertDialogAction
                                      className='text-destructive-foreground bg-destructive hover:bg-destructive/90'
                                      onClick={() =>
                                        deleteMutation.mutate(db.id)
                                      }
                                    >
                                      Delete
                                    </AlertDialogAction>
                                  </AlertDialogFooter>
                                </AlertDialogContent>
                              </AlertDialog>
                            </>
                          )}
                        </TableCell>
                      </TableRow>
                    ))
                  )}
                </TableBody>
              </Table>
            </div>
          </div>
        )}

        {/* ── Tables view ────────────────────────────────────────────────── */}
        {viewState === 'tables' && (
          <div className='space-y-4'>
            <div className='flex items-center justify-between'>
              <h3 className='text-lg font-medium'>
                Tables in {selectedDb?.name}
              </h3>
            </div>
            <div className='rounded-md border'>
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Table Name</TableHead>
                    <TableHead>Stream</TableHead>
                    <TableHead>Status</TableHead>
                    <TableHead>Last Health Check</TableHead>
                    <TableHead className='w-[60px]'></TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {loadingTables ? (
                    <TableRow>
                      <TableCell colSpan={5} className='h-24 text-center'>
                        <Loader2 className='mx-auto h-5 w-5 animate-spin' />
                      </TableCell>
                    </TableRow>
                  ) : !tables || tables.length === 0 ? (
                    <TableRow>
                      <TableCell
                        colSpan={5}
                        className='h-24 text-center text-muted-foreground'
                      >
                        No tables registered in this database yet.
                      </TableCell>
                    </TableRow>
                  ) : (
                    tables.map((tbl: CatalogTable) => (
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
                          {tbl.stream_name}
                        </TableCell>
                        <TableCell>{getStatusBadge(tbl.status)}</TableCell>
                        <TableCell className='text-sm text-muted-foreground'>
                          {tbl.last_health_check_at
                            ? format(
                                new Date(tbl.last_health_check_at),
                                'MMM d, HH:mm'
                              )
                            : '—'}
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

        {/* ── Schema view ────────────────────────────────────────────────── */}
        {viewState === 'schema' && selectedTable && (
          <div className='space-y-6'>
            <div className='flex items-center justify-between'>
              <h3 className='text-lg font-medium'>
                Schema: {selectedTable.table_name}
              </h3>
              {getStatusBadge(selectedTable.status)}
            </div>

            <Card className='bg-muted/30'>
              <CardContent className='pt-6'>
                <dl className='grid grid-cols-2 gap-4 text-sm'>
                  <div>
                    <dt className='text-muted-foreground'>Stream Name</dt>
                    <dd className='mt-1 font-mono text-xs'>
                      {selectedTable.stream_name}
                    </dd>
                  </div>
                  <div>
                    <dt className='text-muted-foreground'>Source Chain ID</dt>
                    <dd className='mt-1'>
                      {selectedTable.source_chain_id ?? '—'}
                    </dd>
                  </div>
                  <div>
                    <dt className='text-muted-foreground'>Last Health Check</dt>
                    <dd className='mt-1'>
                      {selectedTable.last_health_check_at
                        ? format(
                            new Date(selectedTable.last_health_check_at),
                            'MMM d, yyyy HH:mm:ss'
                          )
                        : 'Never'}
                    </dd>
                  </div>
                  <div>
                    <dt className='text-muted-foreground'>Registered At</dt>
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
                    {!selectedTable.schema_json ||
                    Object.keys(selectedTable.schema_json).length === 0 ? (
                      <TableRow>
                        <TableCell
                          colSpan={4}
                          className='h-24 text-center text-muted-foreground'
                        >
                          No schema defined.
                        </TableCell>
                      </TableRow>
                    ) : Array.isArray(selectedTable.schema_json.fields) ? (
                      selectedTable.schema_json.fields.map(
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
