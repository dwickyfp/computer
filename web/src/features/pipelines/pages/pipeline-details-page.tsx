import { useEffect, useRef, useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useParams, Link, useLocation, useRouter } from '@tanstack/react-router'
import { api } from '@/repo/client'
import { pipelinesRepo } from '@/repo/pipelines'
import { sourcesRepo } from '@/repo/sources'
import {
  GitBranch,
  Table2,
  Database,
  ArrowRight,
  RotateCcw,
  ArrowLeft,
  Code,
  Filter,
  Tag,
  Key,
  Activity,
  AlertCircle,
  RefreshCw,
  Table as TableIcon,
  Layers,
} from 'lucide-react'
import { toast } from 'sonner'
import { Badge } from '@/components/ui/badge'
import {
  Breadcrumb,
  BreadcrumbItem,
  BreadcrumbLink,
  BreadcrumbList,
  BreadcrumbPage,
  BreadcrumbSeparator,
} from '@/components/ui/breadcrumb'
import { Button } from '@/components/ui/button'
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from '@/components/ui/card'
// import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs' // Replaced with CustomTabs
import {
  CustomTabs,
  CustomTabsContent,
  CustomTabsList,
  CustomTabsTrigger,
} from '@/components/ui/custom-tabs'
import { Skeleton } from '@/components/ui/skeleton'
import { Header } from '@/components/layout/header'
import { Main } from '@/components/layout/main'
import { Search } from '@/components/search'
import { ThemeSwitch } from '@/components/theme-switch'
import { BackfillDataTab } from '@/features/pipelines/components/backfill-data-tab'
import { LineageFlowDiagram } from '@/features/pipelines/components/lineage-flow-diagram'
import { PipelineDataFlow } from '@/features/pipelines/components/pipeline-data-flow'
import { PipelineFlowTab } from '@/features/pipelines/components/pipeline-flow-tab'
import { PipelineStatusSwitch } from '@/features/pipelines/components/pipeline-status-switch'
import { RestartButton } from '@/features/pipelines/components/restart-button'
import { usePipelineSelection } from '@/features/pipelines/context/pipeline-selection-context'

interface PipelineNavigationState {
  highlightDestination?: number
  openDrawer?: boolean
  openDrawerDestinationId?: number
  highlightTable?: string
}

interface TableSyncDetails {
  id: number
  pipeline: { id: number; name: string; status: string }
  source: { id: number; name: string; database: string }
  destination: { id: number; name: string; type: string }
  table_name: string
  table_name_target: string
  custom_sql: string | null
  filter_sql: string | null
  primary_key_column_target: string | null
  tags: string[]
  record_count: number
  is_error: boolean
  error_message: string | null
  lineage_metadata: LineageMetadata | null
  lineage_status: string
  lineage_error: string | null
  lineage_generated_at: string | null
  created_at: string
  updated_at: string
}

interface LineageMetadata {
  version: number
  source_tables: { table: string; type: string }[]
  source_columns: string[]
  output_columns: string[]
  column_lineage: Record<string, { sources: string[]; transform: string }>
  referenced_tables: string[]
  parsed_at: string
  error?: string
}

export default function PipelineDetailsPage() {
  const { pipelineId } = useParams({
    from: '/_authenticated/pipelines/$pipelineId',
  })
  const location = useLocation()
  const router = useRouter()
  const queryClient = useQueryClient()
  const id = parseInt(pipelineId)

  // Use context for table selection instead of URL params
  const { selection, clearSelection } = usePipelineSelection()
  const selectedDestId = selection.destId
  const selectedSyncId = selection.syncId

  // Consume navigation state once and clear it from history
  const consumedRef = useRef(false)
  const [navState, setNavState] = useState<PipelineNavigationState>({})

  useEffect(() => {
    if (consumedRef.current) return
    const state = location.state as PipelineNavigationState | undefined
    if (state?.highlightDestination || state?.openDrawer) {
      consumedRef.current = true
      setNavState({ ...state })
      // Replace current history entry to wipe state — refresh will have clean state
      router.navigate({
        to: '/pipelines/$pipelineId',
        params: { pipelineId },
        state: {},
        replace: true,
      })
    }
  }, [location.state, pipelineId, router])

  // 1. Fetch Pipeline
  const {
    data: pipeline,
    isLoading: isPipelineLoading,
    error: pipelineError,
  } = useQuery({
    queryKey: ['pipeline', id],
    queryFn: async () => {
      return await pipelinesRepo.get(id)
    },
    retry: false,
    refetchInterval: 5000, // Refetch every 5 seconds
  })

  // 2. Fetch Source Details using pipeline.source_id
  const { data: sourceDetails, isLoading: isSourceLoading } = useQuery({
    queryKey: ['source-details', pipeline?.source_id],
    queryFn: () => sourcesRepo.getDetails(pipeline!.source_id),
    enabled: !!pipeline?.source_id,
  })

  // 3. Fetch Table Sync Details when selected
  const {
    data: tableSyncData,
    isLoading: isTableSyncLoading,
    error: tableSyncError,
  } = useQuery<TableSyncDetails>({
    queryKey: ['table-sync-details', id, selectedDestId, selectedSyncId],
    queryFn: () =>
      api
        .get(
          `/pipelines/${id}/destinations/${selectedDestId}/tables/${selectedSyncId}`
        )
        .then((r) => r.data),
    enabled: !!selectedDestId && !!selectedSyncId && !isNaN(id),
    refetchInterval: 5000,
    retry: 2,
  })

  // 4. Generate Lineage Mutation
  const generateLineage = useMutation({
    mutationFn: () =>
      api.post(
        `/pipelines/${id}/destinations/${selectedDestId}/tables/${selectedSyncId}/lineage/generate`
      ),
    onSuccess: () => {
      toast.success('Lineage generation started')
      setTimeout(() => {
        queryClient.invalidateQueries({
          queryKey: ['table-sync-details', id, selectedDestId, selectedSyncId],
        })
      }, 300)
    },
    onError: (err: Error) => {
      toast.error(`Failed to generate lineage: ${err.message}`)
    },
  })

  const handleRefresh = async () => {
    if (!pipeline) return
    await pipelinesRepo.refresh(id)
    await sourcesRepo.refreshSource(pipeline.source_id)
    toast.success('Pipeline and Source restarted successfully')
  }

  const handleBackToOverview = () => {
    clearSelection()
  }

  if (pipelineError) {
    return (
      <div className='p-8 text-center text-red-500'>
        Failed to load pipeline details.
      </div>
    )
  }

  const isLoading = isPipelineLoading || (!!pipeline && isSourceLoading)

  // Build destinations summary for header
  const destinationNames =
    pipeline?.destinations?.map((d) => d.destination?.name).filter(Boolean) ||
    []
  const destinationsSummary =
    destinationNames.length > 0
      ? destinationNames.length === 1
        ? destinationNames[0]
        : `${destinationNames.length} destinations`
      : 'No destinations'

  // If table sync is selected, show a clean table details page
  if (selectedSyncId && selectedDestId) {
    return (
      <>
        <Header fixed>
          <Search />
          <div className='ms-auto flex items-center space-x-4'>
            <ThemeSwitch />
          </div>
        </Header>

        <Main className='flex flex-1 flex-col gap-4'>
          <Button
            variant='ghost'
            size='sm'
            className='w-fit'
            onClick={handleBackToOverview}
          >
            <ArrowLeft className='mr-2 h-4 w-4' />
            Back to Pipeline Overview
          </Button>

          {isTableSyncLoading ? (
            <>
              <Skeleton className='h-8 w-64' />
              <div className='grid grid-cols-1 gap-4 md:grid-cols-3'>
                <Skeleton className='h-32' />
                <Skeleton className='h-32' />
                <Skeleton className='h-32' />
              </div>
              <Skeleton className='h-96 w-full' />
            </>
          ) : tableSyncData ? (
            <>
              {/* Header with table name */}
              <div className='flex items-center gap-2'>
                <TableIcon className='h-5 w-5' />
                <h3 className='text-2xl font-bold'>
                  {tableSyncData.table_name_target}
                </h3>
                {tableSyncData.is_error && (
                  <Badge variant='destructive'>Error</Badge>
                )}
              </div>

              <CustomTabs defaultValue='details'>
                <CustomTabsList>
                  <CustomTabsTrigger value='details'>Details</CustomTabsTrigger>
                  <CustomTabsTrigger value='lineage'>
                    Data Lineage
                  </CustomTabsTrigger>
                </CustomTabsList>

                <CustomTabsContent value='details' className='space-y-4'>
                  {/* Overview Cards */}
                  <div className='grid grid-cols-1 gap-4 md:grid-cols-3'>
                    <Card>
                      <CardHeader className='pb-2'>
                        <CardTitle className='flex items-center gap-2 text-sm font-medium'>
                          <Database className='h-4 w-4 text-blue-500' />
                          Source
                        </CardTitle>
                      </CardHeader>
                      <CardContent>
                        <p className='text-lg font-semibold'>
                          {tableSyncData.source.name}
                        </p>
                        <p className='text-sm text-muted-foreground'>
                          Database: {tableSyncData.source.database}
                        </p>
                        <p className='mt-1 text-sm text-muted-foreground'>
                          Table:{' '}
                          <code className='rounded bg-muted px-1'>
                            {tableSyncData.table_name}
                          </code>
                        </p>
                      </CardContent>
                    </Card>

                    <Card>
                      <CardHeader className='pb-2'>
                        <CardTitle className='flex items-center gap-2 text-sm font-medium'>
                          <Layers className='h-4 w-4 text-purple-500' />
                          Destination
                        </CardTitle>
                      </CardHeader>
                      <CardContent>
                        <p className='text-lg font-semibold'>
                          {tableSyncData.destination.name}
                        </p>
                        <div className='mt-1 flex items-center gap-2'>
                          <span className='text-sm text-muted-foreground'>
                            Type:
                          </span>
                          <Badge variant='outline'>
                            {tableSyncData.destination.type}
                          </Badge>
                        </div>
                      </CardContent>
                    </Card>

                    <Card>
                      <CardHeader className='pb-2'>
                        <CardTitle className='flex items-center gap-2 text-sm font-medium'>
                          <Activity className='h-4 w-4 text-green-500' />
                          Statistics
                        </CardTitle>
                      </CardHeader>
                      <CardContent>
                        <p className='text-lg font-semibold'>
                          {tableSyncData.record_count.toLocaleString()} records
                        </p>
                        <p className='text-sm text-muted-foreground'>
                          Target:{' '}
                          <code className='rounded bg-muted px-1'>
                            {tableSyncData.table_name_target}
                          </code>
                        </p>
                        {tableSyncData.primary_key_column_target && (
                          <div className='mt-1 flex items-center gap-1 text-sm text-muted-foreground'>
                            <Key className='h-3 w-3' />
                            <span>
                              PK: {tableSyncData.primary_key_column_target}
                            </span>
                          </div>
                        )}
                      </CardContent>
                    </Card>
                  </div>

                  {/* Custom SQL */}
                  {tableSyncData.custom_sql && (
                    <Card>
                      <CardHeader>
                        <CardTitle className='flex items-center gap-2 text-sm font-medium'>
                          <Code className='h-4 w-4 text-orange-500' />
                          Custom SQL
                        </CardTitle>
                        <CardDescription>
                          SQL transformation applied during replication
                        </CardDescription>
                      </CardHeader>
                      <CardContent>
                        <pre className='overflow-x-auto rounded-lg bg-muted p-4 font-mono text-sm whitespace-pre-wrap'>
                          {tableSyncData.custom_sql}
                        </pre>
                      </CardContent>
                    </Card>
                  )}

                  {/* Filter SQL */}
                  {tableSyncData.filter_sql && (
                    <Card>
                      <CardHeader>
                        <CardTitle className='flex items-center gap-2 text-sm font-medium'>
                          <Filter className='h-4 w-4 text-cyan-500' />
                          Filter Configuration
                        </CardTitle>
                        <CardDescription>
                          Row-level filtering applied to source data
                        </CardDescription>
                      </CardHeader>
                      <CardContent>
                        <pre className='overflow-x-auto rounded-lg bg-muted p-4 font-mono text-sm whitespace-pre-wrap'>
                          {tableSyncData.filter_sql}
                        </pre>
                      </CardContent>
                    </Card>
                  )}

                  {/* Tags */}
                  {tableSyncData.tags.length > 0 && (
                    <Card>
                      <CardHeader>
                        <CardTitle className='flex items-center gap-2 text-sm font-medium'>
                          <Tag className='h-4 w-4 text-pink-500' />
                          Tags
                        </CardTitle>
                      </CardHeader>
                      <CardContent>
                        <div className='flex flex-wrap gap-2'>
                          {tableSyncData.tags.map((tag) => (
                            <Badge key={tag} variant='secondary'>
                              {tag}
                            </Badge>
                          ))}
                        </div>
                      </CardContent>
                    </Card>
                  )}

                  {/* Error State */}
                  {tableSyncData.is_error && tableSyncData.error_message && (
                    <Card className='border-destructive'>
                      <CardHeader>
                        <CardTitle className='flex items-center gap-2 text-sm font-medium text-destructive'>
                          <AlertCircle className='h-4 w-4' />
                          Sync Error
                        </CardTitle>
                      </CardHeader>
                      <CardContent>
                        <pre className='text-sm whitespace-pre-wrap text-destructive'>
                          {tableSyncData.error_message}
                        </pre>
                      </CardContent>
                    </Card>
                  )}

                  {/* Metadata */}
                  <Card>
                    <CardHeader>
                      <CardTitle className='text-sm font-medium'>
                        Metadata
                      </CardTitle>
                    </CardHeader>
                    <CardContent>
                      <div className='grid grid-cols-2 gap-4 text-sm'>
                        <div>
                          <span className='text-muted-foreground'>
                            Created:
                          </span>
                          <span className='ml-2'>
                            {new Date(
                              tableSyncData.created_at
                            ).toLocaleString()}
                          </span>
                        </div>
                        <div>
                          <span className='text-muted-foreground'>
                            Updated:
                          </span>
                          <span className='ml-2'>
                            {new Date(
                              tableSyncData.updated_at
                            ).toLocaleString()}
                          </span>
                        </div>
                      </div>
                    </CardContent>
                  </Card>
                </CustomTabsContent>

                <CustomTabsContent value='lineage' className='space-y-4'>
                  <div className='flex items-center justify-between'>
                    <div>
                      <h3 className='flex items-center gap-2 text-lg font-semibold'>
                        <GitBranch className='h-5 w-5 text-primary' />
                        Column-Level Lineage
                      </h3>
                      <p className='text-sm text-muted-foreground'>
                        Visualize how source columns map to destination columns
                      </p>
                    </div>
                    <div className='flex items-center gap-2'>
                      {tableSyncData.lineage_generated_at && (
                        <span className='text-xs text-muted-foreground'>
                          Generated:{' '}
                          {new Date(
                            tableSyncData.lineage_generated_at
                          ).toLocaleString()}
                        </span>
                      )}
                      <Button
                        variant='outline'
                        size='sm'
                        onClick={() => generateLineage.mutate()}
                        disabled={
                          generateLineage.isPending ||
                          tableSyncData.lineage_status === 'GENERATING'
                        }
                      >
                        <RefreshCw
                          className={`mr-2 h-4 w-4 ${
                            generateLineage.isPending ||
                            tableSyncData.lineage_status === 'GENERATING'
                              ? 'animate-spin'
                              : ''
                          }`}
                        />
                        {tableSyncData.lineage_status === 'GENERATING'
                          ? 'Generating...'
                          : 'Generate Lineage'}
                      </Button>
                    </div>
                  </div>

                  {tableSyncData.lineage_status === 'FAILED' &&
                    tableSyncData.lineage_error && (
                      <Card className='border-destructive'>
                        <CardContent className='pt-4'>
                          <div className='flex items-center gap-2 text-destructive'>
                            <AlertCircle className='h-4 w-4' />
                            <p className='text-sm'>
                              {tableSyncData.lineage_error}
                            </p>
                          </div>
                        </CardContent>
                      </Card>
                    )}

                  {tableSyncData.lineage_status === 'PENDING' && (
                    <Card>
                      <CardContent className='py-12 pt-6 text-center'>
                        <GitBranch className='mx-auto mb-4 h-12 w-12 text-muted-foreground' />
                        <p className='mb-4 text-muted-foreground'>
                          Lineage has not been generated yet.
                        </p>
                        <Button
                          variant='default'
                          onClick={() => generateLineage.mutate()}
                          disabled={generateLineage.isPending}
                        >
                          <RefreshCw
                            className={`mr-2 h-4 w-4 ${generateLineage.isPending ? 'animate-spin' : ''}`}
                          />
                          Generate Lineage
                        </Button>
                      </CardContent>
                    </Card>
                  )}

                  {tableSyncData.lineage_status === 'GENERATING' && (
                    <Card>
                      <CardContent className='py-12 pt-6 text-center'>
                        <RefreshCw className='mx-auto mb-4 h-12 w-12 animate-spin text-primary' />
                        <p className='text-muted-foreground'>
                          Analyzing SQL and generating lineage...
                        </p>
                      </CardContent>
                    </Card>
                  )}

                  {tableSyncData.lineage_status === 'COMPLETED' &&
                    tableSyncData.lineage_metadata && (
                      <>
                        {tableSyncData.lineage_metadata.error ? (
                          <Card className='border-yellow-500'>
                            <CardContent className='pt-4'>
                              <div className='flex items-center gap-2 text-yellow-600'>
                                <AlertCircle className='h-4 w-4' />
                                <p className='text-sm'>
                                  Parse warning:{' '}
                                  {tableSyncData.lineage_metadata.error}
                                </p>
                              </div>
                            </CardContent>
                          </Card>
                        ) : null}
                        <Card className='h-[500px]'>
                          <CardContent className='h-full p-0'>
                            <LineageFlowDiagram
                              lineage={tableSyncData.lineage_metadata}
                              destinationName={
                                tableSyncData.table_name_target ||
                                tableSyncData.table_name
                              }
                              sourceName={tableSyncData.source.name}
                            />
                          </CardContent>
                        </Card>
                      </>
                    )}
                </CustomTabsContent>
              </CustomTabs>
            </>
          ) : (
            <div className='flex items-center justify-center p-8'>
              <div className='text-center'>
                <AlertCircle className='mx-auto mb-4 h-12 w-12 text-destructive' />
                <p className='text-destructive'>Failed to load table details</p>
                <p className='mt-2 text-sm text-muted-foreground'>
                  Pipeline: {id}, Destination: {selectedDestId}, Sync:{' '}
                  {selectedSyncId}
                </p>
                {tableSyncError && (
                  <p className='mt-2 text-xs text-destructive/70'>
                    {(tableSyncError as Error).message === 'Network Error'
                      ? 'Cannot connect to backend server. Please ensure the backend is running.'
                      : (tableSyncError as Error).message ||
                        String(tableSyncError)}
                  </p>
                )}
                <Button
                  variant='outline'
                  size='sm'
                  className='mt-4'
                  onClick={() =>
                    queryClient.invalidateQueries({
                      queryKey: [
                        'table-sync-details',
                        id,
                        selectedDestId,
                        selectedSyncId,
                      ],
                    })
                  }
                >
                  <RefreshCw className='mr-2 h-4 w-4' />
                  Retry
                </Button>
              </div>
            </div>
          )}
        </Main>
      </>
    )
  }

  return (
    <>
      <Header fixed>
        <Search />
        <div className='ms-auto flex items-center space-x-4'>
          <ThemeSwitch />
        </div>
      </Header>

      <Main className='flex flex-1 flex-col gap-4'>
        {/* Redesigned Compact Header */}
        <div className='flex flex-col gap-1'>
          <Breadcrumb className='mb-1'>
            <BreadcrumbList>
              <BreadcrumbItem>
                <BreadcrumbLink asChild>
                  <Link to='/pipelines'>Pipelines</Link>
                </BreadcrumbLink>
              </BreadcrumbItem>
              <BreadcrumbSeparator />
              <BreadcrumbItem>
                <BreadcrumbPage>
                  {pipeline?.name || 'Loading...'}
                </BreadcrumbPage>
              </BreadcrumbItem>
            </BreadcrumbList>
          </Breadcrumb>

          <div className='flex items-start justify-between gap-4'>
            <div className='space-y-1'>
              <h2 className='text-3xl font-bold tracking-tight dark:text-[#d5dae4]'>
                {isPipelineLoading ? (
                  <Skeleton className='h-9 w-64' />
                ) : (
                  pipeline?.name
                )}
              </h2>
              <div className='flex items-center gap-2 text-sm text-muted-foreground'>
                {isPipelineLoading ? (
                  <Skeleton className='h-4 w-48' />
                ) : (
                  <div className='inline-flex items-center gap-2 rounded-sm bg-secondary/50 px-3 py-1.5 text-xs font-medium text-[#7b828f] ring-1 ring-gray-500/10 ring-inset dark:bg-[#0f161d] dark:text-[#7b828f]'>
                    <div className='flex items-center gap-1.5 opacity-90 transition-opacity hover:opacity-100'>
                      <Database className='h-3.5 w-3.5' />
                      <span>{pipeline?.source?.name}</span>
                    </div>
                    <ArrowRight className='h-3 w-3 opacity-40' />
                    <div className='flex items-center gap-1.5 opacity-90 transition-opacity hover:opacity-100'>
                      <Database className='h-3.5 w-3.5' />
                      <span>{destinationsSummary}</span>
                    </div>
                  </div>
                )}
              </div>
            </div>
            <div className='flex items-center gap-3 pt-1'>
              {pipeline && <PipelineStatusSwitch pipeline={pipeline} />}
              <RestartButton
                onRestart={handleRefresh}
                disabled={isLoading || pipeline?.status === 'PAUSE'}
              />
            </div>
          </div>
        </div>

        {/* Tabbed Content */}
        <CustomTabs defaultValue='flow-destination' className='w-full flex-1'>
          <CustomTabsList className='mb-4 w-full justify-start border-b'>
            <CustomTabsTrigger value='flow-destination'>
              <GitBranch className='mr-2 h-4 w-4' />
              Flow Destination
            </CustomTabsTrigger>
            <CustomTabsTrigger value='flow-data'>
              <Table2 className='mr-2 h-4 w-4' />
              Flow Data
            </CustomTabsTrigger>
            <CustomTabsTrigger value='backfill'>
              <RotateCcw className='mr-2 h-4 w-4' />
              Backfill Data
            </CustomTabsTrigger>
          </CustomTabsList>

          {/* Flow Destination Tab */}
          <CustomTabsContent value='flow-destination' className='mt-0'>
            {isPipelineLoading ? (
              <div className='flex h-125 items-center justify-center'>
                <Skeleton className='h-full w-full rounded-lg' />
              </div>
            ) : pipeline ? (
              <PipelineFlowTab
                pipeline={pipeline}
                highlightDestination={navState.highlightDestination}
                openDrawer={navState.openDrawer}
                openDrawerDestinationId={navState.openDrawerDestinationId}
                highlightTable={navState.highlightTable}
                onClearHighlight={() => setNavState({})}
              />
            ) : (
              <div className='p-4 text-muted-foreground'>
                Pipeline not found.
              </div>
            )}
          </CustomTabsContent>

          {/* Flow Data Tab */}
          <CustomTabsContent value='flow-data' className='mt-0'>
            {isLoading ? (
              <div className='space-y-2'>
                <Skeleton className='h-10 w-full' />
                <Skeleton className='h-10 w-full' />
                <Skeleton className='h-10 w-full' />
              </div>
            ) : sourceDetails && pipeline ? (
              <PipelineDataFlow pipeline={pipeline} />
            ) : (
              <div className='p-4 text-muted-foreground'>
                No source details available.
              </div>
            )}
          </CustomTabsContent>

          {/* Backfill Data Tab */}
          <CustomTabsContent value='backfill' className='mt-0'>
            {isLoading ? (
              <div className='space-y-2'>
                <Skeleton className='h-10 w-full' />
                <Skeleton className='h-10 w-full' />
                <Skeleton className='h-10 w-full' />
              </div>
            ) : pipeline ? (
              <BackfillDataTab
                pipelineId={pipeline.id}
                sourceId={pipeline.source_id}
                pipeline={pipeline}
              />
            ) : (
              <div className='p-4 text-muted-foreground'>
                Pipeline not found.
              </div>
            )}
          </CustomTabsContent>
        </CustomTabs>
      </Main>
    </>
  )
}
