import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useParams, Link } from '@tanstack/react-router'
import { pipelinesRepo, type TableSyncDetails } from '@/repo/pipelines'
import { pipelineKeys } from '@/repo/query-keys'
import {
  RefreshCw,
  Database,
  Layers,
  Table,
  Code,
  Filter,
  Tag,
  GitBranch,
  Key,
  Activity,
  AlertCircle,
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
import {
  CustomTabs,
  CustomTabsList,
  CustomTabsTrigger,
  CustomTabsContent,
} from '@/components/ui/custom-tabs'
import { Skeleton } from '@/components/ui/skeleton'
import { Header } from '@/components/layout/header'
import { Main } from '@/components/layout/main'
import { Search } from '@/components/search'
import { ThemeSwitch } from '@/components/theme-switch'
import { LineageFlowDiagram } from '../components/lineage-flow-diagram'

export default function TableSyncDetailsPage() {
  const params = useParams({
    from: '/_authenticated/pipelines/$pipelineId/destinations/$destId/tables/$syncId',
  })
  const pipelineId = Number(params.pipelineId)
  const destId = Number(params.destId)
  const syncId = Number(params.syncId)
  const queryClient = useQueryClient()

  const { data, isLoading, error } = useQuery<TableSyncDetails>({
    queryKey: pipelineKeys.tableSyncDetails(pipelineId, destId, syncId),
    queryFn: () => pipelinesRepo.getTableSyncDetail(pipelineId, destId, syncId),
    refetchInterval: 5000,
  })

  const generateLineage = useMutation({
    mutationFn: () => pipelinesRepo.generateLineage(pipelineId, destId, syncId),
    onSuccess: () => {
      toast.success('Lineage generation started')
      setTimeout(() => {
        queryClient.invalidateQueries({
          queryKey: pipelineKeys.tableSyncDetails(pipelineId, destId, syncId),
        })
      }, 300)
    },
    onError: (err: Error) => {
      toast.error(`Failed to generate lineage: ${err.message}`)
    },
  })

  if (isLoading) {
    return (
      <>
        <Header fixed>
          <Search />
          <div className='ml-auto flex items-center space-x-4'>
            <ThemeSwitch />
          </div>
        </Header>
        <Main className='flex flex-1 flex-col gap-4'>
          <Skeleton className='h-8 w-64' />
          <div className='grid grid-cols-1 gap-4 md:grid-cols-3'>
            <Skeleton className='h-32' />
            <Skeleton className='h-32' />
            <Skeleton className='h-32' />
          </div>
          <Skeleton className='h-96 w-full' />
        </Main>
      </>
    )
  }

  if (error || !data) {
    return (
      <>
        <Header fixed>
          <Search />
          <div className='ml-auto flex items-center space-x-4'>
            <ThemeSwitch />
          </div>
        </Header>
        <Main className='flex items-center justify-center'>
          <div className='text-center'>
            <AlertCircle className='mx-auto mb-4 h-12 w-12 text-destructive' />
            <p className='text-destructive'>Failed to load table details</p>
          </div>
        </Main>
      </>
    )
  }

  return (
    <>
      <Header fixed>
        <Search />
        <div className='ml-auto flex items-center space-x-4'>
          <ThemeSwitch />
        </div>
      </Header>
      <Main className='flex flex-1 flex-col gap-4'>
        <Breadcrumb>
          <BreadcrumbList>
            <BreadcrumbItem>
              <BreadcrumbLink asChild>
                <Link to='/pipelines'>Pipelines</Link>
              </BreadcrumbLink>
            </BreadcrumbItem>
            <BreadcrumbSeparator />
            <BreadcrumbItem>
              <BreadcrumbLink asChild>
                <Link
                  to='/pipelines/$pipelineId'
                  params={{ pipelineId: String(pipelineId) }}
                >
                  {data.pipeline.name}
                </Link>
              </BreadcrumbLink>
            </BreadcrumbItem>
            <BreadcrumbSeparator />
            <BreadcrumbItem>
              <BreadcrumbPage className='flex items-center gap-2'>
                <Table className='h-4 w-4' />
                {data.table_name_target}
                {data.is_error && (
                  <Badge variant='destructive' className='ml-2'>
                    Error
                  </Badge>
                )}
              </BreadcrumbPage>
            </BreadcrumbItem>
          </BreadcrumbList>
        </Breadcrumb>

        <CustomTabs defaultValue='details'>
          <CustomTabsList>
            <CustomTabsTrigger value='details'>Details</CustomTabsTrigger>
            <CustomTabsTrigger value='lineage'>Data Lineage</CustomTabsTrigger>
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
                  <p className='text-lg font-semibold'>{data.source.name}</p>
                  <p className='text-sm text-muted-foreground'>
                    Database: {data.source.database}
                  </p>
                  <p className='mt-1 text-sm text-muted-foreground'>
                    Table:{' '}
                    <code className='rounded bg-muted px-1'>
                      {data.table_name}
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
                    {data.destination.name}
                  </p>
                  <div className='mt-1 flex items-center gap-2'>
                    <span className='text-sm text-muted-foreground'>Type:</span>
                    <Badge variant='outline'>{data.destination.type}</Badge>
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
                    {data.record_count.toLocaleString()} records
                  </p>
                  <p className='text-sm text-muted-foreground'>
                    Target:{' '}
                    <code className='rounded bg-muted px-1'>
                      {data.table_name_target}
                    </code>
                  </p>
                  {data.primary_key_column_target && (
                    <div className='mt-1 flex items-center gap-1 text-sm text-muted-foreground'>
                      <Key className='h-3 w-3' />
                      <span>PK: {data.primary_key_column_target}</span>
                    </div>
                  )}
                </CardContent>
              </Card>
            </div>

            {/* Custom SQL */}
            {data.custom_sql && (
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
                    {data.custom_sql}
                  </pre>
                </CardContent>
              </Card>
            )}

            {/* Filter SQL */}
            {data.filter_sql && (
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
                    {data.filter_sql}
                  </pre>
                </CardContent>
              </Card>
            )}

            {/* Tags */}
            {data.tags.length > 0 && (
              <Card>
                <CardHeader>
                  <CardTitle className='flex items-center gap-2 text-sm font-medium'>
                    <Tag className='h-4 w-4 text-pink-500' />
                    Tags
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className='flex flex-wrap gap-2'>
                    {data.tags.map((tag) => (
                      <Badge key={tag} variant='secondary'>
                        {tag}
                      </Badge>
                    ))}
                  </div>
                </CardContent>
              </Card>
            )}

            {/* Error State */}
            {data.is_error && data.error_message && (
              <Card className='border-destructive'>
                <CardHeader>
                  <CardTitle className='flex items-center gap-2 text-sm font-medium text-destructive'>
                    <AlertCircle className='h-4 w-4' />
                    Sync Error
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <pre className='text-sm whitespace-pre-wrap text-destructive'>
                    {data.error_message}
                  </pre>
                </CardContent>
              </Card>
            )}

            {/* Metadata */}
            <Card>
              <CardHeader>
                <CardTitle className='text-sm font-medium'>Metadata</CardTitle>
              </CardHeader>
              <CardContent>
                <div className='grid grid-cols-2 gap-4 text-sm'>
                  <div>
                    <span className='text-muted-foreground'>Created:</span>
                    <span className='ml-2'>
                      {new Date(data.created_at).toLocaleString()}
                    </span>
                  </div>
                  <div>
                    <span className='text-muted-foreground'>Updated:</span>
                    <span className='ml-2'>
                      {new Date(data.updated_at).toLocaleString()}
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
                {data.lineage_generated_at && (
                  <span className='text-xs text-muted-foreground'>
                    Generated:{' '}
                    {new Date(data.lineage_generated_at).toLocaleString()}
                  </span>
                )}
                <Button
                  variant='outline'
                  size='sm'
                  onClick={() => generateLineage.mutate()}
                  disabled={
                    generateLineage.isPending ||
                    data.lineage_status === 'GENERATING'
                  }
                >
                  <RefreshCw
                    className={`mr-2 h-4 w-4 ${
                      generateLineage.isPending ||
                      data.lineage_status === 'GENERATING'
                        ? 'animate-spin'
                        : ''
                    }`}
                  />
                  {data.lineage_status === 'GENERATING'
                    ? 'Generating...'
                    : 'Generate Lineage'}
                </Button>
              </div>
            </div>

            {data.lineage_status === 'FAILED' && data.lineage_error && (
              <Card className='border-destructive'>
                <CardContent className='pt-4'>
                  <div className='flex items-center gap-2 text-destructive'>
                    <AlertCircle className='h-4 w-4' />
                    <p className='text-sm'>{data.lineage_error}</p>
                  </div>
                </CardContent>
              </Card>
            )}

            {data.lineage_status === 'PENDING' && (
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

            {data.lineage_status === 'GENERATING' && (
              <Card>
                <CardContent className='py-12 pt-6 text-center'>
                  <RefreshCw className='mx-auto mb-4 h-12 w-12 animate-spin text-primary' />
                  <p className='text-muted-foreground'>
                    Analyzing SQL and generating lineage...
                  </p>
                </CardContent>
              </Card>
            )}

            {data.lineage_status === 'COMPLETED' && data.lineage_metadata && (
              <>
                {data.lineage_metadata.error ? (
                  <Card className='border-yellow-500'>
                    <CardContent className='pt-4'>
                      <div className='flex items-center gap-2 text-yellow-600'>
                        <AlertCircle className='h-4 w-4' />
                        <p className='text-sm'>
                          Parse warning: {data.lineage_metadata.error}
                        </p>
                      </div>
                    </CardContent>
                  </Card>
                ) : null}
                <Card className='h-[500px]'>
                  <CardContent className='h-full p-0'>
                    <LineageFlowDiagram
                      lineage={data.lineage_metadata}
                      destinationName={
                        data.table_name_target || data.table_name
                      }
                      sourceName={data.source.name}
                    />
                  </CardContent>
                </Card>
              </>
            )}
          </CustomTabsContent>
        </CustomTabs>
      </Main>
    </>
  )
}
