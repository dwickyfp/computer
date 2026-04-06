import { useState, useEffect } from 'react'
import { formatDistanceToNow } from 'date-fns'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useParams, Link } from '@tanstack/react-router'
import {
  flowTasksRepo,
  type FlowTaskRunHistory,
  type FlowTaskRunStatus,
  type FlowTaskNodeStatus,
} from '@/repo/flow-tasks'
import { flowTaskKeys } from '@/repo/query-keys'
import {
  GitBranch,
  Play,
  Square,
  Loader2,
  ChevronDown,
  ChevronRight,
  CheckCircle2,
  XCircle,
  Clock,
  Activity,
  Calendar,
  Database,
  Zap,
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
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from '@/components/ui/tooltip'
import { Header } from '@/components/layout/header'
import { Main } from '@/components/layout/main'
import { Search } from '@/components/search'
import { ThemeSwitch } from '@/components/theme-switch'

// ─── Sub-components ────────────────────────────────────────────────────────────

function RunStatusBadge({ status }: { status: FlowTaskRunStatus }) {
  const map: Record<
    FlowTaskRunStatus,
    { label: string; cls: string; icon: React.ElementType }
  > = {
    RUNNING: {
      label: 'Running',
      cls: 'bg-blue-600 text-white border-blue-600 dark:bg-blue-500 dark:border-blue-500',
      icon: Loader2,
    },
    SUCCESS: {
      label: 'Success',
      cls: 'bg-emerald-600 text-white border-emerald-600 dark:bg-emerald-500 dark:border-emerald-500',
      icon: CheckCircle2,
    },
    FAILED: {
      label: 'Failed',
      cls: 'bg-rose-600 text-white border-rose-600 dark:bg-rose-500 dark:border-rose-500',
      icon: XCircle,
    },
    CANCELLED: {
      label: 'Cancelled',
      cls: 'bg-gray-600 text-white border-gray-600 dark:bg-gray-500 dark:border-gray-500',
      icon: XCircle,
    },
  }
  const { label, cls, icon: Icon } = map[status] || map.CANCELLED
  return (
    <span
      className={`inline-flex items-center gap-1.5 rounded-full border px-2.5 py-0.5 text-xs font-semibold shadow-sm ${cls}`}
    >
      <Icon
        className={`h-3 w-3 ${status === 'RUNNING' ? 'animate-spin' : ''}`}
      />
      {label}
    </span>
  )
}

function NodeStatusIcon({ status }: { status: FlowTaskNodeStatus }) {
  if (status === 'SUCCESS')
    return <CheckCircle2 className='h-4 w-4 text-emerald-500' />
  if (status === 'FAILED') return <XCircle className='h-4 w-4 text-rose-500' />
  if (status === 'RUNNING')
    return <Loader2 className='h-4 w-4 animate-spin text-blue-500' />
  return <Clock className='h-4 w-4 text-muted-foreground/50' />
}

function RunRow({ run }: { run: FlowTaskRunHistory }) {
  const [open, setOpen] = useState(false)
  const duration = run.finished_at
    ? `${Math.round((new Date(run.finished_at).getTime() - new Date(run.started_at).getTime()) / 1000)}s`
    : '—'

  return (
    <>
      <TableRow
        className='group cursor-pointer transition-colors hover:bg-muted/50'
        onClick={() => setOpen(!open)}
      >
        <TableCell className='w-[50px] font-mono text-xs text-muted-foreground'>
          <div className='flex items-center gap-2'>
            <Button
              variant='ghost'
              size='icon'
              className='h-6 w-6 p-0 text-muted-foreground hover:text-foreground'
              onClick={(e) => {
                e.stopPropagation()
                setOpen(!open)
              }}
            >
              {open ? (
                <ChevronDown className='h-4 w-4' />
              ) : (
                <ChevronRight className='h-4 w-4' />
              )}
            </Button>
            <span>#{run.id}</span>
          </div>
        </TableCell>
        <TableCell>
          <RunStatusBadge status={run.status} />
        </TableCell>
        <TableCell>
          <Badge variant='outline' className='font-mono text-[10px] capitalize'>
            {run.trigger_type.toLowerCase()}
          </Badge>
        </TableCell>
        <TableCell className='text-sm'>
          <div className='flex flex-col'>
            <span className='font-medium'>
              {formatDistanceToNow(new Date(run.started_at), {
                addSuffix: true,
              })}
            </span>
            <span className='text-xs text-muted-foreground'>
              {new Date(run.started_at).toLocaleString()}
            </span>
          </div>
        </TableCell>
        <TableCell>
          <div className='flex flex-col'>
            <div className='flex items-center gap-2 text-sm'>
              <span className='w-16 text-right font-mono text-muted-foreground'>
                In:
              </span>
              <span className='font-medium'>
                {run.total_input_records?.toLocaleString() ?? '—'}
              </span>
            </div>
            <div className='mt-0.5 flex items-center gap-2 text-sm'>
              <span className='w-16 text-right font-mono text-muted-foreground'>
                Out:
              </span>
              <span className='font-medium'>
                {run.total_output_records?.toLocaleString() ?? '—'}
              </span>
            </div>
          </div>
        </TableCell>
        <TableCell className='text-sm font-medium text-muted-foreground'>
          {duration}
        </TableCell>
      </TableRow>
      {open && (
        <TableRow className='!border-0 hover:bg-transparent'>
          <TableCell colSpan={6} className='border-0 p-0'>
            <div className='animate-in border-b bg-muted/30 px-4 py-3 shadow-inner duration-200 fade-in slide-in-from-top-1'>
              <div className='overflow-hidden rounded-md border bg-background'>
                <Table>
                  <TableHeader className='bg-muted/50'>
                    <TableRow className='border-b hover:bg-transparent'>
                      <TableHead className='h-8 text-xs font-semibold'>
                        Node
                      </TableHead>
                      <TableHead className='h-8 text-xs font-semibold'>
                        Type
                      </TableHead>
                      <TableHead className='h-8 text-right text-xs font-semibold'>
                        In
                      </TableHead>
                      <TableHead className='h-8 text-right text-xs font-semibold'>
                        Out
                      </TableHead>
                      <TableHead className='h-8 text-right text-xs font-semibold'>
                        Duration
                      </TableHead>
                      <TableHead className='h-8 text-xs font-semibold'>
                        Status
                      </TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {run.node_logs?.map((log) => (
                      <TableRow
                        key={log.id}
                        className='border-b border-border/50 last:border-0 hover:bg-muted/20'
                      >
                        <TableCell className='py-2 text-sm font-medium'>
                          <span className='flex items-center gap-2'>
                            <div
                              className={`h-1.5 w-1.5 rounded-full ${
                                log.status === 'SUCCESS'
                                  ? 'bg-emerald-500'
                                  : log.status === 'FAILED'
                                    ? 'bg-rose-500'
                                    : log.status === 'RUNNING'
                                      ? 'bg-blue-500'
                                      : 'bg-gray-300'
                              }`}
                            />
                            {log.node_label || log.node_id}
                          </span>
                          {log.error_message && (
                            <div className='mt-1'>
                              <TooltipProvider>
                                <Tooltip delayDuration={0}>
                                  <TooltipTrigger asChild>
                                    <div className='inline-block max-w-[400px] cursor-help truncate rounded border border-rose-100 bg-rose-50 px-2 py-1 text-xs text-rose-600 transition-colors hover:bg-rose-100 dark:border-rose-900/30 dark:bg-rose-900/20 dark:text-rose-300 dark:hover:bg-rose-900/40'>
                                      {log.error_message}
                                    </div>
                                  </TooltipTrigger>
                                  <TooltipContent
                                    side='top'
                                    align='start'
                                    className='max-w-[400px] border-border bg-popover break-words text-popover-foreground shadow-md'
                                  >
                                    <div className='p-1 font-mono text-xs'>
                                      <span className='mb-1 block font-semibold text-rose-500'>
                                        Error Detail:
                                      </span>
                                      {log.error_message}
                                    </div>
                                  </TooltipContent>
                                </Tooltip>
                              </TooltipProvider>
                            </div>
                          )}
                        </TableCell>
                        <TableCell className='py-2'>
                          <Badge
                            variant='secondary'
                            className='text-[10px] font-semibold tracking-wider uppercase decoration-0 opacity-80'
                          >
                            {log.node_type}
                          </Badge>
                        </TableCell>
                        <TableCell className='py-2 text-right font-mono text-xs text-muted-foreground'>
                          {log.row_count_in?.toLocaleString() ?? '-'}
                        </TableCell>
                        <TableCell className='py-2 text-right font-mono text-xs text-muted-foreground'>
                          {log.row_count_out?.toLocaleString() ?? '-'}
                        </TableCell>
                        <TableCell className='py-2 text-right font-mono text-xs text-muted-foreground'>
                          {log.duration_ms != null
                            ? `${log.duration_ms}ms`
                            : '-'}
                        </TableCell>
                        <TableCell className='py-2'>
                          <div className='flex items-center gap-2'>
                            <NodeStatusIcon status={log.status} />
                          </div>
                        </TableCell>
                      </TableRow>
                    ))}
                    {(!run.node_logs || run.node_logs.length === 0) && (
                      <TableRow>
                        <TableCell
                          colSpan={6}
                          className='py-4 text-center text-xs text-muted-foreground'
                        >
                          No logs available
                        </TableCell>
                      </TableRow>
                    )}
                  </TableBody>
                </Table>
              </div>
            </div>
          </TableCell>
        </TableRow>
      )}
    </>
  )
}

// ─── Main page ─────────────────────────────────────────────────────────────────

export default function FlowTaskDetailPage() {
  const { flowTaskId } = useParams({
    from: '/_authenticated/flow-tasks/$flowTaskId',
  })
  const id = parseInt(flowTaskId)
  const queryClient = useQueryClient()
  const [pollingTaskId, setPollingTaskId] = useState<string | null>(null)

  useEffect(() => {
    document.title = 'Flow Task Details'
    return () => {
      document.title = 'Rosetta'
    }
  }, [])

  const { data: ftResp, isLoading: ftLoading } = useQuery({
    queryKey: flowTaskKeys.detail(id),
    queryFn: () => flowTasksRepo.get(id),
    refetchInterval: pollingTaskId ? 3000 : false,
  })

  const { data: runsResp, isLoading: runsLoading } = useQuery({
    queryKey: flowTaskKeys.runs(id),
    queryFn: () => flowTasksRepo.getRuns(id),
    refetchInterval: pollingTaskId ? 3000 : false,
  })

  // Poll Celery task status
  const { data: taskStatusData } = useQuery({
    queryKey: flowTaskKeys.status(pollingTaskId),
    queryFn: () => flowTasksRepo.getTaskStatus(pollingTaskId!),
    enabled: !!pollingTaskId,
    refetchInterval: 2000,
  })

  useEffect(() => {
    if (!taskStatusData) return
    if (
      taskStatusData.state === 'SUCCESS' ||
      taskStatusData.state === 'FAILURE'
    ) {
      setPollingTaskId(null)
      setTimeout(() => {
        queryClient.invalidateQueries({ queryKey: flowTaskKeys.detail(id) })
        queryClient.invalidateQueries({ queryKey: flowTaskKeys.runs(id) })
      }, 300)
      if (taskStatusData.state === 'SUCCESS') {
        toast.success('Flow task completed successfully')
      } else {
        toast.error('Flow task failed')
      }
    }
  }, [taskStatusData, id, queryClient])

  const runMutation = useMutation({
    mutationFn: () => flowTasksRepo.run(id),
    onSuccess: (resp) => {
      const { celery_task_id } = resp
      setPollingTaskId(celery_task_id)
      toast.info('Flow task started')
      setTimeout(() => {
        queryClient.invalidateQueries({ queryKey: flowTaskKeys.detail(id) })
      }, 300)
    },
    onError: () => toast.error('Failed to trigger flow task'),
  })

  const cancelMutation = useMutation({
    mutationFn: () => flowTasksRepo.cancelRun(id),
    onSuccess: () => {
      setPollingTaskId(null)
      toast.info('Run cancelled')
      setTimeout(() => {
        queryClient.invalidateQueries({ queryKey: flowTaskKeys.detail(id) })
        queryClient.invalidateQueries({ queryKey: flowTaskKeys.runs(id) })
      }, 300)
    },
    onError: () => toast.error('Failed to cancel run'),
  })

  const ft = ftResp
  const runs = runsResp?.items ?? []

  if (ftLoading) {
    return (
      <Main>
        <div className='flex h-64 items-center justify-center'>
          <Loader2 className='h-6 w-6 animate-spin text-muted-foreground' />
        </div>
      </Main>
    )
  }

  if (!ft) {
    return (
      <Main>
        <div className='py-16 text-center text-muted-foreground'>
          Flow task not found.
        </div>
      </Main>
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

      <Main className='flex flex-1 flex-col gap-6'>
        {/* Breadcrumb */}
        <div>
          <Breadcrumb>
            <BreadcrumbList>
              <BreadcrumbItem>
                <BreadcrumbLink asChild>
                  <Link to='/flow-tasks'>Flow Tasks</Link>
                </BreadcrumbLink>
              </BreadcrumbItem>
              <BreadcrumbSeparator />
              <BreadcrumbItem>
                <BreadcrumbPage>{ft.name}</BreadcrumbPage>
              </BreadcrumbItem>
            </BreadcrumbList>
          </Breadcrumb>
        </div>

        {/* Header card */}
        <Card className='bg-sidebar'>
          <CardHeader>
            <div className='flex items-start justify-between gap-4'>
              <div>
                <CardTitle className='flex items-center gap-2 text-xl'>
                  <GitBranch className='h-5 w-5 text-muted-foreground' />
                  {ft.name}
                </CardTitle>
                {ft.description && (
                  <CardDescription className='mt-1'>
                    {ft.description}
                  </CardDescription>
                )}
              </div>
              <div className='flex flex-shrink-0 gap-2'>
                <Button variant='outline' asChild>
                  <Link
                    to='/flow-tasks/$flowTaskId/flow'
                    params={{ flowTaskId: String(ft.id) }}
                  >
                    <GitBranch className='mr-2 h-4 w-4' />
                    Flow Editor
                  </Link>
                </Button>
                {ft.status === 'RUNNING' || !!pollingTaskId ? (
                  <Button
                    variant='destructive'
                    onClick={() => cancelMutation.mutate()}
                    disabled={cancelMutation.isPending}
                  >
                    {cancelMutation.isPending ? (
                      <Loader2 className='mr-2 h-4 w-4 animate-spin' />
                    ) : (
                      <Square className='mr-2 h-4 w-4' />
                    )}
                    Cancel Run
                  </Button>
                ) : (
                  <Button
                    onClick={() => runMutation.mutate()}
                    disabled={runMutation.isPending}
                  >
                    {runMutation.isPending ? (
                      <Loader2 className='mr-2 h-4 w-4 animate-spin' />
                    ) : (
                      <Play className='mr-2 h-4 w-4' />
                    )}
                    Run Now
                  </Button>
                )}
              </div>
            </div>
          </CardHeader>
          <CardContent>
            <div className='mt-2 grid grid-cols-2 gap-4 text-sm sm:grid-cols-4'>
              <StatItem
                label='Execution Status'
                icon={Activity}
                value={
                  <span
                    className={`inline-flex items-center gap-1.5 rounded px-2 py-0.5 text-xs font-semibold ${
                      ft.status === 'SUCCESS'
                        ? 'border bg-emerald-50 text-emerald-700 dark:bg-emerald-900/30'
                        : ft.status === 'FAILED'
                          ? 'border border-rose-200 bg-rose-50 text-rose-700 dark:bg-rose-900/30'
                          : ft.status === 'RUNNING'
                            ? 'border border-blue-200 bg-blue-50 text-blue-700 dark:bg-blue-900/30'
                            : 'border border-border bg-muted text-muted-foreground'
                    }`}
                  >
                    {ft.status === 'RUNNING' && (
                      <Loader2 className='h-3 w-3 animate-spin' />
                    )}
                    {ft.status}
                  </span>
                }
              />
              <StatItem
                label='Last Run'
                icon={Calendar}
                value={
                  <div className='flex flex-col'>
                    <span>
                      {ft.last_run_at
                        ? formatDistanceToNow(new Date(ft.last_run_at), {
                            addSuffix: true,
                          })
                        : 'Never'}
                    </span>
                    {ft.last_run_at && (
                      <span className='text-[10px] font-normal text-muted-foreground'>
                        {new Date(ft.last_run_at).toLocaleString()}
                      </span>
                    )}
                  </div>
                }
              />
              <StatItem
                label='Records Processed'
                icon={Database}
                value={
                  ft.last_run_record_count != null ? (
                    <span className='font-mono'>
                      {ft.last_run_record_count.toLocaleString()}
                    </span>
                  ) : (
                    '—'
                  )
                }
              />
              <StatItem
                label='Trigger Method'
                icon={Zap}
                value={
                  <span className='capitalize'>
                    {ft.trigger_type.toLowerCase() || 'Manual'}
                  </span>
                }
              />
            </div>
          </CardContent>
        </Card>

        {/* Run History */}
        <Card className='flex flex-col bg-sidebar'>
          <CardHeader className='flex-shrink-0 pb-3'>
            <div className='flex items-center justify-between'>
              <div>
                <CardTitle className='text-base font-semibold'>
                  Run History
                </CardTitle>
                <CardDescription className='mt-1 text-xs'>
                  Detailed logs of past executions. Click a row to inspect node
                  performance.
                </CardDescription>
              </div>
              {/* Add filter or refresh button here later if needed */}
            </div>
          </CardHeader>
          <CardContent className='scrollbar-thin scrollbar-thumb-muted-foreground/20 scrollbar-track-transparent relative max-h-[600px] overflow-auto border-t p-0'>
            <Table>
              <TableHeader className='sticky top-0 z-10 bg-card shadow-sm'>
                <TableRow className='border-b border-border/60 hover:bg-transparent'>
                  <TableHead className='w-[80px]'>Run ID</TableHead>
                  <TableHead className='w-[140px]'>Status</TableHead>
                  <TableHead className='w-[100px]'>Trigger</TableHead>
                  <TableHead className='w-[200px]'>Started</TableHead>
                  <TableHead>Throughput</TableHead>
                  <TableHead className='w-[100px]'>Duration</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {runsLoading && (
                  <TableRow>
                    <TableCell colSpan={6} className='py-8 text-center'>
                      <Loader2 className='mx-auto h-5 w-5 animate-spin text-muted-foreground' />
                    </TableCell>
                  </TableRow>
                )}
                {!runsLoading && runs.length === 0 && (
                  <TableRow>
                    <TableCell
                      colSpan={6}
                      className='py-12 text-center text-sm text-muted-foreground'
                    >
                      No runs yet. Click "Run Now" to start.
                    </TableCell>
                  </TableRow>
                )}
                {runs.map((run) => (
                  <RunRow key={run.id} run={run} />
                ))}
              </TableBody>
            </Table>
          </CardContent>
        </Card>
      </Main>
    </>
  )
}

function StatItem({
  label,
  value,
  icon: Icon,
}: {
  label: string
  value: React.ReactNode
  icon: React.ElementType
}) {
  return (
    <div className='flex flex-col gap-1 rounded-md border bg-card/50 p-3 transition-colors hover:bg-card'>
      <div className='mb-1 flex items-center gap-2 text-muted-foreground'>
        <Icon className='h-3.5 w-3.5' />
        <span className='text-xs font-medium tracking-wider uppercase'>
          {label}
        </span>
      </div>
      <div className='pl-0.5 text-sm font-medium'>{value}</div>
    </div>
  )
}
