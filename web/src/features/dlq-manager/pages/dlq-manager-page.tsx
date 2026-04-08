import {
  useDeferredValue,
  useEffect,
  useMemo,
  useState,
} from 'react'
import {
  useInfiniteQuery,
  useMutation,
  useQuery,
  useQueryClient,
} from '@tanstack/react-query'
import { formatDistanceToNow } from 'date-fns'
import {
  ArchiveX,
  Database,
  Layers,
  RefreshCw,
  Search as SearchIcon,
  Server,
  Workflow,
} from 'lucide-react'
import { toast } from 'sonner'
import { Header } from '@/components/layout/header'
import { Main } from '@/components/layout/main'
import { Search } from '@/components/search'
import { ThemeSwitch } from '@/components/theme-switch'
import { ConfirmDialog } from '@/components/confirm-dialog'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import { ScrollArea } from '@/components/ui/scroll-area'
import { Switch } from '@/components/ui/switch'
import { cn } from '@/lib/utils'
import { getApiErrorMessage } from '@/lib/handle-server-error'
import {
  dlqManagerRepo,
  type DLQMessage,
  type DLQQueueSummary,
} from '@/repo/dlq-manager'
import { dlqKeys } from '@/repo/query-keys'
import { DLQMessagePreviewSheet } from '../components/dlq-message-preview-sheet'
import { DLQMessagesTable } from '../components/dlq-messages-table'

type QueueGroup = {
  destinations: Array<{
    destinationId: number
    destinationName: string
    queues: DLQQueueSummary[]
    totalMessages: number
  }>
  pipelineId: number | null
  pipelineName: string
  totalMessages: number
}

type PreviewState = {
  message: DLQMessage
  queueKey: string
}

function buildQueueKey(queue: {
  destination_id: number
  source_id: number
  table_name: string
}) {
  return `${queue.source_id}:${queue.destination_id}:${queue.table_name}`
}

function formatTimestamp(value: string | null | undefined) {
  if (!value) return 'Unknown'
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) {
    return value
  }
  return `${date.toLocaleString()} (${formatDistanceToNow(date, { addSuffix: true })})`
}

function groupQueues(items: DLQQueueSummary[]): QueueGroup[] {
  const pipelineMap = new Map<string, QueueGroup>()
  for (const queue of items) {
    const pipelineKey = String(queue.pipeline_id ?? `unknown:${queue.source_id}`)
    const destinationName = queue.destination_name ?? `Destination ${queue.destination_id}`
    if (!pipelineMap.has(pipelineKey)) {
      pipelineMap.set(pipelineKey, {
        destinations: [],
        pipelineId: queue.pipeline_id,
        pipelineName:
          queue.pipeline_name ?? `Unknown Pipeline (source ${queue.source_id})`,
        totalMessages: 0,
      })
    }

    const pipelineGroup = pipelineMap.get(pipelineKey)!
    pipelineGroup.totalMessages += queue.message_count

    let destinationGroup = pipelineGroup.destinations.find(
      (item) => item.destinationId === queue.destination_id
    )
    if (!destinationGroup) {
      destinationGroup = {
        destinationId: queue.destination_id,
        destinationName,
        queues: [],
        totalMessages: 0,
      }
      pipelineGroup.destinations.push(destinationGroup)
    }

    destinationGroup.totalMessages += queue.message_count
    destinationGroup.queues.push(queue)
  }

  return Array.from(pipelineMap.values())
    .map((group) => ({
      ...group,
      destinations: group.destinations
        .map((destination) => ({
          ...destination,
          queues: [...destination.queues].sort((a, b) =>
            a.table_name.localeCompare(b.table_name)
          ),
        }))
        .sort((a, b) => a.destinationName.localeCompare(b.destinationName)),
    }))
    .sort((a, b) => a.pipelineName.localeCompare(b.pipelineName))
}

export default function DLQManagerPage() {
  const queryClient = useQueryClient()
  const [hideEmpty, setHideEmpty] = useState(true)
  const [searchInput, setSearchInput] = useState('')
  const [requestedQueueKey, setRequestedQueueKey] = useState<string | null>(null)
  const [previewState, setPreviewState] = useState<PreviewState | null>(null)
  const [queueConfirm, setQueueConfirm] = useState<DLQQueueSummary | null>(null)
  const [pipelineConfirm, setPipelineConfirm] = useState<QueueGroup | null>(null)

  const deferredSearch = useDeferredValue(searchInput.trim())

  useEffect(() => {
    document.title = 'DLQ Manager'
    return () => {
      document.title = 'Rosetta'
    }
  }, [])

  const queuesQuery = useQuery({
    queryKey: dlqKeys.queues({
      include_empty: !hideEmpty,
      search: deferredSearch || undefined,
    }),
    queryFn: () =>
      dlqManagerRepo.getQueues({
        include_empty: !hideEmpty,
        search: deferredSearch || undefined,
      }),
    refetchInterval: 10000,
    refetchIntervalInBackground: false,
  })

  const queueItems = useMemo(() => queuesQuery.data?.items ?? [], [queuesQuery.data?.items])
  const groupedQueues = useMemo(() => groupQueues(queueItems), [queueItems])

  const selectedQueueKey = useMemo(() => {
    if (queueItems.length === 0) {
      return null
    }

    if (
      requestedQueueKey &&
      queueItems.some((item) => buildQueueKey(item) === requestedQueueKey)
    ) {
      return requestedQueueKey
    }

    return buildQueueKey(queueItems[0])
  }, [queueItems, requestedQueueKey])

  const selectedQueue = useMemo(
    () => queueItems.find((item) => buildQueueKey(item) === selectedQueueKey) ?? null,
    [queueItems, selectedQueueKey]
  )

  const messagesQuery = useInfiniteQuery({
    queryKey: dlqKeys.messages(
      selectedQueue
        ? {
            destination_id: selectedQueue.destination_id,
            source_id: selectedQueue.source_id,
            table_name: selectedQueue.table_name,
          }
        : null
    ),
    queryFn: ({ pageParam }) =>
      dlqManagerRepo.getMessages({
        destination_id: selectedQueue!.destination_id,
        source_id: selectedQueue!.source_id,
        table_name: selectedQueue!.table_name,
        before_id: pageParam,
        limit: 50,
      }),
    enabled: Boolean(selectedQueue),
    getNextPageParam: (lastPage) => lastPage.next_before_id ?? undefined,
    initialPageParam: null as string | null,
    refetchInterval: selectedQueue ? 10000 : false,
    refetchIntervalInBackground: false,
  })

  const messages = useMemo(
    () => messagesQuery.data?.pages.flatMap((page) => page.items) ?? [],
    [messagesQuery.data]
  )
  const totalMessageCount =
    messagesQuery.data?.pages[0]?.total_count ?? selectedQueue?.message_count ?? 0
  const selectedMessage = useMemo(() => {
    if (!previewState) {
      return null
    }
    if (previewState.queueKey !== selectedQueueKey) {
      return null
    }
    if (!messages.some((message) => message.message_id === previewState.message.message_id)) {
      return null
    }
    return previewState.message
  }, [messages, previewState, selectedQueueKey])

  const invalidateDLQQueries = async () => {
    await queryClient.invalidateQueries({ queryKey: dlqKeys.all })
  }

  const discardMessagesMutation = useMutation({
    mutationFn: (messageIds: string[]) =>
      dlqManagerRepo.discardMessages({
        destination_id: selectedQueue!.destination_id,
        source_id: selectedQueue!.source_id,
        table_name: selectedQueue!.table_name,
        message_ids: messageIds,
      }),
    onSuccess: async (result, messageIds) => {
      if (selectedMessage && messageIds.includes(selectedMessage.message_id)) {
        setPreviewState(null)
      }
      toast.success(`Cancelled ${result.discarded_count} DLQ row(s)`)
      await invalidateDLQQueries()
    },
    onError: (error) => {
      toast.error(getApiErrorMessage(error, 'Failed to cancel selected DLQ rows'))
    },
  })

  const discardQueueMutation = useMutation({
    mutationFn: (queue: DLQQueueSummary) =>
      dlqManagerRepo.discardQueue({
        destination_id: queue.destination_id,
        source_id: queue.source_id,
        table_name: queue.table_name,
      }),
    onSuccess: async (result, queue) => {
      if (selectedQueueKey === buildQueueKey(queue)) {
        setRequestedQueueKey(null)
        setPreviewState(null)
      }
      setQueueConfirm(null)
      toast.success(`Cancelled ${result.discarded_count} DLQ row(s) in the queue`)
      await invalidateDLQQueries()
    },
    onError: (error) => {
      toast.error(getApiErrorMessage(error, 'Failed to cancel DLQ queue'))
    },
  })

  const discardPipelineMutation = useMutation({
    mutationFn: (group: QueueGroup) => dlqManagerRepo.discardPipeline(group.pipelineId!),
    onSuccess: async (result, group) => {
      if (selectedQueue?.pipeline_id === group.pipelineId) {
        setRequestedQueueKey(null)
        setPreviewState(null)
      }
      setPipelineConfirm(null)
      toast.success(
        `Cancelled ${result.discarded_count} DLQ row(s) across ${result.queues_cleared} queue(s)`
      )
      await invalidateDLQQueries()
    },
    onError: (error) => {
      toast.error(getApiErrorMessage(error, 'Failed to cancel pipeline DLQ'))
    },
  })

  const statCards = [
    {
      description: 'Rows currently waiting in DLQ',
      icon: ArchiveX,
      title: 'Total Messages',
      value: queuesQuery.data?.total_messages ?? 0,
    },
    {
      description: 'Distinct stream queues with DLQ data',
      icon: Layers,
      title: 'Active Queues',
      value: queuesQuery.data?.total_queues ?? 0,
    },
    {
      description: 'Pipelines affected by current filters',
      icon: Workflow,
      title: 'Affected Pipelines',
      value: queuesQuery.data?.total_pipelines ?? 0,
    },
    {
      description: 'Destinations affected by current filters',
      icon: Server,
      title: 'Affected Destinations',
      value: queuesQuery.data?.total_destinations ?? 0,
    },
  ]

  return (
    <>
      <Header fixed>
        <Search />
        <div className='ms-auto flex items-center space-x-4'>
          <ThemeSwitch />
        </div>
      </Header>

      <Main className='flex flex-1 flex-col gap-4'>
        <div className='flex flex-col gap-2 sm:flex-row sm:items-end sm:justify-between'>
          <div>
            <h2 className='text-2xl font-bold tracking-tight'>DLQ Manager</h2>
            <p className='text-muted-foreground'>
              Inspect current DLQ queues, preview rows safely, and permanently
              cancel DLQ data when needed.
            </p>
          </div>
          <Button
            onClick={() => queuesQuery.refetch()}
            variant='outline'
          >
            <RefreshCw
              className={cn(
                'mr-2 h-4 w-4',
                queuesQuery.isFetching && 'animate-spin'
              )}
            />
            Refresh
          </Button>
        </div>

        <div className='grid gap-4 md:grid-cols-2 xl:grid-cols-4'>
          {statCards.map((card) => (
            <Card key={card.title}>
              <CardHeader className='flex flex-row items-center justify-between space-y-0 pb-2'>
                <div className='space-y-1'>
                  <CardTitle className='text-sm font-medium'>{card.title}</CardTitle>
                  <CardDescription>{card.description}</CardDescription>
                </div>
                <card.icon className='h-4 w-4 text-muted-foreground' />
              </CardHeader>
              <CardContent>
                <div className='text-3xl font-bold'>{card.value}</div>
              </CardContent>
            </Card>
          ))}
        </div>

        <div className='grid gap-4 xl:grid-cols-[380px_minmax(0,1fr)]'>
          <Card className='overflow-hidden'>
            <CardHeader className='space-y-4'>
              <div>
                <CardTitle>Queues</CardTitle>
                <CardDescription>
                  Grouped by pipeline, destination, and source table.
                </CardDescription>
              </div>

              <div className='flex flex-col gap-3'>
                <div className='relative'>
                  <SearchIcon className='absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground' />
                  <Input
                    className='pl-9'
                    onChange={(event) => setSearchInput(event.target.value)}
                    placeholder='Search pipelines, destinations, or tables'
                    value={searchInput}
                  />
                </div>

                <div className='flex items-center justify-between rounded-lg border border-border/50 px-3 py-2'>
                  <div>
                    <p className='text-sm font-medium'>Hide empty queues</p>
                    <p className='text-xs text-muted-foreground'>
                      Default view excludes empty stream keys.
                    </p>
                  </div>
                  <Switch checked={hideEmpty} onCheckedChange={setHideEmpty} />
                </div>
              </div>
            </CardHeader>

            <CardContent className='p-0'>
              <ScrollArea className='h-[calc(100vh-21rem)]'>
                <div className='space-y-4 p-4'>
                  {queuesQuery.isLoading ? (
                    <Card className='border-dashed'>
                      <CardContent className='p-6 text-sm text-muted-foreground'>
                        Loading DLQ queues...
                      </CardContent>
                    </Card>
                  ) : groupedQueues.length ? (
                    groupedQueues.map((group) => (
                      <div
                        key={`${group.pipelineId ?? group.pipelineName}`}
                        className='rounded-xl border border-border/60 bg-background'
                      >
                        <div className='flex items-start justify-between gap-3 border-b border-border/50 p-4'>
                          <div className='space-y-2'>
                            <div className='flex items-center gap-2'>
                              <Workflow className='h-4 w-4 text-muted-foreground' />
                              <h3 className='font-semibold'>{group.pipelineName}</h3>
                            </div>
                            <Badge variant='secondary'>
                              {group.totalMessages} row{group.totalMessages === 1 ? '' : 's'}
                            </Badge>
                          </div>
                          {group.pipelineId ? (
                            <Button
                              onClick={() => setPipelineConfirm(group)}
                              size='sm'
                              variant='destructive'
                            >
                              Cancel DLQ
                            </Button>
                          ) : null}
                        </div>

                        <div className='divide-y divide-border/50'>
                          {group.destinations.map((destination) => (
                            <div key={destination.destinationId} className='p-4'>
                              <div className='mb-3 flex items-center justify-between gap-2'>
                                <div className='flex items-center gap-2 text-sm font-medium'>
                                  <Database className='h-4 w-4 text-muted-foreground' />
                                  {destination.destinationName}
                                </div>
                                <Badge variant='outline'>
                                  {destination.totalMessages} row
                                  {destination.totalMessages === 1 ? '' : 's'}
                                </Badge>
                              </div>

                              <div className='space-y-2'>
                                {destination.queues.map((queue) => {
                                  const isSelected =
                                    buildQueueKey(queue) === selectedQueueKey
                                  return (
                                    <button
                                      key={buildQueueKey(queue)}
                                      className={cn(
                                        'w-full rounded-lg border px-3 py-3 text-left transition-colors',
                                        isSelected
                                          ? 'border-primary bg-primary/5'
                                          : 'border-border/50 hover:bg-muted/40'
                                      )}
                                      onClick={() => setRequestedQueueKey(buildQueueKey(queue))}
                                      type='button'
                                    >
                                      <div className='flex items-start justify-between gap-3'>
                                        <div className='space-y-1'>
                                          <p className='font-medium'>{queue.table_name}</p>
                                          <p className='text-sm text-muted-foreground'>
                                            Target:{' '}
                                            <span className='font-mono'>
                                              {queue.table_name_target ?? 'Unknown'}
                                            </span>
                                          </p>
                                          <p className='text-xs text-muted-foreground'>
                                            Latest failure:{' '}
                                            {formatTimestamp(queue.newest_failed_at)}
                                          </p>
                                        </div>
                                        <Badge variant={isSelected ? 'default' : 'secondary'}>
                                          {queue.message_count}
                                        </Badge>
                                      </div>
                                    </button>
                                  )
                                })}
                              </div>
                            </div>
                          ))}
                        </div>
                      </div>
                    ))
                  ) : (
                    <Card className='border-dashed'>
                      <CardContent className='space-y-2 p-6 text-sm text-muted-foreground'>
                        <p>No DLQ queues matched the current filters.</p>
                        <p>
                          Try clearing the search input or disable the empty-queue
                          filter.
                        </p>
                      </CardContent>
                    </Card>
                  )}
                </div>
              </ScrollArea>
            </CardContent>
          </Card>

          <div className='space-y-4'>
            {selectedQueue ? (
              <>
                <Card>
                  <CardHeader className='flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between'>
                    <div className='space-y-3'>
                      <div>
                        <CardTitle>{selectedQueue.table_name}</CardTitle>
                        <CardDescription>
                          {selectedQueue.pipeline_name ?? 'Unknown Pipeline'} {'->'}{' '}
                          {selectedQueue.destination_name ?? 'Unknown Destination'}
                        </CardDescription>
                      </div>

                      <div className='flex flex-wrap items-center gap-2'>
                        <Badge variant='secondary'>
                          {selectedQueue.message_count} row
                          {selectedQueue.message_count === 1 ? '' : 's'}
                        </Badge>
                        <Badge variant='outline'>
                          Target: {selectedQueue.table_name_target ?? 'Unknown'}
                        </Badge>
                        <Badge variant='outline'>
                          Source: {selectedQueue.source_name ?? `Source ${selectedQueue.source_id}`}
                        </Badge>
                      </div>

                      <div className='grid gap-3 text-sm text-muted-foreground sm:grid-cols-2'>
                        <div>
                          <p className='text-xs font-medium tracking-wide uppercase'>
                            Oldest failure
                          </p>
                          <p>{formatTimestamp(selectedQueue.oldest_failed_at)}</p>
                        </div>
                        <div>
                          <p className='text-xs font-medium tracking-wide uppercase'>
                            Newest failure
                          </p>
                          <p>{formatTimestamp(selectedQueue.newest_failed_at)}</p>
                        </div>
                      </div>
                    </div>

                    <Button
                      onClick={() => setQueueConfirm(selectedQueue)}
                      variant='destructive'
                    >
                      Cancel DLQ
                    </Button>
                  </CardHeader>
                </Card>

                <Card>
                  <CardContent className='pt-6'>
                    <DLQMessagesTable
                      hasNextPage={Boolean(messagesQuery.hasNextPage)}
                      isDiscarding={discardMessagesMutation.isPending}
                      isFetchingNextPage={messagesQuery.isFetchingNextPage}
                      isLoading={messagesQuery.status === 'pending'}
                      messages={messages}
                      onDiscardSelected={(messageIds) =>
                        discardMessagesMutation.mutate(messageIds)
                      }
                      onLoadOlder={() => messagesQuery.fetchNextPage()}
                      onPreview={(message) =>
                        setPreviewState({
                          message,
                          queueKey: selectedQueueKey ?? buildQueueKey(selectedQueue),
                        })
                      }
                      totalCount={totalMessageCount}
                    />
                  </CardContent>
                </Card>
              </>
            ) : (
              <Card className='border-dashed'>
                <CardContent className='flex min-h-[420px] flex-col items-center justify-center gap-3 text-center'>
                  <ArchiveX className='h-10 w-10 text-muted-foreground' />
                  <div className='space-y-1'>
                    <h3 className='text-lg font-semibold'>Select a queue</h3>
                    <p className='text-sm text-muted-foreground'>
                      Choose a DLQ queue from the left to inspect its rows and
                      permanently cancel data when needed.
                    </p>
                  </div>
                </CardContent>
              </Card>
            )}
          </div>
        </div>

        <DLQMessagePreviewSheet
          message={selectedMessage}
          open={Boolean(selectedMessage)}
          onOpenChange={(open) => {
            if (!open) {
              setPreviewState(null)
            }
          }}
        />

        <ConfirmDialog
          destructive
          open={Boolean(queueConfirm)}
          onOpenChange={(open) => {
            if (!open) setQueueConfirm(null)
          }}
          title='Cancel DLQ queue'
          desc={
            queueConfirm
              ? `This will permanently discard ${queueConfirm.message_count} DLQ row${queueConfirm.message_count === 1 ? '' : 's'} for ${queueConfirm.table_name}. This action cannot be undone.`
              : ''
          }
          confirmText='Cancel DLQ'
          isLoading={discardQueueMutation.isPending}
          handleConfirm={() => {
            if (queueConfirm) {
              discardQueueMutation.mutate(queueConfirm)
            }
          }}
        />

        <ConfirmDialog
          destructive
          open={Boolean(pipelineConfirm)}
          onOpenChange={(open) => {
            if (!open) setPipelineConfirm(null)
          }}
          title='Cancel pipeline DLQ'
          desc={
            pipelineConfirm
              ? `This will permanently discard ${pipelineConfirm.totalMessages} DLQ row${pipelineConfirm.totalMessages === 1 ? '' : 's'} across the ${pipelineConfirm.pipelineName} pipeline. This action cannot be undone.`
              : ''
          }
          confirmText='Cancel DLQ'
          isLoading={discardPipelineMutation.isPending}
          handleConfirm={() => {
            if (pipelineConfirm?.pipelineId) {
              discardPipelineMutation.mutate(pipelineConfirm)
            }
          }}
        />
      </Main>
    </>
  )
}
