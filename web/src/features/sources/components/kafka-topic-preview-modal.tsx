import { Fragment, useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { ChevronDown, ChevronRight, Loader2 } from 'lucide-react'

import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { sourcesRepo } from '@/repo/sources'

interface KafkaTopicPreviewModalProps {
  sourceId: number
  topicName: string | null
  open: boolean
  onOpenChange: (open: boolean) => void
}

function formatTimestamp(timestamp?: string | null) {
  if (!timestamp) {
    return '--'
  }

  const date = new Date(timestamp)
  if (Number.isNaN(date.getTime())) {
    return timestamp
  }

  return date.toLocaleString()
}

function PayloadPane({
  payload,
  emptyLabel,
}: {
  payload?: string | null
  emptyLabel: string
}) {
  return (
    <div className='rounded-md border border-border/60 bg-muted/20'>
      <pre className='max-h-[320px] overflow-auto p-4 font-mono text-xs leading-relaxed whitespace-pre-wrap break-words'>
        {payload || emptyLabel}
      </pre>
    </div>
  )
}

export function KafkaTopicPreviewModal({
  sourceId,
  topicName,
  open,
  onOpenChange,
}: KafkaTopicPreviewModalProps) {
  const [page, setPage] = useState(1)
  const [expandedRows, setExpandedRows] = useState<Record<string, boolean>>({})
  const [rowTabs, setRowTabs] = useState<Record<string, string>>({})

  const query = useQuery({
    queryKey: ['source-kafka-topic-preview', sourceId, topicName, page],
    queryFn: () => sourcesRepo.getKafkaTopicPreview(sourceId, topicName!, page),
    enabled: open && !!topicName,
    staleTime: 0,
  })

  const messages = query.data?.messages ?? []
  const title = useMemo(() => {
    if (!topicName) {
      return 'Topic Preview'
    }
    return `Topic Preview — ${topicName}`
  }, [topicName])

  const toggleRow = (rowId: string) => {
    setExpandedRows((current) => ({
      ...current,
      [rowId]: !current[rowId],
    }))
    setRowTabs((current) => ({
      ...current,
      [rowId]: current[rowId] || 'value',
    }))
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className='flex max-h-[90vh] flex-col overflow-hidden sm:max-w-6xl'>
        <DialogHeader>
          <DialogTitle>{title}</DialogTitle>
          <DialogDescription>
            Read-only Kafka message preview. Offsets are never committed from this view.
          </DialogDescription>
        </DialogHeader>

        <div className='flex items-center gap-2 text-sm text-muted-foreground'>
          <Badge variant='secondary'>
            {query.data?.total_messages ?? 0} messages
          </Badge>
          <Badge variant='outline'>
            Page {query.data?.page ?? page} of {query.data?.total_pages ?? 1}
          </Badge>
          <Badge variant='outline'>10 per page</Badge>
          {query.isFetching && (
            <span className='inline-flex items-center gap-1 text-xs'>
              <Loader2 className='h-3.5 w-3.5 animate-spin' />
              Loading
            </span>
          )}
        </div>

        <div className='min-h-0 flex-1 overflow-auto rounded-md border border-border/60'>
          <Table>
            <TableHeader className='sticky top-0 z-10 bg-background'>
              <TableRow>
                <TableHead className='w-12 text-center'> </TableHead>
                <TableHead className='w-24'>Offset</TableHead>
                <TableHead className='w-24'>Partition</TableHead>
                <TableHead className='w-44'>Timestamp</TableHead>
                <TableHead className='w-[28%]'>Key</TableHead>
                <TableHead className='w-[34%]'>Value</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {query.isLoading ? (
                <TableRow>
                  <TableCell colSpan={6} className='h-32 text-center'>
                    <div className='inline-flex items-center gap-2 text-sm text-muted-foreground'>
                      <Loader2 className='h-4 w-4 animate-spin' />
                      Loading preview messages...
                    </div>
                  </TableCell>
                </TableRow>
              ) : messages.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={6} className='h-32 text-center text-muted-foreground'>
                    No messages available for this topic.
                  </TableCell>
                </TableRow>
              ) : (
                messages.map((message) => {
                  const rowId = `${message.partition}-${message.offset}`
                  const isExpanded = !!expandedRows[rowId]
                  const activeTab = rowTabs[rowId] || 'value'

                  return (
                    <Fragment key={rowId}>
                      <TableRow key={rowId}>
                        <TableCell className='text-center'>
                          <Button
                            variant='ghost'
                            size='icon'
                            className='h-8 w-8'
                            onClick={() => toggleRow(rowId)}
                            aria-label={
                              isExpanded
                                ? 'Collapse message details'
                                : 'Expand message details'
                            }
                          >
                            {isExpanded ? (
                              <ChevronDown className='h-4 w-4' />
                            ) : (
                              <ChevronRight className='h-4 w-4' />
                            )}
                          </Button>
                        </TableCell>
                        <TableCell className='font-mono text-xs'>
                          {message.offset}
                        </TableCell>
                        <TableCell className='font-mono text-xs'>
                          {message.partition}
                        </TableCell>
                        <TableCell className='text-xs text-muted-foreground'>
                          {formatTimestamp(message.timestamp)}
                        </TableCell>
                        <TableCell className='max-w-0 truncate font-mono text-xs'>
                          {message.key_preview || '--'}
                        </TableCell>
                        <TableCell className='max-w-0 truncate font-mono text-xs'>
                          {message.value_preview || '--'}
                        </TableCell>
                      </TableRow>
                      {isExpanded && (
                        <TableRow className='bg-muted/10'>
                          <TableCell colSpan={6}>
                            <Tabs
                              value={activeTab}
                              onValueChange={(value) =>
                                setRowTabs((current) => ({
                                  ...current,
                                  [rowId]: value,
                                }))
                              }
                              className='gap-4'
                            >
                              <TabsList>
                                <TabsTrigger value='key'>Key</TabsTrigger>
                                <TabsTrigger value='value'>Value</TabsTrigger>
                                <TabsTrigger value='headers'>Headers</TabsTrigger>
                              </TabsList>
                              <TabsContent value='key'>
                                <PayloadPane
                                  payload={message.key}
                                  emptyLabel='No key payload'
                                />
                              </TabsContent>
                              <TabsContent value='value'>
                                <PayloadPane
                                  payload={message.value}
                                  emptyLabel='No value payload'
                                />
                              </TabsContent>
                              <TabsContent value='headers'>
                                <PayloadPane
                                  payload={message.headers}
                                  emptyLabel='No headers'
                                />
                              </TabsContent>
                            </Tabs>
                          </TableCell>
                        </TableRow>
                      )}
                    </Fragment>
                  )
                })
              )}
            </TableBody>
          </Table>
        </div>

        <div className='flex items-center justify-between gap-3 border-t border-border/60 pt-4'>
          <div className='text-sm text-muted-foreground'>
            Showing page {query.data?.page ?? page} for topic{' '}
            <span className='font-mono text-foreground'>{topicName || '--'}</span>
          </div>
          <div className='flex items-center gap-2'>
            <Button
              variant='outline'
              onClick={() => setPage((current) => Math.max(1, current - 1))}
              disabled={page <= 1 || query.isLoading || query.isFetching}
            >
              Previous
            </Button>
            <Button
              variant='outline'
              onClick={() => setPage((current) => current + 1)}
              disabled={
                query.isLoading ||
                query.isFetching ||
                page >= (query.data?.total_pages ?? 1)
              }
            >
              Next
            </Button>
          </div>
        </div>
      </DialogContent>
    </Dialog>
  )
}
