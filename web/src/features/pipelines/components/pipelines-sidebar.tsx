import { useState, useMemo } from 'react'
import { useQueries, useQuery, useQueryClient } from '@tanstack/react-query'
import { useParams, useNavigate } from '@tanstack/react-router'
import { pipelinesRepo, type Pipeline } from '@/repo/pipelines'
import { sourcesRepo, type SourceDetailResponse } from '@/repo/sources'
import {
  Database,
  Table,
  Layers,
  Workflow,
  ChevronRight,
  Loader2,
  Search,
  RefreshCw,
  X,
  FolderInput,
  FolderSync,
  Command,
} from 'lucide-react'
import { cn } from '@/lib/utils'
import { Button } from '@/components/ui/button'
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from '@/components/ui/collapsible'
import {
  CustomTabs,
  CustomTabsList,
  CustomTabsTrigger,
  CustomTabsContent,
} from '@/components/ui/custom-tabs'
import {
  HoverCard,
  HoverCardContent,
  HoverCardTrigger,
} from '@/components/ui/hover-card'
import { Input } from '@/components/ui/input'
import { ScrollArea } from '@/components/ui/scroll-area'
import {
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  SidebarMenuSub,
  SidebarMenuSubButton,
  SidebarMenuSubItem,
} from '@/components/ui/sidebar'
import { usePipelineSelection } from '@/features/pipelines/context/pipeline-selection-context'

function HighlightedText({
  text,
  highlight,
}: {
  text: string
  highlight: string
}) {
  if (!highlight.trim()) {
    return <span>{text}</span>
  }

  const escapedHighlight = highlight.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
  const parts = text.split(new RegExp(`(${escapedHighlight})`, 'gi'))
  return (
    <span>
      {parts.map((part, i) =>
        part.toLowerCase() === highlight.toLowerCase() ? (
          <span key={i} className='bg-[#003e9b] px-0.5 font-medium text-white'>
            {part}
          </span>
        ) : (
          <span key={i}>{part}</span>
        )
      )}
    </span>
  )
}

const explorerSubButtonClassName =
  'h-8 w-full justify-start text-left text-muted-foreground dark:text-[#bec4d6] data-[active=true]:border-border/60 data-[active=true]:bg-accent data-[active=true]:text-accent-foreground'

function mergeExpandedItems(previous: string[], nextItems: string[]) {
  if (nextItems.length === 0) {
    return previous
  }

  const nextSet = new Set(previous)
  let changed = false

  nextItems.forEach((item) => {
    if (!nextSet.has(item)) {
      nextSet.add(item)
      changed = true
    }
  })

  return changed ? Array.from(nextSet) : previous
}

function setExpandedItem(previous: string[], item: string, open: boolean) {
  const hasItem = previous.includes(item)

  if (open) {
    return hasItem ? previous : [...previous, item]
  }

  return hasItem ? previous.filter((value) => value !== item) : previous
}

function ExplorerChevron({
  open,
  className,
}: {
  open: boolean
  className?: string
}) {
  return (
    <ChevronRight
      className={cn(
        'size-4 shrink-0 text-muted-foreground transition-transform duration-200',
        open && 'rotate-90',
        className
      )}
    />
  )
}

// -- Sub-components for clean recursion

function TableItem({
  name,
  isActive,
  highlight,
  type,
  sourceTable,
  onClick,
}: {
  name: string
  isActive?: boolean
  highlight: string
  database?: string
  type?: 'source' | 'destination'
  sourceTable?: string
  onClick?: () => void
}) {
  return (
    <SidebarMenuSubItem>
      <HoverCard openDelay={100} closeDelay={200}>
        <HoverCardTrigger asChild>
          <SidebarMenuSubButton
            asChild
            isActive={isActive}
            className={explorerSubButtonClassName}
          >
            <button type='button' onClick={onClick}>
              <Table className='h-3.5 w-3.5 shrink-0' />
              <span className='min-w-0 flex-1 truncate'>
                <HighlightedText
                  text={name.toUpperCase()}
                  highlight={highlight}
                />
              </span>
            </button>
          </SidebarMenuSubButton>
        </HoverCardTrigger>
        <HoverCardContent className='w-80' side='right' align='start'>
          <div className='space-y-2'>
            <div>
              <h4 className='mb-1 text-sm font-semibold'>
                {type === 'source' ? 'Source Table' : 'Destination Table'}
              </h4>
              <div className='space-y-1 text-xs'>
                <div className='flex items-start gap-2'>
                  <span className='min-w-20 text-muted-foreground'>
                    Table Name:
                  </span>
                  <span className='font-mono font-medium break-all'>
                    {name}
                  </span>
                </div>
                {sourceTable && type === 'destination' && (
                  <div className='flex items-start gap-2'>
                    <span className='min-w-20 text-muted-foreground'>
                      Source Table:
                    </span>
                    <span className='font-mono font-medium break-all'>
                      {sourceTable}
                    </span>
                  </div>
                )}
              </div>
            </div>
          </div>
        </HoverCardContent>
      </HoverCard>
    </SidebarMenuSubItem>
  )
}

function SourceTables({
  tables,
  searchQuery,
}: {
  tables: SourceDetailResponse['tables']
  searchQuery: string
}) {
  // Filter tables here
  const filteredTables = useMemo(() => {
    if (!searchQuery.trim()) return tables
    return tables.filter((t) =>
      t.table_name.toLowerCase().includes(searchQuery.toLowerCase())
    )
  }, [tables, searchQuery])

  if (!filteredTables?.length) {
    if (searchQuery) return null // Hide if no matches during search
    return (
      <SidebarMenuSub className='mt-1'>
        <div className='px-2 py-1 text-xs text-muted-foreground'>
          No tables found
        </div>
      </SidebarMenuSub>
    )
  }

  return (
    <SidebarMenuSub className='mt-1'>
      {filteredTables.map((table) => (
        <TableItem
          key={table.id}
          name={table.table_name}
          highlight={searchQuery}
          type='source'
        />
      ))}
    </SidebarMenuSub>
  )
}

function PipelineItem({
  pipeline,
  sourceDetails,
  checkExpanded,
  searchQuery,
  selectedSyncId,
}: {
  pipeline: Pipeline
  sourceDetails?: SourceDetailResponse | null
  checkExpanded?: string[]
  searchQuery: string
  selectedSyncId: number | null
}) {
  const navigate = useNavigate()
  const { selectTable } = usePipelineSelection()
  const sourceName = pipeline.source?.name || 'Source'
  const destinations = useMemo(
    () => pipeline.destinations ?? [],
    [pipeline.destinations]
  )

  // Use passed source details or empty if not loaded yet
  const sourceTables = sourceDetails?.tables || []

  const [openItems, setOpenItems] = useState<string[]>([])
  const handleOpenChange = (value: string, open: boolean) => {
    setOpenItems((previous) => setExpandedItem(previous, value, open))
  }

  const autoOpenItems = useMemo(() => {
    const nextItems = [...(checkExpanded ?? [])]

    if (selectedSyncId) {
      destinations.forEach((destination) => {
        const hasSelectedSync = destination.table_syncs?.some(
          (sync) => sync.id === selectedSyncId
        )
        if (hasSelectedSync) {
          nextItems.push('destinations', `dest-${destination.id}`)
        }
      })
    }

    return nextItems
  }, [checkExpanded, selectedSyncId, destinations])

  const effectiveOpenItems = useMemo(
    () => mergeExpandedItems(openItems, autoOpenItems),
    [openItems, autoOpenItems]
  )
  const isOpen = (value: string) => effectiveOpenItems.includes(value)

  // Filter destinations logic
  const filteredDestinations = useMemo(() => {
    if (!searchQuery.trim()) return destinations

    return destinations.filter((d) => {
      // If dest name matches, keep it
      if (d.destination.name.toLowerCase().includes(searchQuery.toLowerCase()))
        return true

      // If any table matches, keep it
      if (
        d.table_syncs?.some((s) =>
          (s.table_name_target || s.table_name)
            .toLowerCase()
            .includes(searchQuery.toLowerCase())
        )
      )
        return true

      return false
    })
  }, [destinations, searchQuery])

  return (
    <SidebarMenuSub className='mt-1'>
      <Collapsible
        open={isOpen('sources')}
        onOpenChange={(open) => handleOpenChange('sources', open)}
        className='group/collapsible'
      >
        <SidebarMenuSubItem>
          <CollapsibleTrigger asChild>
            <SidebarMenuSubButton
              asChild
              className={explorerSubButtonClassName}
            >
              <button type='button'>
                <ExplorerChevron open={isOpen('sources')} />
                <FolderInput className='h-4 w-4 shrink-0' />
                <span className='truncate'>Sources</span>
              </button>
            </SidebarMenuSubButton>
          </CollapsibleTrigger>
          <CollapsibleContent className='pt-1'>
            <SidebarMenuSub>
              <Collapsible
                open={isOpen(`src-${pipeline.source_id}`)}
                onOpenChange={(open) =>
                  handleOpenChange(`src-${pipeline.source_id}`, open)
                }
                className='group/collapsible'
              >
                <SidebarMenuSubItem>
                  <CollapsibleTrigger asChild>
                    <SidebarMenuSubButton
                      asChild
                      className={explorerSubButtonClassName}
                    >
                      <button type='button'>
                        <ExplorerChevron
                          open={isOpen(`src-${pipeline.source_id}`)}
                        />
                        <Database className='h-3.5 w-3.5 shrink-0' />
                        <span className='min-w-0 flex-1 truncate'>
                          <HighlightedText
                            text={sourceName}
                            highlight={searchQuery}
                          />
                        </span>
                      </button>
                    </SidebarMenuSubButton>
                  </CollapsibleTrigger>
                  <CollapsibleContent className='pt-1'>
                    <SourceTables
                      tables={sourceTables}
                      searchQuery={searchQuery}
                    />
                  </CollapsibleContent>
                </SidebarMenuSubItem>
              </Collapsible>
            </SidebarMenuSub>
          </CollapsibleContent>
        </SidebarMenuSubItem>
      </Collapsible>

      <Collapsible
        open={isOpen('destinations')}
        onOpenChange={(open) => handleOpenChange('destinations', open)}
        className='group/collapsible'
      >
        <SidebarMenuSubItem>
          <CollapsibleTrigger asChild>
            <SidebarMenuSubButton
              asChild
              className={explorerSubButtonClassName}
            >
              <button type='button'>
                <ExplorerChevron open={isOpen('destinations')} />
                <FolderSync className='h-4 w-4 shrink-0' />
                <span className='truncate'>Destinations</span>
              </button>
            </SidebarMenuSubButton>
          </CollapsibleTrigger>
          <CollapsibleContent className='pt-1'>
            <SidebarMenuSub>
              {filteredDestinations.length === 0 && (
                <div className='px-2 py-1 text-xs text-muted-foreground'>
                  No destinations found
                </div>
              )}
              {filteredDestinations.map((d) => (
                <Collapsible
                  key={d.id}
                  open={isOpen(`dest-${d.id}`)}
                  onOpenChange={(open) =>
                    handleOpenChange(`dest-${d.id}`, open)
                  }
                  className='group/collapsible'
                >
                  <SidebarMenuSubItem>
                    <CollapsibleTrigger asChild>
                      <SidebarMenuSubButton
                        asChild
                        className={explorerSubButtonClassName}
                      >
                        <button type='button'>
                          <ExplorerChevron open={isOpen(`dest-${d.id}`)} />
                          <Layers className='h-3.5 w-3.5 shrink-0' />
                          <span className='min-w-0 flex-1 truncate'>
                            <HighlightedText
                              text={d.destination.name}
                              highlight={searchQuery}
                            />
                          </span>
                        </button>
                      </SidebarMenuSubButton>
                    </CollapsibleTrigger>
                    <CollapsibleContent className='pt-1'>
                      <SidebarMenuSub>
                        {d.table_syncs
                          ?.filter(
                            (sync) =>
                              !searchQuery.trim() ||
                              (sync.table_name_target || sync.table_name)
                                .toLowerCase()
                                .includes(searchQuery.toLowerCase())
                          )
                          ?.map((sync) => (
                            <TableItem
                              key={sync.id}
                              name={sync.table_name_target || sync.table_name}
                              highlight={searchQuery}
                              type='destination'
                              sourceTable={sync.table_name}
                              isActive={selectedSyncId === sync.id}
                              onClick={() => {
                                navigate({
                                  to: '/pipelines/$pipelineId',
                                  params: {
                                    pipelineId: pipeline.id.toString(),
                                  },
                                })
                                selectTable(d.id, sync.id)
                              }}
                            />
                          ))}
                        {(!d.table_syncs || d.table_syncs.length === 0) && (
                          <div className='px-2 py-1 text-xs text-muted-foreground'>
                            No synced tables
                          </div>
                        )}
                      </SidebarMenuSub>
                    </CollapsibleContent>
                  </SidebarMenuSubItem>
                </Collapsible>
              ))}
            </SidebarMenuSub>
          </CollapsibleContent>
        </SidebarMenuSubItem>
      </Collapsible>
    </SidebarMenuSub>
  )
}

export function PipelinesSidebar() {
  const { pipelineId } = useParams({ strict: false }) as { pipelineId?: string }
  const currentId = pipelineId ? parseInt(pipelineId) : null
  const navigate = useNavigate()
  const { selection } = usePipelineSelection()
  const [searchQuery, setSearchQuery] = useState('')
  const [expandedItems, setExpandedItems] = useState<string[]>(() =>
    currentId ? [`pipeline-${currentId}`] : []
  )
  const [isManualRefreshing, setIsManualRefreshing] = useState(false)
  const queryClient = useQueryClient()

  // 1. Fetch Pipelines
  const {
    data: pipelinesData,
    isLoading: isLoadingPipelines,
    isError,
    isFetching,
  } = useQuery({
    queryKey: ['pipelines'],
    queryFn: pipelinesRepo.getAll,
  })

  const pipelines = useMemo(() => pipelinesData?.pipelines ?? [], [pipelinesData])

  // 2. Fetch Source Details (Eager Loading)
  // Extract unique source IDs
  const sourceIds = useMemo(() => {
    return Array.from(new Set(pipelines.map((p) => p.source_id))).filter(
      (id): id is number => typeof id === 'number' && !isNaN(id)
    )
  }, [pipelines])

  const sourceQueries = useQueries({
    queries: sourceIds.map((id) => ({
      queryKey: ['source-details', id],
      queryFn: () => sourcesRepo.getDetails(id),
      staleTime: 1000 * 60 * 5, // Cache for 5 minutes
      retry: false, // Don't retry on failure to avoid spamming the server
    })),
  })

  // Create a map for easy access: sourceId -> details
  const sourceDetailsMap = new Map<number, SourceDetailResponse>()
  sourceQueries.forEach((query, index) => {
    if (query.data) {
      sourceDetailsMap.set(sourceIds[index], query.data)
    }
  })

  // 3. Search Logic
  // We modify this to also return "internal expansion" maps per pipeline
  const { filteredPipelines, itemsToExpand, internalExpansionMap } =
    useMemo(() => {
      if (!searchQuery.trim()) {
        return {
          filteredPipelines: pipelines,
          itemsToExpand: [],
          internalExpansionMap: new Map<number, string[]>(),
        }
      }

      const lowerQuery = searchQuery.toLowerCase()
      const expanded = new Set<string>()
      const internalMap = new Map<number, string[]>()

      const filtered = pipelines.filter((pipeline) => {
        let isMatch = false
        const pId = `pipeline-${pipeline.id}`
        const internalIds = new Set<string>()

        // Check Pipeline Name
        if (pipeline.name.toLowerCase().includes(lowerQuery)) {
          isMatch = true
          // Pipeline name match doesn't force expansion of internals
        }

        // Check Source Name
        if (pipeline.source?.name.toLowerCase().includes(lowerQuery)) {
          isMatch = true
          expanded.add(pId)
          internalIds.add('sources')
        }

        // Check Source Tables
        const sourceDetails =
          pipeline.source_id !== null
            ? sourceDetailsMap.get(pipeline.source_id)
            : undefined
        if (sourceDetails?.tables) {
          const matchingTables = sourceDetails.tables.filter((t) =>
            t.table_name.toLowerCase().includes(lowerQuery)
          )
          if (matchingTables.length > 0) {
            isMatch = true
            expanded.add(pId)
            internalIds.add('sources')
            internalIds.add(`src-${pipeline.source_id}`)
          }
        }

        // Check Destinations & Destination Tables
        pipeline.destinations?.forEach((d) => {
          const dId = d.id

          if (d.destination.name.toLowerCase().includes(lowerQuery)) {
            isMatch = true
            expanded.add(pId)
            internalIds.add('destinations')
          }

          const matchingSyncs = d.table_syncs?.filter((s) =>
            (s.table_name_target || s.table_name)
              .toLowerCase()
              .includes(lowerQuery)
          )
          if (matchingSyncs && matchingSyncs.length > 0) {
            isMatch = true
            expanded.add(pId)
            internalIds.add('destinations')
            internalIds.add(`dest-${dId}`)
          }
        })

        if (isMatch) {
          internalMap.set(pipeline.id, Array.from(internalIds))
        }

        return isMatch
      })

      return {
        filteredPipelines: filtered,
        itemsToExpand: Array.from(expanded),
        internalExpansionMap: internalMap,
      }
    }, [pipelines, searchQuery, sourceDetailsMap])

  const effectiveExpandedItems = useMemo(() => {
    let nextItems = expandedItems

    if (searchQuery.trim()) {
      nextItems = mergeExpandedItems(nextItems, itemsToExpand)
    }

    return nextItems
  }, [expandedItems, itemsToExpand, searchQuery])

  if (isError) {
    return (
      <div className='p-4 text-sm text-destructive'>
        Failed to load pipelines.
      </div>
    )
  }

  if (isLoadingPipelines) {
    return (
      <div className='flex items-center justify-center p-4'>
        <Loader2 className='h-5 w-5 animate-spin text-muted-foreground' />
      </div>
    )
  }

  return (
    <div className='flex h-full flex-col border-r border-sidebar-border bg-background'>
      {/* Header: Title & Branding */}
      <div className='px-4 pt-4 pb-0'>
        <h1 className='text-xl font-bold text-foreground dark:text-[#bec4d6]'>
          Pipelines Explorer
        </h1>
        <div className='mb-4 flex items-center gap-2'>
          <Command className='h-4 w-4 text-cyan-500' />
          <span className='text-sm font-semibold text-cyan-500'>PIPELINES</span>
        </div>
      </div>
      {/* Pipelines Tab with Search */}
      <CustomTabs defaultValue='pipelines' className='flex flex-1 flex-col'>
        <CustomTabsList className='w-full justify-start border-b'>
          <CustomTabsTrigger value='pipelines'>Pipelines</CustomTabsTrigger>
        </CustomTabsList>
        <CustomTabsContent
          value='pipelines'
          className='mt-0 flex flex-1 flex-col'
        >
          <div className='p-3 pt-2'>
            <div className='flex items-center gap-2'>
              <div className='relative flex-1'>
                <Search className='absolute top-1/2 left-2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground' />
                <Input
                  placeholder='Search'
                  className='h-8 border-sidebar-border bg-sidebar-accent/50 pr-8 pl-8 text-xs focus-visible:border-[#3581f2]! focus-visible:ring-1! focus-visible:ring-[#3581f2]!'
                  value={searchQuery}
                  onChange={(e) => setSearchQuery(e.target.value)}
                />
                {searchQuery && (
                  <button
                    onClick={() => setSearchQuery('')}
                    className='absolute top-1/2 right-2 z-10 -translate-y-1/2 text-muted-foreground transition-colors hover:text-foreground'
                    title='Clear search'
                    type='button'
                  >
                    <X className='h-3.5 w-3.5' />
                  </button>
                )}
              </div>
              <Button
                variant='ghost'
                size='icon'
                className='h-8 w-8 text-muted-foreground hover:text-foreground'
                onClick={() => {
                  setIsManualRefreshing(true)
                  queryClient.invalidateQueries({ queryKey: ['pipelines'] })
                  queryClient.invalidateQueries({
                    queryKey: ['source-details'],
                  })
                  setTimeout(() => setIsManualRefreshing(false), 800)
                }}
                title='Refresh pipelines'
                disabled={isFetching || isManualRefreshing}
              >
                {isFetching || isManualRefreshing ? (
                  <Loader2 className='h-3.5 w-3.5 animate-spin' />
                ) : (
                  <RefreshCw className='h-3.5 w-3.5' />
                )}
              </Button>
            </div>
          </div>

          <ScrollArea className='mt-2 flex-1'>
            <div className='p-2'>
              {filteredPipelines.length === 0 && (
                <div className='p-4 text-center text-xs text-muted-foreground'>
                  {searchQuery
                    ? `No results for "${searchQuery}"`
                    : 'No pipelines found'}
                </div>
              )}
              <SidebarMenu>
                {filteredPipelines.map((pipeline) => {
                  const pipelineKey = `pipeline-${pipeline.id}`
                  const isPipelineOpen =
                    effectiveExpandedItems.includes(pipelineKey)

                  return (
                    <Collapsible
                      key={pipeline.id}
                      open={isPipelineOpen}
                      onOpenChange={(open) => {
                        if (currentId !== pipeline.id) return

                        setExpandedItems((previous) =>
                          setExpandedItem(previous, pipelineKey, open)
                        )
                      }}
                      className='group/collapsible'
                    >
                      <SidebarMenuItem className='mb-1'>
                        <CollapsibleTrigger asChild>
                          <SidebarMenuButton
                            isActive={currentId === pipeline.id}
                            className={cn(
                              'justify-start gap-2 text-sm font-medium',
                              currentId === pipeline.id &&
                                'bg-[#d6e6ff] text-[#088ae8] hover:bg-[#d6e6ff] hover:text-[#088ae8] dark:bg-[#002c6e] dark:text-[#5999f7]'
                            )}
                            onClick={(event) => {
                              if (currentId !== pipeline.id) {
                                event.preventDefault()
                                setExpandedItems((previous) =>
                                  mergeExpandedItems(previous, [pipelineKey])
                                )
                                navigate({
                                  to: '/pipelines/$pipelineId',
                                  params: {
                                    pipelineId: pipeline.id.toString(),
                                  },
                                })
                              }
                            }}
                          >
                            <ExplorerChevron
                              open={isPipelineOpen}
                              className={
                                currentId === pipeline.id
                                  ? 'text-[#5999f7]'
                                  : undefined
                              }
                            />
                            <Workflow
                              className={cn(
                                'h-4 w-4 shrink-0',
                                currentId === pipeline.id
                                  ? 'text-[#5999f7]'
                                  : 'text-primary'
                              )}
                            />
                            <span className='min-w-0 flex-1 truncate'>
                              <HighlightedText
                                text={pipeline.name}
                                highlight={searchQuery}
                              />
                            </span>
                          </SidebarMenuButton>
                        </CollapsibleTrigger>
                        <CollapsibleContent className='pt-1'>
                          <PipelineItem
                            pipeline={pipeline}
                            sourceDetails={
                              pipeline.source_id !== null
                                ? sourceDetailsMap.get(pipeline.source_id)
                                : undefined
                            }
                            checkExpanded={internalExpansionMap.get(
                              pipeline.id
                            )}
                            searchQuery={searchQuery}
                            selectedSyncId={selection.syncId}
                          />
                        </CollapsibleContent>
                      </SidebarMenuItem>
                    </Collapsible>
                  )
                })}
              </SidebarMenu>
            </div>
          </ScrollArea>
        </CustomTabsContent>
      </CustomTabs>
    </div>
  )
}
