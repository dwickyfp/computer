import { useQuery } from '@tanstack/react-query'
import { tagsRepo } from '@/repo/tags'
import { Filter, Hash, Search as SearchIcon } from 'lucide-react'
import { useState, useMemo } from 'react'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { TagBadge } from '@/features/pipelines/components/tag-badge'
import { AnimatedSeparator } from '../components/animated-separator'
import { FloatingViewToggle, type SmartTagView } from '../components/floating-view-toggle'
import { TagNetworkVisualization } from '../components/tag-network-visualization'
import { Header } from '@/components/layout/header'
import { Main } from '@/components/layout/main'
import { Search } from '@/components/search'
import { ThemeSwitch } from '@/components/theme-switch'
import { pipelinesRepo } from '@/repo/pipelines'
import { destinationsRepo } from '@/repo/destinations'
import { sourcesRepo } from '@/repo/sources'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from '@/components/ui/popover'
import { Label } from '@/components/ui/label'
import { Badge } from '@/components/ui/badge'

export function SmartTagsPage() {
  const [activeView, setActiveView] = useState<SmartTagView>('list')
  const [selectedPipelineId, setSelectedPipelineId] = useState<string>('all')
  const [selectedDestinationId, setSelectedDestinationId] = useState<string>('all')
  const [selectedSourceId, setSelectedSourceId] = useState<string>('all')
  const [searchQuery, setSearchQuery] = useState('')

  const { data: pipelinesData } = useQuery({
    queryKey: ['pipelines'],
    queryFn: pipelinesRepo.getAll,
  })

  const { data: destinationsData } = useQuery({
    queryKey: ['destinations'],
    queryFn: destinationsRepo.getAll,
  })

  const { data: sourcesData } = useQuery({
    queryKey: ['sources'],
    queryFn: sourcesRepo.getAll,
  })

  const smartTagFilterParams = {
    ...(selectedPipelineId !== 'all' && { pipeline_id: Number(selectedPipelineId) }),
    ...(selectedDestinationId !== 'all' && { destination_id: Number(selectedDestinationId) }),
    ...(selectedSourceId !== 'all' && { source_id: Number(selectedSourceId) }),
  }

  const activeFiltersCount = [
    selectedPipelineId !== 'all',
    selectedDestinationId !== 'all',
    selectedSourceId !== 'all',
  ].filter(Boolean).length

  const { data, isLoading } = useQuery({
    queryKey: ['smart-tags', smartTagFilterParams],
    queryFn: () => tagsRepo.getSmartTags(smartTagFilterParams),
  })

  // Filter tags based on search query
  const filteredGroups = useMemo(() => {
    if (!data?.groups || !searchQuery.trim()) {
      return data?.groups ?? []
    }

    const lowerQuery = searchQuery.toLowerCase()
    return data.groups
      .map((group) => ({
        ...group,
        tags: group.tags.filter((tag) =>
          tag.tag.toLowerCase().includes(lowerQuery)
        ),
        count: group.tags.filter((tag) =>
          tag.tag.toLowerCase().includes(lowerQuery)
        ).length,
      }))
      .filter((group) => group.count > 0)
  }, [data?.groups, searchQuery])

  if (isLoading) {
    return (
      <div className="flex h-[calc(100vh-4rem)] items-center justify-center">
        <div className="text-sm text-muted-foreground">Loading...</div>
      </div>
    )
  }

  return (
    <>
      <Header fixed>
        <Search />
        <div className="ms-auto flex items-center space-x-4">
          <ThemeSwitch />
        </div>
      </Header>

      <Main className="flex flex-1 flex-col gap-4 sm:gap-6">
        {/* List View */}
        <div
          className={`${activeView === 'list' ? 'block animate-in fade-in duration-300' : 'hidden'}`}
        >
          {/* Page Title and Filter */}
          <div className="flex flex-wrap items-end justify-between gap-2">
            <div>
              <h2 className="text-2xl font-bold tracking-tight">Smart Tags</h2>
              <p className="text-muted-foreground">
                Manage and organize your tags across pipelines.
              </p>
            </div>

            <div className="flex flex-wrap items-center gap-2">
              <div className="relative w-64">
                <SearchIcon className="absolute left-2.5 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
                <Input
                  type="text"
                  placeholder="Search tags..."
                  value={searchQuery}
                  onChange={(e) => setSearchQuery(e.target.value)}
                  className="h-9 pl-9 text-sm"
                />
              </div>
              <Popover>
                <PopoverTrigger asChild>
                  <Button variant="outline" size="sm" className="h-9 gap-2">
                    <Filter className="h-4 w-4" />
                    Filter
                    {activeFiltersCount > 0 && (
                      <Badge variant="secondary" className="px-1 text-xs">
                        {activeFiltersCount}
                      </Badge>
                    )}
                  </Button>
                </PopoverTrigger>
                <PopoverContent className="w-80" align="end">
                  <div className="grid gap-4">
                    <div className="space-y-2">
                      <h4 className="font-medium leading-none">Filters</h4>
                      <p className="text-sm text-muted-foreground">
                        Refine tags by pipeline, destination, or source.
                      </p>
                    </div>
                    <div className="grid gap-3">
                      <div className="grid gap-1">
                        <Label htmlFor="pipeline-filter" className="text-xs">
                          Pipeline
                        </Label>
                        <Select
                          value={selectedPipelineId}
                          onValueChange={setSelectedPipelineId}
                        >
                          <SelectTrigger
                            id="pipeline-filter"
                            className="h-8 w-full text-xs"
                          >
                            <SelectValue placeholder="All Pipelines" />
                          </SelectTrigger>
                          <SelectContent>
                            <SelectItem value="all">All Pipelines</SelectItem>
                            {(pipelinesData?.pipelines ?? []).map((pipeline) => (
                              <SelectItem
                                key={pipeline.id}
                                value={String(pipeline.id)}
                              >
                                {pipeline.name}
                              </SelectItem>
                            ))}
                          </SelectContent>
                        </Select>
                      </div>

                      <div className="grid gap-1">
                        <Label htmlFor="destination-filter" className="text-xs">
                          Destination
                        </Label>
                        <Select
                          value={selectedDestinationId}
                          onValueChange={setSelectedDestinationId}
                        >
                          <SelectTrigger
                            id="destination-filter"
                            className="h-8 w-full text-xs"
                          >
                            <SelectValue placeholder="All Destinations" />
                          </SelectTrigger>
                          <SelectContent>
                            <SelectItem value="all">All Destinations</SelectItem>
                            {(destinationsData?.destinations ?? []).map(
                              (destination) => (
                                <SelectItem
                                  key={destination.id}
                                  value={String(destination.id)}
                                >
                                  {destination.name}
                                </SelectItem>
                              )
                            )}
                          </SelectContent>
                        </Select>
                      </div>

                      <div className="grid gap-1">
                        <Label htmlFor="source-filter" className="text-xs">
                          Source
                        </Label>
                        <Select
                          value={selectedSourceId}
                          onValueChange={setSelectedSourceId}
                        >
                          <SelectTrigger
                            id="source-filter"
                            className="h-8 w-full text-xs"
                          >
                            <SelectValue placeholder="All Sources" />
                          </SelectTrigger>
                          <SelectContent>
                            <SelectItem value="all">All Sources</SelectItem>
                            {(sourcesData?.sources ?? []).map((source) => (
                              <SelectItem key={source.id} value={String(source.id)}>
                                {source.name}
                              </SelectItem>
                            ))}
                          </SelectContent>
                        </Select>
                      </div>
                    </div>

                    {activeFiltersCount > 0 && (
                      <Button
                        variant="ghost"
                        size="sm"
                        onClick={() => {
                          setSelectedPipelineId('all')
                          setSelectedDestinationId('all')
                          setSelectedSourceId('all')
                        }}
                        className="w-full text-xs"
                      >
                        Clear Filters
                      </Button>
                    )}
                  </div>
                </PopoverContent>
              </Popover>
            </div>
          </div>

          {/* Tags Content */}
          <div className="space-y-6 pb-20">
            {filteredGroups.map((group, index) => (
              <div key={group.letter}>
                <div className="space-y-3">
                  {/* Letter Header */}
                  <div className="flex items-baseline gap-2">
                    <h3 className="text-lg font-semibold text-foreground">
                      {group.letter}
                    </h3>
                    <span className="text-sm text-muted-foreground">({group.count})</span>
                  </div>

                  {/* Tags Grid */}
                  <div className="flex flex-wrap gap-2">
                    {group.tags.map((tag) => (
                      <TagBadge
                        key={tag.id}
                        tag={tag.tag}
                        tagId={tag.id}
                        variant="outline"
                      />
                    ))}
                  </div>
                </div>

                {/* Separator between sections */}
                {index < filteredGroups.length - 1 && (
                  <AnimatedSeparator className="mt-6" />
                )}
              </div>
            ))}

            {filteredGroups.length === 0 && (
              <div className="flex h-64 items-center justify-center">
                <div className="text-center">
                  <Hash className="mx-auto h-12 w-12 text-muted-foreground/50" />
                  <p className="mt-2 text-sm text-muted-foreground">No tags found</p>
                </div>
              </div>
            )}
          </div>
        </div>

        {/* 3D Network View */}
        <div
          className={`${activeView === 'visualization' ? 'block animate-in fade-in duration-300' : 'hidden'}`}
        >
          <div className="pb-20">
            <TagNetworkVisualization isVisible={activeView === 'visualization'} />
          </div>
        </div>
      </Main>

      {/* Floating View Toggle */}
      <FloatingViewToggle activeView={activeView} onViewChange={setActiveView} />
    </>
  )
}
