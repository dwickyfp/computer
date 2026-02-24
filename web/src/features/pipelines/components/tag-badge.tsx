import { Badge } from '@/components/ui/badge'
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from '@/components/ui/popover'
import { ScrollArea } from '@/components/ui/scroll-area'
import { cn } from '@/lib/utils'
import { tagsRepo } from '@/repo/tags'
import { useQuery } from '@tanstack/react-query'
import { Link, type HistoryState } from '@tanstack/react-router'
import {
  Database,
  ExternalLink,
  Hash,
  Loader2,
  Network,
  Table2,
  X,
} from 'lucide-react'
import { useState } from 'react'

interface TagBadgeProps {
  tag: string
  tagId?: number // Optional for opening popover
  onRemove?: () => void
  variant?: 'default' | 'outline'
  className?: string
}

export function TagBadge({
  tag,
  tagId,
  onRemove,
  variant = 'default',
  className,
}: TagBadgeProps) {
  const [isOpen, setIsOpen] = useState(false)

  const { data: usageData, isLoading } = useQuery({
    queryKey: ['tag-usage', tagId],
    queryFn: () => tagsRepo.getUsage(tagId!),
    enabled: !!tagId && isOpen,
  })

  // Prevent popover from closing when clicking links
  const handleLinkClick = () => {
    setIsOpen(false)
  }

  const badgeContent = (
    <Badge
      variant={variant}
      className={cn(
        'group flex items-center gap-1.5 h-8 px-3.5 text-sm font-medium rounded-md',
        'bg-gradient-to-r from-blue-50 to-indigo-50 text-blue-700 border-blue-200',
        'dark:from-blue-950/30 dark:to-indigo-950/30 dark:text-blue-300 dark:border-blue-800/50',
        'hover:from-blue-100 hover:to-indigo-100 dark:hover:from-blue-950/50 dark:hover:to-indigo-950/50',
        'transition-all',
        tagId && !onRemove && 'cursor-pointer',
        className
      )}
    >
      <Hash className="h-4 w-4 opacity-70" />
      <span className="truncate max-w-[120px]" title={tag}>
        {tag}
      </span>
      {onRemove && (
        <button
          onClick={(e) => {
            e.stopPropagation()
            onRemove()
          }}
          className="ml-1 rounded-full p-0.5 hover:bg-blue-200 dark:hover:bg-blue-800/50 transition-colors"
          aria-label="Remove tag"
        >
          <X className="h-3.5 w-3.5" />
        </button>
      )}
    </Badge>
  )

  if (!tagId || onRemove) {
    return badgeContent
  }

  return (
    <Popover open={isOpen} onOpenChange={setIsOpen}>
      <PopoverTrigger asChild>{badgeContent}</PopoverTrigger>
      <PopoverContent className="w-96 p-0" align="start">
        <div className="border-b px-4 py-3 bg-muted/50">
          <h4 className="font-semibold flex items-center gap-2">
            <Hash className="h-4 w-4 text-muted-foreground" />
            {tag}
          </h4>
          <p className="text-xs text-muted-foreground mt-1">
            Usage details across pipelines
          </p>
        </div>
        <ScrollArea className="h-[300px]">
          <div className="p-4 space-y-4">
            {isLoading ? (
              <div className="flex h-20 items-center justify-center text-muted-foreground">
                <Loader2 className="h-5 w-5 animate-spin mr-2" />
                Loading usage...
              </div>
            ) : usageData?.usage.length === 0 ? (
              <div className="text-sm text-muted-foreground text-center py-4">
                No active usage found for this tag.
              </div>
            ) : (
              usageData?.usage.map((pipeline, i) => (
                <div key={i} className="space-y-3">
                  <Link
                    to="/pipelines/$pipelineId"
                    params={{ pipelineId: String(pipeline.pipeline_id) }}
                    className="group/link flex items-center gap-2 font-medium text-sm text-foreground hover:text-blue-600 transition-colors"
                    onClick={handleLinkClick}
                  >
                    <Network className="h-4 w-4 text-primary" />
                    {pipeline.pipeline_name}
                    <ExternalLink className="h-3 w-3 opacity-50 shrink-0" />
                  </Link>
                  <div className="ml-2 pl-4 border-l-2 border-muted space-y-3">
                    {pipeline.destinations.map((dest, j) => (
                      <div key={j} className="space-y-2">
                        <Link
                          to="/pipelines/$pipelineId"
                          params={{ pipelineId: String(pipeline.pipeline_id) }}
                          state={{ highlightDestination: dest.destination_id } as HistoryState}
                          className="group/link flex items-center gap-2 text-xs font-medium text-muted-foreground hover:text-blue-500 transition-colors cursor-pointer w-fit"
                          onClick={handleLinkClick}
                        >
                          <Database className="h-3 w-3" />
                          {dest.destination_name}
                          <ExternalLink className="h-2.5 w-2.5 opacity-50 shrink-0" />
                        </Link>
                        <div className="space-y-1 ml-1 pl-3 border-l border-muted/50">
                          {dest.tables.map((table, k) => (
                            <Link
                              key={k}
                              to="/pipelines/$pipelineId"
                              params={{ pipelineId: String(pipeline.pipeline_id) }}
                              state={{
                                openDrawer: true,
                                openDrawerDestinationId: dest.destination_id,
                                highlightTable: table,
                              } as HistoryState}
                              className="group/link flex items-center gap-2 text-xs text-muted-foreground/80 py-0.5 hover:text-blue-500 transition-colors cursor-pointer w-fit"
                              onClick={handleLinkClick}
                            >
                              <Table2 className="h-3 w-3 opacity-70" />
                              {table}
                              <ExternalLink className="h-2.5 w-2.5 opacity-50 shrink-0" />
                            </Link>
                          ))}
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              ))
            )}
          </div>
        </ScrollArea>
      </PopoverContent>
    </Popover>
  )
}
