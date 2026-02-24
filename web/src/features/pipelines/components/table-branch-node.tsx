import { Button } from '@/components/ui/button'
import { Filter, Code2, AlertCircle, Database, Trash2, Hash, Key } from 'lucide-react'
import { cn } from '@/lib/utils'
import {
    Popover,
    PopoverContent,
    PopoverTrigger,
} from '@/components/ui/popover'
import { type TableSyncConfig } from '@/repo/pipelines'
import { tagsRepo } from '@/repo/tags'
import { useQuery } from '@tanstack/react-query'

interface TableBranchNodeProps {
    syncConfig: TableSyncConfig
    onEditFilter: () => void
    onEditCustomSql: () => void
    onEditTargetName: () => void
    onEditTags: () => void
    onEditPrimaryKeys: () => void
    onDelete: () => void
    isDeleting: boolean
}

export function TableBranchNode({
    syncConfig,
    onEditFilter,
    onEditCustomSql,
    onEditTargetName,
    onEditTags,
    onEditPrimaryKeys,
    onDelete,
    isDeleting
}: TableBranchNodeProps) {
    // Query tags for this sync config
    const { data: tableSyncTagsData } = useQuery({
        queryKey: ['table-sync-tags', syncConfig.id],
        queryFn: () => tagsRepo.getTableSyncTags(syncConfig.id),
        enabled: !!syncConfig.id,
    })

    const currentTags = tableSyncTagsData?.tags || []
    const hasCustomKeys = syncConfig.primary_key_column_target && syncConfig.primary_key_column_target.trim().length > 0

    return (
        <div className={cn(
            "relative group flex items-center p-2 pr-3 bg-card border rounded-md shadow-sm transition-all hover:shadow-md hover:border-primary/20",
            syncConfig.is_error ? "border-red-200 bg-red-50/10" : "border-border"
        )}>
            {/* Connector Line (Absolute to left) - Visualized by parent but we can add endpoint dot here if needed */}

            <div className="flex flex-col gap-1.5 flex-1 min-w-0">
                {/* Header: Target Name */}
                <div className="flex items-center gap-2">
                    <Database className="h-3.5 w-3.5 text-muted-foreground/70" />
                    <span className="font-mono text-sm font-medium truncate text-foreground" title={syncConfig.table_name_target}>
                        {syncConfig.table_name_target}
                    </span>
                </div>

                {/* Actions Row */}
                <div className="flex items-center gap-1.5 flex-wrap">
                    <Button
                        variant="ghost"
                        size="sm"
                        onClick={(e) => { e.stopPropagation(); onEditTargetName() }}
                        className="h-6 px-1.5 text-[10px] text-muted-foreground hover:text-foreground hover:bg-muted"
                        title="Rename Target Table"
                    >
                        Rename
                    </Button>

                    <div className="w-px h-3 bg-border mx-0.5" />

                    <Button
                        variant="ghost"
                        size="sm"
                        onClick={(e) => { e.stopPropagation(); onEditFilter() }}
                        className={cn(
                            "h-6 px-1.5 text-[10px] gap-1",
                            syncConfig.filter_sql
                                ? "text-blue-600 bg-blue-50 hover:bg-blue-100 hover:text-blue-700 dark:text-blue-400 dark:bg-blue-950/50 dark:hover:bg-blue-950/70 dark:hover:text-blue-300"
                                : "text-muted-foreground hover:text-foreground hover:bg-muted"
                        )}
                        title="Filter Data"
                    >
                        <Filter className="h-3 w-3" />
                        {syncConfig.filter_sql && "Active"}
                    </Button>

                    <Button
                        variant="ghost"
                        size="sm"
                        onClick={(e) => { e.stopPropagation(); onEditCustomSql() }}
                        className={cn(
                            "h-6 px-1.5 text-[10px] gap-1",
                            syncConfig.custom_sql
                                ? "text-purple-600 bg-purple-50 hover:bg-purple-100 hover:text-purple-700 dark:text-purple-400 dark:bg-purple-950/50 dark:hover:bg-purple-950/70 dark:hover:text-purple-300"
                                : "text-muted-foreground hover:text-foreground hover:bg-muted"
                        )}
                        title="Custom SQL Transformation"
                    >
                        <Code2 className="h-3 w-3" />
                        {syncConfig.custom_sql && "Active"}
                    </Button>

                    <Button
                        variant="ghost"
                        size="sm"
                        onClick={(e) => { e.stopPropagation(); onEditPrimaryKeys() }}
                        className={cn(
                            "h-6 px-1.5 text-[10px] gap-1",
                            hasCustomKeys
                                ? "text-amber-600 bg-amber-50 hover:bg-amber-100 hover:text-amber-700 dark:text-amber-400 dark:bg-amber-950/50 dark:hover:bg-amber-950/70 dark:hover:text-amber-300"
                                : "text-muted-foreground hover:text-foreground hover:bg-muted"
                        )}
                        title="Primary Key Configuration"
                    >
                        <Key className="h-3 w-3" />
                        {hasCustomKeys && "Custom"}
                    </Button>

                    <Button
                        variant="ghost"
                        size="sm"
                        onClick={(e) => { e.stopPropagation(); onEditTags() }}
                        className={cn(
                            "h-6 px-1.5 text-[10px] gap-1",
                            currentTags.length > 0
                                ? "text-indigo-600 bg-indigo-50 hover:bg-indigo-100 hover:text-indigo-700 dark:text-indigo-400 dark:bg-indigo-950/50 dark:hover:bg-indigo-950/70 dark:hover:text-indigo-300"
                                : "text-muted-foreground hover:text-foreground hover:bg-muted"
                        )}
                        title="Manage Tags"
                    >
                        <Hash className="h-3 w-3" />
                        {currentTags.length > 0 && currentTags.length}
                    </Button>
                </div>
            </div>

            {/* Right Side: Error & Delete */}
            <div className="flex items-center gap-1 ml-2">
                {syncConfig.is_error && (
                    <Popover>
                        <PopoverTrigger asChild onClick={(e) => e.stopPropagation()}>
                            <Button variant="ghost" size="icon" className="h-6 w-6 text-red-500 hover:text-red-600 hover:bg-red-50">
                                <AlertCircle className="h-3.5 w-3.5" />
                            </Button>
                        </PopoverTrigger>
                        <PopoverContent className="w-64 p-2 text-xs" side="top">
                            <p className="font-medium text-red-600 mb-1">Sync Error</p>
                            <div className="bg-muted p-1.5 rounded text-muted-foreground break-all">
                                {syncConfig.error_message}
                            </div>
                        </PopoverContent>
                    </Popover>
                )}

                <Button
                    variant="ghost"
                    size="icon"
                    onClick={(e) => { e.stopPropagation(); onDelete() }}
                    disabled={isDeleting}
                    className="h-6 w-6 text-muted-foreground/50 hover:text-red-500 hover:bg-red-50 transition-colors opacity-0 group-hover:opacity-100"
                    title="Delete Branch"
                >
                    {isDeleting ? <span className="animate-spin">⟳</span> : <Trash2 className="h-3.5 w-3.5" />}
                </Button>
            </div>
        </div>
    )
}
