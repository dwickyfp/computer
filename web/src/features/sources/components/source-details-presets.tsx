import { useState, useMemo } from 'react'
import { useParams } from '@tanstack/react-router'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { sourcesRepo, type Preset } from '@/repo/sources'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Loader2, Trash2, Eye, Play, Search, Grid3x3, List, Calendar, Table2, Clock } from 'lucide-react'
import { toast } from 'sonner'
import { Badge } from '@/components/ui/badge'
import {
    Dialog,
    DialogContent,
    DialogDescription,
    DialogHeader,
    DialogTitle,
} from '@/components/ui/dialog'
import {
    Tooltip,
    TooltipContent,
    TooltipProvider,
    TooltipTrigger,
} from '@/components/ui/tooltip'
import { Input } from '@/components/ui/input'
import { ScrollArea } from '@/components/ui/scroll-area'
import { Separator } from '@/components/ui/separator'
import { cn } from '@/lib/utils'

type ViewMode = 'grid' | 'list'

export function SourceDetailsPresets() {
    const { sourceId } = useParams({ from: '/_authenticated/sources/$sourceId/details' })
    const id = parseInt(sourceId)
    const queryClient = useQueryClient()
    const [viewPreset, setViewPreset] = useState<Preset | null>(null)
    const [searchQuery, setSearchQuery] = useState('')
    const [viewMode, setViewMode] = useState<ViewMode>('grid')
    const [tableSearchQuery, setTableSearchQuery] = useState('')

    const { data: presets, isLoading } = useQuery({
        queryKey: ['source-presets', id],
        queryFn: () => sourcesRepo.getPresets(id),
        enabled: !!id,
    })

    const deletePresetMutation = useMutation({
        mutationFn: async (presetId: number) => {
            await sourcesRepo.deletePreset(presetId)
        },
        onSuccess: () => {
            toast.success("Preset deleted successfully")
            setTimeout(() => {
                queryClient.invalidateQueries({ queryKey: ['source-presets', id] })
            }, 300)
        },
        onError: () => {
            toast.error("Failed to delete preset")
        }
    })

    // Filter presets based on search query
    const filteredPresets = useMemo(() => {
        if (!presets) return []
        if (!searchQuery) return presets
        
        return presets.filter(preset => 
            preset.name.toLowerCase().includes(searchQuery.toLowerCase()) ||
            preset.table_names.some(table => table.toLowerCase().includes(searchQuery.toLowerCase()))
        )
    }, [presets, searchQuery])

    // Filter tables in detail view
    const filteredTables = useMemo(() => {
        if (!viewPreset) return []
        if (!tableSearchQuery) return viewPreset.table_names
        
        return viewPreset.table_names.filter(table => 
            table.toLowerCase().includes(tableSearchQuery.toLowerCase())
        )
    }, [viewPreset, tableSearchQuery])

    const handleLoadPreset = (preset: Preset) => {
        // TODO: Implement load preset functionality
        toast.info(`Loading preset "${preset.name}"...`)
        console.log('Load preset:', preset)
    }

    const formatDate = (dateString: string) => {
        const date = new Date(dateString)
        const now = new Date()
        const diffMs = now.getTime() - date.getTime()
        const diffDays = Math.floor(diffMs / (1000 * 60 * 60 * 24))

        if (diffDays === 0) return 'Today'
        if (diffDays === 1) return 'Yesterday'
        if (diffDays < 7) return `${diffDays} days ago`
        if (diffDays < 30) return `${Math.floor(diffDays / 7)} weeks ago`
        
        return date.toLocaleDateString('en-US', { 
            month: 'short', 
            day: 'numeric', 
            year: date.getFullYear() !== now.getFullYear() ? 'numeric' : undefined 
        })
    }

    const isRecentlyUpdated = (dateString: string) => {
        const date = new Date(dateString)
        const now = new Date()
        const diffMs = now.getTime() - date.getTime()
        const diffDays = Math.floor(diffMs / (1000 * 60 * 60 * 24))
        return diffDays < 7
    }

    if (isLoading) {
        return (
            <div className="flex justify-center items-center p-12">
                <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
            </div>
        )
    }

    return (
        <TooltipProvider>
            <div className="space-y-4">
                {/* Header with search and view controls */}
                <div className="flex flex-col sm:flex-row gap-3 items-start sm:items-center justify-between">
                    <div className="relative flex-1 w-full sm:max-w-sm">
                        <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 h-4 w-4 text-muted-foreground" />
                        <Input
                            placeholder="Search presets..."
                            value={searchQuery}
                            onChange={(e) => setSearchQuery(e.target.value)}
                            className="pl-9"
                        />
                    </div>
                    <div className="flex items-center gap-2">
                        <div className="flex rounded-md border">
                            <Tooltip>
                                <TooltipTrigger asChild>
                                    <Button
                                        variant={viewMode === 'grid' ? 'secondary' : 'ghost'}
                                        size="sm"
                                        onClick={() => setViewMode('grid')}
                                        className="rounded-r-none"
                                    >
                                        <Grid3x3 className="h-4 w-4" />
                                    </Button>
                                </TooltipTrigger>
                                <TooltipContent>Grid view</TooltipContent>
                            </Tooltip>
                            <Tooltip>
                                <TooltipTrigger asChild>
                                    <Button
                                        variant={viewMode === 'list' ? 'secondary' : 'ghost'}
                                        size="sm"
                                        onClick={() => setViewMode('list')}
                                        className="rounded-l-none"
                                    >
                                        <List className="h-4 w-4" />
                                    </Button>
                                </TooltipTrigger>
                                <TooltipContent>List view</TooltipContent>
                            </Tooltip>
                        </div>
                    </div>
                </div>

                {/* Empty state */}
                {filteredPresets?.length === 0 && (
                    <Card className="border-dashed">
                        <CardContent className="flex flex-col items-center justify-center p-12 text-center">
                            <div className="rounded-full bg-muted p-4 mb-4">
                                <Table2 className="h-8 w-8 text-muted-foreground" />
                            </div>
                            <h3 className="text-lg font-semibold mb-2">
                                {searchQuery ? 'No presets found' : 'No presets yet'}
                            </h3>
                            <p className="text-sm text-muted-foreground max-w-md">
                                {searchQuery 
                                    ? 'Try adjusting your search query to find what you\'re looking for.'
                                    : 'Save table selections from the "List Tables" tab to create reusable presets.'}
                            </p>
                        </CardContent>
                    </Card>
                )}

                {/* Grid View */}
                {viewMode === 'grid' && filteredPresets && filteredPresets.length > 0 && (
                    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                        {filteredPresets.map(preset => (
                            <Card 
                                key={preset.id} 
                                className={cn(
                                    "group relative overflow-hidden transition-all duration-200 hover:shadow-lg hover:border-primary/50",
                                    isRecentlyUpdated(preset.updated_at) && "border-primary/20"
                                )}
                            >
                                {isRecentlyUpdated(preset.updated_at) && (
                                    <div className="absolute top-0 right-0 w-20 h-20 -mr-10 -mt-10">
                                        <div className="absolute transform rotate-45 bg-primary text-primary-foreground text-xs font-semibold py-1 right-[-35px] top-[32px] w-[170px] text-center">
                                            Recent
                                        </div>
                                    </div>
                                )}
                                
                                <CardHeader className="pb-3">
                                    <div className="flex items-start justify-between gap-2">
                                        <div className="flex-1 min-w-0">
                                            <CardTitle className="text-base font-semibold truncate">
                                                {preset.name}
                                            </CardTitle>
                                            <CardDescription className="text-xs mt-1.5 flex items-center gap-1">
                                                <Calendar className="h-3 w-3" />
                                                {formatDate(preset.created_at)}
                                            </CardDescription>
                                        </div>
                                    </div>
                                </CardHeader>
                                
                                <CardContent className="space-y-4">
                                    {/* Stats */}
                                    <div className="flex items-center justify-between">
                                        <div className="flex items-center gap-2">
                                            <div className="rounded-lg bg-primary/10 p-2">
                                                <Table2 className="h-4 w-4 text-primary" />
                                            </div>
                                            <div>
                                                <div className="text-2xl font-bold">
                                                    {preset.table_names.length}
                                                </div>
                                                <div className="text-xs text-muted-foreground">
                                                    {preset.table_names.length === 1 ? 'Table' : 'Tables'}
                                                </div>
                                            </div>
                                        </div>
                                        {preset.updated_at !== preset.created_at && (
                                            <Tooltip>
                                                <TooltipTrigger asChild>
                                                    <div className="flex items-center gap-1 text-xs text-muted-foreground">
                                                        <Clock className="h-3 w-3" />
                                                        <span>{formatDate(preset.updated_at)}</span>
                                                    </div>
                                                </TooltipTrigger>
                                                <TooltipContent>Last updated</TooltipContent>
                                            </Tooltip>
                                        )}
                                    </div>

                                    {/* Table preview */}
                                    <div className="space-y-2">
                                        <div className="text-xs font-medium text-muted-foreground">Tables Preview</div>
                                        <div className="flex flex-wrap gap-1.5">
                                            {preset.table_names.slice(0, 3).map(name => (
                                                <Badge 
                                                    key={name} 
                                                    variant="secondary" 
                                                    className="text-[10px] font-mono px-2 py-0.5"
                                                >
                                                    {name}
                                                </Badge>
                                            ))}
                                            {preset.table_names.length > 3 && (
                                                <Badge variant="outline" className="text-[10px] px-2 py-0.5">
                                                    +{preset.table_names.length - 3}
                                                </Badge>
                                            )}
                                        </div>
                                    </div>

                                    <Separator />

                                    {/* Actions */}
                                    <div className="flex gap-2">
                                        <Tooltip>
                                            <TooltipTrigger asChild>
                                                <Button
                                                    variant="outline"
                                                    size="sm"
                                                    className="flex-1"
                                                    onClick={() => setViewPreset(preset)}
                                                >
                                                    <Eye className="h-3.5 w-3.5" />
                                                </Button>
                                            </TooltipTrigger>
                                            <TooltipContent>View details</TooltipContent>
                                        </Tooltip>
                                        
                                        <Tooltip>
                                            <TooltipTrigger asChild>
                                                <Button
                                                    variant="outline"
                                                    size="sm"
                                                    className="text-destructive hover:bg-destructive/10 hover:text-destructive flex-1"
                                                    onClick={() => deletePresetMutation.mutate(preset.id)}
                                                    disabled={deletePresetMutation.isPending}
                                                >
                                                    <Trash2 className="h-3.5 w-3.5" />
                                                </Button>
                                            </TooltipTrigger>
                                            <TooltipContent>Delete preset</TooltipContent>
                                        </Tooltip>
                                    </div>
                                </CardContent>
                            </Card>
                        ))}
                    </div>
                )}

                {/* List View */}
                {viewMode === 'list' && filteredPresets && filteredPresets.length > 0 && (
                    <div className="space-y-2">
                        {filteredPresets.map(preset => (
                            <Card 
                                key={preset.id} 
                                className={cn(
                                    "group transition-all duration-200 hover:shadow-md hover:border-primary/50",
                                    isRecentlyUpdated(preset.updated_at) && "border-primary/20"
                                )}
                            >
                                <CardContent className="flex items-center gap-4 p-4">
                                    {/* Icon */}
                                    <div className="rounded-lg bg-primary/10 p-3 flex-shrink-0">
                                        <Table2 className="h-5 w-5 text-primary" />
                                    </div>

                                    {/* Info */}
                                    <div className="flex-1 min-w-0">
                                        <div className="flex items-center gap-2 mb-1">
                                            <h3 className="font-semibold truncate">{preset.name}</h3>
                                            {isRecentlyUpdated(preset.updated_at) && (
                                                <Badge variant="default" className="text-[10px] h-5">Recent</Badge>
                                            )}
                                        </div>
                                        <div className="flex items-center gap-4 text-xs text-muted-foreground">
                                            <span className="flex items-center gap-1">
                                                <Table2 className="h-3 w-3" />
                                                {preset.table_names.length} {preset.table_names.length === 1 ? 'table' : 'tables'}
                                            </span>
                                            <span className="flex items-center gap-1">
                                                <Calendar className="h-3 w-3" />
                                                Created {formatDate(preset.created_at)}
                                            </span>
                                            {preset.updated_at !== preset.created_at && (
                                                <span className="flex items-center gap-1">
                                                    <Clock className="h-3 w-3" />
                                                    Updated {formatDate(preset.updated_at)}
                                                </span>
                                            )}
                                        </div>
                                        <div className="mt-2 flex flex-wrap gap-1">
                                            {preset.table_names.slice(0, 5).map(name => (
                                                <Badge 
                                                    key={name} 
                                                    variant="secondary" 
                                                    className="text-[10px] font-mono px-1.5 py-0"
                                                >
                                                    {name}
                                                </Badge>
                                            ))}
                                            {preset.table_names.length > 5 && (
                                                <Badge variant="outline" className="text-[10px] px-1.5 py-0">
                                                    +{preset.table_names.length - 5}
                                                </Badge>
                                            )}
                                        </div>
                                    </div>

                                    {/* Actions */}
                                    <div className="flex gap-2 flex-shrink-0">
                                        <Tooltip>
                                            <TooltipTrigger asChild>
                                                <Button
                                                    variant="outline"
                                                    size="sm"
                                                    onClick={() => setViewPreset(preset)}
                                                >
                                                    <Eye className="h-4 w-4" />
                                                </Button>
                                            </TooltipTrigger>
                                            <TooltipContent>View details</TooltipContent>
                                        </Tooltip>
                                        
                                        <Tooltip>
                                            <TooltipTrigger asChild>
                                                <Button
                                                    variant="outline"
                                                    size="sm"
                                                    className="text-destructive hover:bg-destructive/10 hover:text-destructive"
                                                    onClick={() => deletePresetMutation.mutate(preset.id)}
                                                    disabled={deletePresetMutation.isPending}
                                                >
                                                    <Trash2 className="h-4 w-4" />
                                                </Button>
                                            </TooltipTrigger>
                                            <TooltipContent>Delete preset</TooltipContent>
                                        </Tooltip>
                                    </div>
                                </CardContent>
                            </Card>
                        ))}
                    </div>
                )}
            </div>

            {/* Enhanced View Dialog */}
            <Dialog open={!!viewPreset} onOpenChange={(open) => !open && setViewPreset(null)}>
                <DialogContent className="sm:max-w-[700px] max-h-[85vh] flex flex-col">
                    <DialogHeader>
                        <div className="flex items-start justify-between gap-4">
                            <div className="flex-1">
                                <DialogTitle className="text-xl">{viewPreset?.name}</DialogTitle>
                                <DialogDescription className="mt-2 space-y-1">
                                    <div className="flex items-center gap-4 text-sm">
                                        <span className="flex items-center gap-1">
                                            <Table2 className="h-4 w-4" />
                                            {viewPreset?.table_names.length} {viewPreset?.table_names.length === 1 ? 'table' : 'tables'}
                                        </span>
                                        <span className="flex items-center gap-1">
                                            <Calendar className="h-4 w-4" />
                                            {viewPreset && formatDate(viewPreset.created_at)}
                                        </span>
                                        {viewPreset?.updated_at !== viewPreset?.created_at && (
                                            <span className="flex items-center gap-1">
                                                <Clock className="h-4 w-4" />
                                                Updated {viewPreset && formatDate(viewPreset.updated_at)}
                                            </span>
                                        )}
                                    </div>
                                </DialogDescription>
                            </div>
                        </div>
                    </DialogHeader>

                    {/* Search */}
                    <div className="relative px-6">
                        <Search className="absolute left-9 top-1/2 transform -translate-y-1/2 h-4 w-4 text-muted-foreground" />
                        <Input
                            placeholder="Search tables..."
                            value={tableSearchQuery}
                            onChange={(e) => setTableSearchQuery(e.target.value)}
                            className="pl-9"
                        />
                    </div>

                    {/* Tables list */}
                    <ScrollArea className="flex-1 px-6 -mx-6">
                        <div className="px-6 pb-4">
                            {filteredTables.length === 0 ? (
                                <div className="text-center text-muted-foreground py-8">
                                    No tables found matching "{tableSearchQuery}"
                                </div>
                            ) : (
                                <div className="grid grid-cols-1 sm:grid-cols-2 gap-2">
                                    {filteredTables.map((name, index) => (
                                        <div 
                                            key={name} 
                                            className="flex items-center gap-2 p-3 rounded-lg border bg-card hover:bg-accent/50 transition-colors"
                                        >
                                            <div className="flex items-center justify-center w-6 h-6 rounded bg-muted text-xs font-medium">
                                                {index + 1}
                                            </div>
                                            <span className="font-mono text-sm truncate">{name}</span>
                                        </div>
                                    ))}
                                </div>
                            )}
                        </div>
                    </ScrollArea>

                    {/* Footer stats */}
                    <div className="flex items-center justify-between px-6 pt-4 border-t">
                        <div className="text-sm text-muted-foreground">
                            {filteredTables.length === viewPreset?.table_names.length 
                                ? `Showing all ${filteredTables.length} tables`
                                : `Showing ${filteredTables.length} of ${viewPreset?.table_names.length} tables`
                            }
                        </div>
                        <div className="flex gap-2">
                            <Button
                                variant="outline"
                                onClick={() => setViewPreset(null)}
                            >
                                Close
                            </Button>
                            <Button
                                variant="default"
                                onClick={() => {
                                    if (viewPreset) {
                                        handleLoadPreset(viewPreset)
                                        setViewPreset(null)
                                    }
                                }}
                            >
                                <Play className="h-4 w-4 mr-2" />
                                Load Preset
                            </Button>
                        </div>
                    </div>
                </DialogContent>
            </Dialog>
        </TooltipProvider>
    )
}
