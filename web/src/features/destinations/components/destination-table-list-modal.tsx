import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { destinationsRepo } from '@/repo/destinations'
import { type Destination } from '../data/schema'
import {
    Dialog,
    DialogContent,
    DialogHeader,
    DialogTitle,
    DialogDescription,
} from '@/components/ui/dialog'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Input } from '@/components/ui/input'
import { ScrollArea } from '@/components/ui/scroll-area'
import { Skeleton } from '@/components/ui/skeleton'
import { RefreshCw, Table2, Clock } from 'lucide-react'
import { formatDistanceToNow } from 'date-fns'
import { toast } from 'sonner'
import { cn } from '@/lib/utils'

interface DestinationTableListModalProps {
    destination: Destination
    open: boolean
    onOpenChange: (open: boolean) => void
}

export function DestinationTableListModal({
    destination,
    open,
    onOpenChange,
}: DestinationTableListModalProps) {
    const queryClient = useQueryClient()
    const [search, setSearch] = useState('')

    const queryKey = ['destination-table-list', destination.id]

    const { data, isLoading, isFetching } = useQuery({
        queryKey,
        queryFn: () => destinationsRepo.getTableList(destination.id),
        enabled: open,
        staleTime: 60_000, // 1 minute
    })

    const refreshMutation = useMutation({
        mutationFn: () => destinationsRepo.refreshTableList(destination.id),
        onSuccess: (res) => {
            if (res.task_id) {
                toast.success('Table list refresh dispatched. Results will update shortly.')
                // Poll the list query after a short delay
                setTimeout(async () => {
                    await queryClient.invalidateQueries({ queryKey })
                    await queryClient.invalidateQueries({ queryKey: ['destinations'] })
                    // Also invalidate destination-tables so Flow Task input nodes pick up fresh data
                    queryClient.invalidateQueries({ queryKey: ['destination-tables'] })
                }, 5000)
            } else {
                toast.info(res.message)
            }
        },
        onError: () => {
            toast.error('Failed to dispatch table list refresh')
        },
    })

    const tables: string[] = data?.tables ?? []
    const filteredTables = search
        ? tables.filter((t) => t.toLowerCase().includes(search.toLowerCase()))
        : tables

    const lastCheck = data?.last_table_check_at
        ? formatDistanceToNow(new Date(data.last_table_check_at), { addSuffix: true })
        : null

    return (
        <Dialog open={open} onOpenChange={onOpenChange}>
            <DialogContent className="max-w-lg">
                <DialogHeader>
                    <DialogTitle className="flex items-center gap-2">
                        <Table2 className="h-4 w-4" />
                        Table List — {destination.name}
                    </DialogTitle>
                    <DialogDescription>
                        Tables available in this destination&apos;s database. Updated every 30 minutes by the worker.
                    </DialogDescription>
                </DialogHeader>

                <div className="flex items-center justify-between gap-2">
                    <div className="flex items-center gap-2 text-sm text-muted-foreground">
                        <Badge variant="secondary">
                            {isLoading ? '...' : filteredTables.length} / {data?.total_tables ?? 0} tables
                        </Badge>
                        {lastCheck && (
                            <span className="flex items-center gap-1 text-xs">
                                <Clock className="h-3 w-3" />
                                checked {lastCheck}
                            </span>
                        )}
                    </div>
                    <Button
                        size="sm"
                        variant="outline"
                        onClick={() => refreshMutation.mutate()}
                        disabled={refreshMutation.isPending || isFetching}
                    >
                        <RefreshCw className={cn('h-3.5 w-3.5 mr-1.5', (refreshMutation.isPending || isFetching) && 'animate-spin')} />
                        Refresh
                    </Button>
                </div>

                <Input
                    placeholder="Search tables..."
                    value={search}
                    onChange={(e) => setSearch(e.target.value)}
                    className="h-8 text-sm"
                />

                <ScrollArea className="h-72 rounded-md border">
                    {isLoading ? (
                        <div className="p-3 space-y-2">
                            {Array.from({ length: 8 }).map((_, i) => (
                                <Skeleton key={i} className="h-7 w-full" />
                            ))}
                        </div>
                    ) : filteredTables.length === 0 ? (
                        <div className="flex items-center justify-center h-full text-sm text-muted-foreground py-8">
                            {search ? 'No tables match your search.' : 'No tables found. Try refreshing.'}
                        </div>
                    ) : (
                        <div className="p-2 space-y-0.5">
                            {filteredTables.map((table) => (
                                <div
                                    key={table}
                                    className="flex items-center gap-2 px-3 py-1.5 rounded-sm hover:bg-muted/50 text-sm font-mono"
                                >
                                    <Table2 className="h-3.5 w-3.5 text-muted-foreground shrink-0" />
                                    <span className="truncate">{table}</span>
                                </div>
                            ))}
                        </div>
                    )}
                </ScrollArea>
            </DialogContent>
        </Dialog>
    )
}
