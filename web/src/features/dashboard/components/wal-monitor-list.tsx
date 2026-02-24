import { useQuery } from '@tanstack/react-query'
import { walMonitorRepo } from '@/repo/wal-monitor'
import { formatBytes, cn } from '@/lib/utils'
import { Badge } from '@/components/ui/badge'
import { ScrollArea } from '@/components/ui/scroll-area'
import { Database, AlertTriangle, CheckCircle2 } from 'lucide-react'
import { DashboardPanel } from './dashboard-panel'
import { useRefreshInterval } from '../context/refresh-interval-context'

export function WALMonitorList() {
    const { refreshInterval } = useRefreshInterval()
    const { data } = useQuery({
        queryKey: ['wal-monitor', 'all'],
        queryFn: walMonitorRepo.getAll,
        refetchInterval: refreshInterval,
    })

    const monitors = data?.monitors || []

    const getWalSizeBadgeVariant = (status: 'OK' | 'WARNING' | 'ERROR' | null) => {
        switch (status) {
            case 'OK':
                return 'default' // Green
            case 'WARNING':
                return 'secondary' // Yellow-ish
            case 'ERROR':
                return 'destructive' // Red
            default:
                return 'outline'
        }
    }

    const getBorderColor = (status: 'OK' | 'WARNING' | 'ERROR' | null) => {
        switch (status) {
            case 'OK':
                return 'border-l-2 border-l-emerald-500'
            case 'WARNING':
                return 'border-l-2 border-l-amber-500'
            case 'ERROR':
                return 'border-l-2 border-l-rose-500'
            default:
                return ''
        }
    }

    const getStatusIcon = (status: string) => {
        if (status === 'ACTIVE') return <CheckCircle2 className="h-3.5 w-3.5 text-emerald-500" />
        return <AlertTriangle className="h-3.5 w-3.5 text-rose-500" />
    }

    return (
        <DashboardPanel
            title="WAL Replication Monitor"
            description="Real-time status of replication slots"
            className="h-full"
            noPadding
        >
            <ScrollArea className='h-full'>
                <div className='flex flex-col'>
                    {monitors.length === 0 && (
                        <div className='flex h-[100px] items-center justify-center text-xs text-muted-foreground'>
                            No active WAL monitors found.
                        </div>
                    )}
                    {monitors.map((monitor) => (
                        <div
                            key={monitor.id}
                            className={cn(
                                'flex items-center justify-between p-3 border-b border-border/40 hover:bg-muted/30 transition-colors',
                                getBorderColor(monitor.wal_threshold_status),
                                (monitor.wal_threshold_status === 'ERROR' || monitor.status === 'ERROR') && "bg-rose-500/5"
                            )}
                        >
                            <div className='flex items-center gap-3'>
                                <div className='flex h-8 w-8 items-center justify-center rounded-sm bg-primary/10'>
                                    <Database className='h-4 w-4 text-primary' />
                                </div>
                                <div>
                                    <div className='flex items-center gap-2'>
                                        <span className='font-medium text-sm'>
                                            {monitor.source?.name || `Source #${monitor.source_id}`}
                                        </span>
                                        {getStatusIcon(monitor.status || 'UNKNOWN')}
                                    </div>
                                    <div className='flex items-center gap-2 text-[10px] text-muted-foreground font-mono mt-0.5'>
                                        <span>{monitor.wal_lsn || 'No LSN'}</span>
                                    </div>
                                </div>
                            </div>

                            <div className='flex items-center gap-4 text-right'>
                                <div className='flex flex-col items-end'>
                                    <span className='text-[10px] font-medium text-muted-foreground'>Lag</span>
                                    <span className="font-mono text-xs">
                                        {formatBytes(monitor.replication_lag_bytes || 0)}
                                    </span>
                                </div>

                                <div className='flex flex-col items-end'>
                                    <span className='text-[10px] font-medium text-muted-foreground'>Size</span>
                                    <Badge
                                        variant={getWalSizeBadgeVariant(monitor.wal_threshold_status)}
                                        className={cn("font-mono text-[10px] h-5 px-1.5",
                                            monitor.wal_threshold_status === 'OK' && "bg-emerald-500/15 text-emerald-500 hover:bg-emerald-500/25 border-emerald-500/20",
                                            monitor.wal_threshold_status === 'WARNING' && "bg-amber-500/15 text-amber-500 hover:bg-amber-500/25 border-amber-500/20",
                                            monitor.wal_threshold_status === 'ERROR' && "bg-rose-500/20 text-rose-200 hover:bg-rose-500/30 border-rose-500/30"
                                        )}
                                    >
                                        {monitor.total_wal_size || '0 B'}
                                    </Badge>
                                </div>
                            </div>
                        </div>
                    ))}
                </div>
            </ScrollArea>
        </DashboardPanel>
    )
}
