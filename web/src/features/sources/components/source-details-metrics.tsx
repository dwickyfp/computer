import { Card } from '@/components/ui/card'
import { type WALMonitorResponse, type Source } from '@/repo/sources'
import {
    Activity,
    Database,
    Clock,
    AlertCircle,
    CheckCircle2,
    Server,
    Layers,
} from 'lucide-react'
import { formatDistanceToNow } from 'date-fns'
import { cn } from '@/lib/utils'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'

interface SourceDetailsMetricsProps {
    data: WALMonitorResponse | null
    source: Source
    onPublicationAction?: () => void
    onReplicationAction?: () => void
    isPublicationLoading?: boolean
    isReplicationLoading?: boolean
}

function formatBytes(bytes: number, decimals = 2) {
    if (!+bytes) return '0 Bytes'
    const k = 1024
    const dm = decimals < 0 ? 0 : decimals
    const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB']
    const i = Math.floor(Math.log(bytes) / Math.log(k))
    return `${parseFloat((bytes / Math.pow(k, i)).toFixed(dm))} ${sizes[i]}`
}

export function SourceDetailsMetrics({
    data,
    source,
    onPublicationAction,
    onReplicationAction,
    isPublicationLoading,
    isReplicationLoading
}: SourceDetailsMetricsProps) {
    const isHealthy = data?.status?.toLowerCase() === 'active' || data?.status?.toLowerCase() === 'streaming'
    const lagBytes = data?.replication_lag_bytes || 0

    // Grafana-style Panel Component
    const Panel = ({
        title,
        children,
        className,
        headerAction
    }: {
        title: string,
        children: React.ReactNode,
        className?: string,
        headerAction?: React.ReactNode
    }) => (
        <Card className={cn("flex flex-col h-full bg-card/50 border-border/60 shadow-none", className)}>
            <div className="px-3 py-1.5 border-b border-border/40 flex items-center justify-between min-h-[32px]">
                <h3 className="text-[10px] uppercase tracking-wider text-muted-foreground font-bold flex items-center gap-1.5">
                    {title}
                </h3>
                {headerAction}
            </div>
            <div className="p-2.5 flex-1 flex flex-col justify-center">
                {children}
            </div>
        </Card>
    )

    // Stat Component for consistent metric display
    const Stat = ({
        value,
        label,
        icon: Icon,
        trend,
        status = 'default'
    }: {
        value: React.ReactNode,
        label?: string,
        icon?: any,
        trend?: string,
        status?: 'default' | 'success' | 'warning' | 'error'
    }) => {
        const colorClass = {
            default: 'text-foreground',
            success: 'text-emerald-500',
            warning: 'text-amber-500',
            error: 'text-rose-500'
        }[status]

        return (
            <div className="flex flex-col">
                <div className="flex items-center gap-1.5 mb-0.5">
                    {Icon && <Icon className={cn("h-3.5 w-3.5", colorClass)} />}
                    <span className={cn("text-lg md:text-xl font-bold tracking-tight leading-none", colorClass)}>{value}</span>
                </div>
                {label && <span className="text-[11px] text-muted-foreground leading-tight">{label}</span>}
                {trend && <span className="text-[11px] text-muted-foreground mt-0.5 leading-tight">{trend}</span>}
            </div>
        )
    }

    return (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 xl:grid-cols-5 gap-3">
            {/* Health Status */}
            <Panel title="Health Status" className="col-span-1">
                <div className="flex flex-col">
                    <Stat
                        value={data?.status?.toUpperCase() || 'UNKNOWN'}
                        icon={isHealthy ? CheckCircle2 : AlertCircle}
                        status={isHealthy ? 'success' : 'error'}
                        label={data?.error_message || 'System operational'}
                    />
                </div>
            </Panel>

            {/* Replication Lag */}
            <Panel title="Replication Lag" className="col-span-1">
                <Stat
                    value={formatBytes(lagBytes)}
                    icon={Activity}
                    status={lagBytes > 10 * 1024 * 1024 ? 'error' : lagBytes > 1024 * 1024 ? 'warning' : 'success'}
                    label="Current Lag"
                />
            </Panel>

            {/* Total WAL Size */}
            <Panel title="Total WAL Size" className="col-span-1">
                <Stat
                    value={data?.total_wal_size || 'N/A'}
                    icon={Database}
                    label="Processed Size"
                />
            </Panel>

            {/* Last Received LSN */}
            <Panel title="Last Received LSN" className="col-span-1 lg:col-span-1 xl:col-span-2">
                <div className="flex flex-col gap-0.5">
                    <div className="flex items-center gap-1.5">
                        <Clock className="h-3.5 w-3.5 text-muted-foreground" />
                        <span className="text-lg font-mono font-medium truncate leading-none">
                            {data?.wal_lsn || 'N/A'}
                        </span>
                    </div>
                    <div className="text-[10px] text-muted-foreground flex items-center gap-1 leading-tight">
                        Last received: <span className="text-foreground font-medium">
                            {data?.last_wal_received
                                ? formatDistanceToNow(new Date(data.last_wal_received), { addSuffix: true })
                                : 'Never'}
                        </span>
                    </div>
                </div>
            </Panel>

            {/* Publication Manager */}
            <Panel
                title="Publication"
                className="col-span-1 md:col-span-2 lg:col-span-2"
                headerAction={
                    <Badge variant={source.is_publication_enabled ? "default" : "secondary"} className={cn("h-4 px-1.5 text-[10px]", source.is_publication_enabled && "bg-emerald-600 hover:bg-emerald-700 text-white")}>
                        {source.is_publication_enabled ? "Active" : "Inactive"}
                    </Badge>
                }
            >
                <div className="flex items-center justify-between">
                    <div className="flex items-center gap-2">
                        <Layers className="h-5 w-5 text-muted-foreground/50" />
                        <div className="flex flex-col gap-0.5">
                            <span className="text-[10px] font-semibold uppercase text-muted-foreground leading-none">Name</span>
                            <span className="font-mono text-sm text-foreground leading-none">{source.publication_name}</span>
                        </div>
                    </div>
                    <Button
                        variant="outline"
                        size="sm"
                        onClick={onPublicationAction}
                        disabled={isPublicationLoading}
                        className="h-6 text-[10px] px-2"
                    >
                        {source.is_publication_enabled ? "Drop" : "Create"}
                    </Button>
                </div>
            </Panel>

            {/* Replication Slot Manager */}
            <Panel
                title="Replication Slot"
                className="col-span-1 md:col-span-2 lg:col-span-3"
                headerAction={
                    <Badge variant={source.is_replication_enabled ? "default" : "secondary"} className={cn("h-4 px-1.5 text-[10px]", source.is_replication_enabled && "bg-emerald-600 hover:bg-emerald-700 text-white")}>
                        {source.is_replication_enabled ? "Active" : "Inactive"}
                    </Badge>
                }
            >
                <div className="flex items-center justify-between">
                    <div className="flex items-center gap-4">
                        <div className="flex items-center gap-2">
                            <Server className="h-5 w-5 text-muted-foreground/50" />
                            <div className="flex flex-col gap-0.5">
                                <span className="text-[10px] font-semibold uppercase text-muted-foreground leading-none">Slot ID</span>
                                <span className="font-mono text-sm text-foreground leading-none">{source.replication_name}</span>
                            </div>
                        </div>
                        {data?.replication_slot_name && (
                            <div className="flex flex-col gap-0.5 border-l pl-4 border-border/50">
                                <span className="text-[10px] font-semibold uppercase text-muted-foreground leading-none">Active Slot</span>
                                <span className="font-mono text-sm text-foreground leading-none">{data.replication_slot_name}</span>
                            </div>
                        )}
                    </div>
                    <Button
                        variant="outline"
                        size="sm"
                        onClick={onReplicationAction}
                        disabled={isReplicationLoading}
                        className="h-6 text-[10px] px-2"
                    >
                        {source.is_replication_enabled ? "Drop" : "Create"}
                    </Button>
                </div>
            </Panel>
        </div>
    )
}
