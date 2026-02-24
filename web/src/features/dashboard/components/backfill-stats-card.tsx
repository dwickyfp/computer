import { DashboardPanel } from './dashboard-panel'
import { DatabaseBackup, CheckCircle2, XCircle, Clock, Loader2, AlertCircle } from 'lucide-react'
import { type DashboardSummary } from '@/repo/dashboard'
import { Progress } from '@/components/ui/progress'

interface BackfillStatsCardProps {
    data?: DashboardSummary['backfills']
}

export function BackfillStatsCard({ data }: BackfillStatsCardProps) {
    const active = (data?.PENDING || 0) + (data?.EXECUTING || 0)
    const completed = data?.COMPLETED || 0
    const failed = data?.FAILED || 0
    const cancelled = data?.CANCELLED || 0
    const total = data?.total || 0

    // Calculate success rate based on completed vs total attempted (excluding pending/active/cancelled for now, or just total completed/total)
    // Let's us simple ratio of Completed / (Completed + Failed)
    const finishedCount = completed + failed
    const successRate = finishedCount > 0 ? (completed / finishedCount) * 100 : 0

    return (
        <DashboardPanel
            title="Backfill Operations"
            description="Global backfill job status"
            headerAction={<DatabaseBackup className='h-4 w-4 text-muted-foreground' />}
            className="h-full"
        >
            <div className="flex flex-col gap-6">
                {/* Main Stat: Active Jobs */}
                <div className="flex items-end justify-between">
                    <div className="flex flex-col">
                        <div className="text-3xl font-bold font-mono leading-none">
                            {active}
                        </div>
                        <div className="text-xs text-muted-foreground mt-1">Active Jobs</div>
                    </div>
                    <div className="flex flex-col items-end">
                        <div className="text-xl font-bold font-mono leading-none text-muted-foreground">
                            {total}
                        </div>
                        <div className="text-xs text-muted-foreground mt-1">Total Jobs</div>
                    </div>
                </div>

                {/* Detailed Breakdown */}
                <div className="space-y-3">

                    {/* Running / Pending */}
                    <div className="flex items-center justify-between text-sm">
                        <div className="flex items-center gap-2">
                            <Loader2 className="h-4 w-4 text-blue-500 animate-spin" />
                            <span className="text-muted-foreground">Running</span>
                        </div>
                        <span className="font-medium font-mono">
                            {data?.EXECUTING || 0}
                        </span>
                    </div>

                    <div className="flex items-center justify-between text-sm">
                        <div className="flex items-center gap-2">
                            <Clock className="h-4 w-4 text-amber-500" />
                            <span className="text-muted-foreground">Pending</span>
                        </div>
                        <span className="font-medium font-mono">
                            {data?.PENDING || 0}
                        </span>
                    </div>

                    <div className="h-px bg-border/50 my-2" />

                    {/* Completed */}
                    <div className="flex items-center justify-between text-sm">
                        <div className="flex items-center gap-2">
                            <CheckCircle2 className="h-4 w-4 text-emerald-500" />
                            <span className="text-muted-foreground">Completed</span>
                        </div>
                        <span className="font-medium font-mono">
                            {completed}
                        </span>
                    </div>

                    {/* Failed */}
                    <div className="flex items-center justify-between text-sm">
                        <div className="flex items-center gap-2">
                            <XCircle className="h-4 w-4 text-rose-500" />
                            <span className="text-muted-foreground">Failed</span>
                        </div>
                        <span className="font-medium font-mono text-rose-500">
                            {failed}
                        </span>
                    </div>

                    {/* Cancelled */}
                    {cancelled > 0 && (
                        <div className="flex items-center justify-between text-sm">
                            <div className="flex items-center gap-2">
                                <AlertCircle className="h-4 w-4 text-muted-foreground" />
                                <span className="text-muted-foreground">Cancelled</span>
                            </div>
                            <span className="font-medium font-mono text-muted-foreground">
                                {cancelled}
                            </span>
                        </div>
                    )}

                </div>

                {/* Success Rate Progress */}
                <div className="space-y-1.5">
                    <div className="flex items-center justify-between text-xs">
                        <span className="text-muted-foreground">Success Rate</span>
                        <span className={successRate >= 90 ? "text-emerald-500" : successRate >= 70 ? "text-amber-500" : "text-rose-500"}>
                            {successRate.toFixed(1)}%
                        </span>
                    </div>
                    <Progress value={successRate} className="h-1.5" />
                </div>

            </div>
        </DashboardPanel>
    )
}
