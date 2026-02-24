import { Link, useNavigate } from '@tanstack/react-router'
import {
    type ScheduleListItem,
    type ScheduleRunStatus,
} from '@/repo/schedules'
import { formatDistanceToNow, format } from 'date-fns'
import {
    MoreVertical,
    Play,
    Trash2,
    Settings,
    Clock,
    Activity
} from 'lucide-react'
import { Bar, BarChart, Cell, Tooltip } from 'recharts'
import { cn } from '@/lib/utils'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card } from '@/components/ui/card'
import { Switch } from '@/components/ui/switch'
import {
    DropdownMenu,
    DropdownMenuContent,
    DropdownMenuItem,
    DropdownMenuLabel,
    DropdownMenuSeparator,
    DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu'
import { useSchedules } from './schedules-provider'
import cronstrue from 'cronstrue'

import CronExpressionParser from 'cron-parser'

interface Props {
    schedule: ScheduleListItem
}

export function ScheduleCard({ schedule }: Props) {
    const { setOpen, setCurrentRow } = useSchedules()
    const navigate = useNavigate()

    const runs = [...(schedule.run_history || [])].reverse().slice(0, 20) // Limit to last 20 for compact view

    const getStatusColor = (status: ScheduleRunStatus) => {
        switch (status) {
            case 'SUCCESS':
                return '#10b981' // emerald-500
            case 'FAILED':
                return '#ef4444' // red-500
            case 'RUNNING':
                return '#3b82f6' // blue-500
            default:
                return '#e5e7eb' // gray-200
        }
    }

    const handleToggle = (checked: boolean) => {
        setCurrentRow(schedule)
        if (checked) {
            setOpen('resume')
        } else {
            setOpen('pause')
        }
    }

    const handleDelete = (e: React.MouseEvent) => {
        e.stopPropagation()
        setCurrentRow(schedule)
        setOpen('delete')
    }

    const handleEdit = (e: React.MouseEvent) => {
        e.stopPropagation()
        navigate({
            to: '/schedules/$scheduleId',
            params: { scheduleId: String(schedule.id) },
        })
    }

    const cronHuman = (() => {
        try {
            return cronstrue.toString(schedule.cron_expression, { verbose: false })
        } catch {
            return schedule.cron_expression
        }
    })()

    return (
        <Card className='group relative overflow-hidden transition-all hover:shadow-md border-border/60 bg-sidebar'>
            {/* Status Strip */}
            <div className={cn(
                "absolute left-0 top-0 bottom-0 w-1 transition-colors",
                schedule.status === 'ACTIVE' ? "bg-primary" : "bg-muted"
            )} />

            <div className='flex flex-col sm:flex-row items-start sm:items-center gap-4 p-4 pl-6'>

                {/* Main Info */}
                <div className='flex-1 min-w-0 space-y-1'>
                    <div className='flex items-center gap-2'>
                        <Link
                            to='/schedules/$scheduleId'
                            params={{ scheduleId: String(schedule.id) }}
                            className='font-semibold text-foreground hover:underline truncate text-base'
                        >
                            {schedule.name}
                        </Link>
                        <Badge variant='secondary' className='font-mono text-[10px] px-1.5 py-0 h-5'>
                            {schedule.task_type === 'FLOW_TASK' ? 'Flow' : 'Linked'}
                        </Badge>
                    </div>

                    <div className='flex items-center gap-3 text-xs text-muted-foreground'>
                        <div className='flex items-center gap-1' title={`Cron: ${schedule.cron_expression}`}>
                            <Clock className='h-3 w-3' />
                            <span>{cronHuman}</span>
                        </div>
                        {(() => {
                            // Last Run
                            const sortedRuns = [...(schedule.run_history || [])].sort((a, b) =>
                                new Date(b.triggered_at).getTime() - new Date(a.triggered_at).getTime()
                            )
                            const lastRun = sortedRuns[0]

                            // Calculate Next Run client-side
                            let nextRunDate: Date | null = null
                            try {
                                const options = {
                                    currentDate: new Date(),
                                    iterator: true
                                }
                                const interval = CronExpressionParser.parse(schedule.cron_expression, options)
                                nextRunDate = interval.next().toDate()
                            } catch (err) {
                                console.error('Failed to parse cron:', err)
                                // Fallback to server value if available and in future
                                if (schedule.next_run_at && new Date(schedule.next_run_at) > new Date()) {
                                    nextRunDate = new Date(schedule.next_run_at)
                                }
                            }

                            return (
                                <>
                                    {lastRun && (
                                        <>
                                            <span className='hidden sm:inline-block'>•</span>
                                            <span title="Last Run">
                                                Last: {format(new Date(lastRun.triggered_at), 'dd-MM-yyyy HH:mm:ss')}
                                            </span>
                                        </>
                                    )}

                                    {nextRunDate && (
                                        <>
                                            <span className='hidden sm:inline-block'>•</span>
                                            <span title="Next Run">
                                                Next: {format(nextRunDate, 'dd-MM-yyyy HH:mm:ss')}
                                            </span>
                                        </>
                                    )}
                                </>
                            )
                        })()}
                    </div>
                </div>

                {/* Right Side: Charts & Actions */}
                <div className='flex items-center gap-4 w-full sm:w-auto justify-between sm:justify-end'>

                    {/* Run History Sparkline */}
                    <div className='h-8 w-24 sm:w-32 hidden sm:block'>
                        {runs.length > 0 ? (
                            <BarChart data={runs} width={128} height={32}>
                                <Tooltip
                                    cursor={{ fill: 'transparent' }}
                                    content={({ active, payload }) => {
                                        if (active && payload && payload.length) {
                                            const data = payload[0].payload;
                                            return (
                                                <div className="rounded-md border bg-popover px-2 py-1 text-xs shadow-sm">
                                                    <div className={cn(
                                                        "font-bold",
                                                        data.status === 'SUCCESS' ? 'text-emerald-500' :
                                                            data.status === 'FAILED' ? 'text-red-500' : 'text-blue-500'
                                                    )}>
                                                        {data.status}
                                                    </div>
                                                    <div className="text-muted-foreground">
                                                        {formatDistanceToNow(new Date(data.triggered_at), { addSuffix: true })}
                                                    </div>
                                                    <div>{data.duration_ms ? `${data.duration_ms}ms` : ''}</div>
                                                </div>
                                            );
                                        }
                                        return null;
                                    }}
                                />
                                <Bar dataKey="duration_ms" radius={[1, 1, 0, 0]}>
                                    {runs.map((entry, index) => (
                                        <Cell key={`cell-${index}`} fill={getStatusColor(entry.status)} className="opacity-80 hover:opacity-100 transition-opacity" />
                                    ))}
                                </Bar>
                            </BarChart>
                        ) : (
                            <div className='flex h-full items-center justify-center text-[10px] text-muted-foreground bg-muted/30 rounded-sm'>
                                No runs
                            </div>
                        )}
                    </div>

                    <div className='flex items-center gap-2'>
                        <div className="flex items-center gap-2 mr-2">
                            <span className={cn("text-xs font-medium transition-colors", schedule.status === 'ACTIVE' ? "text-primary" : "text-muted-foreground")}>
                                {schedule.status === 'ACTIVE' ? 'Active' : 'Paused'}
                            </span>
                            <Switch
                                checked={schedule.status === 'ACTIVE'}
                                onCheckedChange={handleToggle}
                                className="scale-90"
                            />
                        </div>

                        <DropdownMenu>
                            <DropdownMenuTrigger asChild>
                                <Button variant='ghost' size='icon' className='h-8 w-8 text-muted-foreground hover:text-foreground'>
                                    <MoreVertical className='h-4 w-4' />
                                    <span className="sr-only">Actions</span>
                                </Button>
                            </DropdownMenuTrigger>
                            <DropdownMenuContent align="end">
                                <DropdownMenuLabel>Actions</DropdownMenuLabel>
                                <DropdownMenuItem disabled title="Not implemented yet">
                                    <Play className="mr-2 h-4 w-4" /> Run Now
                                </DropdownMenuItem>
                                <DropdownMenuItem onClick={handleEdit}>
                                    <Settings className="mr-2 h-4 w-4" /> Edit Configuration
                                </DropdownMenuItem>
                                <DropdownMenuItem onClick={handleEdit}>
                                    <Activity className="mr-2 h-4 w-4" /> View Runs
                                </DropdownMenuItem>
                                <DropdownMenuSeparator />
                                <DropdownMenuItem onClick={handleDelete} className="text-destructive focus:text-destructive">
                                    <Trash2 className="mr-2 h-4 w-4" /> Delete Schedule
                                </DropdownMenuItem>
                            </DropdownMenuContent>
                        </DropdownMenu>
                    </div>
                </div>
            </div>
        </Card>
    )
}
