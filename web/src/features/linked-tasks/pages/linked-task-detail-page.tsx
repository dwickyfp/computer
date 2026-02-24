import { useEffect, useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useParams, Link } from '@tanstack/react-router'
import { Header } from '@/components/layout/header'
import { Main } from '@/components/layout/main'
import { ThemeSwitch } from '@/components/theme-switch'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import {
    Loader2,
    Link2,
    CheckCircle2,
    XCircle,
    Clock,
    AlertCircle,
    SkipForward,
    Plus,
    ArrowDown,
    Save,
    Play,
    ChevronLeft,
    ChevronsUpDown,
    Check,
    GitBranch,
    X,
    Square,
} from 'lucide-react'
import { toast } from 'sonner'
import { formatDistanceToNow, format } from 'date-fns'
import {
    linkedTasksRepo,
    type LinkedTaskDetail,
    type LinkedTaskRunHistory,
    type EdgeCondition,
} from '@/repo/linked-tasks'
import { flowTasksRepo, type FlowTask } from '@/repo/flow-tasks'
import { cn } from '@/lib/utils'
import {
    Command,
    CommandEmpty,
    CommandGroup,
    CommandInput,
    CommandItem,
    CommandList,
} from '@/components/ui/command'
import {
    Popover,
    PopoverContent,
    PopoverTrigger,
} from '@/components/ui/popover'
import {
    Select,
    SelectContent,
    SelectItem,
    SelectTrigger,
    SelectValue,
} from '@/components/ui/select'
import { Separator } from '@/components/ui/separator'

// ─── Types ──────────────────────────────────────────────────────────────────

interface StepItem {
    id: string
    dbId?: number
    flowTaskId: number | null
}

/**
 * A Stage is a group of steps that run IN PARALLEL.
 * Stages are executed sequentially, gated by `gateCondition`.
 */
interface Stage {
    id: string
    steps: StepItem[]
    gateCondition: EdgeCondition // Condition to enter THIS stage (from any of the previous stage)
}

// ─── Status utils ─────────────────────────────────────────────────────────────

function StepStatusIcon({ status }: { status: string }) {
    switch (status) {
        case 'SUCCESS': return <CheckCircle2 className="h-4 w-4 text-emerald-500" />
        case 'FAILED': return <XCircle className="h-4 w-4 text-rose-500" />
        case 'RUNNING': return <Loader2 className="h-4 w-4 animate-spin text-blue-500" />
        case 'SKIPPED': return <SkipForward className="h-4 w-4 text-muted-foreground" />
        default: return <Clock className="h-4 w-4 text-muted-foreground" />
    }
}

function RunStatusBadge({ status }: { status: string }) {
    const cls: Record<string, string> = {
        RUNNING: 'bg-blue-500/15 text-blue-600 border-blue-500/30 dark:text-blue-400',
        SUCCESS: 'bg-emerald-500/15 text-emerald-600 border-emerald-500/30 dark:text-emerald-400',
        FAILED: 'bg-rose-500/15 text-rose-600 border-rose-500/30 dark:text-rose-400',
        CANCELLED: 'bg-muted text-muted-foreground border-border',
    }
    return (
        <span className={cn('inline-flex items-center gap-1.5 px-2 py-0.5 rounded-full text-[11px] font-semibold border', cls[status] ?? cls.CANCELLED)}>
            {status === 'RUNNING' && <Loader2 className="h-3 w-3 animate-spin" />}
            {status}
        </span>
    )
}

// ─── Run History Panel ────────────────────────────────────────────────────────

function RunHistoryPanel({
    detail,
    height,
    isResizing,
    onResizeStart,
    collapsed,
    onToggleCollapse
}: {
    detail: LinkedTaskDetail
    height: number
    isResizing: boolean
    onResizeStart: (e: React.MouseEvent) => void
    collapsed: boolean
    onToggleCollapse: () => void
}) {
    const { data, isLoading } = useQuery({
        queryKey: ['linked-task-runs', detail.id],
        queryFn: () => linkedTasksRepo.getRuns(detail.id, 1, 20),
        refetchInterval: 8_000,
    })

    const runs = data?.data.items ?? []

    return (
        <div
            className="border-t border-border/60 bg-card/20 flex flex-col shrink-0 relative flex-col transition-all duration-300 ease-in-out overflow-hidden"
            style={{ height: collapsed ? 40 : height }}
        >
            {/* Resize Handle - only show when expanded */}
            {!collapsed && (
                <div
                    onMouseDown={onResizeStart}
                    className={cn(
                        "absolute top-0 left-0 right-0 h-1 cursor-ns-resize z-20 hover:bg-primary/50 transition-colors",
                        isResizing && "bg-primary"
                    )}
                />
            )}

            <div
                className={cn(
                    "px-6 border-b border-border/60 flex items-center justify-between shrink-0 bg-background/50 backdrop-blur-sm cursor-pointer hover:bg-muted/50 transition-colors",
                    collapsed ? "py-2 h-10 border-b-0" : "py-3"
                )}
                onClick={collapsed ? onToggleCollapse : undefined}
            >
                <div className="flex items-center gap-2">
                    <Clock className="h-4 w-4 text-muted-foreground" />
                    <p className="text-sm font-semibold text-muted-foreground">Run History</p>
                </div>

                <div className="flex items-center gap-4">
                    {!collapsed && <span className="text-xs text-muted-foreground">{data?.data.total ?? 0} run{(data?.data.total ?? 0) !== 1 ? 's' : ''}</span>}
                    <button
                        onClick={(e) => { e.stopPropagation(); onToggleCollapse() }}
                        className="text-muted-foreground hover:text-foreground p-1 hover:bg-muted rounded transition-colors"
                    >
                        <ChevronLeft className={cn("h-4 w-4 transition-transform duration-300", collapsed ? "rotate-90" : "-rotate-90")} />
                    </button>
                </div>
            </div>

            <div className={cn("flex-1 flex flex-col min-h-0", collapsed && "opacity-0 invisible transition-opacity duration-200")}>
                {isLoading && (
                    <div className="flex items-center justify-center h-full text-muted-foreground">
                        <Loader2 className="h-5 w-5 animate-spin" />
                    </div>
                )}
                {!isLoading && runs.length === 0 && (
                    <div className="flex flex-col items-center justify-center h-full gap-2 text-muted-foreground">
                        <AlertCircle className="h-5 w-5 opacity-40" />
                        <span className="text-xs">No runs yet. Press Run to start.</span>
                    </div>
                )}
                {!isLoading && runs.length > 0 && (
                    <div className="overflow-y-auto flex-1 p-0">
                        {runs.map((run) => (
                            <RunRow key={run.id} run={run} detail={detail} />
                        ))}
                    </div>
                )}
            </div>
        </div>
    )
}

function RunRow({ run, detail }: { run: LinkedTaskRunHistory; detail: LinkedTaskDetail }) {
    const [expanded, setExpanded] = useState(false)
    const queryClient = useQueryClient()
    const failedLog = run.step_logs.find(l => l.status === 'FAILED')
    const duration = run.finished_at && run.started_at
        ? Math.round((new Date(run.finished_at).getTime() - new Date(run.started_at).getTime()) / 1000)
        : null

    const cancelMutation = useMutation({
        mutationFn: () => linkedTasksRepo.cancelRun(detail.id, run.id),
        onSuccess: () => {
            queryClient.invalidateQueries({ queryKey: ['linked-task-runs', detail.id] })
            queryClient.invalidateQueries({ queryKey: ['linked-task', detail.id] })
        },
        onError: () => toast.error('Failed to cancel run'),
    })

    return (
        <div className="border-b border-border/40 last:border-0 group">
            <button
                onClick={() => setExpanded(!expanded)}
                className={cn(
                    "w-full flex items-center gap-4 px-6 py-3 hover:bg-muted/30 transition-colors text-left",
                    expanded && "bg-muted/30"
                )}
            >
                <RunStatusBadge status={run.status} />
                <div className="flex flex-col gap-0.5 flex-1 min-w-0">
                    <div className="flex items-center gap-2">
                        <span className="text-xs font-medium text-foreground">
                            {format(new Date(run.started_at), "MMM d, HH:mm:ss")}
                        </span>
                        <span className="text-[10px] text-muted-foreground">
                            · {formatDistanceToNow(new Date(run.started_at), { addSuffix: true })}
                        </span>
                    </div>
                    <div className="flex items-center gap-2 text-[10px] text-muted-foreground">
                        {duration !== null && <span>{duration}s duration</span>}
                        <span>· {run.trigger_type}</span>
                    </div>
                </div>
                {failedLog && (
                    <div className="hidden sm:flex items-center gap-1.5 text-xs text-rose-500 bg-rose-500/10 px-2 py-0.5 rounded border border-rose-500/20 max-w-[200px] truncate">
                        <AlertCircle className="h-3 w-3 shrink-0" />
                        <span className="truncate">Error in step #{failedLog.step_id}</span>
                    </div>
                )}
                {run.status === 'RUNNING' && (
                    <button
                        onClick={(e) => { e.stopPropagation(); cancelMutation.mutate() }}
                        disabled={cancelMutation.isPending}
                        title="Cancel run"
                        className="flex items-center gap-1 px-2 py-0.5 rounded text-[11px] font-medium text-orange-500 bg-orange-500/10 border border-orange-500/20 hover:bg-orange-500/20 transition-colors disabled:opacity-50"
                    >
                        {cancelMutation.isPending
                            ? <Loader2 className="h-3 w-3 animate-spin" />
                            : <Square className="h-3 w-3 fill-orange-500" />}
                        <span>Cancel</span>
                    </button>
                )}
                <ChevronLeft className={cn("h-4 w-4 text-muted-foreground transition-transform duration-200", expanded ? "-rotate-90" : "rotate-0")} />
            </button>

            {expanded && (
                <div className="bg-muted/20 border-t border-border/40 px-6 py-3 space-y-2">
                    <div className="flex items-center gap-2 mb-2">
                        <p className="text-[10px] font-semibold text-muted-foreground uppercase tracking-wider">Execution Steps</p>
                        <Separator className="flex-1" />
                    </div>
                    {run.step_logs.length === 0 && <p className="text-xs text-muted-foreground italic">No steps executed yet.</p>}
                    {run.step_logs.map((log) => {
                        const step = detail.steps.find((s) => s.id === log.step_id)
                        return (
                            <div key={log.id} className="flex items-start gap-3 py-1.5 px-3 rounded-md hover:bg-background/50 border border-transparent hover:border-border/40 transition-colors">
                                <div className="mt-0.5"><StepStatusIcon status={log.status} /></div>
                                <div className="flex-1 min-w-0 grid gap-0.5">
                                    <div className="flex items-center justify-between gap-4">
                                        <span className={cn("text-xs font-medium", log.status === 'FAILED' ? 'text-rose-500' : 'text-foreground')}>
                                            {step?.flow_task?.name ?? `Step #${log.step_id}`}
                                        </span>
                                        <span className="text-[10px] text-muted-foreground tabular-nums">
                                            {log.finished_at && log.started_at
                                                ? `${((new Date(log.finished_at).getTime() - new Date(log.started_at).getTime()) / 1000).toFixed(1)}s`
                                                : ''}
                                        </span>
                                    </div>
                                    {log.error_message && (
                                        <div className="mt-1 p-2 bg-rose-500/10 border border-rose-500/20 rounded text-[10px] font-mono text-rose-600 dark:text-rose-400 break-words whitespace-pre-wrap">
                                            {log.error_message}
                                        </div>
                                    )}
                                </div>
                            </div>
                        )
                    })}
                </div>
            )}
        </div>
    )
}

// ─── Flow Task Combobox ───────────────────────────────────────────────────────

function FlowTaskCombobox({
    value,
    flowTasks,
    onChange,
    open,
    onOpenChange,
}: {
    value: number | null
    flowTasks: FlowTask[]
    onChange: (id: number) => void
    open: boolean
    onOpenChange: (o: boolean) => void
}) {
    const selected = flowTasks.find(ft => ft.id === value)
    return (
        <Popover open={open} onOpenChange={onOpenChange}>
            <PopoverTrigger asChild>
                <Button
                    variant="outline"
                    role="combobox"
                    className={cn(
                        "w-full justify-between h-9 px-3 font-normal text-xs min-w-[160px]",
                        !value && "text-muted-foreground border-dashed"
                    )}
                >
                    <div className="flex items-center gap-1.5 truncate">
                        <Link2 className="h-3.5 w-3.5 opacity-50 shrink-0" />
                        <span className="truncate">{selected ? selected.name : "Select task..."}</span>
                    </div>
                    <ChevronsUpDown className="ml-1 h-3.5 w-3.5 shrink-0 opacity-50" />
                </Button>
            </PopoverTrigger>
            <PopoverContent className="w-[320px] p-0" align="start">
                <Command>
                    <CommandInput placeholder="Search flow tasks..." />
                    <CommandList>
                        <CommandEmpty>No flow task found.</CommandEmpty>
                        <CommandGroup>
                            {flowTasks.map((ft) => (
                                <CommandItem
                                    key={ft.id}
                                    value={ft.name}
                                    onSelect={() => { onChange(ft.id); onOpenChange(false) }}
                                >
                                    <Check className={cn("mr-2 h-4 w-4", value === ft.id ? "opacity-100" : "opacity-0")} />
                                    <div className="flex flex-col gap-0.5">
                                        <span>{ft.name}</span>
                                        {ft.description && <span className="text-xs text-muted-foreground line-clamp-1">{ft.description}</span>}
                                    </div>
                                </CommandItem>
                            ))}
                        </CommandGroup>
                    </CommandList>
                </Command>
            </PopoverContent>
        </Popover>
    )
}

// ─── Stage Builder ────────────────────────────────────────────────────────────

function StageBuilder({
    stages,
    setStages,
    flowTasks,
}: {
    stages: Stage[]
    setStages: (s: Stage[]) => void
    flowTasks: FlowTask[]
}) {
    const [openCombobox, setOpenCombobox] = useState<string | null>(null) // `${stageId}:${stepId}`

    const addStage = () => {
        setStages([...stages, {
            id: crypto.randomUUID(),
            steps: [{ id: crypto.randomUUID(), flowTaskId: null }],
            gateCondition: 'ON_SUCCESS',
        }])
    }

    const removeStage = (stageId: string) => {
        setStages(stages.filter(s => s.id !== stageId))
    }

    const addStepToStage = (stageId: string) => {
        setStages(stages.map(s =>
            s.id === stageId
                ? { ...s, steps: [...s.steps, { id: crypto.randomUUID(), flowTaskId: null }] }
                : s
        ))
    }

    const removeStepFromStage = (stageId: string, stepId: string) => {
        setStages(stages.map(s => {
            if (s.id !== stageId) return s
            const newSteps = s.steps.filter(st => st.id !== stepId)
            // If stage is now empty, remove it entirely
            return newSteps.length === 0 ? null : { ...s, steps: newSteps }
        }).filter(Boolean) as Stage[])
    }

    const updateStepFlowTask = (stageId: string, stepId: string, flowTaskId: number) => {
        setStages(stages.map(s =>
            s.id === stageId
                ? { ...s, steps: s.steps.map(st => st.id === stepId ? { ...st, flowTaskId } : st) }
                : s
        ))
    }

    const updateStageCondition = (stageId: string, condition: EdgeCondition) => {
        setStages(stages.map(s => s.id === stageId ? { ...s, gateCondition: condition } : s))
    }

    if (stages.length === 0) {
        return (
            <div className="flex flex-col items-center justify-center h-full gap-4">
                <div className="text-center py-12 border-2 border-dashed rounded-xl w-full max-w-xl bg-muted/10">
                    <GitBranch className="h-8 w-8 text-muted-foreground/40 mx-auto mb-3" />
                    <p className="text-muted-foreground font-medium">No stages defined</p>
                    <p className="text-xs text-muted-foreground/60 mt-1 mb-4">Add stages to build a flow. Steps within a stage run in parallel.</p>
                    <Button onClick={addStage} size="sm">
                        <Plus className="h-4 w-4 mr-2" />
                        Add First Stage
                    </Button>
                </div>
            </div>
        )
    }

    return (
        <div className="flex flex-col items-center py-8 px-4 w-full max-w-4xl mx-auto gap-0 pb-24">
            {stages.map((stage, stageIndex) => {
                const isFirst = stageIndex === 0
                return (
                    <div key={stage.id} className="w-full flex flex-col items-center">
                        {/* Gate condition connector (between stages) */}
                        {!isFirst && (
                            <div className="flex flex-col items-center gap-1 py-2 z-10">
                                <div className="w-px h-4 bg-border/60" />
                                <div className="flex items-center gap-2 bg-background border border-border rounded-full px-3 py-1 shadow-sm">
                                    <ArrowDown className="h-3 w-3 text-muted-foreground" />
                                    <Select
                                        value={stage.gateCondition}
                                        onValueChange={(v) => updateStageCondition(stage.id, v as EdgeCondition)}
                                    >
                                        <SelectTrigger className="h-6 text-[11px] border-0 shadow-none p-0 pr-4 w-auto font-medium focus:ring-0">
                                            <SelectValue />
                                        </SelectTrigger>
                                        <SelectContent>
                                            <SelectItem value="ON_SUCCESS">
                                                <div className="flex items-center gap-1.5">
                                                    <div className="h-1.5 w-1.5 rounded-full bg-emerald-500" />
                                                    <span>On Success</span>
                                                </div>
                                            </SelectItem>
                                            <SelectItem value="ALWAYS">
                                                <div className="flex items-center gap-1.5">
                                                    <div className="h-1.5 w-1.5 rounded-full bg-blue-500" />
                                                    <span>Always Run</span>
                                                </div>
                                            </SelectItem>
                                        </SelectContent>
                                    </Select>
                                </div>
                                <div className="w-px h-4 bg-border/60" />
                            </div>
                        )}

                        {/* Stage Card */}
                        <div className={cn(
                            "w-full rounded-xl border bg-sidebar shadow-sm transition-all group/stage",
                            "border-l-4",
                            isFirst ? "border-l-primary" : stage.gateCondition === 'ON_SUCCESS' ? "border-l-emerald-500" : "border-l-blue-500"
                        )}>
                            {/* Stage header */}
                            <div className="flex items-center justify-between px-4 py-2.5 border-b border-border/50">
                                <div className="flex items-center gap-2">
                                    <div className="h-5 w-5 rounded-full bg-primary/10 flex items-center justify-center">
                                        <span className="text-[10px] font-bold text-primary">{stageIndex + 1}</span>
                                    </div>
                                    <span className="text-xs font-semibold text-foreground">Stage {stageIndex + 1}</span>
                                    {stage.steps.length > 1 && (
                                        <Badge variant="secondary" className="text-[10px] h-4 px-1.5 gap-1">
                                            <GitBranch className="h-2.5 w-2.5" />
                                            {stage.steps.length} parallel
                                        </Badge>
                                    )}
                                </div>
                                {stages.length > 1 && (
                                    <button
                                        onClick={() => removeStage(stage.id)}
                                        className="h-6 w-6 flex items-center justify-center rounded text-muted-foreground hover:text-destructive hover:bg-destructive/10 opacity-0 group-hover/stage:opacity-100 transition-all"
                                    >
                                        <X className="h-3.5 w-3.5" />
                                    </button>
                                )}
                            </div>

                            {/* Steps row */}
                            <div className="p-4 flex flex-wrap gap-3 items-stretch">
                                {stage.steps.map((step, stepIndex) => {
                                    const comboKey = `${stage.id}:${step.id}`
                                    const selected = flowTasks.find(ft => ft.id === step.flowTaskId)
                                    return (
                                        <div key={step.id} className="flex flex-col gap-2 group/step">
                                            {/* Parallel indicator */}
                                            {stepIndex > 0 && (
                                                <div className="flex items-center gap-1 mb-0.5">
                                                    <div className="h-px flex-1 bg-border/50" />
                                                    <span className="text-[9px] text-muted-foreground/60 font-medium uppercase tracking-wider px-1">parallel</span>
                                                    <div className="h-px flex-1 bg-border/50" />
                                                </div>
                                            )}
                                            <div className={cn(
                                                "flex flex-col gap-2 p-3 rounded-lg border bg-background/50 min-w-[200px] max-w-[260px] transition-all",
                                                step.flowTaskId ? "border-primary/30 bg-primary/[0.02]" : "border-dashed border-border"
                                            )}>
                                                <div className="flex items-center justify-between gap-2">
                                                    <span className="text-[10px] text-muted-foreground font-medium">
                                                        Step {stages.slice(0, stageIndex).reduce((acc, s) => acc + s.steps.length, 0) + stepIndex + 1}
                                                    </span>
                                                    {stage.steps.length > 1 && (
                                                        <button
                                                            onClick={() => removeStepFromStage(stage.id, step.id)}
                                                            className="h-4 w-4 flex items-center justify-center text-muted-foreground hover:text-destructive opacity-0 group-hover/step:opacity-100 transition-all rounded"
                                                        >
                                                            <X className="h-3 w-3" />
                                                        </button>
                                                    )}
                                                </div>
                                                <FlowTaskCombobox
                                                    value={step.flowTaskId}
                                                    flowTasks={flowTasks}
                                                    onChange={(id) => updateStepFlowTask(stage.id, step.id, id)}
                                                    open={openCombobox === comboKey}
                                                    onOpenChange={(o) => setOpenCombobox(o ? comboKey : null)}
                                                />
                                                {selected && (
                                                    <div className="flex items-center gap-1.5">
                                                        <Badge variant="secondary" className={cn("text-[10px] h-4 px-1.5",
                                                            selected.status === 'SUCCESS' ? "bg-emerald-500/10 text-emerald-600 border-emerald-500/20" :
                                                                selected.status === 'FAILED' ? "bg-rose-500/10 text-rose-600 border-rose-500/20" :
                                                                    "bg-muted text-muted-foreground"
                                                        )}>
                                                            {selected.status}
                                                        </Badge>
                                                        <span className="text-[10px] text-muted-foreground truncate">ID: {selected.id}</span>
                                                    </div>
                                                )}
                                            </div>
                                        </div>
                                    )
                                })}

                                {/* Add parallel step button */}
                                <button
                                    onClick={() => addStepToStage(stage.id)}
                                    className={cn(
                                        "flex flex-col items-center justify-center gap-1.5 p-3 rounded-lg border-2 border-dashed",
                                        "text-muted-foreground/60 hover:text-primary hover:border-primary/40 hover:bg-primary/5",
                                        "transition-all min-w-[120px] min-h-[90px] group/add"
                                    )}
                                >
                                    <Plus className="h-4 w-4 group-hover/add:scale-110 transition-transform" />
                                    <span className="text-[10px] font-medium leading-tight text-center">
                                        Add Parallel<br />Step
                                    </span>
                                </button>
                            </div>
                        </div>
                    </div>
                )
            })}

            {/* Add stage button */}
            <div className="flex flex-col items-center mt-4 gap-1">
                <div className="w-px h-4 bg-border/40" />
                <Button
                    onClick={addStage}
                    variant="outline"
                    className="gap-2 border-dashed border-2 hover:border-primary/50 hover:bg-primary/5 rounded-full px-5"
                >
                    <Plus className="h-4 w-4" />
                    Add Stage
                </Button>
            </div>
        </div>
    )
}

// ─── Helpers: build stages from backend graph ─────────────────────────────────

function buildStagesFromGraph(detail: LinkedTaskDetail): Stage[] {
    const steps = detail.steps
    const edges = detail.edges

    if (steps.length === 0) return []

    // Build predecessors map
    const predecessors: Record<number, number[]> = {}
    for (const s of steps) predecessors[s.id] = []
    for (const e of edges) {
        if (!predecessors[e.target_step_id]) predecessors[e.target_step_id] = []
        predecessors[e.target_step_id].push(e.source_step_id)
    }

    // BFS to assign layers (parallel groups)
    const layers: number[][] = []
    const visited = new Set<number>()
    let queue = steps.filter(s => predecessors[s.id].length === 0).map(s => s.id)

    while (queue.length > 0) {
        layers.push(queue)
        queue.forEach(id => visited.add(id))
        const next: number[] = []
        for (const s of steps) {
            if (!visited.has(s.id) && predecessors[s.id].every(pid => visited.has(pid))) {
                next.push(s.id)
            }
        }
        queue = next
    }

    // Convert layers to Stage objects
    return layers.map((layerIds, i) => {
        const layerSteps = layerIds.map(sid => {
            const s = steps.find(st => st.id === sid)!
            return { id: String(s.id), dbId: s.id, flowTaskId: s.flow_task_id }
        })
        // Get gate condition from any incoming edge to this layer
        let gateCondition: EdgeCondition = 'ON_SUCCESS'
        if (i > 0) {
            const incoming = edges.find(e => layerIds.includes(e.target_step_id))
            if (incoming) gateCondition = incoming.condition as EdgeCondition
        }
        return {
            id: crypto.randomUUID(),
            steps: layerSteps,
            gateCondition,
        }
    })
}

// ─── Main detail page ─────────────────────────────────────────────────────────

export default function LinkedTaskDetailPage() {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const { linkedTaskId } = useParams({ strict: false }) as any
    const queryClient = useQueryClient()

    const [stages, setStages] = useState<Stage[]>([])

    // Load Data
    const { data: detailData, isLoading } = useQuery({
        queryKey: ['linked-task', linkedTaskId],
        queryFn: () => linkedTasksRepo.get(Number(linkedTaskId)),
        refetchInterval: 10_000,
    })

    const { data: flowTasksData } = useQuery({
        queryKey: ['flow-tasks', 'all'],
        queryFn: () => flowTasksRepo.list(1, 1000),
    })

    const detail = detailData?.data as LinkedTaskDetail | undefined
    const allFlowTasks: FlowTask[] = flowTasksData?.data?.items ?? []

    // Initialize stages from backend graph (only on first load)
    useEffect(() => {
        if (!detail) return
        setStages(buildStagesFromGraph(detail))
    }, [detail?.id])

    // ── Save mutation ──────────────────────────────────────────────────────────
    const saveMutation = useMutation({
        mutationFn: async () => {
            if (!detail) throw new Error('No detail')

            const payloadSteps = []
            const payloadEdges = []

            // Flatten all stage steps, assign pos_y per stage and pos_x per step within stage
            let globalStepIndex = 0
            for (let si = 0; si < stages.length; si++) {
                const stage = stages[si]
                for (let ti = 0; ti < stage.steps.length; ti++) {
                    const step = stage.steps[ti]
                    payloadSteps.push({
                        id: step.id, // temp ID (UUID string)
                        flow_task_id: step.flowTaskId!,
                        pos_x: ti * 250,
                        pos_y: si * 150,
                    })
                    globalStepIndex++
                }

                // Connect all steps of previous stage to all steps of THIS stage (cross-product)
                if (si > 0) {
                    const prevStage = stages[si - 1]
                    for (const srcStep of prevStage.steps) {
                        for (const tgtStep of stage.steps) {
                            payloadEdges.push({
                                source_step_id: srcStep.id,
                                target_step_id: tgtStep.id,
                                condition: stage.gateCondition,
                            })
                        }
                    }
                }
            }

            await linkedTasksRepo.saveGraph(detail.id, {
                steps: payloadSteps,
                edges: payloadEdges,
            })
        },
        onSuccess: () => {
            toast.success('Linked task saved successfully')
            queryClient.invalidateQueries({ queryKey: ['linked-task', linkedTaskId] })
        },
        onError: (err) => {
            toast.error('Failed to save linked task')
            console.error(err)
        },
    })

    // ── Run mutation ───────────────────────────────────────────────────────────
    const runMutation = useMutation({
        mutationFn: async () => {
            if (!detail) return
            await linkedTasksRepo.trigger(detail.id)
        },
        onSuccess: () => {
            toast.success('Run triggered successfully')
            queryClient.invalidateQueries({ queryKey: ['linked-task-runs', linkedTaskId] })
        },
        onError: (err) => {
            toast.error('Failed to trigger run')
            console.error(err)
        },
    })

    // ─── Resize Logic ─────────────────────────────────────────────────────────────
    const [historyHeight, setHistoryHeight] = useState(300)
    const [isResizing, setIsResizing] = useState(false)
    const [historyCollapsed, setHistoryCollapsed] = useState(false)

    useEffect(() => {
        const handleMouseMove = (e: MouseEvent) => {
            if (!isResizing) return
            const newHeight = window.innerHeight - e.clientY
            setHistoryHeight(Math.max(100, Math.min(newHeight, window.innerHeight - 200)))
        }

        const handleMouseUp = () => {
            setIsResizing(false)
        }

        if (isResizing) {
            window.addEventListener('mousemove', handleMouseMove)
            window.addEventListener('mouseup', handleMouseUp)
        }
        return () => {
            window.removeEventListener('mousemove', handleMouseMove)
            window.removeEventListener('mouseup', handleMouseUp)
        }
    }, [isResizing])


    if (isLoading) {
        return (
            <div className="flex h-screen items-center justify-center">
                <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
            </div>
        )
    }

    if (!detail) return null

    const totalSteps = stages.reduce((acc, s) => acc + s.steps.length, 0)
    const hasUnconfigured = stages.some(s => s.steps.some(st => !st.flowTaskId))

    return (
        <div className={cn("flex flex-col h-screen overflow-hidden", isResizing && "select-none cursor-ns-resize")}>
            <Header fixed>
                <div className="flex items-center gap-4">
                    <Link to="/linked-tasks" className="text-muted-foreground hover:text-foreground transition-colors">
                        <ChevronLeft className="h-5 w-5" />
                    </Link>
                    <div className="flex flex-col">
                        <div className="flex items-center gap-2">
                            <span className="font-semibold">{detail.name}</span>
                            <Badge variant={detail.status === 'RUNNING' ? 'secondary' : 'outline'} className="text-[10px] h-5">
                                {detail.status}
                            </Badge>
                            {stages.length > 0 && (
                                <Badge variant="secondary" className="text-[10px] h-5 gap-1">
                                    <GitBranch className="h-3 w-3" />
                                    {stages.length} stage{stages.length !== 1 ? 's' : ''} · {totalSteps} step{totalSteps !== 1 ? 's' : ''}
                                </Badge>
                            )}
                        </div>
                        {detail.description && <span className="text-xs text-muted-foreground">{detail.description}</span>}
                    </div>
                </div>
                <div className="ms-auto flex items-center space-x-2">
                    <Button
                        variant="outline"
                        size="sm"
                        onClick={() => saveMutation.mutate()}
                        disabled={saveMutation.isPending || hasUnconfigured}
                    >
                        {saveMutation.isPending && <Loader2 className="mr-2 h-3.5 w-3.5 animate-spin" />}
                        {!saveMutation.isPending && <Save className="mr-2 h-3.5 w-3.5" />}
                        Save Changes
                    </Button>
                    <Button
                        size="sm"
                        onClick={() => runMutation.mutate()}
                        disabled={runMutation.isPending}
                    >
                        {runMutation.isPending ? <Loader2 className="mr-2 h-3.5 w-3.5 animate-spin" /> : <Play className="mr-2 h-3.5 w-3.5" />}
                        Run
                    </Button>
                    <ThemeSwitch />
                </div>
            </Header>

            <Main className="flex-1 flex flex-col p-0 overflow-hidden relative">
                <div className="flex-1 overflow-hidden flex flex-col relative">
                    {/* Background Pattern */}
                    <div className="absolute inset-0 bg-grid-slate-100 dark:bg-grid-slate-900/[0.04] bg-[bottom_1px_center] [mask-image:linear-gradient(to_bottom,transparent,black)] pointer-events-none" />

                    <div className="flex-1 overflow-y-auto relative z-10">
                        <StageBuilder
                            stages={stages}
                            setStages={setStages}
                            flowTasks={allFlowTasks}
                        />
                    </div>
                </div>

                <RunHistoryPanel
                    detail={detail}
                    height={historyHeight}
                    isResizing={isResizing}
                    onResizeStart={(e) => { e.preventDefault(); setIsResizing(true) }}
                    collapsed={historyCollapsed}
                    onToggleCollapse={() => setHistoryCollapsed(!historyCollapsed)}
                />
            </Main>
        </div>
    )
}
