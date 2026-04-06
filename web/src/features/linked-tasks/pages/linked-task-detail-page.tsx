import { useEffect, useState } from 'react'
import { formatDistanceToNow, format } from 'date-fns'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useParams, Link } from '@tanstack/react-router'
import { flowTasksRepo, type FlowTask } from '@/repo/flow-tasks'
import {
  linkedTasksRepo,
  type LinkedTaskDetail,
  type LinkedTaskRunHistory,
  type EdgeCondition,
} from '@/repo/linked-tasks'
import { flowTaskKeys, linkedTaskKeys } from '@/repo/query-keys'
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
import { cn } from '@/lib/utils'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
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
import { Header } from '@/components/layout/header'
import { Main } from '@/components/layout/main'
import { ThemeSwitch } from '@/components/theme-switch'

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
    case 'SUCCESS':
      return <CheckCircle2 className='h-4 w-4 text-emerald-500' />
    case 'FAILED':
      return <XCircle className='h-4 w-4 text-rose-500' />
    case 'RUNNING':
      return <Loader2 className='h-4 w-4 animate-spin text-blue-500' />
    case 'SKIPPED':
      return <SkipForward className='h-4 w-4 text-muted-foreground' />
    default:
      return <Clock className='h-4 w-4 text-muted-foreground' />
  }
}

function RunStatusBadge({ status }: { status: string }) {
  const cls: Record<string, string> = {
    RUNNING:
      'bg-blue-500/15 text-blue-600 border-blue-500/30 dark:text-blue-400',
    SUCCESS:
      'bg-emerald-500/15 text-emerald-600 border-emerald-500/30 dark:text-emerald-400',
    FAILED:
      'bg-rose-500/15 text-rose-600 border-rose-500/30 dark:text-rose-400',
    CANCELLED: 'bg-muted text-muted-foreground border-border',
  }
  return (
    <span
      className={cn(
        'inline-flex items-center gap-1.5 rounded-full border px-2 py-0.5 text-[11px] font-semibold',
        cls[status] ?? cls.CANCELLED
      )}
    >
      {status === 'RUNNING' && <Loader2 className='h-3 w-3 animate-spin' />}
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
  onToggleCollapse,
}: {
  detail: LinkedTaskDetail
  height: number
  isResizing: boolean
  onResizeStart: (e: React.MouseEvent) => void
  collapsed: boolean
  onToggleCollapse: () => void
}) {
  const { data, isLoading } = useQuery({
    queryKey: linkedTaskKeys.runs(detail.id),
    queryFn: () => linkedTasksRepo.getRuns(detail.id, 1, 20),
    refetchInterval: 8_000,
  })

  const runs = data?.items ?? []

  return (
    <div
      className='relative flex shrink-0 flex-col overflow-hidden border-t border-border/60 bg-card/20 transition-all duration-300 ease-in-out'
      style={{ height: collapsed ? 40 : height }}
    >
      {/* Resize Handle - only show when expanded */}
      {!collapsed && (
        <div
          onMouseDown={onResizeStart}
          className={cn(
            'absolute top-0 right-0 left-0 z-20 h-1 cursor-ns-resize transition-colors hover:bg-primary/50',
            isResizing && 'bg-primary'
          )}
        />
      )}

      <div
        className={cn(
          'flex shrink-0 cursor-pointer items-center justify-between border-b border-border/60 bg-background/50 px-6 backdrop-blur-sm transition-colors hover:bg-muted/50',
          collapsed ? 'h-10 border-b-0 py-2' : 'py-3'
        )}
        onClick={collapsed ? onToggleCollapse : undefined}
      >
        <div className='flex items-center gap-2'>
          <Clock className='h-4 w-4 text-muted-foreground' />
          <p className='text-sm font-semibold text-muted-foreground'>
            Run History
          </p>
        </div>

        <div className='flex items-center gap-4'>
          {!collapsed && (
            <span className='text-xs text-muted-foreground'>
              {data?.total ?? 0} run{(data?.total ?? 0) !== 1 ? 's' : ''}
            </span>
          )}
          <button
            onClick={(e) => {
              e.stopPropagation()
              onToggleCollapse()
            }}
            className='rounded p-1 text-muted-foreground transition-colors hover:bg-muted hover:text-foreground'
          >
            <ChevronLeft
              className={cn(
                'h-4 w-4 transition-transform duration-300',
                collapsed ? 'rotate-90' : '-rotate-90'
              )}
            />
          </button>
        </div>
      </div>

      <div
        className={cn(
          'flex min-h-0 flex-1 flex-col',
          collapsed && 'invisible opacity-0 transition-opacity duration-200'
        )}
      >
        {isLoading && (
          <div className='flex h-full items-center justify-center text-muted-foreground'>
            <Loader2 className='h-5 w-5 animate-spin' />
          </div>
        )}
        {!isLoading && runs.length === 0 && (
          <div className='flex h-full flex-col items-center justify-center gap-2 text-muted-foreground'>
            <AlertCircle className='h-5 w-5 opacity-40' />
            <span className='text-xs'>No runs yet. Press Run to start.</span>
          </div>
        )}
        {!isLoading && runs.length > 0 && (
          <div className='flex-1 overflow-y-auto p-0'>
            {runs.map((run) => (
              <RunRow key={run.id} run={run} detail={detail} />
            ))}
          </div>
        )}
      </div>
    </div>
  )
}

function RunRow({
  run,
  detail,
}: {
  run: LinkedTaskRunHistory
  detail: LinkedTaskDetail
}) {
  const [expanded, setExpanded] = useState(false)
  const queryClient = useQueryClient()
  const failedLog = run.step_logs.find((l) => l.status === 'FAILED')
  const duration =
    run.finished_at && run.started_at
      ? Math.round(
          (new Date(run.finished_at).getTime() -
            new Date(run.started_at).getTime()) /
            1000
        )
      : null

  const cancelMutation = useMutation({
    mutationFn: () => linkedTasksRepo.cancelRun(detail.id, run.id),
    onSuccess: () => {
      queryClient.invalidateQueries({
        queryKey: linkedTaskKeys.runs(detail.id),
      })
      queryClient.invalidateQueries({
        queryKey: linkedTaskKeys.detail(detail.id),
      })
    },
    onError: () => toast.error('Failed to cancel run'),
  })

  return (
    <div className='group border-b border-border/40 last:border-0'>
      <button
        onClick={() => setExpanded(!expanded)}
        className={cn(
          'flex w-full items-center gap-4 px-6 py-3 text-left transition-colors hover:bg-muted/30',
          expanded && 'bg-muted/30'
        )}
      >
        <RunStatusBadge status={run.status} />
        <div className='flex min-w-0 flex-1 flex-col gap-0.5'>
          <div className='flex items-center gap-2'>
            <span className='text-xs font-medium text-foreground'>
              {format(new Date(run.started_at), 'MMM d, HH:mm:ss')}
            </span>
            <span className='text-[10px] text-muted-foreground'>
              ·{' '}
              {formatDistanceToNow(new Date(run.started_at), {
                addSuffix: true,
              })}
            </span>
          </div>
          <div className='flex items-center gap-2 text-[10px] text-muted-foreground'>
            {duration !== null && <span>{duration}s duration</span>}
            <span>· {run.trigger_type}</span>
          </div>
        </div>
        {failedLog && (
          <div className='hidden max-w-[200px] items-center gap-1.5 truncate rounded border border-rose-500/20 bg-rose-500/10 px-2 py-0.5 text-xs text-rose-500 sm:flex'>
            <AlertCircle className='h-3 w-3 shrink-0' />
            <span className='truncate'>Error in step #{failedLog.step_id}</span>
          </div>
        )}
        {run.status === 'RUNNING' && (
          <button
            onClick={(e) => {
              e.stopPropagation()
              cancelMutation.mutate()
            }}
            disabled={cancelMutation.isPending}
            title='Cancel run'
            className='flex items-center gap-1 rounded border border-orange-500/20 bg-orange-500/10 px-2 py-0.5 text-[11px] font-medium text-orange-500 transition-colors hover:bg-orange-500/20 disabled:opacity-50'
          >
            {cancelMutation.isPending ? (
              <Loader2 className='h-3 w-3 animate-spin' />
            ) : (
              <Square className='h-3 w-3 fill-orange-500' />
            )}
            <span>Cancel</span>
          </button>
        )}
        <ChevronLeft
          className={cn(
            'h-4 w-4 text-muted-foreground transition-transform duration-200',
            expanded ? '-rotate-90' : 'rotate-0'
          )}
        />
      </button>

      {expanded && (
        <div className='space-y-2 border-t border-border/40 bg-muted/20 px-6 py-3'>
          <div className='mb-2 flex items-center gap-2'>
            <p className='text-[10px] font-semibold tracking-wider text-muted-foreground uppercase'>
              Execution Steps
            </p>
            <Separator className='flex-1' />
          </div>
          {run.step_logs.length === 0 && (
            <p className='text-xs text-muted-foreground italic'>
              No steps executed yet.
            </p>
          )}
          {run.step_logs.map((log) => {
            const step = detail.steps.find((s) => s.id === log.step_id)
            return (
              <div
                key={log.id}
                className='flex items-start gap-3 rounded-md border border-transparent px-3 py-1.5 transition-colors hover:border-border/40 hover:bg-background/50'
              >
                <div className='mt-0.5'>
                  <StepStatusIcon status={log.status} />
                </div>
                <div className='grid min-w-0 flex-1 gap-0.5'>
                  <div className='flex items-center justify-between gap-4'>
                    <span
                      className={cn(
                        'text-xs font-medium',
                        log.status === 'FAILED'
                          ? 'text-rose-500'
                          : 'text-foreground'
                      )}
                    >
                      {step?.flow_task?.name ?? `Step #${log.step_id}`}
                    </span>
                    <span className='text-[10px] text-muted-foreground tabular-nums'>
                      {log.finished_at && log.started_at
                        ? `${((new Date(log.finished_at).getTime() - new Date(log.started_at).getTime()) / 1000).toFixed(1)}s`
                        : ''}
                    </span>
                  </div>
                  {log.error_message && (
                    <div className='mt-1 rounded border border-rose-500/20 bg-rose-500/10 p-2 font-mono text-[10px] break-words whitespace-pre-wrap text-rose-600 dark:text-rose-400'>
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
  const selected = flowTasks.find((ft) => ft.id === value)
  return (
    <Popover open={open} onOpenChange={onOpenChange}>
      <PopoverTrigger asChild>
        <Button
          variant='outline'
          role='combobox'
          className={cn(
            'h-9 w-full min-w-[160px] justify-between px-3 text-xs font-normal',
            !value && 'border-dashed text-muted-foreground'
          )}
        >
          <div className='flex items-center gap-1.5 truncate'>
            <Link2 className='h-3.5 w-3.5 shrink-0 opacity-50' />
            <span className='truncate'>
              {selected ? selected.name : 'Select task...'}
            </span>
          </div>
          <ChevronsUpDown className='ml-1 h-3.5 w-3.5 shrink-0 opacity-50' />
        </Button>
      </PopoverTrigger>
      <PopoverContent className='w-[320px] p-0' align='start'>
        <Command>
          <CommandInput placeholder='Search flow tasks...' />
          <CommandList>
            <CommandEmpty>No flow task found.</CommandEmpty>
            <CommandGroup>
              {flowTasks.map((ft) => (
                <CommandItem
                  key={ft.id}
                  value={ft.name}
                  onSelect={() => {
                    onChange(ft.id)
                    onOpenChange(false)
                  }}
                >
                  <Check
                    className={cn(
                      'mr-2 h-4 w-4',
                      value === ft.id ? 'opacity-100' : 'opacity-0'
                    )}
                  />
                  <div className='flex flex-col gap-0.5'>
                    <span>{ft.name}</span>
                    {ft.description && (
                      <span className='line-clamp-1 text-xs text-muted-foreground'>
                        {ft.description}
                      </span>
                    )}
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
    setStages([
      ...stages,
      {
        id: crypto.randomUUID(),
        steps: [{ id: crypto.randomUUID(), flowTaskId: null }],
        gateCondition: 'ON_SUCCESS',
      },
    ])
  }

  const removeStage = (stageId: string) => {
    setStages(stages.filter((s) => s.id !== stageId))
  }

  const addStepToStage = (stageId: string) => {
    setStages(
      stages.map((s) =>
        s.id === stageId
          ? {
              ...s,
              steps: [
                ...s.steps,
                { id: crypto.randomUUID(), flowTaskId: null },
              ],
            }
          : s
      )
    )
  }

  const removeStepFromStage = (stageId: string, stepId: string) => {
    setStages(
      stages
        .map((s) => {
          if (s.id !== stageId) return s
          const newSteps = s.steps.filter((st) => st.id !== stepId)
          // If stage is now empty, remove it entirely
          return newSteps.length === 0 ? null : { ...s, steps: newSteps }
        })
        .filter(Boolean) as Stage[]
    )
  }

  const updateStepFlowTask = (
    stageId: string,
    stepId: string,
    flowTaskId: number
  ) => {
    setStages(
      stages.map((s) =>
        s.id === stageId
          ? {
              ...s,
              steps: s.steps.map((st) =>
                st.id === stepId ? { ...st, flowTaskId } : st
              ),
            }
          : s
      )
    )
  }

  const updateStageCondition = (stageId: string, condition: EdgeCondition) => {
    setStages(
      stages.map((s) =>
        s.id === stageId ? { ...s, gateCondition: condition } : s
      )
    )
  }

  if (stages.length === 0) {
    return (
      <div className='flex h-full flex-col items-center justify-center gap-4'>
        <div className='w-full max-w-xl rounded-xl border-2 border-dashed bg-muted/10 py-12 text-center'>
          <GitBranch className='mx-auto mb-3 h-8 w-8 text-muted-foreground/40' />
          <p className='font-medium text-muted-foreground'>No stages defined</p>
          <p className='mt-1 mb-4 text-xs text-muted-foreground/60'>
            Add stages to build a flow. Steps within a stage run in parallel.
          </p>
          <Button onClick={addStage} size='sm'>
            <Plus className='mr-2 h-4 w-4' />
            Add First Stage
          </Button>
        </div>
      </div>
    )
  }

  return (
    <div className='mx-auto flex w-full max-w-4xl flex-col items-center gap-0 px-4 py-8 pb-24'>
      {stages.map((stage, stageIndex) => {
        const isFirst = stageIndex === 0
        return (
          <div key={stage.id} className='flex w-full flex-col items-center'>
            {/* Gate condition connector (between stages) */}
            {!isFirst && (
              <div className='z-10 flex flex-col items-center gap-1 py-2'>
                <div className='h-4 w-px bg-border/60' />
                <div className='flex items-center gap-2 rounded-full border border-border bg-background px-3 py-1 shadow-sm'>
                  <ArrowDown className='h-3 w-3 text-muted-foreground' />
                  <Select
                    value={stage.gateCondition}
                    onValueChange={(v) =>
                      updateStageCondition(stage.id, v as EdgeCondition)
                    }
                  >
                    <SelectTrigger className='h-6 w-auto border-0 p-0 pr-4 text-[11px] font-medium shadow-none focus:ring-0'>
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value='ON_SUCCESS'>
                        <div className='flex items-center gap-1.5'>
                          <div className='h-1.5 w-1.5 rounded-full bg-emerald-500' />
                          <span>On Success</span>
                        </div>
                      </SelectItem>
                      <SelectItem value='ALWAYS'>
                        <div className='flex items-center gap-1.5'>
                          <div className='h-1.5 w-1.5 rounded-full bg-blue-500' />
                          <span>Always Run</span>
                        </div>
                      </SelectItem>
                    </SelectContent>
                  </Select>
                </div>
                <div className='h-4 w-px bg-border/60' />
              </div>
            )}

            {/* Stage Card */}
            <div
              className={cn(
                'group/stage w-full rounded-xl border bg-sidebar shadow-sm transition-all',
                'border-l-4',
                isFirst
                  ? 'border-l-primary'
                  : stage.gateCondition === 'ON_SUCCESS'
                    ? 'border-l-emerald-500'
                    : 'border-l-blue-500'
              )}
            >
              {/* Stage header */}
              <div className='flex items-center justify-between border-b border-border/50 px-4 py-2.5'>
                <div className='flex items-center gap-2'>
                  <div className='flex h-5 w-5 items-center justify-center rounded-full bg-primary/10'>
                    <span className='text-[10px] font-bold text-primary'>
                      {stageIndex + 1}
                    </span>
                  </div>
                  <span className='text-xs font-semibold text-foreground'>
                    Stage {stageIndex + 1}
                  </span>
                  {stage.steps.length > 1 && (
                    <Badge
                      variant='secondary'
                      className='h-4 gap-1 px-1.5 text-[10px]'
                    >
                      <GitBranch className='h-2.5 w-2.5' />
                      {stage.steps.length} parallel
                    </Badge>
                  )}
                </div>
                {stages.length > 1 && (
                  <button
                    onClick={() => removeStage(stage.id)}
                    className='flex h-6 w-6 items-center justify-center rounded text-muted-foreground opacity-0 transition-all group-hover/stage:opacity-100 hover:bg-destructive/10 hover:text-destructive'
                  >
                    <X className='h-3.5 w-3.5' />
                  </button>
                )}
              </div>

              {/* Steps row */}
              <div className='flex flex-wrap items-stretch gap-3 p-4'>
                {stage.steps.map((step, stepIndex) => {
                  const comboKey = `${stage.id}:${step.id}`
                  const selected = flowTasks.find(
                    (ft) => ft.id === step.flowTaskId
                  )
                  return (
                    <div
                      key={step.id}
                      className='group/step flex flex-col gap-2'
                    >
                      {/* Parallel indicator */}
                      {stepIndex > 0 && (
                        <div className='mb-0.5 flex items-center gap-1'>
                          <div className='h-px flex-1 bg-border/50' />
                          <span className='px-1 text-[9px] font-medium tracking-wider text-muted-foreground/60 uppercase'>
                            parallel
                          </span>
                          <div className='h-px flex-1 bg-border/50' />
                        </div>
                      )}
                      <div
                        className={cn(
                          'flex max-w-[260px] min-w-[200px] flex-col gap-2 rounded-lg border bg-background/50 p-3 transition-all',
                          step.flowTaskId
                            ? 'border-primary/30 bg-primary/[0.02]'
                            : 'border-dashed border-border'
                        )}
                      >
                        <div className='flex items-center justify-between gap-2'>
                          <span className='text-[10px] font-medium text-muted-foreground'>
                            Step{' '}
                            {stages
                              .slice(0, stageIndex)
                              .reduce((acc, s) => acc + s.steps.length, 0) +
                              stepIndex +
                              1}
                          </span>
                          {stage.steps.length > 1 && (
                            <button
                              onClick={() =>
                                removeStepFromStage(stage.id, step.id)
                              }
                              className='flex h-4 w-4 items-center justify-center rounded text-muted-foreground opacity-0 transition-all group-hover/step:opacity-100 hover:text-destructive'
                            >
                              <X className='h-3 w-3' />
                            </button>
                          )}
                        </div>
                        <FlowTaskCombobox
                          value={step.flowTaskId}
                          flowTasks={flowTasks}
                          onChange={(id) =>
                            updateStepFlowTask(stage.id, step.id, id)
                          }
                          open={openCombobox === comboKey}
                          onOpenChange={(o) =>
                            setOpenCombobox(o ? comboKey : null)
                          }
                        />
                        {selected && (
                          <div className='flex items-center gap-1.5'>
                            <Badge
                              variant='secondary'
                              className={cn(
                                'h-4 px-1.5 text-[10px]',
                                selected.status === 'SUCCESS'
                                  ? 'border-emerald-500/20 bg-emerald-500/10 text-emerald-600'
                                  : selected.status === 'FAILED'
                                    ? 'border-rose-500/20 bg-rose-500/10 text-rose-600'
                                    : 'bg-muted text-muted-foreground'
                              )}
                            >
                              {selected.status}
                            </Badge>
                            <span className='truncate text-[10px] text-muted-foreground'>
                              ID: {selected.id}
                            </span>
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
                    'flex flex-col items-center justify-center gap-1.5 rounded-lg border-2 border-dashed p-3',
                    'text-muted-foreground/60 hover:border-primary/40 hover:bg-primary/5 hover:text-primary',
                    'group/add min-h-[90px] min-w-[120px] transition-all'
                  )}
                >
                  <Plus className='h-4 w-4 transition-transform group-hover/add:scale-110' />
                  <span className='text-center text-[10px] leading-tight font-medium'>
                    Add Parallel
                    <br />
                    Step
                  </span>
                </button>
              </div>
            </div>
          </div>
        )
      })}

      {/* Add stage button */}
      <div className='mt-4 flex flex-col items-center gap-1'>
        <div className='h-4 w-px bg-border/40' />
        <Button
          onClick={addStage}
          variant='outline'
          className='gap-2 rounded-full border-2 border-dashed px-5 hover:border-primary/50 hover:bg-primary/5'
        >
          <Plus className='h-4 w-4' />
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
  let queue = steps
    .filter((s) => predecessors[s.id].length === 0)
    .map((s) => s.id)

  while (queue.length > 0) {
    layers.push(queue)
    queue.forEach((id) => visited.add(id))
    const next: number[] = []
    for (const s of steps) {
      if (
        !visited.has(s.id) &&
        predecessors[s.id].every((pid) => visited.has(pid))
      ) {
        next.push(s.id)
      }
    }
    queue = next
  }

  // Convert layers to Stage objects
  return layers.map((layerIds, i) => {
    const layerSteps = layerIds.map((sid) => {
      const s = steps.find((st) => st.id === sid)!
      return { id: String(s.id), dbId: s.id, flowTaskId: s.flow_task_id }
    })
    // Get gate condition from any incoming edge to this layer
    let gateCondition: EdgeCondition = 'ON_SUCCESS'
    if (i > 0) {
      const incoming = edges.find((e) => layerIds.includes(e.target_step_id))
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
  const linkedTaskIdNum = Number(linkedTaskId)
  const queryClient = useQueryClient()

  const [stages, setStages] = useState<Stage[]>([])

  // Load Data
  const { data: detailData, isLoading } = useQuery({
    queryKey: linkedTaskKeys.detail(linkedTaskIdNum),
    queryFn: () => linkedTasksRepo.get(linkedTaskIdNum),
    refetchInterval: 10_000,
  })

  const { data: flowTasksData } = useQuery({
    queryKey: flowTaskKeys.list(1, 1000),
    queryFn: () => flowTasksRepo.list(1, 1000),
  })

  const detail = detailData as LinkedTaskDetail | undefined
  const allFlowTasks: FlowTask[] = flowTasksData?.items ?? []

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
      queryClient.invalidateQueries({
        queryKey: linkedTaskKeys.detail(linkedTaskIdNum),
      })
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
      queryClient.invalidateQueries({
        queryKey: linkedTaskKeys.runs(linkedTaskIdNum),
      })
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
      setHistoryHeight(
        Math.max(100, Math.min(newHeight, window.innerHeight - 200))
      )
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
      <div className='flex h-screen items-center justify-center'>
        <Loader2 className='h-8 w-8 animate-spin text-muted-foreground' />
      </div>
    )
  }

  if (!detail) return null

  const totalSteps = stages.reduce((acc, s) => acc + s.steps.length, 0)
  const hasUnconfigured = stages.some((s) =>
    s.steps.some((st) => !st.flowTaskId)
  )

  return (
    <div
      className={cn(
        'flex h-screen flex-col overflow-hidden',
        isResizing && 'cursor-ns-resize select-none'
      )}
    >
      <Header fixed>
        <div className='flex items-center gap-4'>
          <Link
            to='/linked-tasks'
            className='text-muted-foreground transition-colors hover:text-foreground'
          >
            <ChevronLeft className='h-5 w-5' />
          </Link>
          <div className='flex flex-col'>
            <div className='flex items-center gap-2'>
              <span className='font-semibold'>{detail.name}</span>
              <Badge
                variant={detail.status === 'RUNNING' ? 'secondary' : 'outline'}
                className='h-5 text-[10px]'
              >
                {detail.status}
              </Badge>
              {stages.length > 0 && (
                <Badge variant='secondary' className='h-5 gap-1 text-[10px]'>
                  <GitBranch className='h-3 w-3' />
                  {stages.length} stage{stages.length !== 1 ? 's' : ''} ·{' '}
                  {totalSteps} step{totalSteps !== 1 ? 's' : ''}
                </Badge>
              )}
            </div>
            {detail.description && (
              <span className='text-xs text-muted-foreground'>
                {detail.description}
              </span>
            )}
          </div>
        </div>
        <div className='ms-auto flex items-center space-x-2'>
          <Button
            variant='outline'
            size='sm'
            onClick={() => saveMutation.mutate()}
            disabled={saveMutation.isPending || hasUnconfigured}
          >
            {saveMutation.isPending && (
              <Loader2 className='mr-2 h-3.5 w-3.5 animate-spin' />
            )}
            {!saveMutation.isPending && <Save className='mr-2 h-3.5 w-3.5' />}
            Save Changes
          </Button>
          <Button
            size='sm'
            onClick={() => runMutation.mutate()}
            disabled={runMutation.isPending}
          >
            {runMutation.isPending ? (
              <Loader2 className='mr-2 h-3.5 w-3.5 animate-spin' />
            ) : (
              <Play className='mr-2 h-3.5 w-3.5' />
            )}
            Run
          </Button>
          <ThemeSwitch />
        </div>
      </Header>

      <Main className='relative flex flex-1 flex-col overflow-hidden p-0'>
        <div className='relative flex flex-1 flex-col overflow-hidden'>
          {/* Background Pattern */}
          <div className='bg-grid-slate-100 dark:bg-grid-slate-900/[0.04] pointer-events-none absolute inset-0 [mask-image:linear-gradient(to_bottom,transparent,black)] bg-[bottom_1px_center]' />

          <div className='relative z-10 flex-1 overflow-y-auto'>
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
          onResizeStart={(e) => {
            e.preventDefault()
            setIsResizing(true)
          }}
          collapsed={historyCollapsed}
          onToggleCollapse={() => setHistoryCollapsed(!historyCollapsed)}
        />
      </Main>
    </div>
  )
}
