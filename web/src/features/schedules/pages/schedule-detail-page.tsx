import { useEffect, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { Link, useNavigate, useParams } from '@tanstack/react-router'
import {
  CalendarClock,
  MoreVertical,
  Star,
  Trash2,
  ArrowLeft,
  Settings,
  Activity
} from 'lucide-react'
import { toast } from 'sonner'
import {
  Breadcrumb,
  BreadcrumbItem,
  BreadcrumbLink,
  BreadcrumbList,
  BreadcrumbPage,
  BreadcrumbSeparator,
} from '@/components/ui/breadcrumb'
import { Button } from '@/components/ui/button'
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu'
import { Header } from '@/components/layout/header'
import { Main } from '@/components/layout/main'
import { Search } from '@/components/search'
import { ThemeSwitch } from '@/components/theme-switch'
import { Badge } from '@/components/ui/badge'
import { Switch } from '@/components/ui/switch'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { schedulesRepo, type ScheduleCreate, type ScheduleUpdate } from '@/repo/schedules'
import { ScheduleForm } from '../components/schedule-form'
import { ScheduleRuns } from '../components/schedule-runs'
import { type ScheduleFormValues } from '../data/schema'
import cronstrue from 'cronstrue'

export default function ScheduleDetailPage() {
  const { scheduleId } = useParams({
    from: '/_authenticated/schedules/$scheduleId',
  })
  const navigate = useNavigate()
  const queryClient = useQueryClient()
  const isNew = scheduleId === 'new'
  const [activeTab, setActiveTab] = useState('overview')

  // ── Queries ───────────────────────────────────────────────────────────

  const { data: schedule, isLoading } = useQuery({
    queryKey: ['schedules', Number(scheduleId)],
    queryFn: () => schedulesRepo.getById(Number(scheduleId)),
    enabled: !isNew && !Number.isNaN(Number(scheduleId)),
  })

  const { data: historyData, isLoading: historyLoading, refetch: refetchHistory } = useQuery({
    queryKey: ['schedules', Number(scheduleId), 'history'],
    queryFn: () =>
      schedulesRepo.getHistory(Number(scheduleId), { skip: 0, limit: 100 }),
    enabled: !isNew && !Number.isNaN(Number(scheduleId)),
    refetchInterval: 30_000,
  })

  // ── Mutations ─────────────────────────────────────────────────────────

  const createMutation = useMutation({
    mutationFn: (data: ScheduleCreate) => schedulesRepo.create(data),
    onSuccess: async (newSchedule) => {
      toast.success('Schedule created')
      await new Promise((r) => setTimeout(r, 300))
      queryClient.invalidateQueries({ queryKey: ['schedules'], refetchType: 'active' })
      navigate({
        to: '/schedules/$scheduleId',
        params: { scheduleId: String(newSchedule.id) },
      })
    },
    onError: (err: any) =>
      toast.error(err?.response?.data?.detail || 'Failed to create schedule'),
  })

  const updateMutation = useMutation({
    mutationFn: (data: ScheduleUpdate) =>
      schedulesRepo.update(Number(scheduleId), data),
    onSuccess: async () => {
      toast.success('Schedule updated')
      queryClient.invalidateQueries({ queryKey: ['schedules'], refetchType: 'active' })
      queryClient.invalidateQueries({ queryKey: ['schedules', Number(scheduleId)] })
    },
    onError: (err: any) =>
      toast.error(err?.response?.data?.detail || 'Failed to update schedule'),
  })

  const pauseMutation = useMutation({
    mutationFn: () => schedulesRepo.pause(Number(scheduleId)),
    onSuccess: () => {
      toast.success('Schedule paused')
      queryClient.invalidateQueries({ queryKey: ['schedules', Number(scheduleId)] })
    },
    onError: () => toast.error('Failed to pause schedule')
  })

  const resumeMutation = useMutation({
    mutationFn: () => schedulesRepo.resume(Number(scheduleId)),
    onSuccess: () => {
      toast.success('Schedule resumed')
      queryClient.invalidateQueries({ queryKey: ['schedules', Number(scheduleId)] })
    },
    onError: () => toast.error('Failed to resume schedule')
  })

  const deleteMutation = useMutation({
    mutationFn: () => schedulesRepo.delete(Number(scheduleId)),
    onSuccess: () => {
      toast.success('Schedule deleted')
      navigate({ to: '/schedules' })
    },
    onError: () => toast.error('Failed to delete schedule')
  })

  function onSubmit(values: ScheduleFormValues) {
    if (isNew) {
      createMutation.mutate(values)
    } else {
      updateMutation.mutate(values)
    }
  }

  // ── Render ────────────────────────────────────────────────────────────

  useEffect(() => {
    document.title = isNew
      ? 'New Schedule'
      : `Schedule: ${schedule?.name || ''}`
    return () => {
      document.title = 'Rosetta'
    }
  }, [isNew, schedule])

  const cronHuman = schedule ? (() => {
    try {
      return cronstrue.toString(schedule.cron_expression, { verbose: false })
    } catch {
      return schedule.cron_expression
    }
  })() : ''

  if (isLoading) {
    return (
      <div className='flex h-screen items-center justify-center'>
        {/* Minimal loader */}
      </div>
    )
  }

  return (
    <>
      <Header fixed>
        <Search />
        <div className='ms-auto flex items-center space-x-4'>
          <ThemeSwitch />
        </div>
      </Header>

      <Main className='flex flex-1 flex-col gap-8 pb-10'>
        {/* Top Breadcrumb */}
        <div className="flex items-center gap-4">
          <Button variant="ghost" size="icon" className="h-8 w-8 -ml-2" onClick={() => navigate({ to: '/schedules' })}>
            <ArrowLeft className="h-4 w-4" />
          </Button>
          <Breadcrumb>
            <BreadcrumbList>
              <BreadcrumbItem>
                <BreadcrumbLink asChild>
                  <Link to='/schedules'>Schedules</Link>
                </BreadcrumbLink>
              </BreadcrumbItem>
              <BreadcrumbSeparator />
              <BreadcrumbItem>
                <BreadcrumbPage>
                  {isNew ? 'New Schedule' : schedule?.name}
                </BreadcrumbPage>
              </BreadcrumbItem>
            </BreadcrumbList>
          </Breadcrumb>
        </div>

        {/* Header Area */}
        <div className='flex flex-col gap-6 md:flex-row md:items-start md:justify-between'>
          <div className='flex items-start gap-4'>
            <div className='flex h-12 w-12 shrink-0 items-center justify-center rounded-lg bg-primary/10 text-primary mt-1'>
              <CalendarClock className='h-6 w-6' />
            </div>
            <div className="space-y-1">
              <h1 className='text-2xl font-bold tracking-tight flex items-center gap-3'>
                {isNew ? 'Create Schedule' : schedule?.name}
                {schedule && (
                  <Badge variant={schedule.status === 'ACTIVE' ? 'default' : 'secondary'} className="rounded-sm">
                    {schedule.status}
                  </Badge>
                )}
              </h1>
              <div className='flex items-center gap-3 text-sm text-muted-foreground'>
                {schedule ? (
                  <>
                    <span className='font-mono bg-muted px-1 rounded-sm'>{schedule.cron_expression}</span>
                    <span>•</span>
                    <span>{cronHuman}</span>
                    <span>•</span>
                    <span className="capitalize">{schedule.task_type.toLowerCase().replace('_', ' ')}</span>
                  </>
                ) : (
                  <span>Define your new schedule execution plan.</span>
                )}
              </div>
            </div>
          </div>

          {!isNew && (
            <div className='flex items-center gap-3'>
              <div className="flex items-center gap-2 mr-2 bg-muted/50 p-1 pl-3 pr-2 rounded-full border">
                <span className="text-xs font-medium text-muted-foreground">Enabled</span>
                <Switch
                  checked={schedule?.status === 'ACTIVE'}
                  onCheckedChange={(c) => c ? resumeMutation.mutate() : pauseMutation.mutate()}
                  className="scale-90"
                />
              </div>

              <Button variant='outline' size='sm'>
                <Star className='mr-2 h-4 w-4' />
                Favorite
              </Button>
              <DropdownMenu>
                <DropdownMenuTrigger asChild>
                  <Button variant='outline' size='icon'>
                    <MoreVertical className='h-4 w-4' />
                  </Button>
                </DropdownMenuTrigger>
                <DropdownMenuContent align='end'>
                  <DropdownMenuItem onClick={() => deleteMutation.mutate()} className='text-destructive focus:text-destructive'>
                    <Trash2 className='mr-2 h-4 w-4' />
                    Delete Schedule
                  </DropdownMenuItem>
                </DropdownMenuContent>
              </DropdownMenu>
            </div>
          )}
        </div>

        {/* Content Tabs */}
        {isNew ? (
          <div className="max-w-3xl">
            <ScheduleForm onSubmit={onSubmit} isSubmitting={createMutation.isPending} />
          </div>
        ) : (
          <Tabs value={activeTab} onValueChange={setActiveTab} className="space-y-6">
            <TabsList className="grid w-full grid-cols-2 lg:w-[400px]">
              <TabsTrigger value="overview" className="gap-2"><Settings className="h-4 w-4" /> Configuration</TabsTrigger>
              <TabsTrigger value="runs" className="gap-2"><Activity className="h-4 w-4" /> Run History</TabsTrigger>
            </TabsList>

            <TabsContent value="overview" className="max-w-3xl focus-visible:outline-none focus-visible:ring-0">
              <p className="text-sm text-muted-foreground mb-6">
                Configure the schedule execution frequency and target task.
              </p>
              <ScheduleForm
                schedule={schedule}
                onSubmit={onSubmit}
                isSubmitting={updateMutation.isPending}
              />
            </TabsContent>

            <TabsContent value="runs" className="focus-visible:outline-none focus-visible:ring-0">
              <p className="text-sm text-muted-foreground mb-6">
                View the execution history of this schedule.
              </p>
              <ScheduleRuns
                data={historyData?.items ?? []}
                isLoading={historyLoading}
                onRefresh={refetchHistory}
              />
            </TabsContent>
          </Tabs>
        )}
      </Main>
    </>
  )
}
