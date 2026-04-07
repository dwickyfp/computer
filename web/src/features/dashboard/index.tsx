import { useQuery } from '@tanstack/react-query'
import {
  Activity,
  ArrowUpRight,
  Calendar,
  CreditCard,
  Loader2,
  Minus,
  RefreshCw,
  Server,
  TrendingDown,
  TrendingUp,
} from 'lucide-react'
import { format } from 'date-fns'
import {
  Area,
  AreaChart,
  CartesianGrid,
  Legend,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import { Header } from '@/components/layout/header'
import { Main } from '@/components/layout/main'
import { Search } from '@/components/search'
import { ThemeSwitch } from '@/components/theme-switch'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { dashboardRepo } from '@/repo/dashboard'
import { cn } from '@/lib/utils'
import { ActivityFeed } from './components/activity-feed'
import { BackfillStatsCard } from './components/backfill-stats-card'
import {
  dashboardTooltipStyle,
  getDashboardSeriesColor,
} from './components/chart-theme'
import { DashboardGrid } from './components/dashboard-grid'
import { DashboardPanel } from './components/dashboard-panel'
import { JobStatusCard } from './components/job-status-card'
import { SourceHealthCard } from './components/source-health-card'
import { SystemHealthWidget } from './components/system-health-widget'
import { SystemLoadCard } from './components/system-load-card'
import { TopTablesChart } from './components/top-tables-chart'
import { WALMonitorList } from './components/wal-monitor-list'
import { WorkerStatusCard } from './components/worker-status-card'
import {
  RefreshIntervalProvider,
  useRefreshInterval,
} from './context/refresh-interval-context'

const REFRESH_INTERVALS = [
  { label: 'Auto', value: 5000 },
  { label: '10 sec', value: 10000 },
  { label: '15 sec', value: 15000 },
  { label: '30 sec', value: 30000 },
  { label: '60 sec', value: 60000 },
] as const

const compactNumberFormatter = new Intl.NumberFormat('en-US', {
  notation: 'compact',
  maximumFractionDigits: 1,
})

function formatCurrency(value: number) {
  return new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    minimumFractionDigits: value < 1 ? 4 : 2,
    maximumFractionDigits: value < 1 ? 4 : 2,
  }).format(value)
}

function DashboardContent() {
  const { refreshInterval, setRefreshInterval } = useRefreshInterval()

  const {
    data: summary,
    dataUpdatedAt: summaryUpdatedAt,
    isFetching: isSummaryFetching,
  } = useQuery({
    queryKey: ['dashboard', 'summary'],
    queryFn: dashboardRepo.getSummary,
    refetchInterval: refreshInterval,
  })

  const { data: flowChart, isLoading: isFlowChartLoading } = useQuery({
    queryKey: ['dashboard', 'flow-chart'],
    queryFn: () => dashboardRepo.getFlowChart(14),
    refetchInterval: refreshInterval,
  })

  const flowToday = summary?.data_flow?.today ?? 0
  const flowYesterday = summary?.data_flow?.yesterday ?? 0
  const flowTrend =
    flowYesterday > 0 ? ((flowToday - flowYesterday) / flowYesterday) * 100 : 0

  const formattedDate = format(new Date(), 'EEEE, MMMM d, yyyy')
  const lastUpdatedText = summaryUpdatedAt
    ? format(new Date(summaryUpdatedAt), 'HH:mm:ss')
    : 'Waiting for first sync'

  return (
    <>
      <Header>
        <Search />
        <div className='ms-auto flex items-center gap-3'>
          <ThemeSwitch />
        </div>
      </Header>

      <Main className='dashboard-shell min-h-screen px-4 py-4 sm:px-6 sm:py-6'>
        <div className='relative z-10 mx-auto flex w-full max-w-[1600px] flex-col gap-6'>
          <section className='flex flex-col gap-4 xl:flex-row xl:items-end xl:justify-between'>
            <div className='space-y-3'>
              <div className='dashboard-chip w-fit rounded-full px-3.5 py-1.5 text-[11px] font-medium uppercase tracking-[0.24em] text-muted-foreground'>
                Operations cockpit
              </div>
              <div className='space-y-3'>
                <h1 className='font-manrope text-4xl font-semibold tracking-[-0.04em] text-foreground sm:text-5xl'>
                  Dashboard
                </h1>
                <p className='max-w-2xl text-sm leading-6 text-muted-foreground sm:text-base'>
                  Monitor source health, pipeline throughput, spend, and
                  infrastructure signals without leaving the page.
                </p>
              </div>
            </div>

            <div className='grid gap-3 sm:grid-cols-3 xl:min-w-[620px]'>
              <div className='dashboard-toolbar-chip rounded-[22px] px-4 py-3.5'>
                <div className='flex items-center gap-2 text-xs text-muted-foreground'>
                  <Calendar className='h-3.5 w-3.5' />
                  Today
                </div>
                <p className='mt-2 text-sm font-medium text-foreground'>
                  {formattedDate}
                </p>
              </div>

              <div className='dashboard-toolbar-chip rounded-[22px] px-4 py-3.5'>
                <div className='flex items-center gap-2 text-xs text-muted-foreground'>
                  {isSummaryFetching ? (
                    <Loader2 className='h-3.5 w-3.5 animate-spin' />
                  ) : (
                    <RefreshCw className='h-3.5 w-3.5' />
                  )}
                  {isSummaryFetching ? 'Refreshing' : 'Last updated'}
                </div>
                <p className='mt-2 text-sm font-medium text-foreground'>
                  {lastUpdatedText}
                </p>
              </div>

              <div className='dashboard-toolbar-chip rounded-[22px] px-4 py-3.5'>
                <div className='text-xs text-muted-foreground'>Auto refresh</div>
                <Select
                  value={refreshInterval.toString()}
                  onValueChange={(value) => setRefreshInterval(Number(value))}
                >
                  <SelectTrigger className='mt-2 h-11 w-full rounded-xl border border-white/8 bg-white/[0.035] px-3 text-left text-sm font-medium text-foreground shadow-none focus:ring-0'>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    {REFRESH_INTERVALS.map((interval) => (
                      <SelectItem
                        key={interval.value}
                        value={interval.value.toString()}
                      >
                        {interval.label}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
            </div>
          </section>

          <DashboardGrid>
            <SourceHealthCard className='xl:col-span-3' href='/sources' />

            <DashboardPanel
              title='Pipeline health'
              description='Running and paused pipelines.'
              headerSlot={
                <div className='dashboard-chip rounded-full p-2.5'>
                  <Server className='h-4 w-4 text-muted-foreground' />
                </div>
              }
              className='min-h-[182px] xl:col-span-3'
              contentClassName='gap-3'
              href='/pipelines'
              interactive
              variant='dense'
            >
              <div className='space-y-1.5'>
                <div className='flex items-end justify-between gap-3'>
                  <div>
                    <p className='text-sm text-muted-foreground'>Total pipelines</p>
                    <p className='mt-1.5 font-mono text-4xl font-semibold tracking-tight sm:text-5xl'>
                      {(summary?.pipelines?.total ?? 0).toLocaleString()}
                    </p>
                  </div>
                  <ArrowUpRight className='h-5 w-5 text-muted-foreground transition-transform group-hover:translate-x-0.5 group-hover:-translate-y-0.5' />
                </div>
                <p className='max-w-[26ch] text-sm leading-6 text-muted-foreground'>
                  Open the workspace to inspect jobs and state changes.
                </p>
              </div>

              <div className='dashboard-inset dashboard-inset-strong grid grid-cols-2 gap-3 rounded-[18px] p-3'>
                <div>
                  <p className='text-xs text-muted-foreground'>Active</p>
                  <p className='mt-1.5 font-mono text-2xl font-semibold text-emerald-300'>
                    {(summary?.pipelines?.START ?? 0).toLocaleString()}
                  </p>
                </div>
                <div>
                  <p className='text-xs text-muted-foreground'>Paused</p>
                  <p className='mt-1.5 font-mono text-2xl font-semibold text-amber-300'>
                    {(summary?.pipelines?.PAUSE ?? 0).toLocaleString()}
                  </p>
                </div>
              </div>
            </DashboardPanel>

            <DashboardPanel
              title='Data velocity'
              description='Fresh ingestion volume versus yesterday.'
              headerSlot={
                <div className='dashboard-chip rounded-full p-2.5'>
                  <Activity className='h-4 w-4 text-muted-foreground' />
                </div>
              }
              className='min-h-[182px] xl:col-span-3'
              contentClassName='gap-3'
              href='/pipelines'
              interactive
              variant='dense'
            >
              <div className='space-y-1.5'>
                <div className='flex items-end justify-between gap-3'>
                  <div>
                    <p className='text-sm text-muted-foreground'>Rows today</p>
                    <p className='mt-1.5 font-mono text-4xl font-semibold tracking-tight sm:text-5xl'>
                      {flowToday.toLocaleString()}
                    </p>
                  </div>
                  <ArrowUpRight className='h-5 w-5 text-muted-foreground transition-transform group-hover:translate-x-0.5 group-hover:-translate-y-0.5' />
                </div>
                <p className='max-w-[26ch] text-sm leading-6 text-muted-foreground'>
                  Track how much fresh data is processing right now.
                </p>
              </div>

              <div className='dashboard-inset dashboard-inset-strong flex items-center justify-between rounded-[18px] px-4 py-2.5'>
                <div className='flex items-center gap-2'>
                  {flowTrend > 0 ? (
                    <TrendingUp className='h-4 w-4 text-emerald-300' />
                  ) : flowTrend < 0 ? (
                    <TrendingDown className='h-4 w-4 text-rose-300' />
                  ) : (
                    <Minus className='h-4 w-4 text-muted-foreground' />
                  )}
                  <span
                    className={cn(
                      'font-mono text-lg font-semibold',
                      flowTrend > 0
                        ? 'text-emerald-300'
                        : flowTrend < 0
                          ? 'text-rose-300'
                          : 'text-muted-foreground'
                    )}
                  >
                    {Math.abs(flowTrend).toFixed(1)}%
                  </span>
                </div>
                <span className='text-xs text-muted-foreground'>
                  Yesterday {compactNumberFormatter.format(flowYesterday)}
                </span>
              </div>
            </DashboardPanel>

            <DashboardPanel
              title='Estimated cost'
              description='Current month run rate.'
              headerSlot={
                <div className='dashboard-chip rounded-full p-2.5'>
                  <CreditCard className='h-4 w-4 text-muted-foreground' />
                </div>
              }
              className='min-h-[182px] xl:col-span-3'
              contentClassName='gap-3'
              href='/destinations'
              interactive
              variant='dense'
            >
              <div className='space-y-1.5'>
                <div className='flex items-end justify-between gap-3'>
                  <div>
                    <p className='text-sm text-muted-foreground'>
                      Projected run rate
                    </p>
                    <p className='mt-1.5 font-mono text-4xl font-semibold tracking-tight sm:text-[3.2rem]'>
                      {formatCurrency(summary?.credits?.month_total ?? 0)}
                    </p>
                  </div>
                  <ArrowUpRight className='h-5 w-5 text-muted-foreground transition-transform group-hover:translate-x-0.5 group-hover:-translate-y-0.5' />
                </div>
                <p className='max-w-[28ch] text-sm leading-6 text-muted-foreground'>
                  Review destinations and adjust workload before spend rises.
                </p>
              </div>

              <div className='dashboard-inset dashboard-inset-strong rounded-[18px] px-4 py-2.5'>
                <p className='text-xs text-muted-foreground'>
                  Based on credits consumed this month so far.
                </p>
              </div>
            </DashboardPanel>

            <DashboardPanel
              title='Data flow volume'
              description='Transaction history over the last 14 days.'
              headerSlot={
                <div className='dashboard-chip rounded-full px-3 py-2 text-xs text-muted-foreground'>
                  14 days
                </div>
              }
              className='min-h-[460px] xl:col-span-8'
              contentClassName='gap-4'
              variant='dense'
            >
              <div className='flex flex-col gap-3 xl:flex-row xl:items-end xl:justify-between'>
                <div>
                  <p className='text-sm text-muted-foreground'>
                    Rows processed today
                  </p>
                  <p className='mt-2 font-mono text-4xl font-semibold tracking-tight sm:text-5xl'>
                    {(flowChart?.total_today ?? flowToday).toLocaleString()}
                  </p>
                </div>

                <div className='dashboard-inset dashboard-inset-strong flex items-center justify-between gap-3 rounded-[20px] px-4 py-3 xl:min-w-[260px]'>
                  <div>
                    <p className='text-xs text-muted-foreground'>
                      Compared with yesterday
                    </p>
                    <p className='mt-1 text-sm text-foreground'>
                      {(flowChart?.total_yesterday ?? flowYesterday).toLocaleString()} rows
                    </p>
                  </div>
                  <div
                    className={cn(
                      'font-mono text-lg font-semibold',
                      flowTrend > 0
                        ? 'text-emerald-300'
                        : flowTrend < 0
                          ? 'text-rose-300'
                          : 'text-muted-foreground'
                    )}
                  >
                    {Math.abs(flowTrend).toFixed(1)}%
                  </div>
                </div>
              </div>

              <div className='dashboard-inset dashboard-inset-strong rounded-[24px] p-4 sm:p-5'>
                {!isFlowChartLoading &&
                (!flowChart?.history || flowChart.history.length === 0) ? (
                  <div className='flex min-h-[320px] flex-col items-center justify-center gap-2 text-center'>
                    <Activity className='h-8 w-8 text-muted-foreground/60' />
                    <div className='space-y-1'>
                      <p className='text-sm font-medium text-foreground'>
                        Flow history is still empty
                      </p>
                      <p className='text-sm text-muted-foreground'>
                        Once pipelines begin processing records, the 14-day trend
                        will appear here.
                      </p>
                    </div>
                  </div>
                ) : (
                  <div className='h-[320px] w-full sm:h-[340px]'>
                    <ResponsiveContainer width='100%' height='100%'>
                      <AreaChart
                        data={flowChart?.history || []}
                        margin={{ top: 8, right: 8, left: -12, bottom: 0 }}
                      >
                        <CartesianGrid
                          stroke='var(--dashboard-grid)'
                          vertical={false}
                          strokeDasharray='3 3'
                        />

                        <defs>
                          {flowChart?.pipelines?.map((pipeline, index) => {
                            const color = getDashboardSeriesColor(index)

                            return (
                              <linearGradient
                                key={pipeline}
                                id={`dashboard-flow-series-${index}`}
                                x1='0'
                                y1='0'
                                x2='0'
                                y2='1'
                              >
                                <stop
                                  offset='5%'
                                  stopColor={color}
                                  stopOpacity={0.48}
                                />
                                <stop
                                  offset='95%'
                                  stopColor={color}
                                  stopOpacity={0}
                                />
                              </linearGradient>
                            )
                          })}
                        </defs>

                        <XAxis
                          dataKey='date'
                          stroke='var(--muted-foreground)'
                          fontSize={12}
                          tickLine={false}
                          axisLine={false}
                          tickMargin={12}
                          tickFormatter={(value) => {
                            const date = new Date(value)
                            return `${date.getMonth() + 1}/${date.getDate()}`
                          }}
                        />
                        <YAxis
                          stroke='var(--muted-foreground)'
                          fontSize={12}
                          tickLine={false}
                          axisLine={false}
                          width={46}
                          tickFormatter={(value) =>
                            compactNumberFormatter.format(Number(value))
                          }
                        />
                        <Tooltip
                          contentStyle={dashboardTooltipStyle}
                          formatter={(value) => [
                            Number(value ?? 0).toLocaleString(),
                            'Rows',
                          ]}
                          labelFormatter={(label) =>
                            format(new Date(label), 'MMM d, yyyy')
                          }
                        />
                        <Legend
                          iconType='circle'
                          verticalAlign='bottom'
                          wrapperStyle={{ paddingTop: 16, fontSize: '12px' }}
                          formatter={(value) => (
                            <span className='text-xs text-muted-foreground'>
                              {value}
                            </span>
                          )}
                        />

                        {flowChart?.pipelines?.map((pipeline, index) => {
                          const color = getDashboardSeriesColor(index)

                          return (
                            <Area
                              key={pipeline}
                              type='monotone'
                              dataKey={pipeline}
                              stackId='1'
                              stroke={color}
                              strokeWidth={2}
                              fill={`url(#dashboard-flow-series-${index})`}
                              activeDot={{ r: 4 }}
                            />
                          )
                        })}
                      </AreaChart>
                    </ResponsiveContainer>
                  </div>
                )}
              </div>
            </DashboardPanel>

            <TopTablesChart className='min-h-[460px] xl:col-span-4' />

            <div className='xl:col-span-8'>
              <ActivityFeed />
            </div>

            <div className='xl:col-span-4'>
              <BackfillStatsCard data={summary?.backfills} />
            </div>
          </DashboardGrid>

          <section className='space-y-4'>
            <div className='flex flex-col gap-2 md:flex-row md:items-end md:justify-between'>
              <div className='space-y-2'>
                <div className='dashboard-chip w-fit rounded-full px-3.5 py-1.5 text-[11px] font-medium uppercase tracking-[0.24em] text-muted-foreground'>
                  Infrastructure
                </div>
                <div>
                  <h2 className='font-manrope text-2xl font-semibold tracking-tight text-foreground'>
                    Platform signals
                  </h2>
                  <p className='text-sm text-muted-foreground'>
                    Health panels for compute, workers, schedulers, and replication.
                  </p>
                </div>
              </div>
            </div>

            <div className='grid items-start gap-4 lg:grid-cols-2 xl:grid-cols-12 xl:gap-5'>
              <div className='xl:col-span-4'>
                <SystemLoadCard />
              </div>
              <div className='xl:col-span-4'>
                <SystemHealthWidget />
              </div>
              <div className='xl:col-span-4'>
                <WorkerStatusCard />
              </div>
              <div className='xl:col-span-7'>
                <JobStatusCard />
              </div>
              <div className='xl:col-span-5'>
                <WALMonitorList />
              </div>
            </div>
          </section>
        </div>
      </Main>
    </>
  )
}

export function Dashboard() {
  return (
    <RefreshIntervalProvider>
      <DashboardContent />
    </RefreshIntervalProvider>
  )
}
