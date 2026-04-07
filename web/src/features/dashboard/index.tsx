import { useQuery } from '@tanstack/react-query'
import { format } from 'date-fns'
import {
  Activity,
  Calendar,
  CreditCard,
  DatabaseBackup,
  Loader2,
  RefreshCw,
  Server,
  TrendingDown,
  TrendingUp,
} from 'lucide-react'
import {
  Area,
  AreaChart,
  CartesianGrid,
  Legend,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import { Header } from '@/components/layout/header'
import { Main } from '@/components/layout/main'
import { Search } from '@/components/search'
import { ThemeSwitch } from '@/components/theme-switch'
import {
  CustomTabs,
  CustomTabsContent,
  CustomTabsList,
  CustomTabsTrigger,
} from '@/components/ui/custom-tabs'
import { useSidebar } from '@/components/ui/sidebar'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { cn } from '@/lib/utils'
import { dashboardRepo } from '@/repo/dashboard'
import { ActivityFeed } from './components/activity-feed'
import { BackfillStatsCard } from './components/backfill-stats-card'
import {
  dashboardTooltipStyle,
  getDashboardSeriesColor,
} from './components/chart-theme'
import { DashboardHeroPanel } from './components/dashboard-hero-panel'
import { DashboardMetricCard } from './components/dashboard-metric-card'
import { DashboardPanel } from './components/dashboard-panel'
import { DashboardResponsiveChart } from './components/dashboard-responsive-chart'
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
import { getDashboardPollingQueryOptions } from './query-defaults'
import { useEffect, useRef, useState } from 'react'

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

type DashboardSection = 'dashboard' | 'analytics' | 'operations' | 'runtime'

function formatCurrency(value: number) {
  return new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    minimumFractionDigits: value < 1 ? 4 : 2,
    maximumFractionDigits: value < 1 ? 4 : 2,
  }).format(value)
}

function getTrendMeta(flowToday: number, flowYesterday: number) {
  if (flowYesterday <= 0) {
    return {
      icon: null,
      label: 'Fresh baseline',
      tone: 'neutral' as const,
    }
  }

  const flowTrend = ((flowToday - flowYesterday) / flowYesterday) * 100

  if (flowTrend > 0) {
    return {
      icon: <TrendingUp className='h-4 w-4 text-emerald-700 dark:text-emerald-300' />,
      label: `+${Math.abs(flowTrend).toFixed(1)}% vs yesterday`,
      tone: 'positive' as const,
    }
  }

  if (flowTrend < 0) {
    return {
      icon: <TrendingDown className='h-4 w-4 text-rose-700 dark:text-rose-300' />,
      label: `-${Math.abs(flowTrend).toFixed(1)}% vs yesterday`,
      tone: 'negative' as const,
    }
  }

  return {
    icon: null,
    label: 'No change vs yesterday',
    tone: 'neutral' as const,
  }
}

function DashboardContent() {
  const { refreshInterval, setRefreshInterval } = useRefreshInterval()
  const { isTransitioning: isSidebarTransitioning } = useSidebar()
  const [activeSection, setActiveSection] =
    useState<DashboardSection>('dashboard')
  const [isHeaderScrolled, setIsHeaderScrolled] = useState(false)
  const [isTabsStuck, setIsTabsStuck] = useState(false)
  const headerSentinelRef = useRef<HTMLDivElement | null>(null)
  const tabsSentinelRef = useRef<HTMLDivElement | null>(null)

  const {
    data: summary,
    dataUpdatedAt: summaryUpdatedAt,
    isFetching: isSummaryFetching,
  } = useQuery({
    queryKey: ['dashboard', 'summary'],
    queryFn: dashboardRepo.getSummary,
    ...getDashboardPollingQueryOptions(refreshInterval),
    notifyOnChangeProps: ['data', 'dataUpdatedAt', 'isFetching'],
  })

  const { data: flowChart, isLoading: isFlowChartLoading } = useQuery({
    queryKey: ['dashboard', 'flow-chart'],
    queryFn: () => dashboardRepo.getFlowChart(14),
    enabled: activeSection === 'analytics',
    ...getDashboardPollingQueryOptions(refreshInterval),
    notifyOnChangeProps: ['data', 'isLoading'],
  })

  const flowToday = summary?.data_flow?.today ?? 0
  const flowYesterday = summary?.data_flow?.yesterday ?? 0
  const formattedDate = format(new Date(), 'EEEE, MMMM d, yyyy')
  const lastUpdatedText = summaryUpdatedAt
    ? format(new Date(summaryUpdatedAt), 'HH:mm:ss')
    : 'Waiting for first sync'
  const activePipelines = summary?.pipelines?.START ?? 0
  const pausedPipelines = summary?.pipelines?.PAUSE ?? 0
  const activeBackfills =
    (summary?.backfills?.PENDING ?? 0) + (summary?.backfills?.EXECUTING ?? 0)
  const trendMeta = getTrendMeta(flowToday, flowYesterday)

  useEffect(() => {
    const sentinel = headerSentinelRef.current

    if (!sentinel || typeof IntersectionObserver === 'undefined') {
      return
    }

    const observer = new IntersectionObserver(
      ([entry]) => {
        setIsHeaderScrolled(!entry.isIntersecting)
      },
      {
        rootMargin: '-64px 0px 0px 0px',
        threshold: 0,
      }
    )

    observer.observe(sentinel)

    return () => observer.disconnect()
  }, [])

  useEffect(() => {
    const sentinel = tabsSentinelRef.current

    if (!sentinel || typeof IntersectionObserver === 'undefined') {
      return
    }

    const observer = new IntersectionObserver(
      ([entry]) => {
        setIsTabsStuck(!entry.isIntersecting)
      },
      {
        rootMargin: '-64px 0px 0px 0px',
        threshold: 0,
      }
    )

    observer.observe(sentinel)

    return () => observer.disconnect()
  }, [])

  return (
    <>
      <Header
        fixed
        trackScroll={false}
        scrolled={isHeaderScrolled}
        data-scrolled={isHeaderScrolled ? 'true' : 'false'}
        data-sidebar-transitioning={isSidebarTransitioning ? 'true' : 'false'}
        className='dashboard-header'
      >
        <Search />
        <div className='ms-auto flex items-center gap-3'>
          <ThemeSwitch />
        </div>
      </Header>

      <Main
        data-sidebar-transitioning={isSidebarTransitioning ? 'true' : 'false'}
        className='dashboard-shell min-h-screen px-4 py-4 sm:px-6 sm:py-6'
      >
        <div
          ref={headerSentinelRef}
          className='dashboard-header-sentinel'
          aria-hidden='true'
        />
        <div className='relative z-10 mx-auto flex w-full max-w-[1600px] flex-col gap-6 lg:gap-7'>
          <section className='flex flex-col gap-5 xl:flex-row xl:items-end xl:justify-between'>
            <div className='space-y-4'>
              <div className='dashboard-chip dashboard-text-muted w-fit rounded-full px-3.5 py-1.5 text-[11px] font-medium uppercase tracking-[0.24em]'>
                Dashboard
              </div>
              <div className='space-y-3'>
                <h1 className='dashboard-text-strong font-manrope text-4xl font-semibold tracking-[-0.05em] sm:text-5xl'>
                  Operational cockpit
                </h1>
                <p className='dashboard-text max-w-2xl text-sm leading-7 sm:text-base'>
                  Monitor delivery throughput, service readiness, and spend from
                  one curated command surface designed for rapid scanning.
                </p>
              </div>
            </div>

            <div className='grid gap-3 sm:grid-cols-3 xl:min-w-[680px]'>
              <div className='dashboard-toolbar-chip rounded-[22px] px-4 py-3.5'>
                <div className='dashboard-text-muted flex items-center gap-2 text-xs'>
                  <Calendar className='h-3.5 w-3.5' />
                  Today
                </div>
                <p className='dashboard-text-strong mt-2 text-sm font-medium'>
                  {formattedDate}
                </p>
              </div>

              <div className='dashboard-toolbar-chip rounded-[22px] px-4 py-3.5'>
                <div className='dashboard-text-muted flex items-center gap-2 text-xs'>
                  {isSummaryFetching ? (
                    <Loader2 className='h-3.5 w-3.5 animate-spin' />
                  ) : (
                    <RefreshCw className='h-3.5 w-3.5' />
                  )}
                  {isSummaryFetching ? 'Refreshing' : 'Last updated'}
                </div>
                <p className='dashboard-text-strong mt-2 text-sm font-medium'>
                  {lastUpdatedText}
                </p>
              </div>

              <div className='dashboard-toolbar-chip rounded-[22px] px-4 py-3.5'>
                <div className='dashboard-text-muted text-xs'>Auto refresh</div>
                <Select
                  value={refreshInterval.toString()}
                  onValueChange={(value) => setRefreshInterval(Number(value))}
                >
                  <SelectTrigger className='dashboard-select-trigger mt-2 h-11 w-full rounded-xl px-3 text-left text-sm font-medium shadow-none focus:ring-0'>
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

          <CustomTabs
            value={activeSection}
            onValueChange={(value) => setActiveSection(value as DashboardSection)}
            className='w-full gap-5'
          >
            {/* Control rendering explicitly so inactive sections do not resize with the sidebar */}
            <div
              ref={tabsSentinelRef}
              className='dashboard-tabs-sentinel'
              aria-hidden='true'
            />
            <div
              className='dashboard-tabs-sticky'
              data-stuck={isTabsStuck ? 'true' : 'false'}
            >
              <div className='dashboard-tabs-frame no-scrollbar overflow-x-auto'>
                <CustomTabsList className='dashboard-tabs-list'>
                  <CustomTabsTrigger
                    value='dashboard'
                    className='dashboard-tabs-trigger'
                  >
                    Dashboard
                  </CustomTabsTrigger>
                  <CustomTabsTrigger
                    value='analytics'
                    className='dashboard-tabs-trigger'
                  >
                    Analytics
                  </CustomTabsTrigger>
                  <CustomTabsTrigger
                    value='operations'
                    className='dashboard-tabs-trigger'
                  >
                    Operations
                  </CustomTabsTrigger>
                  <CustomTabsTrigger
                    value='runtime'
                    className='dashboard-tabs-trigger'
                  >
                    Runtime
                  </CustomTabsTrigger>
                </CustomTabsList>
              </div>
            </div>

            {activeSection === 'dashboard' && (
              <CustomTabsContent
                value='dashboard'
                className='mt-3 space-y-5 lg:space-y-6'
              >
                <section className='grid gap-4 md:grid-cols-2 xl:grid-cols-4 xl:gap-5'>
                  <DashboardMetricCard
                    label='Pipelines'
                    value={(summary?.pipelines?.total ?? 0).toLocaleString()}
                    detail='Delivery routes currently tracked across the workspace.'
                    status={`${activePipelines} active · ${pausedPipelines} paused`}
                    tone={activePipelines > 0 ? 'positive' : 'neutral'}
                    icon={Server}
                    href='/pipelines'
                  />
                  <DashboardMetricCard
                    label='Rows today'
                    value={flowToday.toLocaleString()}
                    detail='Fresh ingestion throughput compared with the previous day.'
                    status={trendMeta.label}
                    tone={trendMeta.tone}
                    icon={Activity}
                    href='/pipelines'
                  />
                  <DashboardMetricCard
                    label='Projected run rate'
                    value={formatCurrency(summary?.credits?.month_total ?? 0)}
                    detail='Estimated credit consumption for the current month.'
                    status='Spend forecast'
                    tone='neutral'
                    icon={CreditCard}
                    href='/destinations'
                  />
                  <DashboardMetricCard
                    label='Active backfills'
                    value={activeBackfills.toLocaleString()}
                    detail='Pending and executing backfill operations across the platform.'
                    status={`${summary?.backfills?.total ?? 0} total jobs`}
                    tone={activeBackfills > 0 ? 'positive' : 'neutral'}
                    icon={DatabaseBackup}
                    href='/pipelines'
                  />
                </section>

                <section className='grid gap-5 xl:grid-cols-12'>
                  <DashboardHeroPanel
                    className='xl:col-span-7'
                    activeBackfills={activeBackfills}
                    activePipelines={activePipelines}
                    pausedPipelines={pausedPipelines}
                    projectedRunRate={formatCurrency(
                      summary?.credits?.month_total ?? 0
                    )}
                    rowsToday={flowToday}
                  />

                  <div className='grid gap-5 xl:col-span-5'>
                    <SourceHealthCard href='/sources' />
                    <SystemHealthWidget />
                  </div>
                </section>
              </CustomTabsContent>
            )}

            {activeSection === 'analytics' && (
              <CustomTabsContent
                value='analytics'
                className='mt-3 space-y-4'
              >
                <section className='space-y-4'>
                  <div className='space-y-2'>
                    <div className='dashboard-chip dashboard-text-muted w-fit rounded-full px-3.5 py-1.5 text-[11px] font-medium uppercase tracking-[0.24em]'>
                      Analytics
                    </div>
                    <div>
                      <h2 className='dashboard-text-strong font-manrope text-2xl font-semibold tracking-[-0.04em]'>
                        Throughput map
                      </h2>
                      <p className='dashboard-text text-sm'>
                        High-signal charts for flow volume and dominant tables.
                      </p>
                    </div>
                  </div>

                  <div className='grid gap-5 xl:grid-cols-12'>
                    <DashboardPanel
                      title='Data flow volume'
                      description='Rolling 14-day pipeline throughput.'
                      headerSlot={
                        <div className='dashboard-chip dashboard-text rounded-full px-3 py-2 text-xs'>
                          14 days
                        </div>
                      }
                      className='min-h-[520px] xl:col-span-8'
                      contentClassName='gap-5'
                      variant='dense'
                    >
                      <div className='flex flex-col gap-4 xl:flex-row xl:items-end xl:justify-between'>
                        <div className='space-y-2'>
                          <p className='dashboard-text text-sm'>
                            Rows processed today
                          </p>
                          <div className='flex flex-wrap items-end gap-3'>
                            <p className='dashboard-text-strong font-mono text-4xl font-semibold tracking-[-0.05em] sm:text-5xl'>
                              {(flowChart?.total_today ?? flowToday).toLocaleString()}
                            </p>
                            <div
                              className={cn(
                                'mb-1 flex items-center gap-2 rounded-full border px-3 py-1 text-xs font-medium',
                                trendMeta.tone === 'positive' &&
                                  'border-emerald-500/18 bg-emerald-500/10 text-emerald-700 dark:text-emerald-200',
                                trendMeta.tone === 'negative' &&
                                  'border-rose-500/18 bg-rose-500/10 text-rose-700 dark:text-rose-200',
                                trendMeta.tone === 'neutral' &&
                                  'dashboard-status-neutral'
                              )}
                            >
                              {trendMeta.icon}
                              {trendMeta.label}
                            </div>
                          </div>
                        </div>

                        <div className='dashboard-inset rounded-[20px] px-4 py-3 xl:min-w-[260px]'>
                          <p className='dashboard-text-muted text-[11px] uppercase tracking-[0.2em]'>
                            Yesterday
                          </p>
                          <p className='dashboard-text-strong mt-2 font-mono text-2xl font-semibold'>
                            {compactNumberFormatter.format(
                              flowChart?.total_yesterday ?? flowYesterday
                            )}
                          </p>
                          <p className='dashboard-text mt-2 text-sm'>
                            Baseline for current change detection.
                          </p>
                        </div>
                      </div>

                      <div className='dashboard-chart-well rounded-[24px] p-4 sm:p-5'>
                        {!isFlowChartLoading &&
                        (!flowChart?.history || flowChart.history.length === 0) ? (
                          <div className='flex min-h-[340px] flex-col items-center justify-center gap-2 text-center'>
                            <Activity className='h-8 w-8 text-slate-300/38' />
                            <div className='space-y-1'>
                              <p className='dashboard-text-strong text-sm font-medium'>
                                Flow history is still empty
                              </p>
                              <p className='dashboard-text text-sm'>
                                Once pipelines begin processing records, the 14-day
                                trend will appear here.
                              </p>
                            </div>
                          </div>
                        ) : (
                          <div className='h-[340px] w-full sm:h-[360px]'>
                            <DashboardResponsiveChart
                              className='h-full w-full'
                              freeze={isSidebarTransitioning}
                            >
                              {({ height, width }) => (
                                <AreaChart
                                  width={width}
                                  height={height}
                                  data={flowChart?.history || []}
                                  margin={{
                                    top: 10,
                                    right: 10,
                                    left: -18,
                                    bottom: 6,
                                  }}
                                >
                                  <CartesianGrid
                                    stroke='var(--dashboard-grid)'
                                    vertical={false}
                                    strokeDasharray='2 6'
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
                                            offset='0%'
                                            stopColor={color}
                                            stopOpacity={0.42}
                                          />
                                          <stop
                                            offset='88%'
                                            stopColor={color}
                                            stopOpacity={0}
                                          />
                                        </linearGradient>
                                      )
                                    })}
                                  </defs>

                                  <XAxis
                                    dataKey='date'
                                    tick={{
                                      fill: 'var(--dashboard-axis-label)',
                                      fontSize: 11,
                                    }}
                                    tickLine={false}
                                    axisLine={false}
                                    tickMargin={12}
                                    tickFormatter={(value) => {
                                      const date = new Date(value)
                                      return `${date.getMonth() + 1}/${date.getDate()}`
                                    }}
                                  />
                                  <YAxis
                                    tick={{
                                      fill: 'var(--dashboard-axis-label)',
                                      fontSize: 11,
                                    }}
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
                                    wrapperStyle={{
                                      paddingTop: 18,
                                      fontSize: '12px',
                                    }}
                                    formatter={(value) => (
                                      <span className='dashboard-text text-xs'>
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
                                        isAnimationActive={false}
                                        activeDot={{
                                          r: 4,
                                          fill: color,
                                          stroke: 'rgba(255,255,255,0.35)',
                                        }}
                                      />
                                    )
                                  })}
                                </AreaChart>
                              )}
                            </DashboardResponsiveChart>
                          </div>
                        )}
                      </div>
                    </DashboardPanel>

                    <TopTablesChart
                      className='min-h-[520px] xl:col-span-4'
                      freezeChart={isSidebarTransitioning}
                    />
                  </div>
                </section>
              </CustomTabsContent>
            )}

            {activeSection === 'operations' && (
              <CustomTabsContent
                value='operations'
                className='mt-3 space-y-4'
              >
                <section className='space-y-4'>
                  <div className='space-y-2'>
                    <div className='dashboard-chip dashboard-text-muted w-fit rounded-full px-3.5 py-1.5 text-[11px] font-medium uppercase tracking-[0.24em]'>
                      Operations
                    </div>
                    <div>
                      <h2 className='dashboard-text-strong font-manrope text-2xl font-semibold tracking-[-0.04em]'>
                        Active work and recent activity
                      </h2>
                      <p className='dashboard-text text-sm'>
                        Ongoing jobs and the latest operational signals.
                      </p>
                    </div>
                  </div>

                  <div className='grid gap-5 xl:grid-cols-12'>
                    <div className='xl:col-span-8'>
                      <ActivityFeed />
                    </div>
                    <div className='xl:col-span-4'>
                      <BackfillStatsCard data={summary?.backfills} />
                    </div>
                  </div>
                </section>
              </CustomTabsContent>
            )}

            {activeSection === 'runtime' && (
              <CustomTabsContent
                value='runtime'
                className='mt-3 space-y-4'
              >
                <section className='space-y-4'>
                  <div className='space-y-2'>
                    <div className='dashboard-chip dashboard-text-muted w-fit rounded-full px-3.5 py-1.5 text-[11px] font-medium uppercase tracking-[0.24em]'>
                      Runtime
                    </div>
                    <div>
                      <h2 className='dashboard-text-strong font-manrope text-2xl font-semibold tracking-[-0.04em]'>
                        Infrastructure detail
                      </h2>
                      <p className='dashboard-text text-sm'>
                        Lower-noise views into system load, workers, scheduler
                        health, and WAL replication.
                      </p>
                    </div>
                  </div>

                  <div className='grid gap-5 xl:grid-cols-12'>
                    <div className='xl:col-span-6'>
                      <SystemLoadCard />
                    </div>
                    <div className='xl:col-span-6'>
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
              </CustomTabsContent>
            )}
          </CustomTabs>
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
