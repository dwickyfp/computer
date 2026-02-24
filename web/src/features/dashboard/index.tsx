import { Header } from '@/components/layout/header'
import { Main } from '@/components/layout/main'
import { Search } from '@/components/search'
import { ThemeSwitch } from '@/components/theme-switch'
import { WALMonitorList } from './components/wal-monitor-list'
import { SystemLoadCard } from './components/system-load-card'
import { SystemHealthWidget } from './components/system-health-widget'
import { JobStatusCard } from './components/job-status-card'
import { WorkerStatusCard } from './components/worker-status-card'
import { SourceHealthCard } from './components/source-health-card'
import { TopTablesChart } from './components/top-tables-chart'
import { ActivityFeed } from './components/activity-feed'
import { BackfillStatsCard } from './components/backfill-stats-card'
import { useQuery } from '@tanstack/react-query'
import { dashboardRepo } from '@/repo/dashboard'
import {
  Activity,
  CreditCard,
  Minus,
  Server,
  TrendingDown,
  TrendingUp,
  RefreshCw,
} from 'lucide-react'
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
import { cn } from '@/lib/utils'
import { DashboardGrid } from './components/dashboard-grid'
import { DashboardPanel } from './components/dashboard-panel'
import { useState, useEffect } from 'react'
import { format } from 'date-fns'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { RefreshIntervalProvider, useRefreshInterval } from './context/refresh-interval-context'

const REFRESH_INTERVALS = [
  { label: 'Auto', value: 5000 },
  { label: '10s', value: 10000 },
  { label: '15s', value: 15000 },
  { label: '30s', value: 30000 },
  { label: '60s', value: 60000 },
] as const

function DashboardContent() {
  const [lastRefresh, setLastRefresh] = useState<Date>(new Date())
  const [, setTick] = useState(0)
  const { refreshInterval, setRefreshInterval } = useRefreshInterval()

  const { data: summary, dataUpdatedAt: summaryUpdatedAt } = useQuery({
    queryKey: ['dashboard', 'summary'],
    queryFn: dashboardRepo.getSummary,
    refetchInterval: refreshInterval,
  })

  // Keep existing queries for consistency
  const { data: flowChart } = useQuery({
    queryKey: ['dashboard', 'flow-chart'],
    queryFn: () => dashboardRepo.getFlowChart(14),
    refetchInterval: refreshInterval,
  })

  // Update last refresh timestamp when data changes
  useEffect(() => {
    if (summaryUpdatedAt) {
      setLastRefresh(new Date(summaryUpdatedAt))
    }
  }, [summaryUpdatedAt])

  // Update the time display every second
  useEffect(() => {
    const interval = setInterval(() => {
      setTick((prev) => prev + 1)
    }, 1000)
    return () => clearInterval(interval)
  }, [])

  // Calculate trends
  const flowTrend =
    summary?.data_flow && summary.data_flow.yesterday > 0
      ? ((summary.data_flow.today - summary.data_flow.yesterday) /
        summary.data_flow.yesterday) *
      100
      : 0

  return (
    <>
      <Header>
        <Search />
        <div className='ms-auto flex items-center space-x-4'>
          <ThemeSwitch />
          
        </div>
      </Header>

      <Main className="bg-background min-h-screen p-4 space-y-4">
        <div className='flex items-center justify-between'>
          <h1 className='text-3xl font-bold tracking-tight'>
            Mission Control
          </h1>
          <div className="flex flex-col items-end gap-1">
            <div className="text-sm text-muted-foreground">
              {new Date().toLocaleDateString('en-US', { weekday: 'long', year: 'numeric', month: 'long', day: 'numeric' })}
            </div>
            <div className="flex items-center gap-2 text-xs text-muted-foreground">
              <div className="flex items-center gap-1.5">
                <RefreshCw className="h-3 w-3 animate-spin-slow" />
                <span>Last updated at {format(lastRefresh, 'HH:mm:ss')}</span>
              </div>
              <Select
                value={refreshInterval.toString()}
                onValueChange={(value) => setRefreshInterval(Number(value))}
              >
                <SelectTrigger className="h-7 w-[110px] text-xs">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {REFRESH_INTERVALS.map((interval) => (
                    <SelectItem
                      key={interval.value}
                      value={interval.value.toString()}
                      className="text-xs"
                    >
                      {interval.label}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </div>
        </div>

        <DashboardGrid>
          {/* Row 1: High Level Stats - 4 cols each (6 cols in 24-grid) */}
          <div className="col-span-24 md:col-span-12 lg:col-span-6 h-32">
            <SourceHealthCard />
          </div>

          <DashboardPanel
            title="Pipeline Health"
            headerAction={<Server className='h-4 w-4 text-muted-foreground' />}
            className="col-span-24 md:col-span-12 lg:col-span-6 h-32"
          >
            <div className='flex items-end gap-2'>
              <div className='text-3xl font-bold font-mono leading-none'>
                {summary?.pipelines?.total || 0}
              </div>
              <div className='text-sm text-muted-foreground mb-1'>Total Pipelines</div>
            </div>
            <div className='mt-4 flex text-xs text-muted-foreground'>
              <div className='flex items-center mr-4'>
                <div className='w-2 h-2 rounded-full bg-emerald-500 mr-2' />
                <span className='font-medium text-foreground'>{summary?.pipelines?.START || 0}</span>
                <span className='ml-1'>Active</span>
              </div>
              <div className='flex items-center'>
                <div className='w-2 h-2 rounded-full bg-amber-500 mr-2' />
                <span className='font-medium text-foreground'>{summary?.pipelines?.PAUSE || 0}</span>
                <span className='ml-1'>Paused</span>
              </div>
            </div>
          </DashboardPanel>

          <DashboardPanel
            title="Data Velocity"
            headerAction={<Activity className='h-4 w-4 text-muted-foreground' />}
            className="col-span-24 md:col-span-12 lg:col-span-6 h-32"
          >
            <div className='flex items-end gap-2'>
              <div className='text-3xl font-bold font-mono leading-none'>
                {summary?.data_flow?.today.toLocaleString() || 0}
              </div>
              <div className='text-xs text-muted-foreground mb-1'>Rows today</div>
            </div>
            <div className='mt-4 flex items-center text-xs'>
              {flowTrend > 0 ? (
                <TrendingUp className='mr-1 h-3 w-3 text-emerald-500' />
              ) : flowTrend < 0 ? (
                <TrendingDown className='mr-1 h-3 w-3 text-rose-500' />
              ) : (
                <Minus className='mr-1 h-3 w-3 text-muted-foreground' />
              )}
              <span
                className={cn(
                  flowTrend > 0 ? 'text-emerald-500' : flowTrend < 0 ? 'text-rose-500' : 'text-muted-foreground',
                  'font-medium ml-1'
                )}
              >
                {Math.abs(flowTrend).toFixed(1)}%
              </span>
              <span className='ml-1 text-muted-foreground'>vs yesterday</span>
            </div>
          </DashboardPanel>

          <DashboardPanel
            title="Est. Cost"
            description="Run rate this month"
            headerAction={<CreditCard className='h-4 w-4 text-muted-foreground' />}
            className="col-span-24 md:col-span-12 lg:col-span-6 h-32"
          >
            <div className='text-3xl font-bold font-mono'>
              ${summary?.credits?.month_total.toFixed(8) || '0.00000000'}
            </div>
          </DashboardPanel>

          {/* Row 2: Main Top Charts */}
          <DashboardPanel
            title="Data Flow Volume"
            description="Transaction volume history (14 Days)"
            className='col-span-24 lg:col-span-16 h-[400px]'
          >
            <div className='h-full w-full min-h-[300px]'>
              <ResponsiveContainer width='100%' height="100%">
                <AreaChart
                  data={flowChart?.history || []}
                  margin={{ top: 10, right: 10, left: 0, bottom: 0 }}
                >
                  <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="rgba(255,255,255,0.05)" />
                  <defs>
                    <linearGradient id="colorGradient" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="5%" stopColor="#8884d8" stopOpacity={0.8} />
                      <stop offset="95%" stopColor="#8884d8" stopOpacity={0} />
                    </linearGradient>
                  </defs>
                  <XAxis
                    dataKey='date'
                    stroke='#888888'
                    fontSize={12}
                    tickLine={false}
                    axisLine={false}
                    tickFormatter={(value) => {
                      const date = new Date(value)
                      return `${date.getMonth() + 1}/${date.getDate()}`
                    }}
                  />
                  <YAxis
                    stroke='#888888'
                    fontSize={12}
                    tickLine={false}
                    axisLine={false}
                    tickFormatter={(value) => `${value}`}
                  />
                  <Tooltip
                    contentStyle={{
                      backgroundColor: 'rgba(23, 23, 23, 0.95)',
                      border: '1px solid rgba(255,255,255,0.1)',
                      borderRadius: '4px',
                      color: '#fff',
                      boxShadow: '0 4px 12px rgba(0,0,0,0.5)',
                      fontSize: '12px'
                    }}
                  />
                  <Legend iconType='circle' />
                  {flowChart?.pipelines?.map((pipeline, index) => {
                    const colors = ["#8884d8", "#82ca9d", "#ffc658", "#ff7300", "#0088fe", "#00C49F"];
                    const color = colors[index % colors.length];
                    return (
                      <Area
                        key={pipeline}
                        type='monotone'
                        dataKey={pipeline}
                        stackId="1"
                        stroke={color}
                        fill={color}
                        fillOpacity={0.6}
                      />
                    )
                  })}
                </AreaChart>
              </ResponsiveContainer>
            </div>
          </DashboardPanel>

          <div className="col-span-24 lg:col-span-8">
            <TopTablesChart />
          </div>

          {/* Row 3: Secondary Charts & Feed */}
          <div className="col-span-24 lg:col-span-16">
            <ActivityFeed />
          </div>
          <div className="col-span-24 lg:col-span-8">
            <BackfillStatsCard data={summary?.backfills} />
          </div>

          {/* Row 4: System Monitor Status (Bottom Row) */}
          <div className="col-span-24 md:col-span-12 lg:col-span-6 h-[280px]">
            <SystemLoadCard />
          </div>
          <div className="col-span-24 md:col-span-12 lg:col-span-6 h-[280px]">
            <SystemHealthWidget />
          </div>
          <div className="col-span-24 md:col-span-12 lg:col-span-6 h-[280px]">
            <WorkerStatusCard />
          </div>
          <div className="col-span-24 md:col-span-12 lg:col-span-6 h-[280px]">
            <JobStatusCard />
          </div>
          <div className="col-span-24 md:col-span-12 lg:col-span-6 h-[280px]">
            <WALMonitorList />
          </div>

        </DashboardGrid>
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
