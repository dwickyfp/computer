import { useQuery } from '@tanstack/react-query'
import { memo } from 'react'
import {
  Bar,
  BarChart,
  CartesianGrid,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import { DatabaseZap } from 'lucide-react'
import { dashboardRepo } from '@/repo/dashboard'
import { useRefreshInterval } from '../context/refresh-interval-context'
import { getDashboardPollingQueryOptions } from '../query-defaults'
import { dashboardTooltipStyle } from './chart-theme'
import { DashboardPanel } from './dashboard-panel'
import { DashboardResponsiveChart } from './dashboard-responsive-chart'

interface TopTablesChartProps {
  className?: string
  freezeChart?: boolean
}

export const TopTablesChart = memo(function TopTablesChart({
  className,
  freezeChart = false,
}: TopTablesChartProps) {
  const { refreshInterval } = useRefreshInterval()
  const { data: topTables, isLoading } = useQuery({
    queryKey: ['dashboard', 'top-tables'],
    queryFn: () => dashboardRepo.getTopTables(5),
    ...getDashboardPollingQueryOptions(refreshInterval),
    notifyOnChangeProps: ['data', 'isLoading'],
    select: (rows) => ({
      rows,
      totalRecords: rows.reduce((sum, table) => sum + table.record_count, 0),
      surfacedCount: rows.length,
    }),
  })

  const tableRows = topTables?.rows ?? []

  return (
    <DashboardPanel
      title='Top tables'
      description='Dominant tables by record volume captured today.'
      headerSlot={
        <div className='dashboard-chip dashboard-text rounded-full px-3 py-2 text-xs'>
          <DatabaseZap className='h-3.5 w-3.5 text-sky-600 dark:text-sky-300' />
          Volume focus
        </div>
      }
      className={className}
      contentClassName='gap-4'
      variant='dense'
    >
      <div className='grid gap-3 sm:grid-cols-2'>
        <div className='dashboard-inset rounded-[22px] px-4 py-3.5'>
          <p className='dashboard-text-muted text-xs uppercase tracking-[0.18em]'>
            Tables surfaced
          </p>
          <p className='dashboard-text-strong mt-2 font-mono text-3xl font-semibold tracking-tight'>
            {isLoading ? '...' : (topTables?.surfacedCount ?? 0).toLocaleString()}
          </p>
        </div>
        <div className='dashboard-inset rounded-[22px] px-4 py-3.5'>
          <p className='dashboard-text-muted text-xs uppercase tracking-[0.18em]'>
            Records represented
          </p>
          <p className='dashboard-text-strong mt-2 font-mono text-3xl font-semibold tracking-tight'>
            {isLoading ? '...' : (topTables?.totalRecords ?? 0).toLocaleString()}
          </p>
        </div>
      </div>

      <div className='dashboard-chart-well rounded-[24px] p-4 sm:p-5'>
        {!isLoading && tableRows.length === 0 ? (
          <div className='flex min-h-[320px] flex-col items-center justify-center gap-2 text-center'>
            <DatabaseZap className='h-8 w-8 text-slate-300/38' />
            <div className='space-y-1'>
              <p className='dashboard-text-strong text-sm font-medium'>
                No table volume yet
              </p>
              <p className='dashboard-text text-sm'>
                Today&apos;s ingestion volume will appear here once records are
                processed.
              </p>
            </div>
          </div>
        ) : (
          <div className='dashboard-chart-container h-[320px] w-full'>
            <DashboardResponsiveChart
              className='h-full w-full'
              freeze={freezeChart}
            >
              {({ height, width }) => (
                <BarChart
                  width={width}
                  height={height}
                  layout='vertical'
                  data={tableRows}
                  margin={{ top: 4, right: 12, left: 8, bottom: 4 }}
                >
                  <CartesianGrid
                    stroke='var(--dashboard-grid)'
                    horizontal={false}
                    strokeDasharray='2 6'
                  />
                  <XAxis type='number' hide />
                  <YAxis
                    dataKey='table_name'
                    type='category'
                    width={108}
                    tick={{ fill: 'var(--dashboard-axis-label)', fontSize: 11 }}
                    axisLine={false}
                    tickLine={false}
                  />
                  <Tooltip
                    cursor={{ fill: 'var(--dashboard-chart-cursor)' }}
                    contentStyle={dashboardTooltipStyle}
                    formatter={(value) => [
                      Number(value ?? 0).toLocaleString(),
                      'Records',
                    ]}
                  />
                  <Bar
                    dataKey='record_count'
                    fill='var(--color-chart-2)'
                    radius={[10, 10, 10, 10]}
                    barSize={22}
                    isAnimationActive={false}
                    background={{
                      fill: 'var(--dashboard-chart-track)',
                      radius: 10,
                    }}
                  />
                </BarChart>
              )}
            </DashboardResponsiveChart>
          </div>
        )}
      </div>
    </DashboardPanel>
  )
})
