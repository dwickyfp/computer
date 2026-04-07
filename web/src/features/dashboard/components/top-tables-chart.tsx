import { useQuery } from '@tanstack/react-query'
import {
  Bar,
  BarChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import { DatabaseZap } from 'lucide-react'
import { dashboardRepo } from '@/repo/dashboard'
import { useRefreshInterval } from '../context/refresh-interval-context'
import { dashboardTooltipStyle } from './chart-theme'
import { DashboardPanel } from './dashboard-panel'

interface TopTablesChartProps {
  className?: string
}

export function TopTablesChart({ className }: TopTablesChartProps) {
  const { refreshInterval } = useRefreshInterval()
  const { data: topTables, isLoading } = useQuery({
    queryKey: ['dashboard', 'top-tables'],
    queryFn: () => dashboardRepo.getTopTables(5),
    refetchInterval: refreshInterval,
  })

  const totalRecords = (topTables ?? []).reduce(
    (sum, table) => sum + table.record_count,
    0
  )

  return (
    <DashboardPanel
      title='High volume tables'
      description='Top tables by record volume captured today.'
      headerSlot={
        <div className='dashboard-chip rounded-full p-2.5'>
          <DatabaseZap className='h-4 w-4 text-muted-foreground' />
        </div>
      }
      className={className}
      contentClassName='gap-4'
      variant='dense'
    >
      <div className='grid gap-3 sm:grid-cols-2'>
        <div className='dashboard-inset rounded-[22px] px-4 py-3.5'>
          <p className='text-xs text-muted-foreground'>Tables surfaced</p>
          <p className='mt-2 font-mono text-3xl font-semibold tracking-tight'>
            {isLoading ? '...' : (topTables?.length ?? 0).toLocaleString()}
          </p>
        </div>
        <div className='dashboard-inset rounded-[22px] px-4 py-3.5'>
          <p className='text-xs text-muted-foreground'>Records represented</p>
          <p className='mt-2 font-mono text-3xl font-semibold tracking-tight'>
            {isLoading ? '...' : totalRecords.toLocaleString()}
          </p>
        </div>
      </div>

      <div className='dashboard-inset dashboard-inset-strong rounded-[24px] p-4 sm:p-5'>
        {!isLoading && (!topTables || topTables.length === 0) ? (
          <div className='flex min-h-[320px] flex-col items-center justify-center gap-2 text-center'>
            <DatabaseZap className='h-8 w-8 text-muted-foreground/60' />
            <div className='space-y-1'>
              <p className='text-sm font-medium text-foreground'>
                No table volume yet
              </p>
              <p className='text-sm text-muted-foreground'>
                Today&apos;s ingestion volume will appear here once records are
                processed.
              </p>
            </div>
          </div>
        ) : (
          <div className='h-[320px] w-full'>
            <ResponsiveContainer width='100%' height='100%' minWidth={1} minHeight={1}>
              <BarChart
                layout='vertical'
                data={topTables ?? []}
                margin={{ top: 4, right: 12, left: 8, bottom: 4 }}
              >
                <CartesianGrid
                  stroke='var(--dashboard-grid)'
                  horizontal={false}
                  strokeDasharray='3 3'
                />
                <XAxis type='number' hide />
                <YAxis
                  dataKey='table_name'
                  type='category'
                  width={108}
                  tick={{ fill: 'var(--muted-foreground)', fontSize: 11 }}
                  axisLine={false}
                  tickLine={false}
                />
                <Tooltip
                  cursor={{ fill: 'rgba(148, 163, 184, 0.08)' }}
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
                  background={{ fill: 'rgba(148, 163, 184, 0.08)', radius: 10 }}
                />
              </BarChart>
            </ResponsiveContainer>
          </div>
        )}
      </div>
    </DashboardPanel>
  )
}
