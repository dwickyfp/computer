import { useQuery } from '@tanstack/react-query'
import { dashboardRepo } from '@/repo/dashboard'
import { DashboardPanel } from './dashboard-panel'
import { Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts'
import { useRefreshInterval } from '../context/refresh-interval-context'

export function TopTablesChart() {
  const { refreshInterval } = useRefreshInterval()
  const { data: topTables } = useQuery({
    queryKey: ['dashboard', 'top-tables'],
    queryFn: () => dashboardRepo.getTopTables(5),
    refetchInterval: refreshInterval,
  })

  return (
    <DashboardPanel
      title="High Volume Tables"
      description="Top 5 tables by record volume today."
      className="col-span-3 h-[400px]"
    >
      <div className="h-full w-full min-h-[300px]">
        <ResponsiveContainer width="100%" height="100%" minWidth={1} minHeight={1}>
          <BarChart
            layout="vertical"
            data={topTables || []}
            margin={{ top: 10, right: 30, left: 40, bottom: 0 }}
          >
            <CartesianGrid strokeDasharray="3 3" horizontal={false} stroke="rgba(255,255,255,0.05)" />
            <XAxis type="number" hide />
            <YAxis
              dataKey="table_name"
              type="category"
              width={100}
              tick={{ fill: '#888888', fontSize: 11, fontFamily: 'monospace' }}
              axisLine={false}
              tickLine={false}
            />
            <Tooltip
              cursor={{ fill: 'rgba(255,255,255,0.05)' }}
              contentStyle={{
                backgroundColor: 'rgba(23, 23, 23, 0.95)',
                border: '1px solid rgba(255,255,255,0.1)',
                borderRadius: '4px',
                color: '#fff',
                boxShadow: '0 4px 12px rgba(0,0,0,0.5)',
                fontSize: '12px'
              }}
              formatter={(value: number | undefined) => [(value || 0).toLocaleString(), 'Records']}
            />
            <Bar
              dataKey="record_count"
              fill="#3b82f6"
              radius={[0, 2, 2, 0]}
              barSize={24}
              background={{ fill: 'rgba(255,255,255,0.02)' }}
            />
          </BarChart>
        </ResponsiveContainer>
      </div>
    </DashboardPanel>
  )
}
