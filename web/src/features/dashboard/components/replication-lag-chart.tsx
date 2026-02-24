import { useQuery } from '@tanstack/react-query'
import { dashboardRepo } from '@/repo/dashboard'
import { DashboardPanel } from './dashboard-panel'
import { Area, AreaChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts'
import { formatBytes } from '@/lib/utils'

export function ReplicationLagChart() {
  const { data: lagData } = useQuery({
    queryKey: ['dashboard', 'replication-lag'],
    queryFn: () => dashboardRepo.getReplicationLag(1), // 24 hours
    refetchInterval: 5000,
  })

  // Pre-defined colors for different sources
  const colors = ["#8884d8", "#82ca9d", "#ffc658", "#ff7300", "#0088fe", "#00C49F"]

  return (
    <DashboardPanel
      title="Replication Lag"
      description="WAL Size lag in bytes over the last 24 hours."
      className="col-span-4 h-[400px]"
    >
      <div className="h-full w-full min-h-[300px]">
        <ResponsiveContainer width="100%" height="100%">
          <AreaChart
            data={lagData?.history || []}
            margin={{ top: 10, right: 10, left: 0, bottom: 0 }}
          >
            <defs>
              {lagData?.sources?.map((source, index) => (
                <linearGradient key={source} id={`color${index}`} x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor={colors[index % colors.length]} stopOpacity={0.8} />
                  <stop offset="95%" stopColor={colors[index % colors.length]} stopOpacity={0} />
                </linearGradient>
              ))}
            </defs>
            <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="rgba(255,255,255,0.05)" />
            <XAxis
              dataKey="date"
              stroke="#888888"
              fontSize={12}
              tickLine={false}
              axisLine={false}
              tickFormatter={(value) => {
                const date = new Date(value)
                return `${date.getHours()}:00`
              }}
            />
            <YAxis
              stroke="#888888"
              fontSize={12}
              tickLine={false}
              axisLine={false}
              tickFormatter={(value) => formatBytes(value, 0)}
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
              formatter={(value: number | undefined) => [formatBytes(value || 0), 'Lag']}
              labelFormatter={(label) => new Date(label).toLocaleString()}
            />
            {lagData?.sources?.map((source, index) => (
              <Area
                key={source}
                type="monotone"
                dataKey={source}
                stroke={colors[index % colors.length]}
                fillOpacity={1}
                fill={`url(#color${index})`}
              />
            ))}
          </AreaChart>
        </ResponsiveContainer>
      </div>
    </DashboardPanel>
  )
}
