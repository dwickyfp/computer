import { type ColumnDef } from '@tanstack/react-table'
import { DataTableColumnHeader } from '@/components/data-table'
import { type SourceTableInfo } from '@/repo/sources'
import { type PipelineStats } from '@/repo/pipelines'
import { AreaChart, Area, ResponsiveContainer, XAxis, YAxis, CartesianGrid, Tooltip } from 'recharts'
import { useMemo, useState, useEffect } from 'react'

const formatYAxis = (num: number) => {
    if (num >= 1000000000) return (num / 1000000000).toFixed(1).replace(/\.0$/, '') + 'G'
    if (num >= 1000000) return (num / 1000000).toFixed(1).replace(/\.0$/, '') + 'M'
    if (num >= 1000) return (num / 1000).toFixed(1).replace(/\.0$/, '') + 'k'
    return num.toString()
}

const MonitoringSparkline = ({ stats }: { stats: PipelineStats | undefined }) => {
    const [now, setNow] = useState(new Date())

    useEffect(() => {
        const timer = setInterval(() => {
            setNow(new Date())
        }, 1000) // Update every second for smooth sliding

        return () => clearInterval(timer)
    }, [])

    const data = useMemo(() => {
        // Sliding window logic: Last 30 seconds in 5s buckets
        // Align to nearest 5s to reduce jitter? Or just raw now.
        // Let's use 5s buckets aligned.
        const bucketSize = 5000 // 5s
        const windowSize = 5 * 60 * 1000 // 5 min
        
        const endTime = Math.floor(now.getTime() / bucketSize) * bucketSize
        const startTime = endTime - windowSize
        
        // Initialize buckets
        const buckets: Record<number, number> = {}
        for (let t = startTime; t <= endTime; t += bucketSize) {
            buckets[t] = 0
        }
        
        // Fill with data
        if (stats && stats.recent_stats) {
            stats.recent_stats.forEach(s => {
                const ts = new Date(s.timestamp).getTime()
                // Find nearest bucket
                // Assuming data comes in ~5s intervals or we just sum/max inside bucket
                // For simplicity, snap to nearest bucket
                const bucketKey = Math.floor(ts / bucketSize) * bucketSize
                    if (bucketKey >= startTime && bucketKey <= endTime) {
                    // If multiple records fall in same bucket (rare if source sends aggregates), sum them?
                    // User code returns 'record_count'.
                    buckets[bucketKey] = (buckets[bucketKey] || 0) + s.count
                    }
            })
        }
        
        // Convert to array
        return Object.entries(buckets)
            .map(([ts, count]) => ({
                time: parseInt(ts),
                count: count,
                label: new Date(parseInt(ts)).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' })
            }))
            .sort((a, b) => a.time - b.time)

    }, [stats, now])

    return (
        <div className="flex h-full items-center justify-center">
            <div className="h-[40px] w-[200px]">
                <ResponsiveContainer width="100%" height="100%">
                    <AreaChart data={data}>
                        <defs>
                            <linearGradient id="colorCount" x1="0" y1="0" x2="0" y2="1">
                                <stop offset="5%" stopColor="#8884d8" stopOpacity={0.8}/>
                                <stop offset="95%" stopColor="#8884d8" stopOpacity={0}/>
                            </linearGradient>
                        </defs>
                        <CartesianGrid strokeDasharray="3 3" vertical={false} />
                        <XAxis 
                            dataKey="label" 
                            hide={true} 
                        />
                        <YAxis 
                            hide={false} 
                            tick={false}
                            width={10}
                            domain={[0, 'auto']}
                            allowDecimals={false}
                            tickFormatter={formatYAxis}
                        />
                        <Tooltip 
                            contentStyle={{ fontSize: '10px', padding: '2px 4px' }}
                            labelStyle={{ fontSize: '8px', color: '#666' }}
                        />
                        <Area 
                            type="monotone" 
                            dataKey="count" 
                            stroke="#8884d8" 
                            fillOpacity={1} 
                            fill="url(#colorCount)" 
                            isAnimationActive={false} // Disable animation for smoother continuous updates, or keep true if preferred
                        />
                    </AreaChart>
                </ResponsiveContainer>
            </div>
        </div>
    )
}

export const getPipelineDetailsTableColumns = (
    statsMap: Record<string, PipelineStats> = {}
): ColumnDef<SourceTableInfo>[] => {

    const columns: ColumnDef<SourceTableInfo>[] = [

        {
            accessorKey: 'table_name',
            header: ({ column }) => (
                <DataTableColumnHeader column={column} title='Table Name' />
            ),
            cell: ({ row }) => {
                const tableName = row.getValue('table_name') as string
                return (
                    <div className="flex h-full items-center px-2">
                        <span className='font-medium uppercase text-sm'>{tableName}</span>
                    </div>
                )
            },
            enableSorting: true,
            enableHiding: false,
            meta: { title: 'Table Name' },
        },
        {
            id: 'message_per_day',
            header: ({ column }) => (
                <DataTableColumnHeader column={column} title='Message Per Day' className='flex w-full justify-center' />
            ),
            cell: ({ row }) => {
                const tableName = row.getValue('table_name') as string
                const stats = statsMap[tableName]
                // Get today's count (last entry in daily_stats usually, or check date)
                const today = new Date().toLocaleDateString('en-CA')
                const todayStat = stats?.daily_stats.find(d => d.date.startsWith(today))
                const count = todayStat ? todayStat.count : 0
                
                return (
                   <div className="flex h-full items-center justify-center">
                       <span className="text-sm font-semibold tabular-nums">
                           {count.toLocaleString()}
                       </span>
                   </div>
                )
            },
            meta: { title: 'Message Per Day' },
        },
        {
            id: 'monitoring',
            header: ({ column }) => (
                <DataTableColumnHeader column={column} title='Monitoring' className='flex w-full justify-center' />
            ),
            cell: ({ row }) => {
                const tableName = row.getValue('table_name') as string
                const stats = statsMap[tableName]
                
                return <MonitoringSparkline stats={stats} />
            },
            meta: { title: 'Monitoring' },
        }
    ]

    return columns
}
