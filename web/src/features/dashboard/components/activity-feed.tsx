import { useQuery } from '@tanstack/react-query'
import { dashboardRepo } from '@/repo/dashboard'
import { DashboardPanel } from './dashboard-panel'
import { BellRing, AlertCircle, Info, CheckCircle2 } from 'lucide-react'
import { ScrollArea } from '@/components/ui/scroll-area'
import { cn } from '@/lib/utils'
import { useRefreshInterval } from '../context/refresh-interval-context'

export function ActivityFeed() {
  const { refreshInterval } = useRefreshInterval()
  const { data: activities } = useQuery({
    queryKey: ['dashboard', 'activity-feed'],
    queryFn: () => dashboardRepo.getActivityFeed(20),
    refetchInterval: refreshInterval,
  })

  // Helper to get icon based on type or message content
  const getIcon = (type: string, message: string) => {
    if (type === 'ERROR' || message.toLowerCase().includes('error')) {
      return <AlertCircle className="w-4 h-4 text-rose-500" />
    }
    if (message.includes('RUNNING') || message.includes('START')) {
      return <CheckCircle2 className="w-4 h-4 text-emerald-500" />
    }
    return <Info className="w-4 h-4 text-blue-500" />
  }

  // Helper to get relative time (simple version, could use date-fns)
  const getTimeAgo = (timestamp: string) => {
    const diff = new Date().getTime() - new Date(timestamp).getTime()
    const minutes = Math.floor(diff / 60000)
    if (minutes < 1) return 'Just now'
    if (minutes < 60) return `${minutes}m ago`
    const hours = Math.floor(minutes / 60)
    if (hours < 24) return `${hours}h ago`
    return new Date(timestamp).toLocaleDateString()
  }

  return (
    <DashboardPanel
      title="Activity Feed"
      headerAction={<BellRing className="w-4 h-4 text-muted-foreground" />}
      className="h-[400px]"
      noPadding
    >
      <ScrollArea className="h-full">
        <div className="flex flex-col">
          {activities?.length === 0 && (
            <div className="text-sm text-muted-foreground text-center py-4">No recent activity</div>
          )}
          {activities?.map((item, i) => (
            <div key={i} className="flex gap-3 items-start group p-3 border-b hover:bg-muted/50 transition-colors">
              <div className={cn(
                "mt-0.5 p-1 rounded-sm bg-muted/50 transition-colors",
                item.type === 'ERROR' ? "text-rose-500 bg-rose-500/10" : "text-emerald-500 bg-emerald-500/10"
              )}>
                {getIcon(item.type, item.message)}
              </div>
              <div className="space-y-0.5 flex-1 min-w-0">
                <div className="flex items-center justify-between">
                  <p className="text-xs font-semibold leading-none truncate">{item.source}</p>
                  <p className="text-[10px] text-muted-foreground/60 whitespace-nowrap ml-2">{getTimeAgo(item.timestamp)}</p>
                </div>
                <p className="text-xs text-muted-foreground break-all line-clamp-2">{item.message}</p>
              </div>
            </div>
          ))}
        </div>
      </ScrollArea>
    </DashboardPanel>
  )
}
