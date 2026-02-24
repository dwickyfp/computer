import { useQuery } from '@tanstack/react-query'
import { dashboardRepo } from '@/repo/dashboard'
import { DashboardPanel } from './dashboard-panel'
import { Database, CheckCircle, AlertOctagon, PauseCircle } from 'lucide-react'
import { useRefreshInterval } from '../context/refresh-interval-context'

export function SourceHealthCard() {
  const { refreshInterval } = useRefreshInterval()
  const { data: health } = useQuery({
    queryKey: ['dashboard', 'source-health'],
    queryFn: dashboardRepo.getSourceHealth,
    refetchInterval: refreshInterval,
  })

  return (
    <DashboardPanel
      title="Source Health"
      headerAction={<Database className="h-4 w-4 text-muted-foreground" />}
      className="h-full"
      noPadding
    >
      <div className="flex items-center justify-between h-full px-4 py-1">
        <div className="flex flex-col items-center justify-center flex-1">
          <div className="p-1.5 mb-1 rounded-full bg-emerald-500/10 text-emerald-500">
            <CheckCircle className="w-4 h-4" />
          </div>
          <span className="text-xl font-bold font-mono leading-none mb-0.5">{health?.ACTIVE || 0}</span>
          <span className="text-[10px] text-muted-foreground uppercase font-medium">Active</span>
        </div>
        <div className="w-px h-8 bg-border/50" />
        <div className="flex flex-col items-center justify-center flex-1">
          <div className="p-1.5 mb-1 rounded-full bg-amber-500/10 text-amber-500">
            <PauseCircle className="w-4 h-4" />
          </div>
          <span className="text-xl font-bold font-mono leading-none mb-0.5">{health?.IDLE || 0}</span>
          <span className="text-[10px] text-muted-foreground uppercase font-medium">Idle</span>
        </div>
        <div className="w-px h-8 bg-border/50" />
        <div className="flex flex-col items-center justify-center flex-1">
          <div className="p-1.5 mb-1 rounded-full bg-rose-500/10 text-rose-500">
            <AlertOctagon className="w-4 h-4" />
          </div>
          <span className="text-xl font-bold font-mono leading-none mb-0.5">{health?.ERROR || 0}</span>
          <span className="text-[10px] text-muted-foreground uppercase font-medium">Error</span>
        </div>
      </div>
    </DashboardPanel>
  )
}
