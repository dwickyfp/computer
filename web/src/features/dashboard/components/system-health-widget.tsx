import { useQuery } from "@tanstack/react-query"
import { DashboardPanel } from "./dashboard-panel"
import { Activity, Database, Server, Cpu } from "lucide-react"
import { api } from "@/repo/client"
import { useRefreshInterval } from '../context/refresh-interval-context'

interface HealthResponse {
  status: string
  version: string
  timestamp: string
  checks: {
    database: boolean
    redis: boolean
    wal_monitor: boolean
    compute: boolean
    worker: boolean
  }
}

const getSystemHealth = async () => {
  const { data } = await api.get<HealthResponse>("/health")
  return data
}

export function SystemHealthWidget() {
  const { refreshInterval } = useRefreshInterval()
  const { data, isLoading, isError } = useQuery({
    queryKey: ["system-health"],
    queryFn: getSystemHealth,
    refetchInterval: refreshInterval,
  })

  const StatusIndicator = ({ healthy }: { healthy?: boolean }) => (
    <div className={`h-2.5 w-2.5 rounded-sm ${healthy ? "bg-emerald-500" : "bg-rose-500"}`} />
  )

  return (
    <DashboardPanel
      title="System Status"
      headerAction={<Activity className="h-4 w-4 text-muted-foreground" />}
      className='h-full'
    >
      {isLoading ? (
        <div className="text-xs text-muted-foreground">Loading status...</div>
      ) : isError || !data ? (
        <div className="text-xs text-rose-500">Failed to fetch status</div>
      ) : (
        <div className="space-y-2">
          <div className="flex items-center justify-between p-2 rounded bg-muted/20 hover:bg-muted/40 transition-colors">
            <div className="flex items-center space-x-2">
              <Database className="h-3.5 w-3.5 text-muted-foreground" />
              <span className="text-xs font-medium">Postgres Core</span>
            </div>
            <StatusIndicator healthy={data.checks.database} />
          </div>

          <div className="flex items-center justify-between p-2 rounded bg-muted/20 hover:bg-muted/40 transition-colors">
            <div className="flex items-center space-x-2">
              <Server className="h-3.5 w-3.5 text-muted-foreground" />
              <span className="text-xs font-medium">Redis Cache</span>
            </div>
            <StatusIndicator healthy={data.checks.redis} />
          </div>

          <div className="flex items-center justify-between p-2 rounded bg-muted/20 hover:bg-muted/40 transition-colors">
            <div className="flex items-center space-x-2">
              <Cpu className="h-3.5 w-3.5 text-muted-foreground" />
              <span className="text-xs font-medium">Compute Node</span>
            </div>
            <StatusIndicator healthy={data.checks.compute} />
          </div>

          <div className="flex items-center justify-between p-2 rounded bg-muted/20 hover:bg-muted/40 transition-colors">
            <div className="flex items-center space-x-2">
              <Cpu className="h-3.5 w-3.5 text-muted-foreground" />
              <span className="text-xs font-medium">Worker</span>
            </div>
            <StatusIndicator healthy={data.checks.worker} />
          </div>

          <div className="flex items-center justify-between pt-2 px-1 border-t border-border/50">
            <span className="text-[10px] text-muted-foreground font-mono">v{data.version}</span>
            <span className="text-[10px] text-muted-foreground">{new Date(data.timestamp).toLocaleTimeString()}</span>
          </div>
        </div>
      )}
    </DashboardPanel>
  )
}
