import { useQuery } from '@tanstack/react-query'
import {
  AlertCircle,
  BellRing,
  CheckCircle2,
  Info,
  Loader2,
} from 'lucide-react'
import { ScrollArea } from '@/components/ui/scroll-area'
import { cn } from '@/lib/utils'
import { dashboardRepo } from '@/repo/dashboard'
import { useRefreshInterval } from '../context/refresh-interval-context'
import { DashboardPanel } from './dashboard-panel'

export function ActivityFeed() {
  const { refreshInterval } = useRefreshInterval()
  const { data: activities, isLoading } = useQuery({
    queryKey: ['dashboard', 'activity-feed'],
    queryFn: () => dashboardRepo.getActivityFeed(20),
    refetchInterval: refreshInterval,
  })

  const getIcon = (type: string, message: string) => {
    if (type === 'ERROR' || message.toLowerCase().includes('error')) {
      return <AlertCircle className='h-4 w-4 text-rose-300' />
    }
    if (message.includes('RUNNING') || message.includes('START')) {
      return <CheckCircle2 className='h-4 w-4 text-emerald-300' />
    }
    return <Info className='h-4 w-4 text-sky-300' />
  }

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
      title='Activity feed'
      description='Recent signals from pipelines, jobs, and background services.'
      headerSlot={
        <div className='dashboard-chip rounded-full p-2.5'>
          <BellRing className='h-4 w-4 text-muted-foreground' />
        </div>
      }
      className='min-h-[420px]'
      noPadding
      variant='dense'
    >
      <ScrollArea className='h-full'>
        <div className='flex flex-col gap-3 px-5 pb-5 pt-4 sm:px-6 sm:pb-6 sm:pt-5'>
          {isLoading && (
            <div className='dashboard-inset flex items-center gap-3 rounded-[22px] px-4 py-4 text-sm text-muted-foreground'>
              <Loader2 className='h-4 w-4 animate-spin' />
              Syncing recent activity...
            </div>
          )}

          {!isLoading && (!activities || activities.length === 0) && (
            <div className='dashboard-inset flex min-h-[220px] flex-col items-center justify-center gap-3 rounded-[24px] px-6 py-8 text-center'>
              <BellRing className='h-8 w-8 text-muted-foreground/60' />
              <div className='space-y-1'>
                <p className='text-sm font-medium text-foreground'>
                  Nothing urgent right now
                </p>
                <p className='text-sm text-muted-foreground'>
                  New pipeline and worker activity will appear here as it
                  happens.
                </p>
              </div>
            </div>
          )}

          {activities?.map((item) => (
            <div
              key={`${item.timestamp}-${item.source}-${item.message}`}
              className='dashboard-row rounded-[22px] px-4 py-4'
            >
              <div className='flex gap-3'>
                <div
                  className={cn(
                    'mt-0.5 flex h-10 w-10 shrink-0 items-center justify-center rounded-full ring-1 ring-inset ring-white/5',
                    item.type === 'ERROR'
                      ? 'bg-rose-500/14 text-rose-300'
                      : 'bg-emerald-500/12 text-emerald-300'
                  )}
                >
                  {getIcon(item.type, item.message)}
                </div>

                <div className='min-w-0 flex-1 space-y-2'>
                  <div className='flex flex-wrap items-start justify-between gap-2'>
                    <div className='min-w-0'>
                      <p className='truncate text-sm font-semibold text-foreground'>
                        {item.source}
                      </p>
                      <p className='text-xs text-muted-foreground'>
                        {getTimeAgo(item.timestamp)}
                      </p>
                    </div>

                    <div className='rounded-full border border-white/10 bg-white/[0.03] px-2.5 py-1 text-[11px] text-muted-foreground'>
                      {item.type === 'ERROR' ? 'Attention' : 'Update'}
                    </div>
                  </div>

                  <p className='text-sm leading-6 text-muted-foreground'>
                    {item.message}
                  </p>
                </div>
              </div>
            </div>
          ))}
        </div>
      </ScrollArea>
    </DashboardPanel>
  )
}
