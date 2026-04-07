import { useQuery } from '@tanstack/react-query'
import { memo } from 'react'
import { AlertTriangle, CheckCircle2, Database, RadioTower } from 'lucide-react'
import { ScrollArea } from '@/components/ui/scroll-area'
import { cn, formatBytes } from '@/lib/utils'
import { walMonitorRepo } from '@/repo/wal-monitor'
import { useRefreshInterval } from '../context/refresh-interval-context'
import { getDashboardPollingQueryOptions } from '../query-defaults'
import { DashboardPanel } from './dashboard-panel'

function getStatusTone(status: 'OK' | 'WARNING' | 'ERROR' | null) {
  switch (status) {
    case 'OK':
      return 'border-emerald-500/20 bg-emerald-500/10 text-emerald-300'
    case 'WARNING':
      return 'border-amber-500/20 bg-amber-500/10 text-amber-300'
    case 'ERROR':
      return 'border-rose-500/20 bg-rose-500/10 text-rose-300'
    default:
      return 'border-white/10 bg-white/[0.03] text-muted-foreground'
  }
}

export const WALMonitorList = memo(function WALMonitorList() {
  const { refreshInterval } = useRefreshInterval()
  const { data: monitors = [] } = useQuery({
    queryKey: ['wal-monitor', 'all'],
    queryFn: walMonitorRepo.getAll,
    ...getDashboardPollingQueryOptions(refreshInterval),
    notifyOnChangeProps: ['data'],
    select: (data) =>
      (data.monitors || []).filter(
        (monitor) => (monitor.source?.type || 'POSTGRES') === 'POSTGRES'
      ),
  })

  return (
    <DashboardPanel
      title='WAL replication'
      description='Replication slots, WAL size, and lag.'
      headerSlot={
        <div className='dashboard-chip rounded-full p-2.5'>
          <RadioTower className='h-4 w-4 text-muted-foreground' />
        </div>
      }
      noPadding
      variant='dense'
    >
      <ScrollArea className='h-full'>
        <div className='flex flex-col gap-3 px-5 pb-5 pt-4 sm:px-6 sm:pb-6 sm:pt-5'>
          {monitors.length === 0 && (
            <div className='dashboard-inset flex min-h-[220px] flex-col items-center justify-center gap-3 rounded-[24px] px-6 py-8 text-center'>
              <RadioTower className='h-8 w-8 text-muted-foreground/60' />
              <div className='space-y-1'>
                <p className='text-sm font-medium text-foreground'>
                  No replication monitors found
                </p>
                <p className='text-sm text-muted-foreground'>
                  Active Postgres replication sources will appear here when they
                  report WAL metrics.
                </p>
              </div>
            </div>
          )}

          {monitors.map((monitor) => (
            <div
              key={monitor.id}
              className={cn(
                'dashboard-row rounded-[22px] px-4 py-4',
                (monitor.wal_threshold_status === 'ERROR' ||
                  monitor.status === 'ERROR') &&
                  'border border-rose-500/20 bg-rose-500/8'
              )}
            >
              <div className='flex items-start justify-between gap-3'>
                <div className='flex min-w-0 items-start gap-3'>
                  <div className='flex h-10 w-10 shrink-0 items-center justify-center rounded-full bg-primary/10 text-primary ring-1 ring-inset ring-white/5'>
                    <Database className='h-4 w-4' />
                  </div>

                  <div className='min-w-0 space-y-2'>
                    <div className='flex flex-wrap items-center gap-2'>
                      <p className='truncate text-sm font-semibold text-foreground'>
                        {monitor.source?.name || `Source #${monitor.source_id}`}
                      </p>
                      {monitor.status === 'ACTIVE' ? (
                        <CheckCircle2 className='h-4 w-4 text-emerald-300' />
                      ) : (
                        <AlertTriangle className='h-4 w-4 text-rose-300' />
                      )}
                    </div>

                    <p className='truncate font-mono text-xs text-muted-foreground'>
                      {monitor.wal_lsn || 'No WAL position reported'}
                    </p>

                    <div className='flex flex-wrap gap-2'>
                      <div className='rounded-full border border-white/10 bg-white/[0.03] px-2.5 py-1 text-[11px] text-muted-foreground'>
                        Lag {formatBytes(monitor.replication_lag_bytes || 0)}
                      </div>
                      <div
                        className={cn(
                          'rounded-full border px-2.5 py-1 text-[11px] font-medium',
                          getStatusTone(monitor.wal_threshold_status)
                        )}
                      >
                        {monitor.total_wal_size || '0 B'}
                      </div>
                    </div>
                  </div>
                </div>

                <div
                  className={cn(
                    'rounded-full border px-2.5 py-1 text-[11px] font-medium',
                    monitor.status === 'ACTIVE'
                      ? 'border-emerald-500/20 bg-emerald-500/10 text-emerald-300'
                      : 'border-rose-500/20 bg-rose-500/10 text-rose-300'
                  )}
                >
                  {monitor.status || 'Unknown'}
                </div>
              </div>
            </div>
          ))}
        </div>
      </ScrollArea>
    </DashboardPanel>
  )
})
