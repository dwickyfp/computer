import { Link } from '@tanstack/react-router'
import { ArrowUpRight, type LucideIcon } from 'lucide-react'
import { memo, type ReactNode } from 'react'
import { cn } from '@/lib/utils'

type MetricTone = 'positive' | 'negative' | 'neutral'

interface DashboardMetricCardProps {
  className?: string
  detail: ReactNode
  href?: string
  icon: LucideIcon
  label: string
  status?: ReactNode
  tone?: MetricTone
  value: ReactNode
}

const toneClasses: Record<MetricTone, string> = {
  positive:
    'border-emerald-500/18 bg-emerald-500/10 text-emerald-700 dark:text-emerald-200',
  negative:
    'border-rose-500/18 bg-rose-500/10 text-rose-700 dark:text-rose-200',
  neutral: 'dashboard-status-neutral',
}

export const DashboardMetricCard = memo(function DashboardMetricCard({
  className,
  detail,
  href,
  icon: Icon,
  label,
  status,
  tone = 'neutral',
  value,
}: DashboardMetricCardProps) {
  const isInteractive = Boolean(href)
  const shellClassName = cn(
    'dashboard-metric-card group px-5 py-5 sm:px-6 sm:py-6',
    className
  )

  const content = (
    <>
      <div className='relative z-10 flex items-start justify-between gap-4'>
        <div className='min-w-0 space-y-3'>
          <p className='dashboard-text-muted text-[11px] font-medium uppercase tracking-[0.24em]'>
            {label}
          </p>
          <div className='space-y-2'>
            <div className='dashboard-text-strong font-mono text-3xl font-semibold tracking-[-0.04em] sm:text-[2.35rem]'>
              {value}
            </div>
            <p className='dashboard-text max-w-[24ch] text-sm leading-6'>
              {detail}
            </p>
          </div>
        </div>

        <div className='dashboard-chip dashboard-icon-tile rounded-[18px] p-3.5'>
          <Icon className='h-5 w-5' />
        </div>
      </div>

      <div className='relative z-10 flex items-center justify-between gap-3'>
        {status ? (
          <div
            className={cn(
              'rounded-full border px-3 py-1 text-[11px] font-medium tracking-[0.18em] uppercase',
              toneClasses[tone]
            )}
          >
            {status}
          </div>
        ) : (
          <span className='dashboard-text-subtle text-xs'>Live data</span>
        )}

        {isInteractive && (
          <ArrowUpRight className='dashboard-text-muted h-4 w-4 transition-transform duration-200 group-hover:translate-x-0.5 group-hover:-translate-y-0.5' />
        )}
      </div>
    </>
  )

  if (href) {
    return (
      <Link
        to={href as never}
        preload='intent'
        className={shellClassName}
        data-interactive='true'
      >
        {content}
      </Link>
    )
  }

  return (
    <div className={shellClassName} data-interactive='false'>
      {content}
    </div>
  )
})
