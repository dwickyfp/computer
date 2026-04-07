import { Link } from '@tanstack/react-router'
import { memo } from 'react'
import {
  ArrowUpRight,
  DatabaseZap,
  GitBranchPlus,
  ShieldCheck,
} from 'lucide-react'
import { DashboardPanel } from './dashboard-panel'

interface DashboardHeroPanelProps {
  activeBackfills: number
  activePipelines: number
  className?: string
  pausedPipelines: number
  projectedRunRate: string
  rowsToday: number
}

const compactNumberFormatter = new Intl.NumberFormat('en-US', {
  notation: 'compact',
  maximumFractionDigits: 1,
})

export const DashboardHeroPanel = memo(function DashboardHeroPanel({
  activeBackfills,
  activePipelines,
  className,
  pausedPipelines,
  projectedRunRate,
  rowsToday,
}: DashboardHeroPanelProps) {
  return (
    <DashboardPanel
      className={className}
      noPadding
      title='Rosetta overview'
      description='A calmer command surface for monitoring ingestion, reliability, and spend.'
      headerSlot={
        <div className='dashboard-chip dashboard-text rounded-full px-3 py-2 text-xs'>
          <span className='h-2 w-2 rounded-full bg-sky-400 shadow-[0_0_18px_rgba(56,189,248,0.7)]' />
          Realtime telemetry
        </div>
      }
      variant='dense'
    >
      <div className='grid gap-5 px-5 pb-5 pt-4 sm:px-6 sm:pb-6 lg:grid-cols-[1.1fr_0.9fr] lg:gap-6'>
        <div className='flex min-h-[300px] flex-col justify-between gap-6'>
          <div className='space-y-5'>
            <div className='dashboard-chip dashboard-text-muted w-fit rounded-full px-3.5 py-1.5 text-[11px] font-medium uppercase tracking-[0.24em]'>
              Rosetta control surface
            </div>

            <div className='space-y-4'>
              <h2 className='dashboard-text-strong max-w-[12ch] font-manrope text-3xl font-semibold tracking-[-0.05em] sm:text-[2.65rem]'>
                Realtime delivery across sources and pipelines.
              </h2>
              <p className='dashboard-text max-w-[56ch] text-sm leading-7'>
                Keep ingestion, processing, and operational reliability in one
                place. The first fold prioritizes throughput, readiness, and
                active work so the team can react without scanning every card.
              </p>
            </div>
          </div>

          <div className='grid gap-3 sm:grid-cols-3'>
            <div className='dashboard-inset rounded-[22px] px-4 py-4'>
              <p className='dashboard-text-muted text-[11px] uppercase tracking-[0.24em]'>
                Active
              </p>
              <p className='dashboard-text-strong mt-3 font-mono text-3xl font-semibold tracking-tight'>
                {activePipelines.toLocaleString()}
              </p>
              <p className='dashboard-text mt-2 text-sm'>Pipelines running now</p>
            </div>

            <div className='dashboard-inset rounded-[22px] px-4 py-4'>
              <p className='dashboard-text-muted text-[11px] uppercase tracking-[0.24em]'>
                Queued
              </p>
              <p className='dashboard-text-strong mt-3 font-mono text-3xl font-semibold tracking-tight'>
                {activeBackfills.toLocaleString()}
              </p>
              <p className='dashboard-text mt-2 text-sm'>Backfills in motion</p>
            </div>

            <div className='dashboard-inset rounded-[22px] px-4 py-4'>
              <p className='dashboard-text-muted text-[11px] uppercase tracking-[0.24em]'>
                Paused
              </p>
              <p className='dashboard-text-strong mt-3 font-mono text-3xl font-semibold tracking-tight'>
                {pausedPipelines.toLocaleString()}
              </p>
              <p className='dashboard-text mt-2 text-sm'>Pipelines awaiting action</p>
            </div>
          </div>

          <div className='flex flex-wrap items-center gap-3'>
            <Link
              to='/pipelines'
              preload='intent'
              className='dashboard-chip dashboard-text-strong rounded-full px-4 py-2 text-sm font-medium transition-colors hover:bg-white/[0.08]'
            >
              Open pipelines
              <ArrowUpRight className='h-4 w-4' />
            </Link>
            <Link
              to='/sources'
              preload='intent'
              className='dashboard-chip dashboard-text rounded-full px-4 py-2 text-sm font-medium transition-colors hover:bg-white/[0.06]'
            >
              Review sources
            </Link>
          </div>
        </div>

        <div className='dashboard-hero-visual min-h-[320px] rounded-[28px] p-5 sm:p-6'>
          <div className='dashboard-hero-grid' />

          <svg
            viewBox='0 0 520 360'
            className='absolute inset-0 h-full w-full opacity-95'
            aria-hidden='true'
          >
            <defs>
              <linearGradient id='hero-line-gradient' x1='0' x2='1' y1='0' y2='1'>
                <stop offset='0%' stopColor='rgba(125, 211, 252, 0.05)' />
                <stop offset='45%' stopColor='rgba(96, 165, 250, 0.72)' />
                <stop offset='100%' stopColor='rgba(34, 211, 238, 0.16)' />
              </linearGradient>
              <linearGradient id='hero-orbit-gradient' x1='0' x2='1' y1='0' y2='1'>
                <stop offset='0%' stopColor='rgba(37, 99, 235, 0.1)' />
                <stop offset='100%' stopColor='rgba(56, 189, 248, 0.42)' />
              </linearGradient>
            </defs>

            <path
              d='M42 236C96 186 166 146 252 146C332 146 396 190 472 156'
              stroke='url(#hero-line-gradient)'
              strokeWidth='3'
              fill='none'
              strokeLinecap='round'
            />
            <path
              d='M82 274C136 212 198 198 260 216C322 234 378 274 452 238'
              stroke='url(#hero-line-gradient)'
              strokeWidth='2.5'
              fill='none'
              strokeLinecap='round'
              opacity='0.7'
            />
            <ellipse
              cx='286'
              cy='170'
              rx='146'
              ry='92'
              fill='url(#hero-orbit-gradient)'
              opacity='0.45'
            />
            {[
              [86, 232, 8],
              [156, 182, 10],
              [232, 152, 12],
              [316, 166, 12],
              [384, 208, 11],
              [450, 160, 9],
              [416, 258, 8],
            ].map(([cx, cy, r], index) => (
              <g key={`${cx}-${cy}`}>
                <circle
                  cx={cx}
                  cy={cy}
                  r={r + 8}
                  fill='rgba(56, 189, 248, 0.08)'
                  opacity={0.8 - index * 0.05}
                />
                <circle
                  cx={cx}
                  cy={cy}
                  r={r}
                  fill='rgba(147, 197, 253, 0.92)'
                  stroke='rgba(255, 255, 255, 0.35)'
                />
              </g>
            ))}
          </svg>

          <div className='relative z-10 flex h-full flex-col justify-between'>
            <div className='flex justify-end'>
              <div className='dashboard-chip dashboard-text rounded-full px-3 py-1.5 text-[11px] uppercase tracking-[0.22em]'>
                <ShieldCheck className='h-3.5 w-3.5 text-emerald-600 dark:text-emerald-300' />
                Multi-signal view
              </div>
            </div>

            <div className='grid gap-3 sm:grid-cols-2'>
              <div className='dashboard-inset rounded-[22px] px-4 py-4'>
                <div className='dashboard-text-muted flex items-center gap-2 text-xs uppercase tracking-[0.2em]'>
                  <DatabaseZap className='h-3.5 w-3.5 text-sky-600 dark:text-sky-300' />
                  Rows today
                </div>
                <p className='dashboard-text-strong mt-3 font-mono text-3xl font-semibold'>
                  {compactNumberFormatter.format(rowsToday)}
                </p>
                <p className='dashboard-text mt-2 text-sm'>
                  Current ingestion volume
                </p>
              </div>

              <div className='dashboard-inset rounded-[22px] px-4 py-4'>
                <div className='dashboard-text-muted flex items-center gap-2 text-xs uppercase tracking-[0.2em]'>
                  <GitBranchPlus className='h-3.5 w-3.5 text-cyan-600 dark:text-cyan-300' />
                  Run rate
                </div>
                <p className='dashboard-text-strong mt-3 font-mono text-3xl font-semibold'>
                  {projectedRunRate}
                </p>
                <p className='dashboard-text mt-2 text-sm'>
                  Projected spend this month
                </p>
              </div>
            </div>
          </div>
        </div>
      </div>
    </DashboardPanel>
  )
})
