import { type CSSProperties } from 'react'

export const DASHBOARD_SERIES_COLORS = [
  'var(--color-chart-1)',
  'var(--color-chart-2)',
  'var(--color-chart-3)',
  'var(--color-chart-4)',
  'var(--color-chart-5)',
] as const

export const dashboardTooltipStyle: CSSProperties = {
  backgroundColor: 'var(--dashboard-tooltip)',
  border: '1px solid var(--dashboard-stroke)',
  borderRadius: '16px',
  color: 'var(--color-foreground)',
  boxShadow: 'var(--dashboard-shadow)',
  backdropFilter: 'blur(16px)',
  WebkitBackdropFilter: 'blur(16px)',
  fontSize: '12px',
  padding: '10px 12px',
}

export function getDashboardSeriesColor(index: number) {
  return DASHBOARD_SERIES_COLORS[index % DASHBOARD_SERIES_COLORS.length]
}
