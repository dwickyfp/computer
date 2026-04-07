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
  borderRadius: '18px',
  color: 'var(--color-foreground)',
  boxShadow: 'var(--dashboard-shadow)',
  fontSize: '12px',
  padding: '12px 14px',
}

export function getDashboardSeriesColor(index: number) {
  return DASHBOARD_SERIES_COLORS[index % DASHBOARD_SERIES_COLORS.length]
}
