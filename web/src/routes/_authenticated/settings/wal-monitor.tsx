import { createFileRoute } from '@tanstack/react-router'
import { SettingsWALMonitor } from '@/features/settings/wal-monitor'

export const Route = createFileRoute('/_authenticated/settings/wal-monitor')({
  component: SettingsWALMonitor,
})
