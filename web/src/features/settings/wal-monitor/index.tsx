import { ContentSection } from '../components/content-section'
import { WALMonitorForm } from './wal-monitor-form'

export function SettingsWALMonitor() {
  return (
    <ContentSection
      title='WAL Monitor'
      desc='Configure Write-Ahead Log monitoring thresholds and alert notifications for PostgreSQL sources.'
      fullWidth
    >
      <WALMonitorForm />
    </ContentSection>
  )
}
