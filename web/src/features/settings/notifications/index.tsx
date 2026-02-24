import { ContentSection } from '../components/content-section'
import { NotificationsForm } from './notifications-form'

export function SettingsNotifications() {
  return (
    <ContentSection
      title='Notification Setting'
      desc='Configure webhook URL and alert thresholds for WAL monitoring notifications.'
      fullWidth
    >
      <NotificationsForm />
    </ContentSection>
  )
}
