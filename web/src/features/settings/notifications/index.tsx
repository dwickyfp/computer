import { ContentSection } from '../components/content-section'
import { NotificationsForm } from './notifications-form'

export function SettingsNotifications() {
  return (
    <ContentSection
      title='Notification Setting'
      desc='Configure internal, webhook, and Telegram notification channels for WAL monitoring alerts.'
      fullWidth
    >
      <NotificationsForm />
    </ContentSection>
  )
}
