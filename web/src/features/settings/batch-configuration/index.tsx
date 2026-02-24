import { ContentSection } from '../components/content-section'
import { BatchConfigurationForm } from './batch-configuration-form'

export function SettingsBatchConfiguration() {
  return (
    <ContentSection
      title='Batch Configuration'
      desc='Configure CDC batch processing settings for optimal performance. Changes will automatically restart active pipelines.'
      fullWidth
    >
      <BatchConfigurationForm />
    </ContentSection>
  )
}
