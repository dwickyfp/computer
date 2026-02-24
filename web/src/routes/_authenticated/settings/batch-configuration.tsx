import { createFileRoute } from '@tanstack/react-router'
import { SettingsBatchConfiguration } from '@/features/settings/batch-configuration'

export const Route = createFileRoute('/_authenticated/settings/batch-configuration')({
  component: SettingsBatchConfiguration,
})
