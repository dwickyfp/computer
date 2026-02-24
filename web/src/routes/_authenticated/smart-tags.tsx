import { createFileRoute } from '@tanstack/react-router'
import { SmartTags } from '@/features/smart-tags'

export const Route = createFileRoute('/_authenticated/smart-tags')({
  component: SmartTags,
})
