import { createFileRoute } from '@tanstack/react-router'
import DLQManagerPage from '@/features/dlq-manager/pages/dlq-manager-page'

export const Route = createFileRoute('/_authenticated/dlq-manager')({
  component: DLQManagerPage,
})
