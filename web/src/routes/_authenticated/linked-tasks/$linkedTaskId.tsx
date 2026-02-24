import { createFileRoute } from '@tanstack/react-router'
import LinkedTaskDetailPage from '@/features/linked-tasks/pages/linked-task-detail-page'

export const Route = createFileRoute('/_authenticated/linked-tasks/$linkedTaskId')({
  component: LinkedTaskDetailPage,
})
