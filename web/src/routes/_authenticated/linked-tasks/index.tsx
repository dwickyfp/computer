import { createFileRoute } from '@tanstack/react-router'
import LinkedTaskListPage from '@/features/linked-tasks/pages/linked-task-list-page'

export const Route = createFileRoute('/_authenticated/linked-tasks/')({
  component: LinkedTaskListPage,
})
