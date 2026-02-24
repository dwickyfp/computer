import { createFileRoute } from '@tanstack/react-router'
import FlowTaskListPage from '@/features/flow-tasks/pages/flow-task-list-page'

export const Route = createFileRoute('/_authenticated/flow-tasks/')({
  component: FlowTaskListPage,
})
