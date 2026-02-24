import { createFileRoute } from '@tanstack/react-router'
import FlowTaskDetailPage from '@/features/flow-tasks/pages/flow-task-detail-page'

export const Route = createFileRoute('/_authenticated/flow-tasks/$flowTaskId/')({
    component: FlowTaskDetailPage,
})
