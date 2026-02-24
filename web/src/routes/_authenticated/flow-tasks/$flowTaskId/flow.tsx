import { createFileRoute } from '@tanstack/react-router'
import FlowTaskFlowPage from '@/features/flow-tasks/pages/flow-task-flow-page'

export const Route = createFileRoute('/_authenticated/flow-tasks/$flowTaskId/flow')({
    component: FlowTaskFlowPage,
})
