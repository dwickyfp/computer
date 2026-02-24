import { createFileRoute } from '@tanstack/react-router'
import PipelineFlowPage from '@/features/pipelines/pages/pipeline-flow-page'

export const Route = createFileRoute('/_authenticated/pipelines/$pipelineId/flow')({
  component: PipelineFlowPage,
})
