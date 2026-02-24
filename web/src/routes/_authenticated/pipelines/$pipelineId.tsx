import { createFileRoute } from '@tanstack/react-router'
import PipelineDetailsPage from '@/features/pipelines/pages/pipeline-details-page'

export const Route = createFileRoute('/_authenticated/pipelines/$pipelineId')({
  component: PipelineDetailsPage,
})
