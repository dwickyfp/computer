import { createFileRoute } from '@tanstack/react-router'
import PipelineList from '@/features/pipelines/components/pipeline-list'

export const Route = createFileRoute('/_authenticated/pipelines/')({
  component: PipelineList,
})
