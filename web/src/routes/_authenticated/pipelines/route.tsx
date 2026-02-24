import { createFileRoute, Outlet } from '@tanstack/react-router'
import { PipelineSelectionProvider } from '@/features/pipelines/context/pipeline-selection-context'
import { PipelinesLayout } from '@/features/pipelines/layout/pipelines-layout'

export const Route = createFileRoute('/_authenticated/pipelines')({
  component: () => (
    <PipelineSelectionProvider>
      <PipelinesLayout>
        <Outlet />
      </PipelinesLayout>
    </PipelineSelectionProvider>
  ),
})
