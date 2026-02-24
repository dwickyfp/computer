import { z } from 'zod'
import { createFileRoute } from '@tanstack/react-router'
import TableSyncDetailsPage from '@/features/pipelines/pages/table-sync-details-page'

export const Route = createFileRoute(
  '/_authenticated/pipelines/$pipelineId/destinations/$destId/tables/$syncId'
)({
  component: TableSyncDetailsPage,
  parseParams: (params) => ({
    destId: z.number().int().parse(Number(params.destId)),
    syncId: z.number().int().parse(Number(params.syncId)),
  }),
})
