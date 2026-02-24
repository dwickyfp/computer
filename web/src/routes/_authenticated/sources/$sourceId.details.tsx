import { createFileRoute } from '@tanstack/react-router'
import SourceDetailsPage from '@/features/sources/pages/source-details-page'

export const Route = createFileRoute(
  '/_authenticated/sources/$sourceId/details',
)({
  component: SourceDetailsPage,
})
