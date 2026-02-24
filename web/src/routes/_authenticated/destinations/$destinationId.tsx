import { createFileRoute } from '@tanstack/react-router'
import { DestinationDetailsPage } from '@/features/destinations'
import { z } from 'zod'

export const Route = createFileRoute(
    '/_authenticated/destinations/$destinationId',
)({
    component: DestinationDetailsPage,
    parseParams: (params) => ({
        destinationId: z.number().int().parse(Number(params.destinationId)),
    }),
})
