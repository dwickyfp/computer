import { createFileRoute } from '@tanstack/react-router'
import { Destinations } from '@/features/destinations'

export const Route = createFileRoute('/_authenticated/destinations/')({
    component: Destinations,
})
