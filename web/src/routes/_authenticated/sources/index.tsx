import { createFileRoute } from '@tanstack/react-router'
import { Sources } from '@/features/sources'
import { z } from 'zod'

const searchSchema = z.object({
    page: z.number().optional().catch(1),
    pageSize: z.number().optional().catch(10),
    filter: z.string().optional().catch(''),
})

export const Route = createFileRoute('/_authenticated/sources/')({
    validateSearch: searchSchema,
    component: Sources,
})
