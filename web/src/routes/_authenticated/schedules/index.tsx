import { z } from 'zod'
import { createFileRoute } from '@tanstack/react-router'
import { Schedules } from '@/features/schedules'

const searchSchema = z.object({
  page: z.number().optional().catch(1),
  pageSize: z.number().optional().catch(10),
  filter: z.string().optional().catch(''),
})

export const Route = createFileRoute('/_authenticated/schedules/')({
  validateSearch: searchSchema,
  component: Schedules,
})
