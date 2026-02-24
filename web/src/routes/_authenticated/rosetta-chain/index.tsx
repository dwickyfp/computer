import { z } from 'zod'
import { createFileRoute } from '@tanstack/react-router'
import { RosettaChain } from '@/features/rosetta-chain'

const searchSchema = z.object({
  page: z.number().optional().catch(1),
  pageSize: z.number().optional().catch(10),
  filter: z.string().optional().catch(''),
})

export const Route = createFileRoute('/_authenticated/rosetta-chain/')({
  validateSearch: searchSchema,
  component: RosettaChain,
})
