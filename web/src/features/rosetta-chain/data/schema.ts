import { z } from 'zod'

// ─── Chain Client Schema ────────────────────────────────────────────────────

export const chainTableSchema = z.object({
  id: z.number(),
  chain_client_id: z.number(),
  table_name: z.string(),
  table_schema: z.record(z.string(), z.unknown()),
  record_count: z.number(),
  created_at: z.string(),
  updated_at: z.string(),
})

export type ChainTable = z.infer<typeof chainTableSchema>

export const chainClientSchema = z.object({
  id: z.number(),
  name: z.string(),
  url: z.string(),
  chain_key: z.string(),
  description: z.string().nullable(),
  is_active: z.boolean(),
  last_connected_at: z.string().nullable(),
  tables: z.array(chainTableSchema),
  created_at: z.string(),
  updated_at: z.string(),
})

export type ChainClient = z.infer<typeof chainClientSchema>

// ─── Chain Client Form Schema ───────────────────────────────────────────────

export const chainClientFormSchema = z.object({
  name: z
    .string()
    .min(1, 'Name is required')
    .regex(/^\S*$/, 'Name must not contain whitespace'),
  url: z
    .string()
    .min(1, 'URL is required')
    .url('Must be a valid URL (e.g., http://host:8001)'),
  chain_key: z.string().min(1, 'Chain key is required'),
  description: z.string().optional(),
  is_active: z.boolean().optional().default(true),
})

export type ChainClientForm = z.infer<typeof chainClientFormSchema>
