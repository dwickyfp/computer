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

export const chainDatabaseSchema = z.object({
  id: z.number(),
  chain_client_id: z.number(),
  name: z.string(),
  created_at: z.string(),
  updated_at: z.string(),
})

export type ChainDatabase = z.infer<typeof chainDatabaseSchema>

export const chainClientSchema = z.object({
  id: z.number(),
  name: z.string(),
  url: z.string(),
  port: z.number().default(8001),
  is_active: z.boolean(),
  description: z.string().nullable().optional(),
  last_connected_at: z.string().nullable().optional(),
  tables: z.array(chainTableSchema).default([]),
  databases: z.array(chainDatabaseSchema).default([]),
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
    .min(1, 'Host/IP is required')
    .refine((val) => !val.startsWith('http://') && !val.startsWith('https://'), {
      message: 'Do not include http:// or https://, just the host/IP',
    }),
  port: z.coerce.number().int().min(1).max(65535).default(8001),
  chain_key: z.string().optional(),
  description: z.string().optional(),
  is_active: z.boolean().optional().default(true),
})

export type ChainClientForm = z.infer<typeof chainClientFormSchema>
