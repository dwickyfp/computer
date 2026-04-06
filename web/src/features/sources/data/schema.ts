import { z } from 'zod'

const sourceTypeSchema = z.enum(['POSTGRES', 'KAFKA'])

export const sourceSchema = z.object({
  id: z.number(),
  name: z.string(),
  type: sourceTypeSchema,
  config: z.record(z.string(), z.any()),
  is_publication_enabled: z.boolean(),
  is_replication_enabled: z.boolean(),
  last_check_replication_publication: z.string().nullable(),
  total_tables: z.number(),
  created_at: z.string(),
  updated_at: z.string(),
  pg_host: z.string().optional(),
  pg_port: z.number().optional(),
  pg_database: z.string().optional(),
  pg_username: z.string().optional(),
  publication_name: z.string().optional(),
  replication_name: z.string().optional(),
  bootstrap_servers: z.string().optional(),
  topic_prefix: z.string().optional(),
  group_id: z.string().optional(),
  auto_offset_reset: z.string().optional(),
  format: z.string().optional(),
})

export type Source = z.infer<typeof sourceSchema>

export const sourceFormSchema = z
  .object({
    name: z
      .string()
      .min(1, 'Name is required')
      .regex(/^\S*$/, 'Name must not contain whitespace'),
    type: sourceTypeSchema.default('POSTGRES'),
    pg_host: z.string().optional(),
    pg_port: z.coerce.number().optional(),
    pg_database: z.string().optional(),
    pg_username: z.string().optional(),
    pg_password: z.string().optional(),
    publication_name: z.string().optional(),
    replication_name: z.string().optional(),
    bootstrap_servers: z.string().optional(),
    topic_prefix: z.string().optional(),
    auto_offset_reset: z.string().optional(),
    security_protocol: z.string().optional(),
    sasl_mechanism: z.string().optional(),
    sasl_username: z.string().optional(),
    sasl_password: z.string().optional(),
    ssl_ca_location: z.string().optional(),
    ssl_certificate_location: z.string().optional(),
    ssl_key_location: z.string().optional(),
    format: z.string().optional(),
  })
  .superRefine((data, ctx) => {
    if (data.type === 'POSTGRES') {
      if (!data.pg_host) {
        ctx.addIssue({
          code: z.ZodIssueCode.custom,
          message: 'Host is required',
          path: ['pg_host'],
        })
      }
      if (!data.pg_port) {
        ctx.addIssue({
          code: z.ZodIssueCode.custom,
          message: 'Port is required',
          path: ['pg_port'],
        })
      }
      if (!data.pg_database) {
        ctx.addIssue({
          code: z.ZodIssueCode.custom,
          message: 'Database is required',
          path: ['pg_database'],
        })
      }
      if (!data.pg_username) {
        ctx.addIssue({
          code: z.ZodIssueCode.custom,
          message: 'Username is required',
          path: ['pg_username'],
        })
      }
      if (!data.publication_name) {
        ctx.addIssue({
          code: z.ZodIssueCode.custom,
          message: 'Publication name is required',
          path: ['publication_name'],
        })
      }
      if (!data.replication_name) {
        ctx.addIssue({
          code: z.ZodIssueCode.custom,
          message: 'Replication name is required',
          path: ['replication_name'],
        })
      }
    }

    if (data.type === 'KAFKA') {
      if (!data.bootstrap_servers) {
        ctx.addIssue({
          code: z.ZodIssueCode.custom,
          message: 'Bootstrap servers are required',
          path: ['bootstrap_servers'],
        })
      }
      if (!data.topic_prefix) {
        ctx.addIssue({
          code: z.ZodIssueCode.custom,
          message: 'Topic prefix is required',
          path: ['topic_prefix'],
        })
      }
    }
  })

export type SourceForm = z.infer<typeof sourceFormSchema>
