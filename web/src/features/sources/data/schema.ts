import { z } from 'zod'

export const sourceSchema = z.object({
    id: z.number(),
    name: z.string(),
    pg_host: z.string(),
    pg_port: z.number(),
    pg_database: z.string(),
    pg_username: z.string(),
    publication_name: z.string(),
    replication_name: z.string(),
    is_publication_enabled: z.boolean(),
    is_replication_enabled: z.boolean(),
    last_check_replication_publication: z.string().nullable(),
    total_tables: z.number(),
    created_at: z.string(),
    updated_at: z.string(),
})

export type Source = z.infer<typeof sourceSchema>

export const sourceFormSchema = z.object({
    name: z.string().min(1, 'Name is required').regex(/^\S*$/, 'Name must not contain whitespace'),
    pg_host: z.string().min(1, 'Host is required'),
    pg_port: z.coerce.number().min(1, 'Port is required'),
    pg_database: z.string().min(1, 'Database is required'),
    pg_username: z.string().min(1, 'Username is required'),
    pg_password: z.string().optional(),
    publication_name: z.string().min(1, 'Publication name is required'),
    replication_name: z.string().min(1, 'Replication name is required'),
})

export type SourceForm = z.infer<typeof sourceFormSchema>
