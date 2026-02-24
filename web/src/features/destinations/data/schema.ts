import { z } from 'zod'

export const destinationSchema = z.object({
    id: z.number(),
    name: z.string(),
    type: z.string(),
    config: z.record(z.string(), z.any()), // JSONB config
    created_at: z.string(),
    updated_at: z.string(),
    is_used_in_active_pipeline: z.boolean().optional(),
    total_tables: z.number().optional().default(0),
    last_table_check_at: z.string().nullable().optional(),
})

export type Destination = z.infer<typeof destinationSchema>

export const destinationFormSchema = z.object({
    name: z.string().min(1, 'Name is required').regex(/^[a-zA-Z0-9_]+$/, 'Name must contain only alphanumeric characters and underscores'),
    type: z.string().min(1, 'Type is required'),
    config: z.object({
        account: z.string().optional(),
        user: z.string().optional(),
        database: z.string().optional(),
        schema: z.string().optional(),
        landing_database: z.string().optional(),
        landing_schema: z.string().optional(),
        role: z.string().optional(),
        private_key: z.string().optional(),
        private_key_passphrase: z.string().optional(),
        warehouse: z.string().optional(),
        // Postgres
        host: z.string().optional(),
        port: z.number().optional(),
        password: z.string().optional(),
    }).optional(),
}).superRefine((data, ctx) => {
    if (data.type === 'POSTGRES') {
        if (!data.config?.host) {
            ctx.addIssue({
                code: z.ZodIssueCode.custom,
                message: 'Host is required',
                path: ['config', 'host'],
            })
        }
        if (!data.config?.port) {
            ctx.addIssue({
                code: z.ZodIssueCode.custom,
                message: 'Port is required',
                path: ['config', 'port'],
            })
        }
        if (!data.config?.database) {
            ctx.addIssue({
                code: z.ZodIssueCode.custom,
                message: 'Database is required',
                path: ['config', 'database'],
            })
        }
        if (!data.config?.user) {
            ctx.addIssue({
                code: z.ZodIssueCode.custom,
                message: 'User is required',
                path: ['config', 'user'],
            })
        }
        if (!data.config?.password) {
            ctx.addIssue({
                code: z.ZodIssueCode.custom,
                message: 'Password is required',
                path: ['config', 'password'],
            })
        }
    }
})

export type DestinationForm = z.infer<typeof destinationFormSchema>
