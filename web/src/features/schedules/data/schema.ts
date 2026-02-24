import { z } from 'zod'

// ─── Base schemas ─────────────────────────────────────────────────────────────

export const scheduleRunHistorySchema = z.object({
  id: z.number(),
  schedule_id: z.number(),
  task_type: z.enum(['FLOW_TASK', 'LINKED_TASK']),
  task_id: z.number(),
  triggered_at: z.string(),
  completed_at: z.string().nullable(),
  duration_ms: z.number().nullable(),
  status: z.enum(['RUNNING', 'SUCCESS', 'FAILED']),
  message: z.string().nullable(),
})

export const scheduleListSchema = z.object({
  id: z.number(),
  name: z.string(),
  description: z.string().nullable(),
  task_type: z.enum(['FLOW_TASK', 'LINKED_TASK']),
  task_id: z.number(),
  cron_expression: z.string(),
  status: z.enum(['ACTIVE', 'PAUSED']),
  last_run_at: z.string().nullable(),
  next_run_at: z.string().nullable(),
  created_at: z.string(),
  updated_at: z.string(),
})

export const scheduleDetailSchema = scheduleListSchema.extend({
  run_history: z.array(scheduleRunHistorySchema),
})

// ─── Form schema ──────────────────────────────────────────────────────────────

const CRON_PARTS_REGEX = /^(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)$/

export const scheduleFormSchema = z.object({
  name: z
    .string()
    .min(1, 'Name is required')
    .max(255, 'Name is too long')
    .regex(/^\S+$/, 'Name must not contain spaces'),
  description: z.string().nullable().optional(),
  task_type: z.enum(['FLOW_TASK', 'LINKED_TASK']),
  task_id: z.number().int().min(1, 'Task is required'),
  cron_expression: z
    .string()
    .min(1, 'Cron expression is required')
    .refine((val) => CRON_PARTS_REGEX.test(val.trim()), {
      message:
        'Must be a valid 5-part cron expression (minute hour day month weekday)',
    }),
  status: z.enum(['ACTIVE', 'PAUSED']),
})

export type ScheduleFormValues = z.infer<typeof scheduleFormSchema>
export type ScheduleListItem = z.infer<typeof scheduleListSchema>
export type ScheduleDetail = z.infer<typeof scheduleDetailSchema>
export type ScheduleRunHistoryEntry = z.infer<typeof scheduleRunHistorySchema>
