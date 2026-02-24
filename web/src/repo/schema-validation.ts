import { api } from './client'

// ─── Types ───────────────────────────────────────────────────────────────────

export interface SchemaIssue {
  table_name: string
  column_name: string | null
  severity: 'ERROR' | 'WARNING'
  message: string
}

export interface SchemaValidationResult {
  is_compatible: boolean
  issues: SchemaIssue[]
  tables_checked: number
}

// ─── Repository ──────────────────────────────────────────────────────────────

export const schemaValidationRepo = {
  validate(params: {
    source_id: number
    table_name: string
    destination_id: number
    target_table?: string
  }) {
    return api.get<SchemaValidationResult>('/schema/validate-schema', {
      params,
    })
  },
}
