import { useState, useEffect } from 'react'
import { type TableWithSyncInfo } from '@/repo/pipelines'
import { Button } from '@/components/ui/button'
import { X } from 'lucide-react'
import AceEditor from 'react-ace'

// Import ace modes and themes
import 'ace-builds/src-noconflict/mode-mysql'
import 'ace-builds/src-noconflict/theme-solarized_light'
import 'ace-builds/src-noconflict/ext-language_tools'

interface SqlEditorModalProps {
  open: boolean
  onOpenChange: (open: boolean) => void
  table: TableWithSyncInfo | null
  onSave: (customSql: string) => void
}

export function SqlEditorModal({
  open,
  onOpenChange,
  table,
  onSave,
}: SqlEditorModalProps) {
  const [sql, setSql] = useState('')

  // Load existing SQL when modal opens
  useEffect(() => {
    if (open && table?.sync_configs?.[0]?.custom_sql) {
      setSql(table.sync_configs[0].custom_sql)
    } else if (open) {
      // Default template
      setSql(
        `-- Custom SQL for ${table?.table_name || 'table'}\n-- This query will be used to extract data from the source\n\nSELECT *\nFROM ${table?.table_name || 'table_name'}\nWHERE 1=1`
      )
    }
  }, [open, table])

  const handleSave = () => {
    onSave(sql)
  }

  if (!open || !table) return null

  return (
    <div
      className="fixed inset-0 z-50 flex items-start justify-start"
      onClick={() => onOpenChange(false)}
    >
      {/* Overlay */}
      <div className="absolute inset-0 bg-black/20" />

      {/* Modal positioned to the right of the drawer */}
      <div
        className="absolute left-[520px] top-[60px] z-50 w-[600px] bg-background border rounded-lg shadow-lg overflow-hidden"
        onClick={(e) => e.stopPropagation()}
      >
        {/* Header */}
        <div className="flex items-center justify-between px-4 py-3 border-b">
          <div>
            <h3 className="font-semibold">Custom SQL</h3>
            <p className="text-xs text-muted-foreground">
              {table.table_name}
            </p>
          </div>
          <Button
            variant="ghost"
            size="sm"
            onClick={() => onOpenChange(false)}
            className="h-8 w-8 p-0"
          >
            <X className="h-4 w-4" />
          </Button>
        </div>

        {/* Editor */}
        <div className="p-4">
          <AceEditor
            mode="mysql"
            theme="solarized_light"
            name="sql-editor"
            value={sql}
            onChange={setSql}
            width="100%"
            height="400px"
            fontSize={14}
            showPrintMargin={false}
            showGutter={true}
            highlightActiveLine={true}
            setOptions={{
              enableBasicAutocompletion: true,
              enableLiveAutocompletion: true,
              enableSnippets: true,
              showLineNumbers: true,
              tabSize: 2,
            }}
            editorProps={{ $blockScrolling: true }}
          />
        </div>

        {/* Column Reference */}
        <div className="px-4 pb-2">
          <div className="text-xs text-muted-foreground mb-1">
            Available columns:
          </div>
          <div className="flex flex-wrap gap-1 max-h-[60px] overflow-y-auto">
            {table.columns.map((col) => (
              <span
                key={col.column_name}
                className="text-xs bg-muted px-1.5 py-0.5 rounded cursor-pointer hover:bg-muted/80"
                onClick={() => {
                  // Insert column name at cursor
                  setSql((prev) => prev + col.column_name)
                }}
                title={col.data_type}
              >
                {col.column_name}
              </span>
            ))}
          </div>
        </div>

        {/* Actions */}
        <div className="flex gap-2 px-4 py-3 border-t">
          <Button
            variant="outline"
            onClick={() => onOpenChange(false)}
            className="flex-1"
          >
            Cancel
          </Button>
          <Button onClick={handleSave} className="flex-1">
            Save SQL
          </Button>
        </div>
      </div>
    </div>
  )
}
