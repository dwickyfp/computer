import { useState, useEffect, useRef } from 'react'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { X, Key, Loader2, Plus, Pencil, Trash2, CheckCircle2 } from 'lucide-react'
import { toast } from 'sonner'
import { type TableSyncConfig, type ColumnSchema } from '@/repo/pipelines'

interface PrimaryKeyDrawerProps {
  syncConfig: TableSyncConfig
  columns: ColumnSchema[]
  open: boolean
  onClose: () => void
  onSave: (keys: string) => Promise<void>
}

export function PrimaryKeyDrawer({
  syncConfig,
  columns,
  open,
  onClose,
  onSave,
}: PrimaryKeyDrawerProps) {
  const [keys, setKeys] = useState<string[]>([])
  const [keyInput, setKeyInput] = useState('')
  const [editingIndex, setEditingIndex] = useState<number | null>(null)
  const [editingValue, setEditingValue] = useState('')
  const [saving, setSaving] = useState(false)
  const [showSuggestions, setShowSuggestions] = useState(false)
  const inputRef = useRef<HTMLInputElement>(null)

  // Parse existing keys on mount / when syncConfig changes
  useEffect(() => {
    if (syncConfig.primary_key_column_target) {
      const parsedKeys = syncConfig.primary_key_column_target
        .split(';')
        .map((k) => k.trim())
        .filter((k) => k.length > 0)
      setKeys(parsedKeys)
    } else {
      setKeys([])
    }
  }, [syncConfig.primary_key_column_target])

  // Focus input when opened
  useEffect(() => {
    if (open) {
      setTimeout(() => {
        inputRef.current?.focus()
      }, 100)
    }
  }, [open])

  const handleAddKey = () => {
    const trimmedKey = keyInput.trim()
    if (!trimmedKey) {
      toast.error('Key name cannot be empty')
      return
    }

    // Check if key already exists (case-insensitive)
    if (keys.some((k) => k.toLowerCase() === trimmedKey.toLowerCase())) {
      toast.error('Key already exists')
      return
    }

    setKeys([...keys, trimmedKey])
    setKeyInput('')
    setShowSuggestions(false)
  }

  const handleDeleteKey = (index: number) => {
    setKeys(keys.filter((_, i) => i !== index))
  }

  const handleStartEdit = (index: number) => {
    setEditingIndex(index)
    setEditingValue(keys[index])
  }

  const handleSaveEdit = (index: number) => {
    const trimmedValue = editingValue.trim()
    
    if (!trimmedValue) {
      toast.error('Key name cannot be empty')
      return
    }

    // Check if key already exists in other positions (case-insensitive)
    if (keys.some((k, i) => i !== index && k.toLowerCase() === trimmedValue.toLowerCase())) {
      toast.error('Key already exists')
      return
    }

    const newKeys = [...keys]
    newKeys[index] = trimmedValue
    setKeys(newKeys)
    setEditingIndex(null)
    setEditingValue('')
  }

  const handleCancelEdit = () => {
    setEditingIndex(null)
    setEditingValue('')
  }

  const handleSave = async () => {
    setSaving(true)
    try {
      const keysString = keys.join(';')
      await onSave(keysString)
      toast.success('Primary keys updated successfully')
      onClose()
    } catch (error: any) {
      toast.error(error.response?.data?.detail || 'Failed to update primary keys')
    } finally {
      setSaving(false)
    }
  }

  const handleClear = () => {
    setKeys([])
  }

  const handleKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter') {
      e.preventDefault()
      handleAddKey()
    } else if (e.key === 'Escape') {
      setShowSuggestions(false)
    }
  }

  const handleEditKeyDown = (e: React.KeyboardEvent<HTMLInputElement>, index: number) => {
    if (e.key === 'Enter') {
      e.preventDefault()
      handleSaveEdit(index)
    } else if (e.key === 'Escape') {
      handleCancelEdit()
    }
  }

  // Filter columns based on input for suggestions
  const filteredColumns = columns.filter(
    (col) =>
      col.column_name.toLowerCase().includes(keyInput.toLowerCase()) &&
      !keys.includes(col.column_name)
  ).slice(0, 5)

  if (!open) return null

  return (
    <div
      className="fixed top-2 bottom-2 left-[520px] flex w-[600px] flex-col rounded-2xl border bg-background shadow-2xl animate-in duration-300 slide-in-from-left-4"
      style={{ zIndex: 100 }}
      onClick={(e) => e.stopPropagation()}
      onMouseDown={(e) => e.stopPropagation()}
    >
      {/* Header */}
      <div className="flex items-center justify-between border-b px-6 py-4">
        <div className="flex items-center gap-3">
          <div className="flex h-10 w-10 items-center justify-center rounded-xl bg-amber-100 dark:bg-amber-950/50">
            <Key className="h-5 w-5 text-amber-600 dark:text-amber-400" />
          </div>
          <div>
            <h2 className="text-lg font-semibold">Primary Key Configuration</h2>
            <p className="text-sm text-muted-foreground">
              {syncConfig.table_name} → {syncConfig.table_name_target}
            </p>
          </div>
        </div>
        <Button
          variant="ghost"
          size="icon"
          onClick={onClose}
          className="h-8 w-8 rounded-lg"
        >
          <X className="h-4 w-4" />
        </Button>
      </div>

      {/* Content */}
      <div className="flex-1 overflow-y-auto p-6">
        {/* Info Box */}
        <div className="mb-4 rounded-lg border border-amber-200 bg-amber-50 dark:border-amber-900/50 dark:bg-amber-950/20 p-3">
          <div className="flex gap-2 text-xs text-amber-800 dark:text-amber-200">
            <Key className="h-4 w-4 flex-shrink-0 mt-0.5" />
            <div>
              <p className="font-medium mb-1">Custom Primary Keys</p>
              <p className="text-amber-700 dark:text-amber-300">
                Leave empty to use default primary key detection. 
                Add column names manually to create a composite key for the MERGE INTO operation.
              </p>
            </div>
          </div>
        </div>

        {/* Input Section */}
        <div className="mb-6">
          <div className="flex items-center justify-between mb-2">
            <label className="text-sm font-medium">Add Primary Key Column</label>
            {keys.length > 0 && (
              <Button
                variant="ghost"
                size="sm"
                onClick={handleClear}
                className="h-7 text-xs text-muted-foreground hover:text-foreground"
              >
                Clear all
              </Button>
            )}
          </div>
          
          <div className="relative tag-input-container">
            <div className="flex gap-2">
              <Input
                ref={inputRef}
                placeholder="Enter column name (e.g., id, user_id)"
                value={keyInput}
                onChange={(e) => {
                  setKeyInput(e.target.value)
                  setShowSuggestions(e.target.value.length > 0)
                }}
                onKeyDown={handleKeyDown}
                onFocus={() => setShowSuggestions(keyInput.length > 0)}
                className="flex-1"
              />
              <Button
                onClick={handleAddKey}
                disabled={!keyInput.trim()}
                size="sm"
                className="gap-1.5"
              >
                <Plus className="h-4 w-4" />
                Add
              </Button>
            </div>

            {/* Column Suggestions */}
            {showSuggestions && filteredColumns.length > 0 && (
              <div className="absolute top-full left-0 right-0 mt-1 z-50 rounded-md border bg-popover shadow-md">
                <div className="p-1">
                  <div className="text-xs text-muted-foreground px-2 py-1">
                    Suggestions from table columns:
                  </div>
                  {filteredColumns.map((col) => (
                    <button
                      key={col.column_name}
                      onClick={() => {
                        setKeyInput(col.column_name)
                        setShowSuggestions(false)
                        inputRef.current?.focus()
                      }}
                      className="w-full text-left px-2 py-1.5 text-sm hover:bg-accent rounded-sm"
                    >
                      <div className="font-mono font-medium">{col.column_name}</div>
                      <div className="text-xs text-muted-foreground">
                        {col.data_type}
                      </div>
                    </button>
                  ))}
                </div>
              </div>
            )}
          </div>
        </div>

        {/* Keys List */}
        {keys.length > 0 ? (
          <div className="space-y-2">
            <div className="text-sm font-medium text-muted-foreground mb-2">
              Primary Key Columns ({keys.length})
            </div>
            {keys.map((key, index) => (
              <div
                key={index}
                className="flex items-center gap-2 p-3 rounded-lg border bg-card hover:shadow-sm transition-all group"
              >
                <div className="flex h-6 w-6 items-center justify-center rounded-md bg-amber-100 dark:bg-amber-900/30 flex-shrink-0">
                  <span className="text-xs font-semibold text-amber-800 dark:text-amber-400">
                    {index + 1}
                  </span>
                </div>

                {editingIndex === index ? (
                  <>
                    <Input
                      autoFocus
                      value={editingValue}
                      onChange={(e) => setEditingValue(e.target.value)}
                      onKeyDown={(e) => handleEditKeyDown(e, index)}
                      className="flex-1 h-8"
                    />
                    <Button
                      variant="ghost"
                      size="icon"
                      onClick={() => handleSaveEdit(index)}
                      className="h-8 w-8"
                    >
                      <CheckCircle2 className="h-4 w-4 text-green-600" />
                    </Button>
                    <Button
                      variant="ghost"
                      size="icon"
                      onClick={handleCancelEdit}
                      className="h-8 w-8"
                    >
                      <X className="h-4 w-4" />
                    </Button>
                  </>
                ) : (
                  <>
                    <div className="flex-1 min-w-0">
                      <div className="font-mono text-sm font-medium truncate">
                        {key}
                      </div>
                    </div>
                    <Button
                      variant="ghost"
                      size="icon"
                      onClick={() => handleStartEdit(index)}
                      className="h-8 w-8 opacity-0 group-hover:opacity-100 transition-opacity"
                      title="Edit"
                    >
                      <Pencil className="h-3.5 w-3.5" />
                    </Button>
                    <Button
                      variant="ghost"
                      size="icon"
                      onClick={() => handleDeleteKey(index)}
                      className="h-8 w-8 text-red-500 hover:text-red-600 hover:bg-red-50 opacity-0 group-hover:opacity-100 transition-opacity"
                      title="Delete"
                    >
                      <Trash2 className="h-3.5 w-3.5" />
                    </Button>
                  </>
                )}
              </div>
            ))}
          </div>
        ) : (
          <div className="flex flex-col items-center justify-center py-12 text-center border-2 border-dashed rounded-lg">
            <Key className="h-8 w-8 text-muted-foreground mb-2" />
            <p className="text-sm font-medium">No primary keys configured</p>
            <p className="text-xs text-muted-foreground mt-1">
              Add column names above to create custom merge keys
            </p>
          </div>
        )}
      </div>

      {/* Footer */}
      <div className="flex items-center justify-between border-t px-6 py-4 bg-muted/30">
        <div className="text-xs text-muted-foreground">
          {keys.length === 0 ? (
            'Default key detection will be used'
          ) : (
            <>
              {keys.length} key{keys.length !== 1 ? 's' : ''} configured
              {keys.length > 1 && ' (composite key)'}
            </>
          )}
        </div>
        <div className="flex gap-2">
          <Button variant="outline" onClick={onClose} disabled={saving}>
            Cancel
          </Button>
          <Button onClick={handleSave} disabled={saving}>
            {saving && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
            Save Keys
          </Button>
        </div>
      </div>
    </div>
  )
}
