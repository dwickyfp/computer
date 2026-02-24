import React, { createContext, useContext, useState } from 'react'
import { type ScheduleListItem } from '@/repo/schedules'

// ─── Context ──────────────────────────────────────────────────────────────────

type DialogType = 'delete' | 'pause' | 'resume' | null

interface SchedulesContextValue {
  open: DialogType
  setOpen: (open: DialogType) => void
  currentRow: ScheduleListItem | null
  setCurrentRow: (row: ScheduleListItem | null) => void
}

const SchedulesContext = createContext<SchedulesContextValue | null>(null)

export function SchedulesProvider({ children }: { children: React.ReactNode }) {
  const [open, setOpen] = useState<DialogType>(null)
  const [currentRow, setCurrentRow] = useState<ScheduleListItem | null>(null)

  return (
    <SchedulesContext.Provider
      value={{ open, setOpen, currentRow, setCurrentRow }}
    >
      {children}
    </SchedulesContext.Provider>
  )
}

export function useSchedules() {
  const ctx = useContext(SchedulesContext)
  if (!ctx)
    throw new Error('useSchedules must be used within SchedulesProvider')
  return ctx
}
