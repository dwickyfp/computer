import React, { useState } from 'react'
import useDialogState from '@/hooks/use-dialog-state'
import { type LinkedTask } from '@/repo/linked-tasks'

type LinkedTasksDialogType = 'create' | 'update' | 'delete'

interface LinkedTasksContextType {
    open: LinkedTasksDialogType | null
    setOpen: (str: LinkedTasksDialogType | null) => void
    currentRow: LinkedTask | null
    setCurrentRow: React.Dispatch<React.SetStateAction<LinkedTask | null>>
}

const LinkedTasksContext = React.createContext<LinkedTasksContextType | null>(null)

export function LinkedTasksProvider({ children }: { children: React.ReactNode }) {
    const [open, setOpen] = useDialogState<LinkedTasksDialogType>(null)
    const [currentRow, setCurrentRow] = useState<LinkedTask | null>(null)

    return (
        <LinkedTasksContext.Provider value={{ open, setOpen, currentRow, setCurrentRow }}>
            {children}
        </LinkedTasksContext.Provider>
    )
}

export const useLinkedTasks = () => {
    const context = React.useContext(LinkedTasksContext)

    if (!context) {
        throw new Error('useLinkedTasks has to be used within <LinkedTasksContext>')
    }

    return context
}
