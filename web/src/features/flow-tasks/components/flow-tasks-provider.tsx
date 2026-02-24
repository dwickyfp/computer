import React, { useState } from 'react'
import useDialogState from '@/hooks/use-dialog-state'
import { type FlowTask } from '@/repo/flow-tasks'

type FlowTasksDialogType = 'create' | 'update' | 'delete'

interface FlowTasksContextType {
    open: FlowTasksDialogType | null
    setOpen: (str: FlowTasksDialogType | null) => void
    currentRow: FlowTask | null
    setCurrentRow: React.Dispatch<React.SetStateAction<FlowTask | null>>
}

const FlowTasksContext = React.createContext<FlowTasksContextType | null>(null)

export function FlowTasksProvider({ children }: { children: React.ReactNode }) {
    const [open, setOpen] = useDialogState<FlowTasksDialogType>(null)
    const [currentRow, setCurrentRow] = useState<FlowTask | null>(null)

    return (
        <FlowTasksContext.Provider value={{ open, setOpen, currentRow, setCurrentRow }}>
            {children}
        </FlowTasksContext.Provider>
    )
}

export const useFlowTasks = () => {
    const context = React.useContext(FlowTasksContext)

    if (!context) {
        throw new Error('useFlowTasks has to be used within <FlowTasksContext>')
    }

    return context
}
